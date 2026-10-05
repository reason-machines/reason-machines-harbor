"""A trial sees worker readiness and the Session's end promptly, and its cleanup
overlaps API teardown with guest state removal without skipping any step."""
import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest

from reasonmachines_harbor._driver.driver import ReasonSessionTerminalError
from test_rolling_deploy import CHILD, DEVICE, PARENT, SESSION, Guest, Server, run_trial

ORG = f"/v3/organizations/{CHILD}"
DEVICE_PATH = f"{ORG}/devices/{DEVICE}"
RETIRE_PATH = f"/v3/organizations/{PARENT}/ephemeral-workspaces/{CHILD}/retire"


class Trial(Server):
    """Records guest commands and API calls in one order. The worker can report
    `starting` and the Device offline before both are ready, and the Session can
    run for some polls before it ends."""

    def __init__(self, starting_polls=0, offline_reads=0, running_polls=0, final_status="exit"):
        super().__init__(capacity_rejections=0)
        self.starting_polls = starting_polls
        self.offline_reads = offline_reads
        self.running_polls = running_polls
        self.final_status = final_status
        self.order = []
        # The workspace retirement and the guest state removal each wait for the
        # other to start: only a cleanup that overlaps them completes both.
        self.retiring = asyncio.Event()
        self.removing = asyncio.Event()

    async def retire_once_removal_starts(self, request):
        await asyncio.wait_for(self.removing.wait(), 1)
        return super().__call__(request)

    def __call__(self, request):
        path, method = request.url.path, request.method
        self.order.append(("api", method, path))
        if method == "GET" and path == DEVICE_PATH and self.offline_reads:
            self.offline_reads -= 1
            self.calls.append((method, path))
            return httpx.Response(200, json={"device": {
                "roots": [{"id": "root", "path_fingerprint": self.fingerprint}],
                "online": False, "compatible": False, "compatibility_reason": "offline"}})
        if path == f"{ORG}/sessions/{SESSION}/events":
            self.calls.append((method, path))
            status = "running" if self.running_polls else self.final_status
            self.running_polls = max(0, self.running_polls - 1)
            return httpx.Response(200, json={"session": {"status": status}, "items": [], "next_seq": 0})
        if path == RETIRE_PATH:
            self.retiring.set()
            return self.retire_once_removal_starts(request)
        return super().__call__(request)


class RecordingGuest(Guest):
    def __init__(self, trial):
        self.trial = trial

    async def exec(self, command, timeout_sec=None, **_):
        bare = command.split("__reason_guest_output__; ", 1)[-1]
        self.trial.order.append(("guest", bare))
        if bare.startswith("rm -rf"):
            self.trial.removing.set()
            await asyncio.wait_for(self.trial.retiring.wait(), 1)
        if "worker status" in bare and self.trial.starting_polls:
            self.trial.starting_polls -= 1
            return SimpleNamespace(return_code=0, stderr="", stdout="__reason_guest_output__\n" + json.dumps(
                {"deviceId": DEVICE, "live": {"phase": "starting"}}))
        return await super().exec(command, timeout_sec)


def trial_run(tmp_path, monkeypatch, trial, **options):
    guest = RecordingGuest(trial)
    return run_trial(tmp_path, monkeypatch, 0, server=trial, guest=lambda: guest, **options)


def position(trial, predicate):
    return next(index for index, entry in enumerate(trial.order) if predicate(entry))


def test_worker_readiness_and_device_online_are_rechecked_within_half_a_second(tmp_path, monkeypatch):
    trial = Trial(starting_polls=1, offline_reads=1)
    _, context, sleeps = trial_run(tmp_path, monkeypatch, trial)
    assert context.metadata["stage"] == "complete"
    # One wait for the worker to start running, one for its Device to come online.
    assert sleeps == [0.5, 0.5]
    assert trial.calls.count(("GET", DEVICE_PATH)) == 2


@pytest.mark.parametrize(("options", "interval"), [({}, 3.0), ({"poll_interval_sec": 2.5}, 2.5)])
def test_session_events_are_polled_at_the_configured_interval(tmp_path, monkeypatch, options, interval):
    trial = Trial(running_polls=2)
    _, context, sleeps = trial_run(tmp_path, monkeypatch, trial, **options)
    assert context.metadata["stage"] == "complete"
    assert sleeps == [interval, interval]
    assert trial.calls.count(("GET", f"{ORG}/sessions/{SESSION}/events")) == 3


def assert_fully_cleaned_up(tmp_path, trial):
    stop = position(trial, lambda entry: entry[0] == "guest" and "worker stop" in entry[1])
    revoke = position(trial, lambda entry: entry == ("api", "DELETE", DEVICE_PATH))
    remove = position(trial, lambda entry: entry[0] == "guest" and entry[1].startswith("rm -rf"))
    retire = position(trial, lambda entry: entry == ("api", "POST", RETIRE_PATH))
    # The Device is revoked only after its worker stops, and before the workspace
    # that owns its key is retired. Guest removal also follows the stop; it and the
    # retirement overlap (see Trial), and both finished before cleanup returned.
    assert stop < revoke < retire
    assert stop < remove
    cleanup = json.loads((tmp_path / "logs" / "cleanup.json").read_text())
    assert cleanup["errors"] == []
    assert cleanup["ephemeral_workspace"]["retired"] is True


def test_cleanup_revokes_and_retires_while_guest_state_is_removed(tmp_path, monkeypatch):
    trial = Trial()
    _, context, _ = trial_run(tmp_path, monkeypatch, trial)
    assert context.metadata["stage"] == "complete"
    assert_fully_cleaned_up(tmp_path, trial)


def test_a_failed_session_is_still_fully_cleaned_up(tmp_path, monkeypatch):
    trial = Trial(final_status="error")
    with pytest.raises(ReasonSessionTerminalError):
        trial_run(tmp_path, monkeypatch, trial)
    assert_fully_cleaned_up(tmp_path, trial)
