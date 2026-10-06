"""An offline task keeps its isolation: without named features its Session runs
none of Reason's cloud-side tools (web search and fetch run outside the task's
network policy), and the worker is gone before the framework grades."""
import asyncio
import json
from types import SimpleNamespace

import pytest

from reasonmachines_harbor import ReasonAgent
from test_rolling_deploy import ALL_FEATURES, Guest, run_trial


@pytest.mark.parametrize("mode", ["allowlist", "no-network"])
def test_an_offline_task_runs_without_cloud_side_tools(tmp_path, monkeypatch, mode):
    server, context, _ = run_trial(tmp_path, monkeypatch, capacity_rejections=0, runtime_profile=None,
                                   advertised_features=ALL_FEATURES, agent_network_mode=mode)
    assert server.create_body["experimental_runtime_features"] == []
    receipt = json.loads((tmp_path / "logs" / "receipt.json").read_text())
    assert receipt["runtime_features"] == [] and receipt["runtime_features_source"] == "offline_task"
    assert receipt["agent_network_mode"] == mode
    assert context.metadata["stage"] == "complete"


def test_a_public_task_runs_the_deployment_default(tmp_path, monkeypatch):
    server, _, _ = run_trial(tmp_path, monkeypatch, capacity_rejections=0, runtime_profile=None,
                             advertised_features=ALL_FEATURES, agent_network_mode="public")
    assert "experimental_runtime_features" not in server.create_body
    assert json.loads((tmp_path / "logs" / "receipt.json").read_text())["agent_network_mode"] == "public"


def test_named_features_win_on_an_offline_task(tmp_path, monkeypatch):
    server, _, _ = run_trial(tmp_path, monkeypatch, capacity_rejections=0, runtime_profile=None,
                             runtime_features="sdk_index", advertised_features=ALL_FEATURES,
                             agent_network_mode="allowlist")
    assert server.create_body["experimental_runtime_features"] == ["sdk_index"]
    assert json.loads((tmp_path / "logs" / "receipt.json").read_text())["runtime_features_source"] == "requested"


def test_a_profile_that_reads_no_features_is_sent_none(tmp_path, monkeypatch):
    monkeypatch.setenv("REASON_API_KEY", "parent-key")
    agent = ReasonAgent(logs_dir=tmp_path / "logs", model_name="openai/gpt-6-luna",
                        runtime_profile="runtime/reason-v7-sync-yield-v1")
    agent.driver._apply_agent_network("allowlist")
    assert agent.driver.runtime_features is None


def test_a_task_without_reason_egress_names_the_flags(tmp_path, monkeypatch):
    monkeypatch.setenv("REASON_API_KEY", "parent-key")
    agent = ReasonAgent(logs_dir=tmp_path / "logs", model_name="openai/gpt-6-luna")
    agent.driver.workspace = "/app"
    environment = SimpleNamespace(network_policy=SimpleNamespace(network_mode=SimpleNamespace(value="no-network")))
    with pytest.raises(ValueError, match="--allow-agent-host api.reasonmachines.com --allow-agent-host relay.reasonmachines.com"):
        asyncio.run(agent.run("task", environment, SimpleNamespace(metadata=None)))


class SlowWorker(Guest):
    """The worker outlives `worker stop`; `exit` is what the wait reports."""

    def __init__(self, exit_status):
        self.exit_status = exit_status
        self.commands = []

    async def exec(self, command, timeout_sec=None, **_):
        bare = command.split("__reason_guest_output__; ", 1)[-1]
        self.commands.append(bare)
        if "worker.pid" in bare and "kill" in bare:
            return SimpleNamespace(return_code=0, stdout="__reason_guest_output__\n" + self.exit_status + "\n", stderr="")
        return await super().exec(command, timeout_sec)


@pytest.mark.parametrize(("status", "fails"), [("forced", False), ("alive", True)])
def test_cleanup_waits_for_the_worker_to_exit_before_removing_its_state(tmp_path, monkeypatch, status, fails):
    guest = SlowWorker(status)
    if fails:
        with pytest.raises(RuntimeError, match=r"worker did not exit \(alive\)"):
            run_trial(tmp_path, monkeypatch, capacity_rejections=0, guest=lambda: guest)
    else:
        run_trial(tmp_path, monkeypatch, capacity_rejections=0, guest=lambda: guest)
    order = [next(i for i, c in enumerate(guest.commands) if needle in c)
             for needle in ("worker stop", "kill -9", "rm -rf")]
    assert order == sorted(order)
    assert json.loads((tmp_path / "logs" / "cleanup.json").read_text())["worker_exit"] == status
