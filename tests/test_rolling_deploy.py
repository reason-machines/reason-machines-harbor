"""A trial survives what a rolling deploy and a shared parent do to it: the parent's
active-workspace cap is waited out, and the server's report of the Session's
profile and policy is recorded rather than required to match the request."""
import asyncio
import hashlib
import io
import json
import tarfile
from types import SimpleNamespace

import httpx
import pytest

from reasonmachines_harbor import ReasonAgent
from reasonmachines_harbor._driver import driver as driver_module

PARENT = "11111111-1111-4111-8111-111111111111"
CHILD = "22222222-2222-4222-8222-222222222222"
DEVICE = "33333333-3333-4333-8333-333333333333"
SESSION = "44444444-4444-4444-8444-444444444444"
PROFILE = "runtime/reason-c2-tools-v7"
STATUS = {"deviceId": DEVICE, "live": {
    "phase": "running", "executionEligible": True,
    "capabilityReadiness": {"hands.connect": True, "environment.install": True},
    "supportedOperations": ["hands.connect", "environment.install"]}}


class Guest:
    async def exec(self, command, timeout_sec=None, **_):
        marked = "__reason_guest_output__; " in command
        command = command.split("__reason_guest_output__; ", 1)[-1]
        if command == "pwd -P":
            stdout = "/root\n"
        elif command.endswith("--version"):
            stdout = "1.2.3\n"
        elif "worker status" in command:
            stdout = json.dumps(STATUS)
        else:
            stdout = ""
        return SimpleNamespace(return_code=0, stdout=("__reason_guest_output__\n" if marked else "") + stdout, stderr="")

    async def upload_file(self, source_path, target_path):
        pass


class Server:
    def __init__(self, capacity_rejections, runtime_features=None, records_features=True):
        self.capacity_rejections = capacity_rejections
        # What GET /deployment advertises; None: a deployment that predates the field.
        self.runtime_features = runtime_features
        self.calls = []
        self.fingerprint = None
        self.create_body = None
        # False: the create lands on a replica that predates the field and ignores it.
        self.records_features = records_features

    def __call__(self, request):
        path, method = request.url.path, request.method
        self.calls.append((method, path))
        body = json.loads(request.content) if request.content else None
        child = request.headers["authorization"] == "Bearer child-key"
        org = f"/v3/organizations/{CHILD}"
        if path == "/v3/self":
            return httpx.Response(200, json={"org_id": CHILD if child else PARENT})
        if path == "/v3/deployment":
            return httpx.Response(200, json={
                "session_modes": ["ephemeral"], "workspace_ownerships": ["externally-managed"],
                "experimental_runtime_profiles": [PROFILE], "commit_sha": "a" * 40,
                **({} if self.runtime_features is None else {"experimental_runtime_features": self.runtime_features})})
        if path == f"/v3/organizations/{PARENT}/ephemeral-workspaces":
            if self.capacity_rejections:
                self.capacity_rejections -= 1
                return httpx.Response(409, json={"error": "ephemeral_workspace_limit_reached"})
            return httpx.Response(201, json={"organization_id": CHILD, "parent_organization_id": PARENT,
                                             "reference": body["reference"], "api_key": "child-key"})
        if path == f"/v3/organizations/{PARENT}/ephemeral-workspaces/{CHILD}/retire":
            return httpx.Response(200, json={"organization_id": CHILD, "retired_at": "2026-10-04T00:00:00Z"})
        if path == f"{org}/devices/enrollment-tickets":
            self.fingerprint = body["roots"][0]["path_fingerprint"]
            return httpx.Response(201, json={"device_kind": "headless", "lifecycle": "ephemeral",
                                             "enrollment_ticket": "ticket"})
        if path == f"{org}/devices/{DEVICE}":
            if method == "DELETE":
                return httpx.Response(200, json={})
            return httpx.Response(200, json={"device": {
                "roots": [{"id": "root", "path_fingerprint": self.fingerprint}],
                "online": True, "compatible": True, "compatibility_reason": None}})
        if path == f"{org}/sessions" and method == "POST":
            self.create_body = body
            # A deploy moved the Session to a newer profile and reshaped the policy.
            return httpx.Response(201, json={"session_id": SESSION, "session_policy": {"version": 2},
                                             "experimental_runtime_profile": "runtime/reason-c2-tools-v8",
                                             **({"experimental_runtime_features": body.get("experimental_runtime_features")}
                                                if self.records_features else {})})
        if path == f"{org}/sessions/{SESSION}/events":
            return httpx.Response(200, json={"session": {"status": "exit"}, "items": [], "next_seq": 0})
        if path == f"{org}/sessions/{SESSION}/trajectory":
            return httpx.Response(200, json={"steps": [], "final_metrics": {}})
        return httpx.Response(404, json={"error": "session_not_found"})


def run_trial(tmp_path, monkeypatch, capacity_rejections, runtime_features=None, advertised_features=None,
              records_features=True, server=None, runtime_profile=PROFILE, guest=Guest, **options):
    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w:gz") as bundle:
        bundle.addfile(tarfile.TarInfo("package/bin/ara"), io.BytesIO(b""))
    archive = tmp_path / "cli.tgz"
    archive.write_bytes(data.getvalue())
    monkeypatch.setenv("REASON_API_KEY", "parent-key")
    server = server or Server(capacity_rejections, advertised_features, records_features)
    client = httpx.AsyncClient

    def mocked(*args, **kwargs):
        return client(*args, transport=httpx.MockTransport(server), **kwargs)

    sleeps = []
    real_sleep = asyncio.sleep

    async def sleep(delay):
        sleeps.append(delay)
        await real_sleep(0)

    monkeypatch.setattr(driver_module.httpx, "AsyncClient", mocked)
    monkeypatch.setattr(driver_module.asyncio, "sleep", sleep)
    agent = ReasonAgent(logs_dir=tmp_path / "logs", model_name="openai/gpt-6-luna", runtime_profile=runtime_profile,
                        runtime_features=runtime_features, cli_archive_path=str(archive),
                        cli_archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(), **options)
    context = SimpleNamespace(metadata=None, n_input_tokens=None, n_output_tokens=None, cost_usd=None)

    async def trial():
        await agent.driver.setup(guest())
        await agent.driver.run("Explain the repository.", guest(), context)

    asyncio.run(trial())
    return server, context, sleeps


def test_waits_out_the_parent_workspace_cap_then_runs(tmp_path, monkeypatch):
    server, context, sleeps = run_trial(tmp_path, monkeypatch, capacity_rejections=3)
    creates = [call for call in server.calls if call == ("POST", f"/v3/organizations/{PARENT}/ephemeral-workspaces")]
    assert len(creates) == 4
    assert len([delay for delay in sleeps if delay >= 5]) == 3
    assert context.metadata["stage"] == "complete"
    wait = json.loads((tmp_path / "logs" / "ephemeral-workspace-capacity-retry.json").read_text())
    assert wait["attempt"] == 3 and wait["code"] == "ephemeral_workspace_limit_reached"


def test_records_the_servers_profile_and_policy_without_requiring_a_match(tmp_path, monkeypatch):
    server, context, _ = run_trial(tmp_path, monkeypatch, capacity_rejections=0)
    assert context.metadata["stage"] == "complete"
    receipt = json.loads((tmp_path / "logs" / "receipt.json").read_text())
    assert receipt["runtime_profile"] == PROFILE
    assert receipt["server_experimental_runtime_profile"] == "runtime/reason-c2-tools-v8"
    assert receipt["session_policy"] == {"version": 2}
    # The Session is read only through its event stream and trajectory.
    assert ("GET", f"/v3/organizations/{CHILD}/sessions/{SESSION}") not in server.calls


def test_sends_the_earlier_c2_name_under_its_public_name(tmp_path, monkeypatch):
    server, context, _ = run_trial(tmp_path, monkeypatch, capacity_rejections=0,
                                   runtime_profile="runtime/ara-pi-c2-tools-v7")
    assert context.metadata["stage"] == "complete"
    assert server.create_body["experimental_runtime_profile"] == PROFILE
    assert json.loads((tmp_path / "logs" / "receipt.json").read_text())["runtime_profile"] == PROFILE


ALL_FEATURES = ["banner", "sdk_index", "service_tier", "output_cap", "git_metadata", "serial_reads"]


def test_runtime_features_are_sent_with_the_session_and_recorded(tmp_path, monkeypatch):
    server, context, _ = run_trial(tmp_path, monkeypatch, capacity_rejections=0,
                                   runtime_features="sdk_index, banner", advertised_features=ALL_FEATURES)
    assert context.metadata["stage"] == "complete"
    assert server.create_body["experimental_runtime_features"] == ["banner", "sdk_index"]
    receipt = json.loads((tmp_path / "logs" / "receipt.json").read_text())
    assert receipt["runtime_features"] == ["banner", "sdk_index"]
    assert receipt["server_experimental_runtime_features"] == ["banner", "sdk_index"]
    deployment = json.loads((tmp_path / "logs" / "deployment.json").read_text())
    assert deployment["runtime_features"] == ["banner", "sdk_index"]


def test_an_empty_feature_list_is_sent_as_stock_shape(tmp_path, monkeypatch):
    server, _, _ = run_trial(tmp_path, monkeypatch, capacity_rejections=0,
                             runtime_features="", advertised_features=ALL_FEATURES)
    assert server.create_body["experimental_runtime_features"] == []


def test_no_runtime_features_sends_nothing(tmp_path, monkeypatch):
    server, _, _ = run_trial(tmp_path, monkeypatch, capacity_rejections=0)
    assert "experimental_runtime_features" not in server.create_body


def test_a_deployment_that_predates_runtime_features_is_refused_before_any_session(tmp_path, monkeypatch):
    # It would ignore the field and run its default set instead.
    with pytest.raises(RuntimeError, match="runtime features"):
        run_trial(tmp_path, monkeypatch, capacity_rejections=0, runtime_features="sdk_index")


def test_a_session_created_by_a_replica_that_ignored_the_features_is_cancelled(tmp_path, monkeypatch):
    # GET /deployment reached a new replica, the create an old one during the rollout.
    server = Server(0, ALL_FEATURES, records_features=False)
    with pytest.raises(RuntimeError, match="did not record the requested runtime features"):
        run_trial(tmp_path, monkeypatch, capacity_rejections=0, runtime_features="sdk_index", server=server)
    assert ("POST", f"/v3/organizations/{CHILD}/sessions/{SESSION}/cancel") in server.calls


@pytest.mark.parametrize(("kwargs", "error"), [
    ({"runtime_profile": PROFILE, "runtime_features": "sdk_index,checkpoints"}, "unsupported runtime_features: checkpoints"),
    ({"runtime_features": "sdk_index"}, "requires runtime_profile"),
    ({"runtime_profile": PROFILE, "runtime_features": "sdk_index", "mode": "standard"}, "requires runtime_profile"),
])
def test_runtime_features_are_checked_before_any_request(tmp_path, monkeypatch, kwargs, error):
    monkeypatch.setenv("REASON_API_KEY", "parent-key")
    with pytest.raises(ValueError, match=error):
        ReasonAgent(logs_dir=tmp_path / "logs", model_name="openai/gpt-6-luna", **kwargs)
