"""A Session whose task machine was lost, and that ran no tool afterwards, stopped for
the infrastructure: the trial is a resource loss the framework retries, not a graded 0."""
import json

import httpx
import pytest

from reasonmachines_harbor._driver.driver import ReasonSessionResourceUnavailableError
from test_rolling_deploy import CHILD, SESSION, Server, run_trial

LOST = ('The machine running this Session is unavailable (recovery_quiescence_unconfirmed), '
        'so this tool did not run. The conversation is intact.')


class EventsServer(Server):
    def __init__(self, results):
        super().__init__(capacity_rejections=0)
        self.results = results

    def __call__(self, request):
        if request.url.path == f"/v3/organizations/{CHILD}/sessions/{SESSION}/events":
            self.calls.append((request.method, request.url.path))
            items = [{"id": str(i), "seq": i, "type": "tool_result", "text": "completed",
                      "data": {"tool": "bash", "content": content}} for i, content in enumerate(self.results)]
            return httpx.Response(200, json={"session": {"status": "exit"}, "items": items, "next_seq": len(items)})
        return super().__call__(request)


def test_a_session_that_ends_on_a_lost_machine_is_a_resource_loss(tmp_path, monkeypatch):
    with pytest.raises(ReasonSessionResourceUnavailableError, match="reason_code=hands_lost"):
        run_trial(tmp_path, monkeypatch, 0, server=EventsServer(["ok", LOST]))
    failure = json.loads((tmp_path / "logs" / "failure.json").read_text())
    assert failure["failure_class"] == "resource_loss" and failure["metrics"]["resource_loss"]


def test_a_session_that_worked_after_losing_its_machine_is_graded(tmp_path, monkeypatch):
    _, context, _ = run_trial(tmp_path, monkeypatch, 0, server=EventsServer([LOST, "ok"]))
    assert context.metadata["stage"] == "complete"
