"""Harbor's trial usage comes from the exported trajectory's totals.

They include the trial's subagent Sessions, which the polled Session usage
leaves out, and the cached tokens a cost split needs.
"""
import json
from types import SimpleNamespace

from reasonmachines_harbor._driver.driver import ReasonDriver


def test_copies_the_trajectory_totals_including_cached_tokens(tmp_path, monkeypatch):
    monkeypatch.setenv("REASON_API_KEY", "test-key")
    driver = ReasonDriver("Harbor", tmp_path, "openai/gpt-6-luna")
    (tmp_path / "trajectory.json").write_text(json.dumps({"final_metrics": {
        "total_prompt_tokens": 1_400, "total_completion_tokens": 30, "total_cached_tokens": 1_200,
        "total_cost_usd": 2.25, "total_steps": 4,
    }}))
    # What polling left: the trial's own Session only.
    context = SimpleNamespace(n_input_tokens=1_000, n_output_tokens=10, n_cache_tokens=None, cost_usd=1.5, n_agent_steps=None)
    driver.populate_context(context)
    assert (context.n_input_tokens, context.n_output_tokens, context.n_cache_tokens, context.cost_usd, context.n_agent_steps) == (
        1_400, 30, 1_200, 2.25, 4)
