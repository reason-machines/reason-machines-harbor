"""An ephemeral trial that names no runtime_profile runs the current frozen release, Reason Agent 1.2.

Benchmark results then name one frozen harness without the command asking for it.
Naming a profile overrides it, and standard mode keeps the server's default.
"""
import pytest

from reasonmachines_harbor._driver.driver import (
    C2_TOOLS_V7_RUNTIME_PROFILE,
    DEFAULT_EPHEMERAL_RUNTIME_PROFILE,
    REASON_AGENT_1_2_RUNTIME_PROFILE,
    REASON_V7_2026_10_RUNTIME_PROFILE,
    REASON_V7_2026_11_RUNTIME_PROFILE,
    REASON_V8_1_2026_10_RUNTIME_PROFILE,
    REASON_V8_2026_10_RUNTIME_PROFILE,
    ReasonDriver,
)


@pytest.fixture
def driver(tmp_path, monkeypatch):
    monkeypatch.setenv("REASON_API_KEY", "test-key")
    return lambda **kwargs: ReasonDriver("Harbor", tmp_path, "openai/gpt-6-luna", **kwargs)


def test_ephemeral_without_a_profile_runs_the_frozen_release(driver):
    assert DEFAULT_EPHEMERAL_RUNTIME_PROFILE == REASON_AGENT_1_2_RUNTIME_PROFILE == "runtime/reason-agent-1.2"
    assert driver().runtime_profile == REASON_AGENT_1_2_RUNTIME_PROFILE
    assert driver(mode="ephemeral").runtime_profile == REASON_AGENT_1_2_RUNTIME_PROFILE


def test_a_named_profile_overrides_the_default(driver):
    assert driver(runtime_profile=C2_TOOLS_V7_RUNTIME_PROFILE).runtime_profile == C2_TOOLS_V7_RUNTIME_PROFILE
    assert driver(runtime_profile=REASON_V7_2026_10_RUNTIME_PROFILE).runtime_profile == REASON_V7_2026_10_RUNTIME_PROFILE


def test_standard_mode_keeps_the_server_default(driver):
    assert driver(mode="standard").runtime_profile is None


def test_the_release_reads_runtime_features(driver):
    assert driver(runtime_features=["sdk_index"]).runtime_features == ["sdk_index"]


def test_the_release_runs_its_pinned_cli_and_anything_else_the_stable_one():
    from reasonmachines_harbor._driver.cli_release import RELEASE_CLI_ARCHIVES, release_archive

    pinned = release_archive(REASON_V7_2026_10_RUNTIME_PROFILE)
    assert pinned["source"] == "release_pin"
    assert pinned["selected_version"] == RELEASE_CLI_ARCHIVES[REASON_V7_2026_10_RUNTIME_PROFILE]["version"] == "0.2.151"
    assert pinned["archive_url"] == ("https://reasonmachines.com/cli/releases/download/cli-v0.2.151/"
                                     "reason-machines-cli-linux-x64-0.2.151.tgz")
    assert pinned["archive_sha256"] == "2c3ea45c1e28c9e65046d36249ed0f297c7fc1edff30c9fc3a6de0a9244c7955"
    assert pinned["musl_archive_url"].endswith("/cli-v0.2.151/reason-machines-cli-linux-x64-musl-0.2.151.tgz")
    assert release_archive(C2_TOOLS_V7_RUNTIME_PROFILE) is None
    assert release_archive(None) is None


def test_the_next_release_is_named_explicitly_and_runs_its_own_pinned_cli(driver):
    from reasonmachines_harbor._driver.cli_release import RELEASE_CLI_ARCHIVES, release_archive

    assert driver(runtime_profile=REASON_V7_2026_11_RUNTIME_PROFILE).runtime_profile == REASON_V7_2026_11_RUNTIME_PROFILE
    with pytest.raises(Exception):
        driver(mode="standard", runtime_profile=REASON_V7_2026_11_RUNTIME_PROFILE)
    pinned = release_archive(REASON_V7_2026_11_RUNTIME_PROFILE)
    assert pinned["source"] == "release_pin"
    assert pinned["selected_version"] == RELEASE_CLI_ARCHIVES[REASON_V7_2026_11_RUNTIME_PROFILE]["version"] == "0.2.185"
    assert pinned["archive_url"] == ("https://reasonmachines.com/cli/releases/download/cli-v0.2.185/"
                                     "reason-machines-cli-linux-x64-0.2.185.tgz")
    assert pinned["archive_sha256"] == "52dcb28b61aa497ee0aab6a5558a8ab08233ca023316b2628f206257c97d8eb5"
    assert pinned["musl_archive_url"].endswith("/cli-v0.2.185/reason-machines-cli-linux-x64-musl-0.2.185.tgz")
    assert pinned["musl_archive_sha256"] == "91698c6706a3f451b472f92f629adc107233495097401e2196694cceebc8b51e"


def test_the_r_release_is_named_explicitly_and_runs_the_worker_r_ran(driver):
    from reasonmachines_harbor._driver.cli_release import RELEASE_CLI_ARCHIVES, release_archive

    assert driver(runtime_profile=REASON_V8_2026_10_RUNTIME_PROFILE).runtime_profile == REASON_V8_2026_10_RUNTIME_PROFILE
    with pytest.raises(Exception):
        driver(mode="standard", runtime_profile=REASON_V8_2026_10_RUNTIME_PROFILE)
    pinned = release_archive(REASON_V8_2026_10_RUNTIME_PROFILE)
    assert pinned["selected_version"] == RELEASE_CLI_ARCHIVES[REASON_V8_2026_10_RUNTIME_PROFILE]["version"] == "0.2.151"
    assert pinned["archive_sha256"] == "2c3ea45c1e28c9e65046d36249ed0f297c7fc1edff30c9fc3a6de0a9244c7955"
    assert pinned["musl_archive_sha256"] == "a1de656866b444f4eca1a9d0e8c8a251c845e1f0dd7ef9b514c03d730e8f50c8"


def test_v8_1_is_named_explicitly_runs_v8s_worker_and_installs_its_pinned_agent_tools(driver):
    from reasonmachines_harbor._driver.cli_release import RELEASE_AGENT_TOOLS, release_archive

    release = driver(runtime_profile=REASON_V8_1_2026_10_RUNTIME_PROFILE)
    assert release.runtime_profile == REASON_V8_1_2026_10_RUNTIME_PROFILE
    with pytest.raises(Exception):
        driver(mode="standard", runtime_profile=REASON_V8_1_2026_10_RUNTIME_PROFILE)
    assert release_archive(REASON_V8_1_2026_10_RUNTIME_PROFILE) == release_archive(REASON_V8_2026_10_RUNTIME_PROFILE)
    pin = RELEASE_AGENT_TOOLS[REASON_V8_1_2026_10_RUNTIME_PROFILE]
    assert pin["url"] == ("https://github.com/reason-machines/benchmark/releases/download/"
                          "agent-tools-2026-10-22/agent-tools-linux-x64.tgz")
    assert release.installs_agent_tools and release.agent_tools_url == pin["url"]
    assert release.agent_tools_sha == pin["sha256"]
    # The release id defines its toolset: it takes no other archive or checksum; a release without a pin installs none.
    with pytest.raises(ValueError, match="pins its agent tools"):
        driver(runtime_profile=REASON_V8_1_2026_10_RUNTIME_PROFILE,
               agent_tools_archive_path="/tmp/tools.tgz", agent_tools_archive_sha256="a" * 64)
    with pytest.raises(ValueError, match="pins its agent tools"):
        driver(runtime_profile=REASON_V8_1_2026_10_RUNTIME_PROFILE, agent_tools_archive_sha256="a" * 64)
    for profile in (REASON_V8_2026_10_RUNTIME_PROFILE, REASON_V7_2026_10_RUNTIME_PROFILE, C2_TOOLS_V7_RUNTIME_PROFILE):
        assert not driver(runtime_profile=profile).installs_agent_tools


def test_the_default_release_runs_v8_2s_worker_and_installs_v8_2s_pinned_agent_tools(driver):
    from reasonmachines_harbor._driver.cli_release import RELEASE_AGENT_TOOLS, release_archive
    from reasonmachines_harbor._driver.driver import REASON_V8_2_2026_10_RUNTIME_PROFILE

    release = driver()
    assert release.runtime_profile == REASON_AGENT_1_2_RUNTIME_PROFILE
    with pytest.raises(Exception):
        driver(mode="standard", runtime_profile=REASON_AGENT_1_2_RUNTIME_PROFILE)
    # Reason Agent 1.2 is v8.2 under a standalone name: the same worker and the same agent tools archive.
    assert release_archive(REASON_AGENT_1_2_RUNTIME_PROFILE) == release_archive(REASON_V8_2_2026_10_RUNTIME_PROFILE)
    assert release_archive(REASON_AGENT_1_2_RUNTIME_PROFILE)["selected_version"] == "0.2.151"
    assert RELEASE_AGENT_TOOLS[REASON_AGENT_1_2_RUNTIME_PROFILE] == RELEASE_AGENT_TOOLS[REASON_V8_2_2026_10_RUNTIME_PROFILE] == {
        "url": "https://github.com/reason-machines/benchmark/releases/download/agent-tools-2026-10-23/agent-tools-linux-x64.tgz",
        "sha256": "059652fc47297dfab47833917f11d02858c8e34fb9bfd45bcd66d381d695cab5",
    }
    assert release.installs_agent_tools
    assert release.agent_tools_sha == "059652fc47297dfab47833917f11d02858c8e34fb9bfd45bcd66d381d695cab5"
