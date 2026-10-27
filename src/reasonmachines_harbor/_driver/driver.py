"""Benchmark-side driver for the published Reason CLI worker.

Framework adapters (Harbor, Pier) construct one ReasonDriver per trial and pass
their task environment as a Sandbox. The controller keeps every credential;
only a one-use enrollment ticket enters the task guest.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import math
import os
import random
import re
import secrets
import shlex
import sys
import tempfile
import time
import tomllib
from contextlib import asynccontextmanager
from email.utils import parsedate_to_datetime
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Protocol
from urllib.parse import quote, urlparse
from uuid import uuid4
from xml.etree.ElementTree import Element, SubElement, indent, tostring

import httpx
import yaml

from .bootstrap import resolve_organization, resource_id
from .cli_release import (
    DEFAULT_CLI_ARCHIVE_SHA256,
    DEFAULT_CLI_ARCHIVE_URL,
    RELEASE_AGENT_TOOLS,
    fetch_archive,
    release_archive,
    resolve_stable_archive,
    verify_archive,
)
from .config import DEFAULT_RUNTIME_CONFIG

SESSION_MODES = {"standard", "ephemeral"}
# A guest's login shell can print before a command runs: the micromamba images in
# Terminal-Bench (freecad-*, vba-userform-port) print an activation banner as root.
# Every parsed command first prints this marker, and only what follows it is output.
GUEST_OUTPUT_MARKER = "__reason_guest_output__"
# Requested as experimental_runtime_profile. Requestable before the
# deployment offers one: preflight refuses a profile the deployment does not
# advertise for this credential's organization (an ephemeral trial's parent).
C2_TOOLS_V7_RUNTIME_PROFILE = "runtime/reason-c2-tools-v7"
# A dated release: v7 as it ran in October 2026, frozen. Any organization may
# name it, in ephemeral mode only; published benchmark passes do.
REASON_V7_2026_10_RUNTIME_PROFILE = "runtime/reason-v7-2026-10"
# The next dated release: v7 as it ran when it was cut on 2026-10-12, frozen the
# same way. Its worker CLI names Bash's full-output file reason-bash-<id>.log.
REASON_V7_2026_11_RUNTIME_PROFILE = "runtime/reason-v7-2026-11"
# The R harness as a dated release: bash-reason (Bash and the
# `reason` script tool, no read, edit or write) as it ran when cut on 2026-10-19.
REASON_V8_2026_10_RUNTIME_PROFILE = "runtime/reason-v8-2026-10"
# runtime/reason-v8-2026-10 with every Bash call yielding at its timeout, cut on
# 2026-10-22; the driver installs its pinned agent tools (RELEASE_AGENT_TOOLS).
REASON_V8_1_2026_10_RUNTIME_PROFILE = "runtime/reason-v8.1-2026-10"
# runtime/reason-v8.1-2026-10 without the 25-turn guideline in section zero, cut on
# 2026-10-24; its pinned agent tools add ps (RELEASE_AGENT_TOOLS).
REASON_V8_2_2026_10_RUNTIME_PROFILE = "runtime/reason-v8.2-2026-10"
# Reason Agent 1.2: runtime/reason-v8.2-2026-10 under a standalone, dateless
# release name (only the id differs; same worker CLI and agent tools archive).
# Immutable: a change ships as runtime/reason-agent-1.3.
REASON_AGENT_1_2_RUNTIME_PROFILE = "runtime/reason-agent-1.2"
# What an ephemeral trial runs when its config names no runtime_profile: the
# current frozen release, so a benchmark result names one frozen harness without
# asking for it. Naming a profile overrides it; C2_TOOLS_V7_RUNTIME_PROFILE runs
# the server's current default harness, which changes with every deploy.
DEFAULT_EPHEMERAL_RUNTIME_PROFILE = REASON_AGENT_1_2_RUNTIME_PROFILE
V7_RUNTIME_PROFILES = {C2_TOOLS_V7_RUNTIME_PROFILE, REASON_V7_2026_10_RUNTIME_PROFILE, REASON_V7_2026_11_RUNTIME_PROFILE,
                       REASON_V8_2026_10_RUNTIME_PROFILE, REASON_V8_1_2026_10_RUNTIME_PROFILE,
                       REASON_V8_2_2026_10_RUNTIME_PROFILE, REASON_AGENT_1_2_RUNTIME_PROFILE}
EPHEMERAL_ONLY_RUNTIME_PROFILES = {REASON_V7_2026_10_RUNTIME_PROFILE, REASON_V7_2026_11_RUNTIME_PROFILE,
                                   REASON_V8_2026_10_RUNTIME_PROFILE, REASON_V8_1_2026_10_RUNTIME_PROFILE,
                                   REASON_V8_2_2026_10_RUNTIME_PROFILE, REASON_AGENT_1_2_RUNTIME_PROFILE}
RUNTIME_PROFILES = {*V7_RUNTIME_PROFILES, *EPHEMERAL_ONLY_RUNTIME_PROFILES}
# Requested as experimental_runtime_features: the Reason features an ephemeral v7
# Session runs on top of the stock harness. An empty list is stock's shape; None sends
# nothing, so the deployment's default set runs. Preflight refuses features the
# deployment does not advertise: one that predates the field would ignore it.
# recovery_defer_idle_hands is an opt-in recovery experiment, never a default, and
# so are the fewer_tests_* rungs asking for fewer new tests (the server refuses two at once),
# and bash_yield and sync_bash, which carry the retired sync-yield harness's Bash into v7,
# and turn_line_removed and turn_line_questions, v8's turn-guideline experiments (v8 and v8.1 only),
# and output_cap_continue, image_history_append_only and cache_ttl_1h, which change provider request
# options and loop mechanics only (any v7 profile, runtime/reason-agent-1.2 included).
TEST_WRITING_FEATURES = ("fewer_tests_low", "fewer_tests_medium", "fewer_tests_high", "fewer_tests_max",
                         "fewer_tests_max_alone", "verify_line_alone", "addendum_placebo")
RUNTIME_FEATURES = ("banner", "sdk_index", "service_tier", "output_cap", "git_metadata", "serial_reads",
                    "recovery_defer_idle_hands", *TEST_WRITING_FEATURES, "bash_yield", "sync_bash",
                    "turn_line_removed", "turn_line_questions",
                    "output_cap_continue", "image_history_append_only", "cache_ttl_1h")
# sdk_index puts Reason's tools in codemode, and they include web_search and
# web_fetch, which run in Reason's cloud and so bypass the task's network policy.
# An offline task (agent phase not public: DeepSWE) that names no features gets
# this list instead of the deployment default, as Artificial Analysis disables
# provider-hosted search and browsing to keep such tasks offline.
OFFLINE_TASK_RUNTIME_FEATURES: list[str] = []
WORKSPACE_OWNERSHIP = "externally-managed"
# Public headless Sessions install a private environment before attaching Hands.
REQUIRED_WORKER_CAPABILITIES = ("hands.connect", "environment.install")
FAILURE_METRICS = ("worker_startup_timeout", "resource_loss", "suspended_session", "artifact_failure")
ORIGINS = {
    "staging": "https://staging-api.reasonmachines.com",
    "production": "https://api.reasonmachines.com",
}
# The worker refuses to start with any variable cli/device-broker-environment.ts
# treats as unsafe (exit 78), and its host shell drops them from the task anyway.
# Task images set some: oven/bun sets BUN_RUNTIME_TRANSPILER_CACHE_PATH.
_UNSAFE_WORKER_ENV = ("ALL_PROXY", "BUN_BE_BUN", "BUN_OPTIONS", "CURL_CA_BUNDLE", "GIT_SSL_CAINFO",
                      "GRPC_DEFAULT_SSL_ROOTS_FILE_PATH", "HTTPS_PROXY", "HTTP_PROXY", "NO_PROXY",
                      "NODE_EXTRA_CA_CERTS", "NODE_OPTIONS", "NODE_TLS_REJECT_UNAUTHORIZED", "REQUESTS_CA_BUNDLE",
                      "SSLKEYLOGFILE", "SSL_CERT_DIR", "SSL_CERT_FILE", "all_proxy", "http_proxy", "https_proxy",
                      "no_proxy")


def _clean_worker_wrapper(binary: str) -> str:
    """A sh script that runs the worker without the environment it refuses."""
    unset = " ".join(f"-u {name}" for name in _UNSAFE_WORKER_ENV)
    # The here-document keeps the loop in this shell. A multi-line value yields
    # lines that are not names; `unset` on one would abort a POSIX shell.
    return ("#!/bin/sh\n"
            "while IFS== read -r name value; do\n"
            "  case $name in\n"
            "    *[!A-Za-z0-9_]* | BUN_INSTALL) ;;\n"
            "    BUN_* | DYLD_* | NODE_DEBUG*) unset \"$name\" ;;\n"
            "  esac\n"
            "done <<EOF\n"
            "$(env)\n"
            "EOF\n"
            f"exec env {unset} {shlex.quote(binary)} \"$@\"\n")


# The worker opts in (REASON_RELAY_CUSTOM_DOMAIN) to the relay's
# reasonmachines.com domain, so the API hands it relay URLs on that name only.
RELAY_HOSTS = {
    "staging": ("staging-relay.reasonmachines.com",),
    "production": ("relay.reasonmachines.com",),
}

SAFE_API_ERRORS = frozenset({
    "resource_unavailable", "run_cancelled",
    "quota_exhausted", "group_max_runs_per_day", "group_spend_limit_exceeded",
    "invalid_request", "invalid_model", "invalid_reasoning_effort", "missing_scope",
    "unauthorized", "forbidden", "invalid_api_key", "api_key_expired", "api_key_revoked",
    "session_not_found", "device_not_found", "session_target_unavailable",
    "headless_workers_unavailable", "cloud_agents_disabled", "runtime_preflight_unavailable",
    "central_relay_unavailable",
    "agent_model_key_missing", "local_device_incompatible", "idempotency_profile_conflict", "idempotency_target_conflict",
    "benchmark_profile_requires_headless_project_root", "benchmark_profile_requires_fresh_session",
    "prompt_required", "prompt_too_large", "request_too_large", "rate_limit_exceeded", "instruction_blocked",
    "ephemeral_workspace_reference_conflict", "ephemeral_workspace_retired",
    "ephemeral_workspace_limit_reached", "ephemeral_workspace_key_unavailable",
    "ephemeral_workspace_parent_payment_required", "ephemeral_workspace_parent_credit_exhausted", "ephemeral_workspace_required",
    "ephemeral_workspace_mode_required", "ephemeral_workspace_idempotency_key_required",
    "ephemeral_requires_external_workspace", "ephemeral_workspace_consumed", "hands_lost",
})
# The server's tool result when the task machine is gone (hands-tool-runtime.ts handsLostOutput).
HANDS_LOST_OUTPUT = re.compile(r"The machine running this Session (?:is unavailable|stopped responding) \(")


class Sandbox(Protocol):
    """The task guest. Harbor and Pier environments satisfy this directly.

    `exec` returns an object with `return_code`, `stdout` and `stderr`.
    """

    async def exec(self, command: str, timeout_sec: int | None = None) -> Any: ...

    async def upload_file(self, source_path: Path, target_path: str) -> Any: ...


def parse_runtime_features(value: str | list[str] | tuple[str, ...] | None) -> list[str] | None:
    """Comma-separated (as Harbor's --ak passes it) or a list; "" is the explicit empty list."""
    if value is None:
        return None
    names = value.split(",") if isinstance(value, str) else list(value)
    requested = {str(name).strip().lower() for name in names} - {""}
    unknown = requested - set(RUNTIME_FEATURES)
    if unknown:
        raise ValueError(f"unsupported runtime_features: {', '.join(sorted(unknown))}")
    return [feature for feature in RUNTIME_FEATURES if feature in requested]


class ReasonAPITransportError(RuntimeError):
    """Safe transport failure, distinct from a framework-owned trial deadline."""


class ReasonAPIRequestTimeout(ReasonAPITransportError):
    """Adapter request/read deadline, never a built-in TimeoutError."""


class ReasonWorkerStartupError(RuntimeError):
    """Worker setup or readiness failure, not a framework-owned deadline."""


def _cancelled(value: object) -> bool:
    """A cancelled run's error class: run_cancelled, or the earlier spelling some servers still report."""
    return isinstance(value, str) and value.endswith("_cancelled")


def _public_session_code(value: object) -> str | None:
    return value if isinstance(value, str) and (value in SAFE_API_ERRORS or _cancelled(value)) else None


class ReasonSessionTerminalError(RuntimeError):
    """A bounded, framework-visible terminal Session failure."""

    def __init__(self, *, status: object, error_class: object = None, reason_code: object = None):
        self.status = status if status in {"exit", "error", "cancelled", "suspended"} else "unknown"
        self.error_class = _public_session_code(error_class)
        self.reason_code = _public_session_code(reason_code)
        details = [f"status={self.status}"]
        if self.error_class:
            details.append(f"error_class={self.error_class}")
        if self.reason_code:
            details.append(f"reason_code={self.reason_code}")
        super().__init__("Reason Session ended: " + ", ".join(details))


class ReasonSessionResourceUnavailableError(ReasonSessionTerminalError):
    """The public API reported loss of the Session's external execution resource."""

    def __init__(self, *, status: object = "error", reason_code: object = None):
        super().__init__(status=status, error_class="resource_unavailable", reason_code=reason_code)


@asynccontextmanager
async def _api_transport_errors():
    # Enter outside our timeout scope: only its own expiry becomes TimeoutError.
    # External cancellation stays CancelledError and is deliberately not caught.
    try:
        yield
    except (TimeoutError, httpx.TimeoutException):
        raise ReasonAPIRequestTimeout("Reason API request timed out") from None
    except httpx.TransportError:
        # Transport exception chains may contain URLs or credentials.
        raise ReasonAPITransportError("Reason API transport failed") from None


class ReasonAPIError(RuntimeError):
    """HTTP status and a deliberately bounded public machine code; never a body."""
    def __init__(self, status: int, code: str | None = None, guidance: str | None = None):
        self.status = status
        self.code = code if code in SAFE_API_ERRORS else None
        self.guidance = guidance
        self.retry_after_sec = None
        # This specific admission rejection occurs before Session persistence.
        # Unknown conflicts remain ambiguous and require idempotent recovery.
        self.definitive_rejection = (400 <= status < 500 and status not in {408, 409}) or (
            status == 409 and self.code == "local_device_incompatible"
        )
        msg = f"Reason API failed: HTTP {status}" + (f" ({self.code})" if self.code else "")
        if self.guidance:
            msg += f"\n\n{self.guidance}"
        super().__init__(msg)

    @classmethod
    def from_response(cls, response: httpx.Response) -> ReasonAPIError:
        try:
            body = response.json()
            code = body.get("error") if isinstance(body, dict) else None
        except ValueError:
            code = None
        error = cls(response.status_code, code if isinstance(code, str) else None)
        header = response.headers.get("Retry-After")
        if header:
            try:
                delay = float(header)
            except ValueError:
                try:
                    delay = parsedate_to_datetime(header).timestamp() - time.time()
                except (TypeError, ValueError, OverflowError):
                    delay = float("nan")
            if math.isfinite(delay):
                error.retry_after_sec = min(DEFAULT_RUNTIME_CONFIG.poll_interval_max_sec, max(0, delay))
        return error


def task_agent_timeout_sec(environment_dir: Path | str | None) -> float | None:
    """The agent time limit from the task's task.toml (`[agent] timeout_sec`), if set.

    Harbor and Pier enforce it but do not pass it to the agent. The task's
    environment directory sits beside its task.toml.
    """
    if environment_dir is None:
        return None
    try:
        with (Path(environment_dir).parent / "task.toml").open("rb") as file:
            agent = tomllib.load(file).get("agent")
    except (OSError, tomllib.TOMLDecodeError):
        return None
    value = agent.get("timeout_sec") if isinstance(agent, dict) else None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 < value < math.inf:
        return None
    return float(value)


class ReasonDriver:
    """One benchmark trial: install, enroll and start a worker, then run one Session.

    `framework` ("Harbor" or "Pier") labels guest paths and idempotency keys.
    The caller's AgentContext is used in place:
    only `metadata`, `n_input_tokens`, `n_output_tokens`, `cost_usd` and, where
    present, `n_agent_steps` are set.
    """

    def __init__(self, framework: str, logs_dir: Path, model_name: str | None,
                 logger: logging.Logger | None = None, *,
                 channel: str | None = None,
                 cli_archive_sha256: str | None = None,
                 cli_archive_path: str | None = None,
                 cli_archive_url: str | None = None,
                 cli_archive_musl_path: str | None = None,
                 cli_archive_musl_sha256: str | None = None,
                 agent_tools_archive_path: str | None = None,
                 agent_tools_archive_sha256: str | None = None,
                 api_origin: str | None = None, worker_api_origin: str | None = None,
                 docker_host_api: bool = False,
                 poll_interval_sec: float = DEFAULT_RUNTIME_CONFIG.poll_interval_sec,
                 reasoning_effort: str = "medium",
                 mode: str = "ephemeral",
                 runtime_profile: str | None = None,
                 runtime_features: str | list[str] | tuple[str, ...] | None = None,
                 ephemeral_rcus: str | None = None,
                 mcp_servers: list[Any] | None = None, skills_dir: str | None = None,
                 extra_env: dict[str, str] | None = None, load_trajectory: Any = None):
        self.framework = framework
        self.label = framework.lower()
        self.logs_dir = Path(logs_dir)
        self.model_name = model_name
        self.logger = logger or logging.getLogger(__name__)
        if mode not in SESSION_MODES:
            raise ValueError("mode must be standard or ephemeral")
        self.mode = mode
        # The parent's budget for an ephemeral workspace; "0" runs only on the
        # parent's own provider accounts (BYOK). The server defaults it to 30.
        if ephemeral_rcus is None:
            ephemeral_rcus = os.environ.get("REASON_EPHEMERAL_RCUS") or None
        if ephemeral_rcus is not None and not re.fullmatch(r"\d{1,3}(\.\d{1,9})?", str(ephemeral_rcus)):
            raise ValueError("ephemeral_rcus must be a decimal credit amount")
        self.ephemeral_rcus = None if ephemeral_rcus is None else str(ephemeral_rcus)
        if runtime_profile is None and mode == "ephemeral":
            runtime_profile = DEFAULT_EPHEMERAL_RUNTIME_PROFILE
        if runtime_profile is not None and runtime_profile not in RUNTIME_PROFILES:
            raise ValueError(f"unsupported runtime_profile {runtime_profile}: name one of {sorted(RUNTIME_PROFILES)}, "
                             "or none for the default (the sync-yield and capability-shell harnesses were removed)")
        if runtime_profile in EPHEMERAL_ONLY_RUNTIME_PROFILES and mode != "ephemeral":
            raise ValueError(f"runtime_profile {runtime_profile} requires ephemeral mode")
        self.runtime_profile = runtime_profile
        self.runtime_features = parse_runtime_features(runtime_features)
        if self.runtime_features is not None and not self._features_apply():
            raise ValueError(f"runtime_features requires ephemeral mode and the default harness ({C2_TOOLS_V7_RUNTIME_PROFILE}) or a dated release")
        # Why runtime_features was sent: "requested" (configured) or "offline_task".
        self.runtime_features_source = None if self.runtime_features is None else "requested"
        if channel is None:
            channel = os.environ.get("REASON_CHANNEL")
            if not channel:
                channel = "production"
        if channel not in ORIGINS:
            raise ValueError("channel must explicitly be staging or production")
        if not cli_archive_path and not cli_archive_url:
            cli_archive_path = os.environ.get("REASON_CLI_ARCHIVE_PATH")
            cli_archive_url = os.environ.get("REASON_CLI_ARCHIVE_URL")
        if not cli_archive_sha256:
            cli_archive_sha256 = os.environ.get("REASON_CLI_ARCHIVE_SHA256") or ""
        self.uses_default_archive = channel == "production" and not any(
            (cli_archive_path, cli_archive_url, cli_archive_sha256)
        )
        if self.uses_default_archive:
            cli_archive_url = DEFAULT_CLI_ARCHIVE_URL
            cli_archive_sha256 = DEFAULT_CLI_ARCHIVE_SHA256
        if not re.fullmatch(r"[a-f0-9]{64}", cli_archive_sha256):
            raise ValueError("a lowercase SHA256 for the platform CLI archive is required")
        if bool(cli_archive_path) == bool(cli_archive_url):
            raise ValueError("provide exactly one CLI archive path or HTTPS URL")
        if cli_archive_url:
            parsed = urlparse(cli_archive_url)
            if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
                raise ValueError("CLI archive URL must be HTTPS without credentials")
        if not math.isfinite(float(poll_interval_sec)) or not 0 < float(poll_interval_sec) <= DEFAULT_RUNTIME_CONFIG.poll_interval_max_sec:
            raise ValueError(
                f"poll_interval_sec must be between 0 and {DEFAULT_RUNTIME_CONFIG.poll_interval_max_sec}"
            )
        # The only refusal of framework-injected task context, for every adapter.
        # Task MCP servers and skills are described in the prompt instead.
        if extra_env or load_trajectory:
            raise ValueError(
                f"adapter does not support {framework}-injected environment or history in any mode"
            )
        self.mcp_servers = list(mcp_servers or [])
        self.skills_dir = skills_dir
        self.skills_section = ""
        if not self.model_name:
            raise ValueError("an explicit public API model ID is required")
        prefix = "REASONMACHINES_STAGING" if channel == "staging" else "REASONMACHINES_PRODUCTION"
        self.key = (os.environ.get(f"{prefix}_API_KEY")
                    or os.environ.get("REASON_API_KEY")
                    or os.environ.get("REASONMACHINES_API_KEY")
                    or "")
        self.expected_org = (os.environ.get(f"{prefix}_ORG_ID")
                    or os.environ.get("REASON_ORG_ID")
                    or os.environ.get("REASONMACHINES_ORG_ID")
                    or "")
        if not self.key:
            raise ValueError(f"set REASON_API_KEY or {prefix}_API_KEY on the controller")
        self.org = ""
        # Ephemeral mode: the controller key only creates and retires the
        # single-use child workspace; the child key stays in its client's memory.
        self.parent_org = ""
        self.ephemeral_org: str | None = None
        self.ephemeral_cleanup: dict | None = None
        self.parent_client: httpx.AsyncClient | None = None
        self.channel = channel
        def origin_override(value):
            if value is None:
                return ORIGINS[channel]
            parsed = urlparse(value)
            if (parsed.scheme != "http" or parsed.hostname not in {"localhost", "127.0.0.1", "::1"}
                    or parsed.username or parsed.password or parsed.path or parsed.query or parsed.fragment):
                raise ValueError("development API overrides must be HTTP loopback origins")
            return value
        self.api_origin = origin_override(api_origin)
        self.worker_api_origin = origin_override(worker_api_origin)
        if bool(api_origin) != bool(worker_api_origin):
            raise ValueError("local qualification requires both controller and worker loopback origins")
        if not isinstance(docker_host_api, bool):
            raise ValueError("docker_host_api must be a boolean")
        if docker_host_api and (not api_origin or self.worker_api_origin != "http://127.0.0.1:443"):
            raise ValueError("docker_host_api requires explicit loopback API origins and worker_api_origin=http://127.0.0.1:443")
        self.docker_host_api = docker_host_api
        self.archive_path = cli_archive_path
        self.archive_url = cli_archive_url
        self.archive_sha = cli_archive_sha256
        # musl (Alpine) task images cannot exec the glibc build.
        self.musl_archive_path = cli_archive_musl_path or os.environ.get("REASON_CLI_ARCHIVE_MUSL_PATH") or None
        self.musl_archive_sha = cli_archive_musl_sha256 or os.environ.get("REASON_CLI_ARCHIVE_MUSL_SHA256") or ""
        # Set from the stable release when the default archive is used.
        self.musl_archive_url: str | None = None
        if self.musl_archive_path and not re.fullmatch(r"[a-f0-9]{64}", self.musl_archive_sha):
            raise ValueError("cli_archive_musl_path requires a lowercase SHA256")
        # Agent tools (agent-tools/): static commands the models reach for that task images often lack. The
        # worker's PATH ends with their bin/, so the image's own command of the same name always wins, and only
        # the agent's shells see them: the verifier runs with the image's environment.
        self.agent_tools_path = agent_tools_archive_path or None
        self.agent_tools_sha = agent_tools_archive_sha256 or ""
        if self.agent_tools_path and not re.fullmatch(r"[a-f0-9]{64}", self.agent_tools_sha):
            raise ValueError("agent_tools_archive_path requires a lowercase SHA256")
        # A dated release that pins agent tools (cli_release.RELEASE_AGENT_TOOLS) downloads its archive
        # when no explicit one is given; the archive must match the pinned SHA256.
        self.agent_tools_url: str | None = None
        release_tools = RELEASE_AGENT_TOOLS.get(self.runtime_profile or "")
        # The release id must define the toolset a published result ran with: a release that pins its tools
        # takes no other archive, and a checksum alone names no archive.
        if release_tools and (agent_tools_archive_path or agent_tools_archive_sha256):
            raise ValueError(f"{self.runtime_profile} pins its agent tools; omit agent_tools_archive_path and "
                             "agent_tools_archive_sha256")
        if agent_tools_archive_sha256 and not agent_tools_archive_path:
            raise ValueError("agent_tools_archive_sha256 requires agent_tools_archive_path")
        if release_tools:
            self.agent_tools_url = release_tools["url"]
            self.agent_tools_sha = release_tools["sha256"]
        if reasoning_effort not in {"minimal", "low", "medium", "high", "xhigh", "max"}:
            raise ValueError("unsupported public reasoning_effort")
        self.reasoning_effort = reasoning_effort
        self.create_body = None
        self.poll_interval = float(poll_interval_sec)
        self.trial_key = str(uuid4())
        self.remote = f"/tmp/reason-{self.label}-{self.trial_key}"
        # Outside the worker's own directory (mode 0700): a jobs user must be able to run them.
        self.agent_tools_dir = f"/tmp/reason-{self.label}-tools-{self.trial_key}"
        self.binary = f"{self.remote}/package/bin/reason"
        self.config_path = f"{self.remote}/worker.json"
        self.device_id: str | None = None
        self.root_fingerprint: str | None = None
        self.ara_session_id: str | None = None
        self.finished = False
        self.client: httpx.AsyncClient | None = None
        self.workspace = ""
        self.image_env: dict[str, str] = {}
        self.agent_timeout_sec: float | None = None
        # The framework's agent-phase network mode ("public", "allowlist", "no-network").
        self.agent_network_mode: str | None = None
        self.session_wall_clock_max_ms = DEFAULT_RUNTIME_CONFIG.default_session_wall_clock_max_ms
        self.cli_version = "unknown"
        self.logs_dir.mkdir(parents=True, exist_ok=True)

    def _features_apply(self) -> bool:
        """Only an ephemeral Session of the default harness or a v7 profile reads runtime features."""
        return self.mode == "ephemeral" and (self.runtime_profile is None or self.runtime_profile in V7_RUNTIME_PROFILES)

    def _apply_agent_network(self, agent_network_mode: str | None) -> None:
        """An offline task runs without Reason's cloud-side web tools unless features were named."""
        self.agent_network_mode = agent_network_mode
        if (agent_network_mode not in (None, "public") and self.runtime_features is None
                and self._features_apply()):
            self.runtime_features = list(OFFLINE_TASK_RUNTIME_FEATURES)
            self.runtime_features_source = "offline_task"

    def _format_model_credential_error(self, error: ReasonAPIError) -> ReasonAPIError:
        domain = "staging.reasonmachines.ai" if self.channel == "staging" else "reasonmachines.ai"
        if error.code == "agent_model_key_missing":
            provider = self.model_name.split(":")[1] if self.model_name.startswith("byok:") else "openai"
            guidance = (
                f"Error: Model '{self.model_name}' requires a connected '{provider}' API key for organization {self.org}.\n\n"
                f"Connect your API key:\n"
                f"  • Web: https://{domain}/customize?tab=models\n"
                f"  • API: PUT /v3/organizations/{self.org}/providers/{provider}\n"
                f"  • API Reference: https://docs.reasonmachines.ai/api-reference/"
            )
        elif error.code == "invalid_model":
            guidance = (
                f"Error: Model '{self.model_name}' is not recognized or not available in this workspace.\n\n"
                f"Manage your available models and connected providers:\n"
                f"  • Web: https://{domain}/customize?tab=models\n"
                f"  • API Reference: https://docs.reasonmachines.ai/api-reference/"
            )
        else:
            return error
        return ReasonAPIError(error.status, error.code, guidance=guidance)

    async def _preflight(self):
        assert self.client is not None
        # Independent reads, sent together. A refused deployment is still
        # reported first, and enrollment still waits for both.
        response, org = await asyncio.gather(
            self._request_api("GET", "/v3/deployment"),
            resolve_organization(self._request_api, self.ephemeral_org if self.mode == "ephemeral" else self.expected_org),
            return_exceptions=True,
        )
        if isinstance(response, BaseException):
            raise response
        deployment = response.json()
        supported = deployment.get("session_modes")
        ownerships = deployment.get("workspace_ownerships")
        if (not isinstance(supported, list) or self.mode not in supported
                or not isinstance(ownerships, list) or WORKSPACE_OWNERSHIP not in ownerships):
            raise RuntimeError("deployment does not advertise the required Session mode and workspace ownership; refusing enrollment")
        runtime_profiles = deployment.get("experimental_runtime_profiles")
        if self.runtime_profile is not None and (
            not isinstance(runtime_profiles, list) or self.runtime_profile not in runtime_profiles
        ):
            raise RuntimeError(f"deployment does not advertise runtime profile {self.runtime_profile}; refusing enrollment"
                               f" (runtime_profile={C2_TOOLS_V7_RUNTIME_PROFILE} runs the deployment's default harness)")
        runtime_features = deployment.get("experimental_runtime_features")
        if self.runtime_features is not None and (
            not isinstance(runtime_features, list) or not set(self.runtime_features) <= set(runtime_features)
        ):
            raise RuntimeError("deployment does not advertise the requested runtime features; refusing enrollment")
        # Project only the authenticated non-secret deployment contract.
        receipt = {"mode": self.mode, "workspace_ownership": WORKSPACE_OWNERSHIP, "channel": self.channel}
        if self.runtime_profile is not None:
            receipt["runtime_profile"] = self.runtime_profile
        if self.runtime_features is not None:
            receipt["runtime_features"] = self.runtime_features
            receipt["runtime_features_source"] = self.runtime_features_source
        for field in ("service", "environment", "commit_sha", "artifact_sha", "deployment_id", "checked_at"):
            value = deployment.get(field)
            if value is None or isinstance(value, (str, bool)):
                receipt[field] = value
        self._save("deployment.json", receipt)
        if isinstance(org, BaseException):
            raise org
        self.org = org

    def _session_request(self, instruction: str, root_id: str) -> dict:
        body = {
            "prompt": instruction, "model": self.model_name,
            "reasoning_effort": self.reasoning_effort,
            "execution_budget": {"wall_clock_max_ms": self.session_wall_clock_max_ms},
            "idempotency_key": f"{self.label}-{self.trial_key}", "completion_mode": "run_outcome",
            "mode": self.mode, "workspace_ownership": WORKSPACE_OWNERSHIP,
            "target": {"kind": "headless", "device_id": self.device_id,
                       "root_id": root_id, "workspace_mode": "project_root"},
        }
        if self.runtime_profile in V7_RUNTIME_PROFILES:
            # v7 is told the trial's working directory (the image WORKDIR, or
            # `pwd -P` without one; see setup) as the model's cwd. Other
            # profiles keep main's request bytes.
            body["target"]["workspace_path"] = self.workspace
        if self.runtime_profile is not None:
            body["experimental_runtime_profile"] = self.runtime_profile
        if self.runtime_features is not None:
            body["experimental_runtime_features"] = self.runtime_features
        return body

    def _task_prompt(self, instruction: str) -> str:
        """Describe task MCP servers and skills, which Reason cannot attach natively.

        The wording follows Harbor's terminus-2 agent.
        """
        prompt = instruction
        if self.mcp_servers:
            prompt += ("\n\nMCP Servers:\nThe following MCP servers are available for this task "
                       "and reachable from the task environment.\n")
            for server in self.mcp_servers:
                if server.transport == "stdio":
                    prompt += f"- {server.name}: stdio transport, command: {server.command} {' '.join(server.args)}\n"
                else:
                    prompt += f"- {server.name}: {server.transport} transport, url: {server.url}\n"
        return prompt + self.skills_section

    async def _describe_skills(self, env) -> str:
        """Return an <available_skills> block for `<skills_dir>/*/SKILL.md` in the guest."""
        result = await self._guest_exec(
            env, f"cd {shlex.quote(self.skills_dir)} && find \"$PWD\" -mindepth 2 -maxdepth 2 -name SKILL.md -type f | sort",
            DEFAULT_RUNTIME_CONFIG.diagnostics_timeout_sec,
        )
        if result.return_code != 0 or not (result.stdout or "").strip():
            return ""
        root = Element("available_skills")
        for location in result.stdout.strip().splitlines():
            content = await self._guest_exec(env, f"cat {shlex.quote(location)}",
                                             DEFAULT_RUNTIME_CONFIG.diagnostics_timeout_sec)
            match = re.match(r"^---\n(.*?)\n---", content.stdout or "", re.DOTALL) if content.return_code == 0 else None
            try:
                frontmatter = yaml.safe_load(match.group(1)) if match else None
            except yaml.YAMLError:
                frontmatter = None
            if not isinstance(frontmatter, dict) or "name" not in frontmatter or "description" not in frontmatter:
                continue
            skill = SubElement(root, "skill")
            SubElement(skill, "name").text = str(frontmatter["name"])
            SubElement(skill, "description").text = str(frontmatter["description"])
            SubElement(skill, "location").text = location
        if not len(root):
            return ""
        indent(root, space="  ")
        return ("\n\nSkills:\nThe following skills are available in the task environment. Before using "
                "a skill, read its SKILL.md at the listed location with your tools.\n" + tostring(root, encoding="unicode"))

    def _cli_command(self, instruction: str) -> str:
        """The equivalent `reason` CLI invocation, recorded for reproduction."""
        args = ["reason", instruction, "--model", self.model_name,
                "--reasoning-effort", self.reasoning_effort]
        if self.runtime_profile is not None:
            args.extend(["--runtime-profile", self.runtime_profile])
        if self.mode != "standard":
            args.extend(["--mode", self.mode])
        command = shlex.join(args)
        # The CLI has no flag for these; the API request carried them.
        if self.runtime_features is not None:
            command += f"  # experimental_runtime_features={','.join(self.runtime_features)}"
        return command

    async def _api(self, method: str, suffix: str, body=None):
        response = await self._request_api(method, f"/v3/organizations/{quote(self.org, safe='')}/{suffix}", body)
        return response.json()

    async def _request_api(self, method: str, path: str, body=None, *, client: httpx.AsyncClient | None = None):
        client = client or self.client
        assert client is not None
        # Mutation ambiguity belongs to its existing idempotency/reconciliation
        # owner. Only safe reads can be automatically retried here.
        attempts = DEFAULT_RUNTIME_CONFIG.api_read_attempts if method.upper() == "GET" else 1
        # Retry waits share the existing request bound, and cancellation from
        # run()'s absolute timeout always propagates through this nested scope.
        async with _api_transport_errors(), asyncio.timeout(DEFAULT_RUNTIME_CONFIG.api_read_timeout_sec):
            for attempt in range(attempts):
                response = None
                try:
                    response = await client.request(method, path, **({"json": body} if body is not None else {}))
                except (TimeoutError, httpx.TransportError):
                    if attempt + 1 == attempts:
                        raise
                else:
                    if response.is_success:
                        return response
                    transient = response.status_code in {408, 429} or 500 <= response.status_code <= 599
                    if not transient or attempt + 1 == attempts:
                        # Never echo backend bodies, headers, or credentials.
                        raise ReasonAPIError.from_response(response)
                delay = DEFAULT_RUNTIME_CONFIG.api_retry_backoff_base_sec * (
                    DEFAULT_RUNTIME_CONFIG.api_retry_backoff_multiplier ** attempt
                )
                if response is not None and (retry_after := response.headers.get("Retry-After")):
                    try:
                        delay = float(retry_after)
                    except ValueError:
                        try:
                            delay = parsedate_to_datetime(retry_after).timestamp() - time.time()
                        except (TypeError, ValueError, OverflowError):
                            pass
                if not math.isfinite(delay):
                    delay = DEFAULT_RUNTIME_CONFIG.api_retry_backoff_base_sec * (
                        DEFAULT_RUNTIME_CONFIG.api_retry_backoff_multiplier ** attempt
                    )
                await asyncio.sleep(min(DEFAULT_RUNTIME_CONFIG.api_retry_max_delay_sec, max(0, delay)))

    def _retry_diagnostic(self, stage, error, attempt, delay):
        # Fixed, bounded metadata only; no bodies, URLs or exception text.
        self._save(stage + "-retry.json", {
            "attempt": attempt, "delay_sec": delay,
            "status": error.status if isinstance(error, ReasonAPIError) else None,
            "code": error.code if isinstance(error, ReasonAPIError) else None,
            "retry_after_sec": error.retry_after_sec if isinstance(error, ReasonAPIError) else None,
            "transport_error": isinstance(error, (ReasonAPITransportError, httpx.TransportError)),
            "timeout": isinstance(error, (ReasonAPIRequestTimeout, TimeoutError, httpx.TimeoutException)),
        })

    async def _cleanup_api(self, method, suffix, body=None):
        # Repeat explicit rate-limit rejections only, never ambiguous mutations.
        async with _api_transport_errors(), asyncio.timeout(DEFAULT_RUNTIME_CONFIG.api_read_timeout_sec):
            for attempt in range(DEFAULT_RUNTIME_CONFIG.api_read_attempts):
                try:
                    if body is None:
                        return await self._api(method, suffix)
                    return await self._api(method, suffix, body)
                except ReasonAPIError as error:
                    delay = min(DEFAULT_RUNTIME_CONFIG.api_retry_max_delay_sec, 2 ** attempt)
                    if error.retry_after_sec is not None:
                        delay = max(delay, error.retry_after_sec)
                    self._retry_diagnostic("cleanup-" + method.lower(), error, attempt + 1, delay)
                    if error.status != 429 or attempt + 1 == DEFAULT_RUNTIME_CONFIG.api_read_attempts:
                        raise
                    await asyncio.sleep(delay)

    async def _ephemeral_api(self, method, suffix, body, stage):
        """Parent-key workspace lifecycle call; repeating the identical body is safe."""
        assert self.parent_client is not None
        path = f"/v3/organizations/{quote(self.parent_org, safe='')}/ephemeral-workspaces{suffix}"
        attempts = 1 + DEFAULT_RUNTIME_CONFIG.uncertain_session_recovery_attempts
        for attempt in range(attempts):
            try:
                return (await self._request_api(method, path, body, client=self.parent_client)).json()
            except (ReasonAPIError, ReasonAPITransportError) as error:
                transient = not isinstance(error, ReasonAPIError) or error.status in {408, 429} or error.status >= 500
                if not transient or attempt + 1 == attempts:
                    raise
                delay = min(DEFAULT_RUNTIME_CONFIG.api_retry_max_delay_sec, 2 ** attempt)
                if isinstance(error, ReasonAPIError) and error.retry_after_sec is not None:
                    delay = max(delay, error.retry_after_sec)
                self._retry_diagnostic(stage, error, attempt + 1, delay)
                await asyncio.sleep(delay)

    async def _create_ephemeral_workspace(self) -> str:
        """Create this trial's single-use workspace and return its key, which is never persisted."""
        self.parent_org = await resolve_organization(
            partial(self._request_api, client=self.parent_client), self.expected_org,
        )
        reference = f"{self.label}-{self.trial_key}"
        config = DEFAULT_RUNTIME_CONFIG
        session_minutes = math.ceil(self.session_wall_clock_max_ms / 60_000)
        ttl_minutes = min(config.ephemeral_workspace_max_ttl_minutes,
                          max(config.ephemeral_workspace_ttl_minutes,
                              session_minutes + config.ephemeral_workspace_ttl_margin_minutes))
        rcus = self.ephemeral_rcus
        if rcus is None and self.agent_timeout_sec is not None:
            rcus = str(min(config.ephemeral_rcus_max, max(
                config.ephemeral_rcus_default, math.ceil(session_minutes / 60 * config.ephemeral_rcus_per_hour))))
        body = {
            "reference": reference, "ttl_minutes": ttl_minutes,
            **({"rcus": rcus} if rcus is not None else {}),
        }
        waits = 0
        while True:
            try:
                created = await self._ephemeral_api("POST", "", body, "ephemeral-workspace-create")
                break
            except ReasonAPIError as error:
                # The parent's active-workspace cap is capacity, not a failure: wait
                # for another trial to retire its workspace. No workspace exists yet,
                # and the framework's trial deadline cancels this wait.
                if error.status != 409 or error.code != "ephemeral_workspace_limit_reached":
                    raise
                waits += 1
                delay = min(DEFAULT_RUNTIME_CONFIG.capacity_wait_max_sec, 5 * 2 ** min(waits, 6)) * random.uniform(0.5, 1)
                self._retry_diagnostic("ephemeral-workspace-capacity", error, waits, delay)
                await asyncio.sleep(delay)
        created = created if isinstance(created, dict) else {}
        self.ephemeral_org = resource_id(created.get("organization_id"))
        key = created.get("api_key")
        if (resource_id(created.get("parent_organization_id")) != self.parent_org
                or created.get("reference") != reference or not isinstance(key, str) or not key.strip()):
            raise RuntimeError("server did not return this trial's ephemeral workspace and key")
        self._save("ephemeral-workspace.json", {
            "organization_id": self.ephemeral_org, "parent_organization_id": self.parent_org,
            "reference": reference, "expires_at": created.get("expires_at"),
        })
        return key

    async def _retire_ephemeral_workspace(self) -> dict:
        """Never raises: the server reaper retires the workspace at expiry if this fails."""
        if self.ephemeral_cleanup is None:
            errors = []
            try:
                retired = await self._ephemeral_api(
                    "POST", f"/{quote(self.ephemeral_org, safe='')}/retire", {}, "ephemeral-workspace-retire",
                )
                if (not isinstance(retired, dict) or retired.get("organization_id") != self.ephemeral_org
                        or not retired.get("retired_at")):
                    errors.append("retire receipt did not confirm retirement")
            except (ReasonAPIError, ReasonAPITransportError) as error:
                errors.append(str(error))
            except Exception as error:
                errors.append(f"retire failed: {type(error).__name__}")
            self.ephemeral_cleanup = {"organization_id": self.ephemeral_org, "retired": not errors, "errors": errors}
        return self.ephemeral_cleanup

    async def _create_session(self):
        """Recover an ambiguous create with the same idempotency key before failing the trial."""
        assert self.create_body is not None
        try:
            return await self._api("POST", "sessions", self.create_body)
        except (ReasonAPIRequestTimeout, ReasonAPITransportError) as original_error:
            for attempt in range(DEFAULT_RUNTIME_CONFIG.uncertain_session_recovery_attempts):
                try:
                    return await self._api("POST", "sessions", self.create_body)
                except (ReasonAPIRequestTimeout, ReasonAPITransportError) as error:
                    self._retry_diagnostic("create-recovery", error, attempt + 1, 0)
            raise original_error

    @staticmethod
    def _diagnostic(text):
        """Project only the CLI's deliberately public codes, never raw logs."""
        codes = sorted(set(re.findall(r"\bworker_[a-z_]+\b", text)))
        connections = []
        startup = []
        for line in text.splitlines():
            try:
                item = json.loads(line)
            except (ValueError, TypeError):
                continue
            if isinstance(item, dict) and item.get("event") == "worker.startup":
                stage, pid, timestamp = item.get("stage"), item.get("pid"), item.get("timestamp")
                if (isinstance(stage, str) and stage in {"lock_acquired", "enrollment_started", "enrollment_completed",
                              "readiness_started", "readiness_completed", "inventory_completed",
                              "control_ready", "ready"}
                        and type(pid) is int and 0 < pid < 2**31
                        and isinstance(timestamp, str)
                        and re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", timestamp)):
                    startup.append({"event": "worker.startup", "stage": stage, "pid": pid, "timestamp": timestamp})
            if isinstance(item, dict) and item.get("event") == "worker.connection_failed":
                projected = {"event": "worker.connection_failed"}
                for field in ("stage", "category"):
                    value = item.get(field)
                    if isinstance(value, str) and re.fullmatch(r"[a-z_]{1,64}", value):
                        projected[field] = value
                value = item.get("httpStatus")
                if isinstance(value, int) and 100 <= value <= 599:
                    projected["httpStatus"] = value
                connections.append(projected)
        return {"worker_codes": codes, "connections": connections,
                "output_bytes": len(text.encode()), **({"startup": startup} if startup else {})}

    async def _capture_worker_diagnostics(self, env):
        try:
            result = await self._guest_exec(
                env, f"tail -c {DEFAULT_RUNTIME_CONFIG.diagnostics_tail_bytes} {self.remote}/worker.log",
                DEFAULT_RUNTIME_CONFIG.diagnostics_timeout_sec,
            )
            projected = self._diagnostic(result.stdout + "\n" + result.stderr)
            projected["read_exit_code"] = result.return_code
            self._save("worker-diagnostics.json", projected)
        except Exception:
            self._save("worker-diagnostics.json", {"unavailable": True})

    async def _exec(self, env, command, timeout=None):
        timeout = DEFAULT_RUNTIME_CONFIG.command_timeout_sec if timeout is None else timeout
        timeout_sec = int(math.ceil(timeout)) if timeout is not None else None
        result = await self._guest_exec(env, command, timeout_sec)
        if result.return_code != 0:
            diagnostic = self._diagnostic(result.stdout + "\n" + result.stderr)
            diagnostic["exit_code"] = result.return_code
            self._save("command-error.json", diagnostic)
            raise RuntimeError("Reason worker command failed: " + json.dumps(diagnostic))
        return result.stdout

    @staticmethod
    async def _guest_exec(env, command, timeout_sec):
        """Run a command whose output is parsed, without the shell's startup output."""
        result = await env.exec(f"printf '%s\\n' {GUEST_OUTPUT_MARKER}; {command}", timeout_sec=timeout_sec)
        stdout = result.stdout or ""
        _, marker, output = stdout.partition(GUEST_OUTPUT_MARKER + "\n")
        return SimpleNamespace(return_code=result.return_code, stdout=output if marker else stdout,
                               stderr=getattr(result, "stderr", "") or "")

    async def _upload_json(self, env, value, target):
        with tempfile.TemporaryDirectory(prefix=f"reason-{self.label}-ticket-") as directory:
            path = Path(directory) / "input.json"
            path.write_text(json.dumps(value))
            # Both temporary parent directories are 0700. A readable transfer
            # file lets a non-root guest copy it even when Docker preserves the
            # controller's UID; the final worker file is guest-owned 0600.
            path.chmod(0o644)
            await env.upload_file(source_path=path, target_path=target + ".incoming")
        # Docker's archive upload preserves the controller UID. Recreate the
        # file as the executing guest user: chmod alone cannot satisfy the
        # worker's owner-only credential-file contract.
        await self._exec(env, f"install -m 600 {shlex.quote(target + '.incoming')} {shlex.quote(target + '.ready')} "
                         f"&& mv {shlex.quote(target + '.ready')} {shlex.quote(target)} "
                         f"&& rm -f {shlex.quote(target + '.incoming')}")

    async def _resolve_default_archive(self, downloader: httpx.AsyncClient) -> None:
        # A dated release runs its pinned CLI; anything else the newest stable one.
        resolution = release_archive(self.runtime_profile) or await resolve_stable_archive(downloader)
        self.archive_url = resolution["archive_url"]
        self.archive_sha = resolution["archive_sha256"]
        if not self.musl_archive_path and resolution.get("musl_archive_url"):
            self.musl_archive_url = resolution["musl_archive_url"]
            self.musl_archive_sha = resolution["musl_archive_sha256"]
        self._save("cli-resolution.json", resolution)
        if resolution["fallback_reason"]:
            self.logger.warning("Reason CLI packaged_fallback: %s; selected=%s sha256=%s",
                                resolution["fallback_reason"], resolution["selected_version"], self.archive_sha)
        else:
            self.logger.info("Reason CLI %s: selected=%s sha256=%s",
                             resolution["source"], resolution["selected_version"], self.archive_sha)

    async def _ensure_dependencies(self, env):
        # These are the portable worker runtime prerequisites. Install
        # missing tools only; never upgrade or alter the task's network policy.
        checks = "for tool in bash git tar gzip; do command -v \"$tool\" >/dev/null 2>&1 || printf '%s\\n' \"$tool\"; done; "
        checks += "command -v sha256sum >/dev/null 2>&1 || command -v shasum >/dev/null 2>&1 || printf 'checksum\\n'"
        missing = (await self._exec(env, checks)).split()
        if any(tool not in {"bash", "git", "tar", "gzip", "checksum"} for tool in missing):
            raise RuntimeError("unexpected runtime dependency probe output")
        installed = []
        manager = None
        if missing:
            uid = (await self._exec(env, "id -u")).strip()
            if uid != "0":
                raise RuntimeError("missing worker dependencies require root installation: " + ", ".join(missing))
            manager = (await self._exec(env,
                "if command -v apt-get >/dev/null 2>&1; then printf apt-get; "
                "elif command -v apk >/dev/null 2>&1; then printf apk; "
                "elif command -v dnf >/dev/null 2>&1; then printf dnf; "
                "elif command -v yum >/dev/null 2>&1; then printf yum; else printf unsupported; fi")).strip()
            packages = sorted({"coreutils" if tool == "checksum" else tool for tool in missing})
            if manager == "apt-get":
                command = "apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-upgrade --no-install-recommends " + " ".join(packages)
            elif manager == "apk":
                command = "apk add --no-cache " + " ".join(packages)
            elif manager in {"dnf", "yum"}:
                command = f"{manager} install -y -q " + " ".join(packages)
            else:
                raise RuntimeError("missing worker dependencies on unsupported package manager: " + ", ".join(missing))
            result = await env.exec(command, timeout_sec=DEFAULT_RUNTIME_CONFIG.dependency_install_timeout_sec)
            if result.return_code != 0:
                raise RuntimeError(f"{manager} could not install missing worker dependencies (exit {result.return_code}); existing task network policy was preserved")
            installed = packages
            if (await self._exec(env, checks)).strip():
                raise RuntimeError("runtime dependencies still missing after package installation")
        versions = {}
        for tool in ("bash", "git", "tar", "gzip"):
            result = await self._guest_exec(env, f"{tool} --version", DEFAULT_RUNTIME_CONFIG.tool_version_timeout_sec)
            first = (result.stdout or result.stderr or "").splitlines()
            version = first[0][:160] if first else "unavailable"
            versions[tool] = version if re.fullmatch(r"[A-Za-z0-9 ()+.,_/:;=-]{1,160}", version) else "available"
        checksum = (await self._exec(env, "if command -v sha256sum >/dev/null 2>&1; then printf sha256sum; else printf shasum; fi")).strip()
        self._save("runtime-dependencies.json", {"installed_packages": installed, "package_manager": manager,
                                                 "tools": versions, "checksum_tool": checksum})

    async def setup(self, sandbox: Sandbox, *, egress_proxy: str | None = None,
                    image_env: dict[str, str] | None = None, workdir: str | None = None,
                    agent_timeout_sec: float | None = None) -> None:
        """Install the worker. `egress_proxy` is the guest's HTTP(S) proxy URL, if any.

        `image_env` and `workdir` are the task image's ENV and WORKDIR. Guests
        such as E2B sandboxes do not apply the image config to exec shells, so
        the worker (and through it every Hands shell) starts with them
        explicitly. Without them the exec shell's environment and `pwd -P` stand.
        `agent_timeout_sec` is the task's agent time limit; the Session deadline
        follows it (see AdapterRuntimeConfig).
        """
        self.image_env = dict(image_env or {})
        self.agent_timeout_sec = agent_timeout_sec
        if agent_timeout_sec is not None:
            self.session_wall_clock_max_ms = min(
                DEFAULT_RUNTIME_CONFIG.session_wall_clock_max_ms,
                math.ceil((agent_timeout_sec + DEFAULT_RUNTIME_CONFIG.session_deadline_margin_sec) * 1000),
            )
        try:
            await self._setup(sandbox, egress_proxy, workdir)
        except (TimeoutError, httpx.TimeoutException):
            raise ReasonWorkerStartupError("Reason worker setup timed out") from None
        except httpx.TransportError:
            raise ReasonWorkerStartupError("Reason worker setup transport failed") from None

    async def _setup(self, environment: Sandbox, egress_proxy: str | None, workdir: str | None) -> None:
        # No Device/Session is allocated in setup: run's finally owns their lifetime.
        if self.uses_default_archive:
            guidance = ("The automatic CLI archive requires a Linux x86_64 guest with glibc or musl. "
                        "Provide an explicit worker archive via cli_archive_path or cli_archive_url "
                        "and cli_archive_sha256 for this guest.")
            try:
                abi = await self._exec(environment, "uname -s && uname -m && { getconf GNU_LIBC_VERSION 2>/dev/null"
                                                    " || { [ -e /lib/ld-musl-x86_64.so.1 ] && echo musl; }; }")
            except RuntimeError:
                raise ValueError(guidance) from None
            if not re.fullmatch(r"Linux\nx86_64\n(?:glibc [0-9]+(?:\.[0-9]+)+|musl)\n?", abi):
                raise ValueError(guidance)
        self.workspace = (await self._exec(
            environment, f"cd {shlex.quote(workdir)} && pwd -P" if workdir else "pwd -P")).strip()
        if not self.workspace.startswith("/") or self.workspace == "/" or "\n" in self.workspace:
            raise ValueError("task must have a canonical non-root working directory")
        if self.skills_dir:
            self.skills_section = await self._describe_skills(environment)
        await self._exec(environment, f"umask 077; mkdir {shlex.quote(self.remote)}")
        try:
            await self._ensure_dependencies(environment)
            musl = (await self._exec(environment, "if [ -e /lib/ld-musl-x86_64.so.1 ]; then printf musl; fi")).strip() == "musl"
            if musl and not self.musl_archive_path and not self.uses_default_archive:
                raise ValueError("musl task image requires cli_archive_musl_path and cli_archive_musl_sha256")
            with tempfile.TemporaryDirectory(prefix=f"reason-{self.label}-archive-") as directory:
                archive = Path(directory) / "worker.tgz"
                if musl and self.musl_archive_path:
                    archive.write_bytes(Path(self.musl_archive_path).expanduser().read_bytes())
                elif not musl and self.archive_path:
                    archive.write_bytes(Path(self.archive_path).expanduser().read_bytes())
                else:
                    async with httpx.AsyncClient(
                        timeout=DEFAULT_RUNTIME_CONFIG.archive_download_timeout_sec,
                        follow_redirects=False,
                    ) as downloader:
                        if self.uses_default_archive:
                            await self._resolve_default_archive(downloader)
                        if musl and not self.musl_archive_url:
                            raise ValueError("the stable Reason CLI release has no linux-x64-musl archive; "
                                             "provide cli_archive_musl_path and cli_archive_musl_sha256")
                        archive.write_bytes(await fetch_archive(
                            downloader, self.musl_archive_url if musl else self.archive_url))
                verify_archive(archive, self.musl_archive_sha if musl else self.archive_sha)
                await environment.upload_file(source_path=archive, target_path=f"{self.remote}/worker.tgz")
            await self._exec(environment, f"tar -xzf {self.remote}/worker.tgz -C {self.remote}")
            # A release from before the rename (a dated release's pinned CLI) names it bin/ara.
            if (await self._guest_exec(environment, f"test -x {self.binary}",
                                       int(math.ceil(DEFAULT_RUNTIME_CONFIG.command_timeout_sec)))).return_code != 0:
                self.binary = f"{self.remote}/package/bin/ara"
                await self._exec(environment, f"test -x {self.binary}")
            wrapper = f"{self.remote}/reason-clean"
            await self._exec(environment, f"printf %s {shlex.quote(_clean_worker_wrapper(self.binary))} > {shlex.quote(wrapper)} "
                                          f"&& chmod 0755 {shlex.quote(wrapper)}")
            self.binary = wrapper
            self.cli_version = (await self._exec(environment, f"{self.binary} --version")).strip()
            if self.docker_host_api and not egress_proxy:
                raise ValueError(f"docker_host_api requires {self.framework}'s egress proxy")
            if egress_proxy:
                await self._start_proxy_bridge(environment, egress_proxy)
            config = {"version": 1, "apiOrigin": self.worker_api_origin, "channel": self.channel,
                      "stateDirectory": f"{self.remote}/state", "workspaceRoot": self.workspace,
                      "lifetime": "ephemeral", "executionMode": "host",
                      "name": f"{self.framework} {self.trial_key}",
                      "slots": DEFAULT_RUNTIME_CONFIG.worker_slots,
                      "shutdownGraceMs": DEFAULT_RUNTIME_CONFIG.worker_shutdown_grace_ms}
            await self._upload_json(environment, config, self.config_path)
            if self.installs_agent_tools:
                await self._install_agent_tools(environment)
        except BaseException as original_error:
            try:
                await environment.exec(
                    f"rm -rf {self.remote} {self.agent_tools_dir}",
                    timeout_sec=DEFAULT_RUNTIME_CONFIG.cleanup_timeout_sec,
                )
            except Exception:
                original_error.add_note("Reason worker setup cleanup also failed")
            raise

    @property
    def installs_agent_tools(self) -> bool:
        """Whether the worker gets agent tools: an explicit archive, or its dated release's pin."""
        return bool(self.agent_tools_path or self.agent_tools_url)

    async def _release_agent_tools(self) -> bytes:
        """The release's pinned archive, from a host cache keyed by its SHA256 (parallel trials download it once)."""
        cache = (Path(os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache")
                 / "reason-machines" / "agent-tools" / f"{self.agent_tools_sha}.tgz")
        if cache.is_file():
            data = cache.read_bytes()
            if hashlib.sha256(data).hexdigest() == self.agent_tools_sha:
                return data
        try:
            async with httpx.AsyncClient(timeout=DEFAULT_RUNTIME_CONFIG.archive_download_timeout_sec,
                                         follow_redirects=False) as downloader:
                data = await fetch_archive(downloader, self.agent_tools_url)
        except Exception as error:
            raise RuntimeError(f"agent tools download failed ({self.agent_tools_url}): {error}") from None
        if hashlib.sha256(data).hexdigest() == self.agent_tools_sha:
            cache.parent.mkdir(parents=True, exist_ok=True)
            partial = cache.with_name(f"{cache.name}.{self.trial_key}.partial")
            partial.write_bytes(data)
            partial.replace(cache)
        return data

    async def _install_agent_tools(self, environment) -> None:
        if self.agent_tools_path:
            data = Path(self.agent_tools_path).expanduser().read_bytes()
        else:
            data = await self._release_agent_tools()
        if hashlib.sha256(data).hexdigest() != self.agent_tools_sha:
            raise ValueError("agent tools archive checksum mismatch")
        tools = shlex.quote(self.agent_tools_dir)
        await self._exec(environment, f"mkdir -m 0755 {tools}")
        # A 0644 copy: Docker's upload keeps the controller's mode, and a non-root guest must read it.
        with tempfile.TemporaryDirectory(prefix=f"reason-{self.label}-tools-") as directory:
            archive = Path(directory) / "agent-tools.tgz"
            archive.write_bytes(data)
            archive.chmod(0o644)
            await environment.upload_file(source_path=archive, target_path=f"{self.agent_tools_dir}/agent-tools.tgz")
        await self._exec(environment, f"tar -xzf {tools}/agent-tools.tgz -C {tools} && rm {tools}/agent-tools.tgz "
                                      f"&& chmod -R a+rX {tools}")
        # Which of them the image already has (those stay the image's), for the run's records.
        image_env = "".join(f" {shlex.quote(f'{key}={value}')}" for key, value in self.image_env.items())
        names = (await self._exec(environment, f"ls {tools}/bin")).split()
        probe = "; ".join(f"command -v {shlex.quote(name)} >/dev/null 2>&1 && printf '%s\\n' {shlex.quote(name)}"
                          for name in names) + "; true"
        # Records only: a probe that cannot run (an image PATH without /bin) must not fail the trial.
        result = await self._guest_exec(environment, f"env{image_env} BASH_ENV=/dev/null ENV=/dev/null /bin/sh -c {shlex.quote(probe)}",
                                        int(math.ceil(DEFAULT_RUNTIME_CONFIG.command_timeout_sec)))
        present = (result.stdout or "").split() if result.return_code == 0 else []
        self._save("agent-tools.json", {"sha256": self.agent_tools_sha, "directory": self.agent_tools_dir,
                                         "source": "path" if self.agent_tools_path else "release_pin",
                                         **({"url": self.agent_tools_url} if self.agent_tools_url else {}),
                                         "tools": names, "image_has": sorted(set(present) & set(names))})

    def relay_hosts(self) -> list[str]:
        """Relay hostnames the worker may be handed."""
        return list(RELAY_HOSTS[self.channel])

    async def _start_proxy_bridge(self, environment, proxy_url):
        """Route the worker's TLS through the guest's authenticated egress proxy.

        The worker keeps its direct HTTPS contract: /etc/hosts points the API
        and relay names at a loopback bridge that tunnels each connection
        through the proxy with CONNECT, selecting the target by TLS SNI.
        """
        p_parsed = urlparse(proxy_url)
        p_host = p_parsed.hostname or ""
        p_port = p_parsed.port or 8080
        p_auth = f"{p_parsed.username}:{p_parsed.password}" if p_parsed.username else ""
        api_host = urlparse(self.worker_api_origin).hostname or "staging-api.reasonmachines.com"
        # The worker retains its loopback-only development contract. The
        # authenticated proxy reaches the API on the Docker host; no task
        # network policy or framework core is changed.
        if self.docker_host_api:
            api_host = "host.docker.internal"
        relay_hosts = " ".join(shlex.quote(host) for host in self.relay_hosts())
        bridge_path = f"{self.remote}/bridge.py"
        encoded_bridge = base64.b64encode(_PROXY_BRIDGE.encode("utf-8")).decode("ascii")
        await self._exec(environment, f"echo {shlex.quote(encoded_bridge)} | base64 -d > {shlex.quote(bridge_path)}")
        await self._exec(environment, f"echo '127.0.0.1 {shlex.quote(api_host)} {relay_hosts}' >> /etc/hosts || true")
        await self._exec(environment, f"nohup python3 {shlex.quote(bridge_path)} 443 {shlex.quote(api_host)} 443 {shlex.quote(p_host)} {p_port} {shlex.quote(p_auth)} > {shlex.quote(self.remote)}/bridge.log 2>&1 &")
        await asyncio.sleep(0.5)

    async def _start_worker(self, env):
        try:
            return await self._enroll_and_wait_for_worker(env)
        except (TimeoutError, httpx.TimeoutException):
            raise ReasonWorkerStartupError("Reason worker startup timed out") from None
        except httpx.TransportError:
            raise ReasonWorkerStartupError("Reason worker startup transport failed") from None

    async def _enroll_and_wait_for_worker(self, env):
        fingerprint_key = secrets.token_bytes(32)
        verifier = secrets.token_urlsafe(48)
        fingerprint = hmac.new(fingerprint_key, self.workspace.encode(), hashlib.sha256).hexdigest()
        self.root_fingerprint = fingerprint
        permissions = dict(read=True, write=True, execute=True, discover_environment=True, expose_mcp=False)
        root = {"path_fingerprint": fingerprint, "permissions": permissions, "execution_mode": "host"}
        ticket = await self._api("POST", "devices/enrollment-tickets", {
            "install_id": self.trial_key, "lifecycle": "ephemeral",
            "ttl_seconds": DEFAULT_RUNTIME_CONFIG.worker_device_ttl_sec,
            "code_challenge": base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip("="),
            "roots": [root],
        })
        if ticket.get("device_kind") != "headless" or ticket.get("lifecycle") != "ephemeral":
            raise ReasonWorkerStartupError("invalid worker enrollment receipt")
        enrollment = {"version": 1, "organization_id": self.org,
                      "enrollment_ticket": ticket["enrollment_ticket"], "code_verifier": verifier,
                      "root_fingerprint_key": fingerprint_key.hex(), "root": root}
        await self._upload_json(env, enrollment, f"{self.remote}/enrollment.json")
        variables = dict(self.image_env)
        path_tail = ""
        if self.installs_agent_tools:
            tools_bin = f"{self.agent_tools_dir}/bin"
            if "PATH" in variables:
                variables["PATH"] = f"{variables['PATH']}:{tools_bin}"
            else:
                path_tail = f' PATH="$PATH":{shlex.quote(tools_bin)}'
        image_env = "".join(f" {shlex.quote(f'{key}={value}')}" for key, value in variables.items()) + path_tail
        # Background only the redirected command: `cd X && cmd &` would background
        # the whole list in a subshell that keeps the exec's stdout open until the
        # worker exits, so the exec never returns.
        await self._exec(env, f"cd {shlex.quote(self.workspace)} && {{ nohup env{image_env} REASON_RELAY_CUSTOM_DOMAIN=1 "
                         f"{self.binary} worker start --config {self.config_path} "
                         f"--enrollment-file {self.remote}/enrollment.json "
                         f">{self.remote}/worker.log 2>&1 </dev/null & echo $! >{self.remote}/worker.pid; }}")
        deadline = time.monotonic() + DEFAULT_RUNTIME_CONFIG.worker_start_timeout_sec
        interval = DEFAULT_RUNTIME_CONFIG.worker_readiness_poll_interval_sec
        status_retries = []
        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            status_timeout = min(DEFAULT_RUNTIME_CONFIG.worker_status_timeout_sec, max(1, math.ceil(remaining)))
            try:
                result = await self._guest_exec(
                    env, f"{self.binary} worker status --config {self.config_path} --json", status_timeout,
                )
            except (TimeoutError, httpx.TimeoutException, httpx.TransportError, RuntimeError) as error:
                if isinstance(error, RuntimeError) and not self._is_worker_status_timeout(error):
                    raise
                status_retries.append({
                    "attempt": len(status_retries) + 1,
                    "timeout_sec": status_timeout,
                    "error_type": type(error).__name__,
                })
                self._save("worker-status-retry.json", {"attempts": status_retries})
                await asyncio.sleep(min(interval, max(0, deadline - time.monotonic())))
                continue
            if result.return_code != 0:
                diagnostic = self._diagnostic((result.stdout or "") + "\n" + (result.stderr or ""))
                diagnostic["exit_code"] = result.return_code
                self._save("worker-status-error.json", diagnostic)
                # Startup may temporarily have no control/state receipt. Retry only
                # a bounded interval; permanent policy/config errors fail promptly.
                permanent = {"worker_unsafe_environment", "worker_file_not_private", "worker_unknown_command",
                             "worker_unknown_or_duplicate_flag", "worker_parent_not_canonical",
                             "worker_state_directory_not_private_or_canonical"}
                if permanent.intersection(diagnostic["worker_codes"]):
                    raise ReasonWorkerStartupError("worker status rejected: " + json.dumps(diagnostic))
                await asyncio.sleep(interval)
                continue
            try:
                status = json.loads(result.stdout or "")
            except ValueError:
                await asyncio.sleep(interval)
                continue
            if status.get("deviceId"):
                self.device_id = status["deviceId"]
            live = status.get("live")
            live = live if isinstance(live, dict) else {}
            if live.get("phase") == "running" or live.get("executionEligible") is True:
                # Execution eligibility also permits file-only workers. Startup
                # readiness is frozen, so a missing coding lane cannot heal by
                # polling longer or attempting a Session create.
                readiness = live.get("capabilityReadiness")
                readiness = readiness if isinstance(readiness, dict) else {}
                supported = live.get("supportedOperations")
                supported = supported if isinstance(supported, list) else []
                unavailable = [name for name in REQUIRED_WORKER_CAPABILITIES
                               if readiness.get(name) is not True or name not in supported]
                if unavailable:
                    self._save("worker-readiness.json", {
                        "reason_code": "worker_required_capabilities_unavailable",
                        "unavailable_capabilities": unavailable,
                    })
                    raise ReasonWorkerStartupError("worker required capabilities unavailable: " + ", ".join(unavailable))
            if self.device_id and live.get("executionEligible") is True:
                device = (await self._api("GET", f"devices/{quote(self.device_id, safe='')}"))["device"]
                roots = device["roots"]
                if len(roots) != 1 or roots[0].get("path_fingerprint") != fingerprint:
                    raise ReasonWorkerStartupError("expected exactly one benchmark root")
                # Local readiness does not prove the API accepts this worker.
                # The server owns artifact/protocol compatibility; never bypass it.
                compatible = (device.get("compatible") is True
                              and "compatibility_reason" in device
                              and device["compatibility_reason"] is None)
                reason = device.get("compatibility_reason")
                reason = reason if isinstance(reason, str) and reason in {
                    "offline", "protocol_unsupported", "worker_update_required",
                    "execution_ineligible", "file_access_only", "revoked",
                } else "unknown"
                self._save("worker-compatibility.json", {
                    "online": device.get("online") is True,
                    "compatible": compatible,
                    "reason_code": None if compatible else reason,
                    "cli_sha256": self.archive_sha,
                })
                if device.get("online") is False and reason == "offline":
                    await asyncio.sleep(interval)
                    continue
                if device.get("online") is not True or not compatible:
                    guidance = (" Install an official CLI archive compatible with the target API "
                                "and pin its SHA256; republish the adapter if its default is stale."
                                if reason == "worker_update_required" else "")
                    raise ReasonWorkerStartupError(
                        f"server rejected worker compatibility: {reason}." + guidance
                    )
                # Keep the one-use enrollment until the controller accepts this Device.
                await self._exec(env, f"rm -f {self.remote}/enrollment.json")
                return roots[0]["id"]
            await asyncio.sleep(interval)
        raise ReasonWorkerStartupError(
            "worker did not become execution eligible within "
            f"{DEFAULT_RUNTIME_CONFIG.worker_start_timeout_sec:g} seconds"
        )

    def _save(self, name, value):
        path = self.logs_dir / name
        path.write_text(json.dumps(value, indent=2) + "\n")
        path.chmod(0o600)

    @staticmethod
    def _is_worker_status_timeout(error):
        return (
            isinstance(error, (TimeoutError, httpx.TimeoutException))
            or (isinstance(error, RuntimeError) and re.search(
                r"\bcommand timed out after\b", str(error), re.IGNORECASE
            ))
        )

    def _record_failure(self, context, error):
        metadata = context.metadata if isinstance(context.metadata, dict) else {}
        metrics = {name: False for name in FAILURE_METRICS}
        retry_count = metadata.get("worker_status_retry_count", 0)
        retry_path = self.logs_dir / "worker-status-retry.json"
        if retry_path.exists():
            try:
                retry_count = len(json.loads(retry_path.read_text()).get("attempts", []))
            except (OSError, ValueError, TypeError):
                pass
        session = {}
        session_path = self.logs_dir / "session.json"
        if session_path.exists():
            try:
                session = json.loads(session_path.read_text())
            except (OSError, ValueError):
                session = {}
        if isinstance(error, ReasonSessionResourceUnavailableError) or session.get("error_class") == "resource_unavailable":
            failure_class = "resource_loss"
            metrics["resource_loss"] = True
        elif session.get("status") == "suspended" or _cancelled(session.get("error_class")):
            failure_class = "suspended_session"
            metrics["suspended_session"] = True
        elif isinstance(error, ReasonWorkerStartupError) and "timed out" in str(error).lower():
            failure_class = "worker_startup_timeout"
            metrics["worker_startup_timeout"] = True
        else:
            failure_class = type(error).__name__
        failure = {
            "failure_class": failure_class,
            "failure_stage": metadata.get("stage", "unknown"),
            "error_type": type(error).__name__,
            "session_id": self.ara_session_id,
            "device_id": self.device_id,
            "metrics": metrics,
            "worker_status_retry_count": retry_count,
        }
        metadata.update({"failure_class": failure_class, "failure_stage": failure["failure_stage"],
                         "failure_metrics": metrics, "worker_status_retry_count": retry_count})
        context.metadata = metadata
        self._save("failure.json", failure)

    async def _await_worker_exit(self, env) -> str:
        """Wait out the worker's shutdown grace, then kill it: "exited", "forced",
        "not_started" (no pid recorded), "alive" or "unknown" (the check failed)."""
        grace = math.ceil(DEFAULT_RUNTIME_CONFIG.worker_shutdown_grace_ms / 1000) + 5
        pid_file = shlex.quote(f"{self.remote}/worker.pid")
        # A worker reparented to a PID 1 that never reaps stays a zombie, which
        # `kill -0` still finds: a zombie has exited.
        script = (f"p=$(cat {pid_file} 2>/dev/null); case $p in ''|*[!0-9]*) echo not_started; exit 0;; esac; "
                  "gone() { ! kill -0 \"$p\" 2>/dev/null || [ \"$(sed -n 's/.*) \\(.\\).*/\\1/p' /proc/$p/stat 2>/dev/null)\" = Z ]; }; "
                  f"i=0; while [ $i -lt {grace} ]; do gone && {{ echo exited; exit 0; }}; sleep 1; i=$((i+1)); done; "
                  "kill -9 \"$p\" 2>/dev/null; sleep 1; gone && echo forced || echo alive")
        try:
            result = await self._guest_exec(env, script, grace + 10)
        except Exception:
            return "unknown"
        if getattr(result, "return_code", 1) != 0:
            return "unknown"
        status = (getattr(result, "stdout", "") or "").strip().splitlines()
        return status[-1] if status and status[-1] in ("exited", "forced", "not_started", "alive") else "unknown"

    async def _cleanup(self, env):
        failures = []
        await self._capture_worker_diagnostics(env)
        if self.create_body and not self.ara_session_id:
            # A lost POST response does not imply the server failed to start work.
            # Recover the same idempotent receipt before stopping the Device.
            for _ in range(DEFAULT_RUNTIME_CONFIG.uncertain_session_recovery_attempts):
                try:
                    receipt = await self._api("POST", "sessions", self.create_body)
                    self.ara_session_id = receipt["session_id"]
                    break
                except Exception:
                    continue
            if not self.ara_session_id:
                failures.append("could not recover uncertain Session creation; inspect idempotency key "
                                f"{self.label}-{self.trial_key}")
        if self.root_fingerprint and not self.device_id:
            # Enrollment can succeed just before polling/transport fails. Locate only
            # this trial's unguessable root fingerprint before revoking its Device.
            try:
                inventory = await self._api("GET", "devices")
                matches = [device for device in inventory["devices"]
                           if any(root.get("path_fingerprint") == self.root_fingerprint
                                  for root in device.get("roots", []))]
                if len(matches) == 1:
                    self.device_id = matches[0]["id"]
            except Exception:
                failures.append("could not recover uncertain Device enrollment")
        if self.ara_session_id and not self.finished:
            try:
                await self._cleanup_api("POST", f"sessions/{quote(self.ara_session_id, safe='')}/cancel", {})
            except Exception:
                failures.append("remote Session cancellation failed")
        try:
            await self._exec(
                env,
                f"{self.binary} worker stop --config {self.config_path}",
                timeout=DEFAULT_RUNTIME_CONFIG.worker_stop_timeout_sec,
            )
        except Exception:
            failures.append("worker stop failed")
        # `worker stop` only asks; the worker can keep running for its shutdown
        # grace. It must be gone before the framework grades in this container.
        worker_exit = await self._await_worker_exit(env)
        if worker_exit not in ("exited", "forced", "not_started"):
            failures.append(f"worker did not exit ({worker_exit})")

        # Revoke only after the stop: a worker revoked first exits on its own,
        # and its stop then fails with worker_revoked_or_expired. The Device
        # revocation uses the child key, so it precedes the workspace retirement
        # that ends that key. These API calls run while the guest state is
        # removed; both finish before cleanup returns.
        async def revoke_device_and_retire_workspace():
            errors = []
            if self.device_id:
                try:
                    await self._cleanup_api("DELETE", f"devices/{quote(self.device_id, safe='')}")
                except Exception:
                    errors.append("Device revocation failed")
            return errors, (await self._retire_ephemeral_workspace() if self.ephemeral_org else None)

        async def remove_guest_state():
            try:
                await self._exec(
                    env,
                    f"rm -rf {self.remote} {self.agent_tools_dir}",
                    timeout=DEFAULT_RUNTIME_CONFIG.cleanup_timeout_sec,
                )
            except Exception:
                return ["guest state removal failed"]
            return []

        (revoke_errors, retired), guest_errors = await asyncio.gather(
            revoke_device_and_retire_workspace(), remove_guest_state(),
        )
        failures += revoke_errors + guest_errors
        receipt = {"session_id": self.ara_session_id, "device_id": self.device_id, "worker_exit": worker_exit,
                   "errors": failures}
        if retired is not None:
            receipt["ephemeral_workspace"] = retired
        self._save("cleanup.json", receipt)
        if failures:
            raise RuntimeError("; ".join(failures))

    async def run(self, instruction: str, environment: Sandbox, context: Any,
                  agent_network_mode: str | None = None) -> None:
        if not self.workspace:
            raise RuntimeError("setup must run before run")
        self._apply_agent_network(agent_network_mode)
        if self.mode != "ephemeral":
            return await self._run(instruction, environment, context, self.key)
        async with httpx.AsyncClient(
            base_url=self.api_origin,
            timeout=DEFAULT_RUNTIME_CONFIG.api_read_timeout_sec,
            follow_redirects=False,
            headers={"Authorization": f"Bearer {self.key}"}
        ) as self.parent_client:
            try:
                context.metadata = {
                    "stage": "ephemeral_workspace",
                    "failure_metrics": {name: False for name in FAILURE_METRICS},
                    "worker_status_retry_count": 0,
                }
                try:
                    key = await self._create_ephemeral_workspace()
                except Exception as error:
                    self._record_failure(context, error)
                    raise
                await self._run(instruction, environment, context, key)
            finally:
                if self.ephemeral_org and self.ephemeral_cleanup is None:
                    # Failed before Device/Session cleanup took ownership.
                    retire = asyncio.create_task(self._retire_ephemeral_workspace())
                    try:
                        await asyncio.shield(retire)
                    except asyncio.CancelledError:
                        await retire
                        raise
                    finally:
                        if retire.done() and not retire.cancelled():
                            self._save("cleanup.json", {"session_id": None, "device_id": None, "errors": [],
                                                        "ephemeral_workspace": retire.result()})

    async def _run(self, instruction: str, environment: Sandbox, context: Any, key: str) -> None:
        async with httpx.AsyncClient(
            base_url=self.api_origin,
            timeout=DEFAULT_RUNTIME_CONFIG.api_read_timeout_sec,
            follow_redirects=False,
            headers={"Authorization": f"Bearer {key}"}
        ) as self.client:
            context.metadata = {
                "stage": "preflight",
                "failure_metrics": {name: False for name in FAILURE_METRICS},
                "worker_status_retry_count": 0,
            }
            try:
                await self._preflight()
                context.metadata["stage"] = "worker_startup"
                root_id = await self._start_worker(environment)
                # The framework owns the outer task deadline; the driver does
                # not impose an inference budget of its own.
                async with asyncio.timeout(None):
                    context.metadata["stage"] = "session_create"
                    prompt = self._task_prompt(instruction)
                    self.create_body = self._session_request(prompt, root_id)
                    command_path = self.logs_dir / "cli-command.txt"
                    command_path.write_text(self._cli_command(prompt) + "\n")
                    command_path.chmod(0o600)
                    try:
                        created = await self._create_session()
                    except ReasonAPIError as error:
                        if error.definitive_rejection:
                            self.create_body = None
                        if error.code in ("agent_model_key_missing", "invalid_model"):
                            raise self._format_model_credential_error(error) from error
                        raise
                    self.ara_session_id = created["session_id"]
                    context.metadata = {"stage": "session_poll",
                                        "failure_metrics": {name: False for name in FAILURE_METRICS},
                                        "worker_status_retry_count": 0,
                                        "session_id": self.ara_session_id, "device_id": self.device_id,
                                        "org_id": self.org,
                                        "channel": self.channel,
                                        "cli_sha256": self.archive_sha, "cli_version": self.cli_version,
                                        "reasoning_effort": self.reasoning_effort}
                    if self.runtime_profile is not None:
                        context.metadata["runtime_profile"] = self.runtime_profile
                    if self.runtime_features is not None:
                        context.metadata["runtime_features"] = self.runtime_features
                        context.metadata["runtime_features_source"] = self.runtime_features_source
                    if self.agent_network_mode is not None:
                        context.metadata["agent_network_mode"] = self.agent_network_mode
                    # Recorded, not enforced: the deployment's advertised profiles and
                    # modes were checked in preflight, and a rolling deploy may change
                    # how a live Session reports them without changing what it runs.
                    context.metadata["session_policy"] = created.get("session_policy")
                    context.metadata["server_experimental_runtime_profile"] = created.get("experimental_runtime_profile")
                    context.metadata["server_experimental_runtime_features"] = created.get("experimental_runtime_features")
                    if self.mode == "ephemeral":
                        context.metadata["parent_org_id"] = self.parent_org
                    self._save("receipt.json", context.metadata)
                    # Enforced, unlike the fields above: a replica that predates the
                    # field ignores it, so its Session runs another configuration.
                    # Cleanup cancels it; the framework may retry the trial.
                    if self.runtime_features is not None and created.get("experimental_runtime_features") != self.runtime_features:
                        raise RuntimeError("the Session did not record the requested runtime features; refusing to run another configuration")
                    cursor = -1
                    last_tool_result = ""
                    poll_failures = 0
                    retry_delay = max(1.0, self.poll_interval)
                    poll_delay = self.poll_interval
                    idle_poll_delay_max = max(self.poll_interval, DEFAULT_RUNTIME_CONFIG.poll_idle_interval_max_sec)
                    while True:
                        poll_started = time.monotonic()
                        try:
                            event_batch = await self._api(
                                "GET",
                                f"sessions/{quote(self.ara_session_id, safe='')}/events?after_seq={cursor}"
                                f"&limit={DEFAULT_RUNTIME_CONFIG.event_page_size}&wait_ms={DEFAULT_RUNTIME_CONFIG.poll_wait_ms}",
                            )
                        except (ReasonAPIError, ReasonAPITransportError) as poll_error:
                            if isinstance(poll_error, ReasonAPIError) and poll_error.definitive_rejection and poll_error.status != 429:
                                raise
                            poll_failures += 1
                            delay = min(DEFAULT_RUNTIME_CONFIG.poll_interval_max_sec, retry_delay)
                            if isinstance(poll_error, ReasonAPIError) and poll_error.retry_after_sec is not None:
                                delay = max(delay, poll_error.retry_after_sec)
                            self._retry_diagnostic("poll", poll_error, poll_failures, delay)
                            await asyncio.sleep(delay)
                            retry_delay = min(DEFAULT_RUNTIME_CONFIG.poll_interval_max_sec, delay * 2)
                            continue
                        poll_failures = 0
                        retry_delay = max(1.0, self.poll_interval)
                        session = event_batch["session"]
                        items = event_batch["items"]
                        if items:
                            with (self.logs_dir / "events.jsonl").open("a") as events:
                                for item in items:
                                    events.write(json.dumps(item) + "\n")
                                    if item.get("type") == "tool_result":
                                        last_tool_result = str((item.get("data") or {}).get("content") or item.get("text") or "")
                            (self.logs_dir / "events.jsonl").chmod(0o600)
                            cursor = event_batch["next_seq"]
                            # A full page means more are waiting: fetch them now.
                            poll_delay = 0.0 if len(items) >= DEFAULT_RUNTIME_CONFIG.event_page_size else self.poll_interval
                        self._save("session.json", session)
                        context.n_input_tokens = (session.get("usage") or {}).get("input_tokens")
                        context.n_output_tokens = (session.get("usage") or {}).get("output_tokens")
                        context.cost_usd = session.get("cost_usd")
                        # A full page that arrives with the Session's end has more behind it: page it in first,
                        # so events.jsonl and the last tool result are complete.
                        if session["status"] != "running" and len(items) >= DEFAULT_RUNTIME_CONFIG.event_page_size:
                            continue
                        if session["status"] != "running":
                            self.finished = session["status"] in {"exit", "error"}
                            try:
                                trajectory = await self._api(
                                    "GET",
                                    f"sessions/{quote(self.ara_session_id, safe='')}/trajectory",
                                )
                                self._save("trajectory.json", trajectory)
                            except Exception as traj_err:
                                self.logger.debug(f"Failed to fetch session trajectory: {traj_err}")
                            if session["status"] != "exit":
                                if session["status"] == "error" and session.get("error_class") == "resource_unavailable":
                                    raise ReasonSessionResourceUnavailableError(
                                        status=session["status"], reason_code=session.get("reason_code")
                                    )
                                raise ReasonSessionTerminalError(
                                    status=session["status"],
                                    error_class=session.get("error_class"),
                                    reason_code=session.get("reason_code"),
                                )
                            # The task machine was lost and no tool ran after it: the agent
                            # stopped for the infrastructure, so grading it would score the
                            # outage. The framework retries a resource loss instead.
                            if HANDS_LOST_OUTPUT.search(last_tool_result):
                                raise ReasonSessionResourceUnavailableError(status="exit", reason_code="hands_lost")
                            break
                        # Each poll that brings no new items doubles the spacing to the
                        # next poll start, up to the idle cap; any new item resets it to
                        # the base interval. Time the server spent waiting counts.
                        await asyncio.sleep(max(0.0, poll_delay - (time.monotonic() - poll_started)))
                        poll_delay = min(idle_poll_delay_max, max(self.poll_interval, poll_delay * 2))
                self.populate_context(context)
                context.metadata["stage"] = "complete"
            finally:
                # Framework cancellation must not abandon the remote agent. Shield the bounded cleanup.
                original_error = sys.exception()
                if original_error is not None and not isinstance(original_error, asyncio.CancelledError):
                    self._record_failure(context, original_error)
                cleanup = asyncio.create_task(self._cleanup(environment))
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError as cancellation:
                    try:
                        await cleanup
                    except Exception:
                        cancellation.add_note("Reason cleanup also failed")
                    raise
                except Exception as cleanup_error:
                    if original_error is None:
                        raise
                    original_error.add_note(f"Cleanup also failed: {cleanup_error}")

    def populate_context(self, context: Any) -> None:
        """Copy the exported trajectory's final metrics onto the framework context.

        They include the subagent Sessions the trial started; the polled Session
        usage covers only the trial's own Session.
        """
        traj_path = self.logs_dir / "trajectory.json"
        if traj_path.exists():
            try:
                data = json.loads(traj_path.read_text())
                fm = data.get("final_metrics") or {}
                if fm.get("total_prompt_tokens") is not None:
                    context.n_input_tokens = fm.get("total_prompt_tokens")
                if fm.get("total_completion_tokens") is not None:
                    context.n_output_tokens = fm.get("total_completion_tokens")
                if fm.get("total_cached_tokens") is not None:
                    context.n_cache_tokens = fm.get("total_cached_tokens")
                if fm.get("total_cost_usd") is not None:
                    context.cost_usd = fm.get("total_cost_usd")
                if hasattr(context, "n_agent_steps") and fm.get("total_steps") is not None:
                    context.n_agent_steps = fm.get("total_steps")
            except Exception as exc:
                self.logger.debug(f"Failed to populate context from trajectory: {exc}")


# Run inside the guest by _start_proxy_bridge: a loopback TLS forwarder that
# tunnels each connection through an HTTP CONNECT proxy, chosen by SNI.
_PROXY_BRIDGE = (
    "import sys, socket, threading, base64, time, struct\n"
    "local_port, default_remote_host, default_remote_port, proxy_host, proxy_port = "
    "int(sys.argv[1]), sys.argv[2], int(sys.argv[3]), sys.argv[4], int(sys.argv[5])\n"
    "proxy_auth = sys.argv[6] if len(sys.argv) > 6 else ''\n"
    "def log(msg):\n"
    "    sys.stderr.write(f'[{time.time():.3f}] {msg}\\n')\n"
    "    sys.stderr.flush()\n"
    "def parse_sni(data):\n"
    "    try:\n"
    "        if len(data) < 43 or data[0] != 0x16 or data[5] != 0x01: return None\n"
    "        pos = 43\n"
    "        if pos >= len(data): return None\n"
    "        pos += 1 + data[pos]\n"
    "        if pos + 2 > len(data): return None\n"
    "        pos += 2 + struct.unpack('!H', data[pos:pos+2])[0]\n"
    "        if pos + 1 > len(data): return None\n"
    "        pos += 1 + data[pos]\n"
    "        if pos + 2 > len(data): return None\n"
    "        ext_len = struct.unpack('!H', data[pos:pos+2])[0]\n"
    "        pos += 2\n"
    "        end = pos + ext_len\n"
    "        while pos + 4 <= end:\n"
    "            etype, elen = struct.unpack('!HH', data[pos:pos+4])\n"
    "            pos += 4\n"
    "            if etype == 0:\n"
    "                if pos + 5 <= end:\n"
    "                    nlen = struct.unpack('!H', data[pos+3:pos+5])[0]\n"
    "                    return data[pos+5:pos+5+nlen].decode('ascii', errors='replace')\n"
    "                return None\n"
    "            pos += elen\n"
    "    except Exception: pass\n"
    "    return None\n"
    "def forward(src, dst, label):\n"
    "    try:\n"
    "        while True:\n"
    "            data = src.recv(65536)\n"
    "            if not data: break\n"
    "            dst.sendall(data)\n"
    "    except Exception as e:\n"
    "        log(f'{label} err: {e}')\n"
    "    finally:\n"
    "        try: dst.shutdown(socket.SHUT_WR)\n"
    "        except Exception: pass\n"
    "def handle_client(client_sock, cid):\n"
    "    log(f'conn {cid} accepted')\n"
    "    proxy_sock = None\n"
    "    try:\n"
    "        initial_data = client_sock.recv(4096)\n"
    "        if not initial_data: client_sock.close(); return\n"
    "        sni = parse_sni(initial_data)\n"
    "        remote_host = sni if sni else default_remote_host\n"
    "        remote_port = default_remote_port\n"
    "        log(f'conn {cid} remote: {remote_host}:{remote_port}')\n"
    "        proxy_sock = socket.create_connection((proxy_host, proxy_port), timeout=15)\n"
    "        headers = [f'CONNECT {remote_host}:{remote_port} HTTP/1.1', f'Host: {remote_host}:{remote_port}']\n"
    "        if proxy_auth:\n"
    "            b64_auth = base64.b64encode(proxy_auth.encode('utf-8')).decode('ascii')\n"
    "            headers.append(f'Proxy-Authorization: Basic {b64_auth}')\n"
    "        headers.append('\\r\\n')\n"
    "        proxy_sock.sendall('\\r\\n'.join(headers).encode('latin1'))\n"
    "        resp = b''\n"
    "        while b'\\r\\n\\r\\n' not in resp:\n"
    "            chunk = proxy_sock.recv(4096)\n"
    "            if not chunk: break\n"
    "            resp += chunk\n"
    "        first_line = resp.split(b'\\r\\n')[0].decode('latin1', errors='replace')\n"
    "        log(f'conn {cid} proxy resp: {first_line}')\n"
    "        if ' 200 ' not in first_line:\n"
    "            return\n"
    "        proxy_sock.settimeout(None)\n"
    "        client_sock.settimeout(None)\n"
    "        proxy_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)\n"
    "        client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)\n"
    "        proxy_sock.sendall(initial_data)\n"
    "        t1 = threading.Thread(target=forward, args=(client_sock, proxy_sock, f'{cid} c->p'), daemon=True)\n"
    "        t2 = threading.Thread(target=forward, args=(proxy_sock, client_sock, f'{cid} p->c'), daemon=True)\n"
    "        t1.start()\n"
    "        t2.start()\n"
    "        t1.join()\n"
    "        t2.join()\n"
    "    except Exception as e:\n"
    "        log(f'conn {cid} setup err: {e}')\n"
    "    finally:\n"
    "        try: client_sock.close()\n"
    "        except Exception: pass\n"
    "        if proxy_sock:\n"
    "            try: proxy_sock.close()\n"
    "            except Exception: pass\n"
    "        log(f'conn {cid} closed')\n"
    "server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)\n"
    "server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\n"
    "server.bind(('127.0.0.1', local_port))\n"
    "server.listen(128)\n"
    "log(f'bridge listening on 127.0.0.1:{local_port} -> {proxy_host}:{proxy_port}')\n"
    "cid = 0\n"
    "while True:\n"
    "    cs, _ = server.accept()\n"
    "    cid += 1\n"
    "    threading.Thread(target=handle_client, args=(cs, cid), daemon=True).start()\n"
)
