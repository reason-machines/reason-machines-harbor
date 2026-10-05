"""Operational defaults for the benchmark driver.

These values tune transport, worker lifecycle, and artifact handling. They are
not provider billing policy or benchmark policy; those remain owned by Reason
Machines and the experiment runner respectively.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AdapterRuntimeConfig:
    # Session event polling: the API has no event stream for API keys, so a
    # Session's end is seen up to one interval late. Each poll costs ~8 DB
    # statements; at 100 concurrent trials 1 s polling would add ~550/s.
    poll_interval_sec: float = 3.0
    poll_interval_max_sec: float = 30.0

    api_read_attempts: int = 3
    api_read_timeout_sec: float = 30.0
    api_retry_max_delay_sec: float = 5.0
    api_retry_backoff_base_sec: float = 0.5
    api_retry_backoff_multiplier: float = 2.0

    diagnostics_tail_bytes: int = 32 * 1024
    diagnostics_timeout_sec: int = 10
    command_timeout_sec: int = 30
    cleanup_timeout_sec: int = 15
    worker_stop_timeout_sec: int = 25

    archive_redirect_max_hops: int = 6
    archive_download_timeout_sec: int = 120
    dependency_install_timeout_sec: int = 180
    tool_version_timeout_sec: int = 10

    worker_start_timeout_sec: int = 180
    # The enrolled worker Device must outlive the whole run: an expiring lease
    # starts handing the run off and ends it as run_unsettled. The trial
    # deadline is not passed to the agent, so request the server's ephemeral
    # maximum (24 h); cleanup revokes the Device on every normal exit.
    worker_device_ttl_sec: int = 86_400
    worker_status_timeout_sec: int = 60
    # Worker startup readiness (guest `worker status`, then Device online) is
    # usually reached within seconds; a short interval sees it promptly.
    worker_readiness_poll_interval_sec: float = 0.5
    worker_shutdown_grace_ms: int = 10_000
    worker_slots: int = 1

    event_page_size: int = 500
    # Longest pause between Session-capacity retries (the parent's active
    # ephemeral-workspace cap); the trial deadline bounds the whole wait.
    capacity_wait_max_sec: float = 60.0
    uncertain_session_recovery_attempts: int = 2

    # A Session with no deadline on worker Hands gets no Reason capability plane
    # (memory, skills, MCP), so every Session requests one: the task's own agent
    # time limit plus a margin, so the framework's timeout fires first and grades
    # the attempt as it stands. The server covers deadlines up to 12 h with the
    # plane's credential; a task with no known limit gets 170 minutes.
    session_deadline_margin_sec: int = 600
    session_wall_clock_max_ms: int = (12 * 60 - 1) * 60 * 1000
    default_session_wall_clock_max_ms: int = 170 * 60 * 1000
    # The ephemeral workspace outlives the Session by a margin; cleanup retires
    # it, before grading, on every normal exit. The server allows 720 minutes.
    ephemeral_workspace_ttl_margin_minutes: int = 20
    ephemeral_workspace_ttl_minutes: int = 240
    ephemeral_workspace_max_ttl_minutes: int = 720
    # The workspace budget is held on the parent's wallet and charged for actual
    # use. It scales with the Session deadline between the server default and cap.
    ephemeral_rcus_per_hour: int = 10
    ephemeral_rcus_default: int = 30
    ephemeral_rcus_max: int = 300


DEFAULT_RUNTIME_CONFIG = AdapterRuntimeConfig()
