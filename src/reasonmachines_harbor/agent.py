"""Harbor wiring for the Reason benchmark driver."""
from __future__ import annotations

from urllib.parse import urlparse

from harbor.agents.base import BaseAgent
from harbor.environments.base import BaseEnvironment
from harbor.models.agent.context import AgentContext
from ._driver import DEFAULT_RUNTIME_CONFIG, ReasonDriver, task_agent_timeout_sec
from ._driver.driver import ReasonWorkerStartupError

from .docker_dns import prepare_control_plane_dns
from .image_runtime import resolve_image_runtime


class ReasonAgent(BaseAgent):
    """Linux task containers with outbound Reason transport; native Harbor grading."""
    SUPPORTS_ATIF: bool = True

    def __init__(self, *args, channel: str | None = None,
                 pr_environment: str | None = None, pr_relay_origin: str | None = None,
                 cli_archive_sha256: str | None = None,
                 cli_archive_path: str | None = None,
                 cli_archive_url: str | None = None,
                 cli_archive_musl_path: str | None = None,
                 cli_archive_musl_sha256: str | None = None,
                 api_origin: str | None = None, worker_api_origin: str | None = None,
                 poll_interval_sec: float = DEFAULT_RUNTIME_CONFIG.poll_interval_sec,
                 reasoning_effort: str = "medium",
                 mode: str = "ephemeral",
                 runtime_profile: str | None = None,
                 runtime_features: str | list[str] | None = None,
                 ephemeral_rcus: str | None = None,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.driver = ReasonDriver(
            "Harbor", self.logs_dir, self.model_name, self.logger,
            channel=channel, pr_environment=pr_environment, pr_relay_origin=pr_relay_origin,
            cli_archive_sha256=cli_archive_sha256, cli_archive_path=cli_archive_path,
            cli_archive_url=cli_archive_url, cli_archive_musl_path=cli_archive_musl_path,
            cli_archive_musl_sha256=cli_archive_musl_sha256, api_origin=api_origin, worker_api_origin=worker_api_origin,
            poll_interval_sec=poll_interval_sec, reasoning_effort=reasoning_effort, mode=mode,
            runtime_profile=runtime_profile, runtime_features=runtime_features, ephemeral_rcus=ephemeral_rcus,
            mcp_servers=self.mcp_servers, skills_dir=self.skills_dir,
            extra_env=self.extra_env, load_trajectory=self.load_trajectory,
        )

    @staticmethod
    def name() -> str:
        return "reason"

    def version(self) -> str:
        return "0.3.12"

    async def setup(self, environment: BaseEnvironment) -> None:
        driver = self.driver
        api_host = urlparse(driver.worker_api_origin).hostname
        # Local qualification must remain independent of public control-plane DNS.
        if api_host not in {"localhost", "127.0.0.1", "::1"}:
            try:
                await prepare_control_plane_dns(environment, [host for host in (api_host, *driver.relay_hosts()) if host])
            except TimeoutError:
                raise ReasonWorkerStartupError("Reason worker setup timed out") from None
        image_env, workdir = await resolve_image_runtime(environment, self.logger)
        await driver.setup(environment, image_env=image_env, workdir=workdir,
                           agent_timeout_sec=task_agent_timeout_sec(getattr(environment, "environment_dir", None)))

    async def run(self, instruction: str, environment: BaseEnvironment, context: AgentContext) -> None:
        # Harbor switches to the agent phase's policy around run(), not setup().
        mode = getattr(getattr(environment, "network_policy", None), "network_mode", None)
        mode = None if mode is None else str(getattr(mode, "value", mode))
        if mode == "no-network":
            hosts = " ".join(f"--allow-agent-host {host}" for host in self._reason_hosts())
            raise ValueError(f"the worker needs Reason egress on this offline task: run Harbor with {hosts}")
        await self.driver.run(instruction, environment, context, agent_network_mode=mode)

    def _reason_hosts(self) -> list[str]:
        return [host for host in (urlparse(self.driver.worker_api_origin).hostname, *self.driver.relay_hosts()[:1]) if host]

    def populate_context_post_run(self, context: AgentContext) -> None:
        self.driver.populate_context(context)
