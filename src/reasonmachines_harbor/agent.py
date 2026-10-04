"""Harbor wiring for the Reason benchmark driver."""
from __future__ import annotations

from urllib.parse import urlparse

from harbor.agents.base import BaseAgent
from harbor.environments.base import BaseEnvironment
from harbor.models.agent.context import AgentContext
from ._driver import DEFAULT_RUNTIME_CONFIG, ReasonDriver
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
                 mode: str = "isolated",
                 runtime_profile: str | None = None,
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
            runtime_profile=runtime_profile, ephemeral_rcus=ephemeral_rcus,
            mcp_servers=self.mcp_servers, skills_dir=self.skills_dir,
            extra_env=self.extra_env, load_trajectory=self.load_trajectory,
        )

    @staticmethod
    def name() -> str:
        return "reason"

    def version(self) -> str:
        return "0.3.1"

    async def setup(self, environment: BaseEnvironment) -> None:
        mode = getattr(getattr(environment, "network_policy", None), "network_mode", "")
        mode = str(getattr(mode, "value", mode))
        if mode.endswith("no_network"):
            raise ValueError("connected worker requires Reason egress; offline tasks are not supported")
        driver = self.driver
        api_host = urlparse(driver.worker_api_origin).hostname
        # Local qualification must remain independent of public control-plane DNS.
        if api_host not in {"localhost", "127.0.0.1", "::1"}:
            if driver.channel == "pr":
                relay_host = urlparse(driver.pr_relay_origin).hostname if driver.pr_relay_origin else None
            else:
                relay_host = f"reason-central-relay-{driver.channel}.up.railway.app"
            try:
                await prepare_control_plane_dns(environment, [host for host in (api_host, relay_host) if host])
            except TimeoutError:
                raise ReasonWorkerStartupError("Reason worker setup timed out") from None
        image_env, workdir = await resolve_image_runtime(environment, self.logger)
        await driver.setup(environment, image_env=image_env, workdir=workdir)

    async def run(self, instruction: str, environment: BaseEnvironment, context: AgentContext) -> None:
        await self.driver.run(instruction, environment, context)

    def populate_context_post_run(self, context: AgentContext) -> None:
        self.driver.populate_context(context)
