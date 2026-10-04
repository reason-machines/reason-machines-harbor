"""Harbor Docker environment with platform-aligned egress sidecars."""
import os

from harbor.environments.docker.docker import DockerEnvironment
from harbor.environments.docker.utils import default_docker_platform, ensure_docker_image_built


class ReasonDockerEnvironment(DockerEnvironment):
    """Build Harbor's egress sidecar for the task container platform."""

    async def _ensure_egress_control_sidecar_image_built(self) -> None:
        platform = os.environ.get("DOCKER_DEFAULT_PLATFORM") or await default_docker_platform()
        self._env_vars.egress_control_sidecar_image_name = (
            await ensure_docker_image_built(
                docker_name=self._EGRESS_CONTROL_SIDECAR_DOCKER_NAME,
                docker_build_context=self._EGRESS_CONTROL_SIDECAR_CONTEXT_PATH,
                dockerfile_path=self._egress_control_sidecar_dockerfile_path(),
                build_args={},
                platform=platform,
                logger=self.logger,
            )
        )
