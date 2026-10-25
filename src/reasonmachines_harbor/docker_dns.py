"""Resolve control-plane names outside Harbor's UDP-blocking Docker sidecar."""
import asyncio
import ipaddress
import shlex
import socket

from harbor.environments.docker.docker import DockerEnvironment


async def prepare_control_plane_dns(environment, hosts):
    if not isinstance(environment, DockerEnvironment) or not hosts:
        return
    if not environment.capabilities.dynamic_network_policy:
        return
    mappings = []
    for host in sorted(set(hosts)):
        try:
            answers = await asyncio.wait_for(asyncio.to_thread(
                socket.getaddrinfo, host, 443, socket.AF_INET, socket.SOCK_STREAM,
            ), timeout=10)
            addresses = sorted({str(ipaddress.ip_address(answer[4][0])) for answer in answers})
            if not addresses or any(not ipaddress.ip_address(ip).is_global for ip in addresses):
                raise ValueError('non-public address')
        except (OSError, ValueError, TimeoutError):
            raise RuntimeError('Could not resolve a public Reason control-plane address') from None
        mappings.extend(f'{ip} {host}\n' for ip in addresses)
    command = 'printf %s ' + shlex.quote(''.join(mappings)) + ' >> /etc/hosts'
    # Harbor 0.22's transparent proxy shares the network namespace with main.
    # Its restricted policy blocks UDP DNS, including the proxy's own lookups.
    # Both files need mappings; this changes resolution, never the egress policy.
    result = await environment.service_exec(command,
        service='harbor-docker-egress-control-sidecar', user='root', timeout_sec=10)
    if result.return_code:
        raise RuntimeError('Could not prepare Harbor sidecar control-plane DNS')
    result = await environment.exec(command, user='root', timeout_sec=10)
    if result.return_code:
        raise RuntimeError('Could not prepare Harbor guest control-plane DNS')
