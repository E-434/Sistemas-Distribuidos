"""Descoberta do front-end eleito no canal externo UDP."""

from __future__ import annotations

import ipaddress
import socket
from typing import Final

from protocolo import (
    TYPE_FRONTEND_INFO,
    decode_message,
    make_discover_frontend,
    make_frontend_info,
)

BUFFER_SIZE: Final[int] = 4096
DISCOVERY_TIMEOUT_SECONDS: Final[float] = 0.150
DISCOVERY_RETRIES: Final[int] = 3


def get_local_ip(peer_ip: str = "8.8.8.8") -> str:
    """Retorna o IP local da interface de saída, com fallback para 127.0.0.1."""
    if peer_ip in ("127.0.0.1", "localhost", "0.0.0.0"):
        peer_ip = "8.8.8.8"
    temp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        temp.connect((peer_ip, 1))
        ip = temp.getsockname()[0]
        if ip and not ip.startswith("0."):
            return ip
    except OSError:
        pass
    finally:
        temp.close()
    return "127.0.0.1"


def discovery_targets() -> tuple[str, ...]:
    """Monta endereços de descoberta (loopback, broadcast global e /24 local)."""
    targets: list[str] = ["127.0.0.1", "255.255.255.255", "127.255.255.255"]
    local_ip = get_local_ip()
    if local_ip not in targets:
        targets.append(local_ip)
    try:
        addr = ipaddress.ip_address(local_ip)
        if addr.version == 4 and not addr.is_loopback:
            net = ipaddress.ip_network(f"{local_ip}/24", strict=False)
            bcast = str(net.broadcast_address)
            if bcast not in targets:
                targets.append(bcast)
    except ValueError:
        pass
    return tuple(targets)


def discover_frontend(
    sock: socket.socket,
    port: int,
    client_id: str,
    exclude_frontend_id: str | None = None,
) -> tuple[tuple[str, int], str]:
    """Localiza o front-end que responde à descoberta UDP."""
    sock.settimeout(DISCOVERY_TIMEOUT_SECONDS)
    discovery = make_discover_frontend(client_id)
    targets = discovery_targets()

    for _ in range(DISCOVERY_RETRIES):
        for target_ip in targets:
            try:
                sock.sendto(discovery, (target_ip, port))
            except OSError:
                continue

        while True:
            try:
                data, sender = sock.recvfrom(BUFFER_SIZE)
            except ConnectionResetError:
                continue
            except socket.timeout:
                break
            except OSError:
                break

            message = decode_message(data)
            if message is None or message.get("type") != TYPE_FRONTEND_INFO:
                continue

            frontend_ip = message.get("frontend_addr") or sender[0]
            frontend_id = message.get("frontend_id")
            if not isinstance(frontend_ip, str):
                continue
            if not isinstance(frontend_id, str) or not frontend_id:
                continue
            if exclude_frontend_id and frontend_id == exclude_frontend_id:
                continue
            if sender[0] == "127.0.0.1":
                frontend_ip = "127.0.0.1"
            return (frontend_ip, port), frontend_id

    raise RuntimeError("nenhum front-end encontrado via descoberta UDP")


def respond_to_discovery(
    sock: socket.socket,
    sender: tuple[str, int],
    frontend_id: str,
    advertised_ip: str | None = None,
) -> None:
    """Responde unicast à descoberta com a identidade/endereço do front-end."""
    client_ip, client_port = sender
    if client_ip == "127.0.0.1":
        frontend_ip = "127.0.0.1"
    elif advertised_ip:
        frontend_ip = advertised_ip
    else:
        frontend_ip = get_local_ip(client_ip)

    sock.sendto(
        make_frontend_info(frontend_id, frontend_ip),
        (client_ip, client_port),
    )