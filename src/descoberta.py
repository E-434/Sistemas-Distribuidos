"""Descoberta do front-end eleito no canal externo UDP."""

from __future__ import annotations

import socket
from typing import Final

from protocolo import (
    TYPE_FRONTEND_INFO,
    decode_message,
    make_discover_frontend,
    make_frontend_info,
)

BUFFER_SIZE: Final[int] = 4096
BROADCAST_ADDRESSES: Final[tuple[str, ...]] = (
    "255.255.255.255",
    "127.255.255.255",
)
DISCOVERY_TIMEOUT_SECONDS: Final[float] = 0.500
DISCOVERY_RETRIES: Final[int] = 5


def discover_frontend(
    sock: socket.socket,
    port: int,
    client_id: str,
) -> tuple[tuple[str, int], str]:
    """Localiza o front-end que responde à descoberta UDP."""
    sock.settimeout(DISCOVERY_TIMEOUT_SECONDS)
    discovery = make_discover_frontend(client_id)

    for _ in range(DISCOVERY_RETRIES):
        for broadcast_ip in BROADCAST_ADDRESSES:
            try:
                sock.sendto(discovery, (broadcast_ip, port))
            except OSError:
                continue

        while True:
            try:
                data, sender = sock.recvfrom(BUFFER_SIZE)
            except socket.timeout:
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
            if sender[0] == "127.0.0.1":
                frontend_ip = "127.0.0.1"
            return (frontend_ip, port), frontend_id

    raise RuntimeError("nenhum front-end encontrado via broadcast")


def respond_to_discovery(
    sock: socket.socket,
    sender: tuple[str, int],
    frontend_id: str,
) -> None:
    """Responde unicast à descoberta com a identidade/endereço do front-end."""
    client_ip, client_port = sender
    if client_ip == "127.0.0.1":
        frontend_ip = "127.0.0.1"
    else:
        temp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            temp.connect((client_ip, 1))
            frontend_ip = temp.getsockname()[0]
        finally:
            temp.close()

    sock.sendto(
        make_frontend_info(frontend_id, frontend_ip),
        (client_ip, client_port),
    )
