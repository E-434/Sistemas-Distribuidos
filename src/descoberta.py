"""Rotinas do subserviço de descoberta via broadcast UDP."""

import socket
from typing import Final

from protocolo import (
    TYPE_DISCOVER_ACK,
    decode_message,
    make_discover,
    make_discover_ack,
)

BUFFER_SIZE: Final[int] = 4096
BROADCAST_ADDRESS: Final[str] = "255.255.255.255"
DISCOVERY_TIMEOUT_SECONDS: Final[float] = 0.500
DISCOVERY_RETRIES: Final[int] = 5


def discover_server(sock: socket.socket, port: int) -> tuple[str, int]:
    """Envia descoberta por broadcast e retorna o endereço do servidor."""
    sock.settimeout(DISCOVERY_TIMEOUT_SECONDS)
    discovery = make_discover()

    for _ in range(DISCOVERY_RETRIES):
        try:
            sock.sendto(discovery, (BROADCAST_ADDRESS, port))
        except OSError as exc:
            raise RuntimeError(f"falha ao enviar descoberta: {exc}") from exc

        while True:
            try:
                data, sender = sock.recvfrom(BUFFER_SIZE)
            except socket.timeout:
                break

            message = decode_message(data)
            if message is None or message.get("type") != TYPE_DISCOVER_ACK:
                continue

            return sender[0], port

    raise RuntimeError("nenhum servidor encontrado via broadcast")


def respond_to_discovery(
    sock: socket.socket,
    sender: tuple[str, int],
) -> None:
    """Responde unicast à descoberta usando o endereço local da interface."""
    client_ip, client_port = sender
    temp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        temp.connect((client_ip, 1))
        server_ip = temp.getsockname()[0]
    except OSError:
        server_ip = "0.0.0.0"
    finally:
        temp.close()

    sock.sendto(make_discover_ack(server_ip), (client_ip, client_port))