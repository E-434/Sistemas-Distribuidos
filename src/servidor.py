#!/usr/bin/env python3
"""Servidor do serviço distribuído de soma sobre UDP."""

from __future__ import annotations

import socket
import sys
from typing import Final

from descoberta import respond_to_discovery
from interface import server_processing_event, server_started
from processamento import RequestProcessor
from protocolo import (
    TYPE_DISCOVER,
    TYPE_REQUEST,
    decode_message,
)

BUFFER_SIZE: Final[int] = 4096


class AggregationServer:
    def __init__(self, port: int) -> None:
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock.bind(("", port))

        self.processor = RequestProcessor()

    def process_discovery(self, sender: tuple[str, int]) -> None:
        # Usa IP:Porta para diferençar clientes rodando no mesmo IP (VM / Loopback)
        client_key = f"{sender[0]}:{sender[1]}"
        self.processor.register_client(client_key)
        respond_to_discovery(self.sock, sender)

    def process_request(self, message: dict, sender: tuple[str, int]) -> None:
        response, event = self.processor.process_request(message, sender)
        if event is not None:
            print(server_processing_event(event), flush=True)
        if response is not None:
            self.sock.sendto(response, sender)

    def run(self) -> None:
        print(
            server_started(
                self.processor.num_reqs.value,
                self.processor.total_sum.value,
            ),
            flush=True,
        )

        try:
            while True:
                data, sender = self.sock.recvfrom(BUFFER_SIZE)
                message = decode_message(data)
                if message is None:
                    continue

                message_type = message.get("type")
                if message_type == TYPE_DISCOVER:
                    self.process_discovery(sender)
                elif message_type == TYPE_REQUEST:
                    self.process_request(message, sender)
        except KeyboardInterrupt:
            pass
        finally:
            self.sock.close()


def parse_port(argv: list[str]) -> int:
    if len(argv) != 2:
        print(f"Uso: python3 {argv[0]} <porta_udp>", file=sys.stderr)
        raise SystemExit(2)

    try:
        port = int(argv[1])
    except ValueError:
        print("A porta deve ser um inteiro.", file=sys.stderr)
        raise SystemExit(2)

    if not 1 <= port <= 65535:
        print("A porta deve estar entre 1 e 65535.", file=sys.stderr)
        raise SystemExit(2)

    return port


def main() -> None:
    port = parse_port(sys.argv)
    server = AggregationServer(port)
    server.run()


if __name__ == "__main__":
    main()
