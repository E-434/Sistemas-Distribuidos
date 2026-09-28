#!/usr/bin/env python3
"""Servidor do serviço distribuído de soma sobre UDP."""

from __future__ import annotations

import ctypes
import socket
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Final

from protocolo import (
    STATUS_DUPLICATE,
    STATUS_OUT_OF_ORDER,
    STATUS_PROCESSED,
    TYPE_DISCOVER,
    TYPE_REQUEST,
    decode_message,
    make_ack,
    make_discover_ack,
)

UINT64_MAX: Final[int] = (1 << 64) - 1
BUFFER_SIZE: Final[int] = 4096


@dataclass
class ClientState:
    """Estado da última requisição processada de um cliente."""

    last_req: int = 0
    # Snapshot do estado agregado no momento da última requisição deste cliente.
    last_num_reqs: int = 0
    last_total_sum: int = 0
    # Guardado apenas para a interface de diagnóstico de duplicatas.
    last_value: int = 0


class AggregationServer:
    def __init__(self, port: int) -> None:
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock.bind(("", port))

        # Python não possui um inteiro unsigned de largura fixa na linguagem.
        # ctypes.c_uint64 torna explícito o uso de unsigned int de 64 bits.
        self.num_reqs = ctypes.c_uint64(0)
        self.total_sum = ctypes.c_uint64(0)

        # Os totais são globais; a sequência de ids é mantida por endereço IP.
        self.clients: dict[str, ClientState] = {}

    @staticmethod
    def now() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def validate_uint64(value: int, field_name: str) -> None:
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError(f"{field_name} deve ser inteiro")
        if value < 0 or value > UINT64_MAX:
            raise ValueError(f"{field_name} fora do intervalo uint64")

    def register_client(self, address: str) -> ClientState:
        state = self.clients.get(address)
        if state is None:
            state = ClientState()
            self.clients[address] = state
        return state

    def _server_ip_for_client(self, client_ip: str) -> str:
        """
        Descobre o endereço local da interface que seria usada para alcançar
        o cliente. Não há envio de dados nesse socket auxiliar.
        """
        temp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            temp.connect((client_ip, 1))
            return temp.getsockname()[0]
        except OSError:
            # Em caso de falha, usar o endereço de origem do socket principal
            # não é possível quando ele está ligado a 0.0.0.0; neste cenário
            # o cliente ainda consegue usar o IP de origem do pacote UDP.
            return "0.0.0.0"
        finally:
            temp.close()

    def process_discovery(self, sender: tuple[str, int]) -> None:
        client_ip, client_port = sender
        self.register_client(client_ip)

        server_ip = self._server_ip_for_client(client_ip)
        response = make_discover_ack(server_ip)
        self.sock.sendto(response, (client_ip, client_port))

    def process_request(self, message: dict, sender: tuple[str, int]) -> None:
        client_ip, client_port = sender
        state = self.register_client(client_ip)

        req_id = message.get("id_req")
        value = message.get("value")

        try:
            self.validate_uint64(req_id, "id_req")
            self.validate_uint64(value, "value")
        except ValueError:
            return

        # O primeiro id esperado por cliente é 1.
        expected = state.last_req + 1

        if req_id == expected:
            # Uma requisição nova atualiza os totais e o snapshot do cliente.
            # Garante que a soma agregada caiba em uint64.
            if int(self.total_sum.value) > UINT64_MAX - value:
                # A especificação não define um tratamento para overflow.
                # Neste caso, descartamos a requisição em vez de produzir um
                # resultado inconsistente.
                return

            self.total_sum.value = int(self.total_sum.value) + value
            self.num_reqs.value = int(self.num_reqs.value) + 1

            state.last_req = req_id
            state.last_num_reqs = int(self.num_reqs.value)
            state.last_total_sum = int(self.total_sum.value)
            state.last_value = value

            print(
                f"{self.now()} client {client_ip} id_req {req_id} value {value} "
                f"num_reqs {self.num_reqs.value} total_sum {self.total_sum.value}",
                flush=True,
            )

            response = make_ack(
                req_id=state.last_req,
                num_reqs=state.last_num_reqs,
                total_sum=state.last_total_sum,
                status=STATUS_PROCESSED,
            )
            self.sock.sendto(response, (client_ip, client_port))
            return

        if req_id <= state.last_req:
            # Duplicata: não alterar o acumulador. Reenvia exatamente o
            # snapshot salvo no último processamento desse cliente.
            print(
                f"{self.now()} client {client_ip} DUP!! id_req {req_id} value {value} "
                f"num_reqs {state.last_num_reqs} total_sum {state.last_total_sum}",
                flush=True,
            )

            response = make_ack(
                req_id=state.last_req,
                num_reqs=state.last_num_reqs,
                total_sum=state.last_total_sum,
                status=STATUS_DUPLICATE,
            )
            self.sock.sendto(response, (client_ip, client_port))
            return

        # req_id > expected: alguma requisição anterior não chegou.
        print(
            f"{self.now()} client {client_ip} OUT-OF-ORDER!! id_req {req_id} value {value} "
            f"expected {expected} last_req {state.last_req} "
            f"num_reqs {state.last_num_reqs} total_sum {state.last_total_sum}",
            flush=True,
        )

        response = make_ack(
            req_id=state.last_req,
            num_reqs=state.last_num_reqs,
            total_sum=state.last_total_sum,
            status=STATUS_OUT_OF_ORDER,
        )
        self.sock.sendto(response, (client_ip, client_port))

    def run(self) -> None:
        print(
            f"{self.now()} num_reqs {self.num_reqs.value} total_sum {self.total_sum.value}",
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

