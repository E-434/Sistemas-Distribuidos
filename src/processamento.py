#!/usr/bin/env python3
"""Sequenciamento global e estado local temporário do front-end."""

from __future__ import annotations

import socket
import threading
from dataclasses import dataclass
from typing import Final, Any

from protocolo import (
    STATUS_DUPLICATE,
    STATUS_OUT_OF_ORDER,
    STATUS_PROCESSED,
    TYPE_SUM_RESULT,
    decode_message,
    make_sum_request,
    make_sum_result,
)

UINT64_MAX: Final[int] = (1 << 64) - 1
BUFFER_SIZE: Final[int] = 4096


@dataclass
class ClientState:
    """Controle de idempotência e ordem por cliente no front-end."""

    last_request_id: int = 0
    last_seq_id: int = 0
    last_num_reqs: int = 0
    last_total_sum: int = 0


@dataclass(frozen=True)
class ProcessingEvent:
    """Informações para o log do processamento no front-end."""

    kind: str
    client_ip: str
    req_id: int
    value: int
    seq_id: int
    num_reqs: int
    total_sum: int
    expected: int = 0


class RequestProcessor:
    """Sequenciador global para a fase inicial sem replicação ativa.

    O estado local mantém o serviço utilizável enquanto os Replica Managers
    não são conectados. `seq_id` é o contrato de ordenação que será disseminado
    pelo front-end quando o transporte intra-cluster for implementado.
    """

    def __init__(self) -> None:
        self.num_reqs = 0
        self.total_sum = 0
        self.next_seq_id = 1
        self.clients: dict[str, ClientState] = {}

    @staticmethod
    def validate_uint64(value: Any, field_name: str) -> None:
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError(f"{field_name} deve ser inteiro")
        if value < 0 or value > UINT64_MAX:
            raise ValueError(f"{field_name} fora do intervalo uint64")

    def process_request(
        self,
        message: dict[str, Any],
        sender: tuple[str, int],
    ) -> tuple[bytes | None, ProcessingEvent | None]:
        client_id = message.get("client_id")
        request_id = message.get("request_id")
        value = message.get("value")
        if not isinstance(client_id, str) or not client_id or len(client_id) > 128:
            return None, None

        try:
            self.validate_uint64(request_id, "request_id")
            self.validate_uint64(value, "value")
        except ValueError:
            return None, None
        if request_id == 0:
            return None, None

        client = self.clients.setdefault(client_id, ClientState())
        expected = client.last_request_id + 1
        if request_id <= client.last_request_id:
            response = make_sum_result(
                client_id,
                request_id,
                client.last_seq_id,
                client.last_num_reqs,
                client.last_total_sum,
                STATUS_DUPLICATE,
            )
            return response, ProcessingEvent(
                STATUS_DUPLICATE,
                sender[0],
                request_id,
                value,
                client.last_seq_id,
                client.last_num_reqs,
                client.last_total_sum,
            )
        if request_id != expected:
            response = make_sum_result(
                client_id,
                request_id,
                self.next_seq_id - 1,
                self.num_reqs,
                self.total_sum,
                STATUS_OUT_OF_ORDER,
            )
            return response, ProcessingEvent(
                STATUS_OUT_OF_ORDER,
                sender[0],
                request_id,
                value,
                self.next_seq_id - 1,
                self.num_reqs,
                self.total_sum,
                expected=expected,
            )

        if self.next_seq_id > UINT64_MAX or self.num_reqs == UINT64_MAX:
            return None, None
        if self.total_sum > UINT64_MAX - value:
            return None, None

        seq_id = self.next_seq_id
        self.next_seq_id += 1
        self.num_reqs += 1
        self.total_sum += value
        client.last_request_id = request_id
        client.last_seq_id = seq_id
        client.last_num_reqs = self.num_reqs
        client.last_total_sum = self.total_sum

        response = make_sum_result(
            client_id,
            request_id,
            seq_id,
            self.num_reqs,
            self.total_sum,
            STATUS_PROCESSED,
        )
        event = ProcessingEvent(
            STATUS_PROCESSED,
            sender[0],
            request_id,
            value,
            seq_id,
            self.num_reqs,
            self.total_sum,
        )
        return response, event


def send_request(
    sock: socket.socket,
    frontend_addr: tuple[str, int],
    client_id: str,
    request_id: int,
    value: int,
) -> None:
    sock.sendto(
        make_sum_request(client_id, request_id, value),
        frontend_addr,
    )


def wait_for_ack(
    sock: socket.socket,
    frontend_addr: tuple[str, int],
    client_id: str,
    request_id: int,
    stop_event: threading.Event,
) -> dict[str, Any] | None:
    """Aguarda a resposta correlacionada do front-end."""
    while not stop_event.is_set():
        try:
            data, sender = sock.recvfrom(BUFFER_SIZE)
        except ConnectionResetError:
            continue
        except socket.timeout:
            return None
        except OSError:
            if stop_event.is_set():
                return None
            raise

        loopback_ips = {"127.0.0.1", "0.0.0.0", "localhost"}
        if sender[0] != frontend_addr[0] and not (
            sender[0] in loopback_ips and frontend_addr[0] in loopback_ips
        ):
            continue

        message = decode_message(data)
        if message is None or message.get("type") != TYPE_SUM_RESULT:
            continue
        if message.get("client_id") != client_id:
            continue
        if message.get("request_id") != request_id:
            continue
        if not isinstance(message.get("seq_id"), int):
            continue
        if not isinstance(message.get("num_reqs"), int):
            continue
        if not isinstance(message.get("total_sum"), int):
            continue
        return message
    return None
