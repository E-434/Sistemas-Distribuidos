"""Lógica de envio e processamento de requisições do serviço de soma."""

from __future__ import annotations

import ctypes
import socket
import threading
from dataclasses import dataclass
from typing import Final

from protocolo import (
    STATUS_DUPLICATE,
    STATUS_OUT_OF_ORDER,
    STATUS_PROCESSED,
    TYPE_ACK,
    decode_message,
    make_ack,
    make_request,
)

UINT64_MAX: Final[int] = (1 << 64) - 1
BUFFER_SIZE: Final[int] = 4096


@dataclass
class ClientState:
    """Estado da última requisição processada de um cliente."""

    last_req: int = 0
    last_num_reqs: int = 0
    last_total_sum: int = 0
    last_value: int = 0


@dataclass(frozen=True)
class ProcessingEvent:
    """Dados necessários para a interface relatar o resultado do servidor."""

    kind: str
    client_ip: str
    req_id: int
    value: int
    num_reqs: int
    total_sum: int
    expected: int = 0
    last_req: int = 0


class RequestProcessor:
    """Mantém os acumuladores e decide como responder às requisições."""

    def __init__(self) -> None:
        self.num_reqs = ctypes.c_uint64(0)
        self.total_sum = ctypes.c_uint64(0)
        self.clients: dict[str, ClientState] = {}

    def register_client(self, address: str) -> ClientState:
        state = self.clients.get(address)
        if state is None:
            state = ClientState()
            self.clients[address] = state
        return state

    @staticmethod
    def validate_uint64(value: int, field_name: str) -> None:
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError(f"{field_name} deve ser inteiro")
        if value < 0 or value > UINT64_MAX:
            raise ValueError(f"{field_name} fora do intervalo uint64")

    def process_request(
        self,
        message: dict,
        sender: tuple[str, int],
    ) -> tuple[bytes | None, ProcessingEvent | None]:
        client_ip, _ = sender
        state = self.register_client(client_ip)
        req_id = message.get("id_req")
        value = message.get("value")

        try:
            self.validate_uint64(req_id, "id_req")
            self.validate_uint64(value, "value")
        except ValueError:
            return None, None

        expected = state.last_req + 1
        if req_id == expected:
            if int(self.total_sum.value) > UINT64_MAX - value:
                return None, None

            self.total_sum.value = int(self.total_sum.value) + value
            self.num_reqs.value = int(self.num_reqs.value) + 1
            state.last_req = req_id
            state.last_num_reqs = int(self.num_reqs.value)
            state.last_total_sum = int(self.total_sum.value)
            state.last_value = value

            response = make_ack(
                req_id=state.last_req,
                num_reqs=state.last_num_reqs,
                total_sum=state.last_total_sum,
                status=STATUS_PROCESSED,
            )
            event = ProcessingEvent(
                STATUS_PROCESSED,
                client_ip,
                req_id,
                value,
                state.last_num_reqs,
                state.last_total_sum,
            )
            return response, event

        if req_id <= state.last_req:
            response = make_ack(
                req_id=state.last_req,
                num_reqs=state.last_num_reqs,
                total_sum=state.last_total_sum,
                status=STATUS_DUPLICATE,
            )
            event = ProcessingEvent(
                STATUS_DUPLICATE,
                client_ip,
                req_id,
                value,
                state.last_num_reqs,
                state.last_total_sum,
                last_req=state.last_req,
            )
            return response, event

        response = make_ack(
            req_id=state.last_req,
            num_reqs=state.last_num_reqs,
            total_sum=state.last_total_sum,
            status=STATUS_OUT_OF_ORDER,
        )
        event = ProcessingEvent(
            STATUS_OUT_OF_ORDER,
            client_ip,
            req_id,
            value,
            state.last_num_reqs,
            state.last_total_sum,
            expected=expected,
            last_req=state.last_req,
        )
        return response, event


def send_request(
    sock: socket.socket,
    server_addr: tuple[str, int],
    req_id: int,
    value: int,
) -> None:
    sock.sendto(make_request(req_id=req_id, value=value), server_addr)


def wait_for_ack(
    sock: socket.socket,
    server_addr: tuple[str, int],
    req_id: int,
    stop_event: threading.Event,
) -> dict | None:
    """Aguarda o ACK da requisição atual, descartando respostas incompatíveis."""
    while not stop_event.is_set():
        try:
            data, sender = sock.recvfrom(BUFFER_SIZE)
        except socket.timeout:
            return None
        except OSError:
            if stop_event.is_set():
                return None
            raise

        if sender[0] != server_addr[0]:
            continue

        message = decode_message(data)
        if message is None or message.get("type") != TYPE_ACK:
            continue

        ack_id = message.get("id_req")
        num_reqs = message.get("num_reqs")
        total_sum = message.get("total_sum")
        if not isinstance(ack_id, int):
            continue
        if not isinstance(num_reqs, int):
            continue
        if not isinstance(total_sum, int):
            continue
        if ack_id != req_id:
            continue

        return message

    return None