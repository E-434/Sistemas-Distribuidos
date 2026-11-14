"""Formatação das mensagens exibidas pelos clientes e pelo servidor."""

from datetime import datetime

from processamento import ProcessingEvent
from protocolo import STATUS_DUPLICATE, STATUS_OUT_OF_ORDER, STATUS_PROCESSED


def now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def client_server_addr(server_ip: str, frontend_id: str) -> str:
    return f"{now()} frontend_addr {server_ip} frontend_id {frontend_id}"


def invalid_value(value: object) -> str:
    return f"{now()} invalid_value {value}"


def request_sent(server_ip: str, req_id: int, value: int) -> str:
    return f"{now()} send frontend {server_ip} request_id {req_id} value {value}"


def request_resent(req_id: int, value: int) -> str:
    return f"{now()} resend request_id {req_id} value {value}"


def ack_received(
    server_ip: str,
    req_id: int,
    value: int,
    seq_id: int,
    num_reqs: int,
    total_sum: int,
) -> str:
    return (
        f"{now()} frontend {server_ip} request_id {req_id} seq_id {seq_id} value {value} "
        f"num_reqs {num_reqs} total_sum {total_sum}"
    )


def server_started(num_reqs: int, total_sum: int, frontend_id: str) -> str:
    return (
        f"{now()} role frontend frontend_id {frontend_id} "
        f"num_reqs {num_reqs} total_sum {total_sum}"
    )


def server_processing_event(event: ProcessingEvent) -> str:
    if event.kind == STATUS_PROCESSED:
        return (
            f"{now()} client {event.client_ip} request_id {event.req_id} "
            f"seq_id {event.seq_id} value {event.value} num_reqs {event.num_reqs} "
            f"total_sum {event.total_sum}"
        )
    if event.kind == STATUS_DUPLICATE:
        return (
            f"{now()} client {event.client_ip} DUP!! request_id {event.req_id} "
            f"seq_id {event.seq_id} value {event.value} num_reqs {event.num_reqs} "
            f"total_sum {event.total_sum}"
        )
    if event.kind == STATUS_OUT_OF_ORDER:
        return (
            f"{now()} client {event.client_ip} OUT-OF-ORDER!! "
            f"request_id {event.req_id} seq_id {event.seq_id} value {event.value} "
            f"expected {event.expected} num_reqs {event.num_reqs} "
            f"total_sum {event.total_sum}"
        )
    raise ValueError(f"tipo de evento desconhecido: {event.kind}")