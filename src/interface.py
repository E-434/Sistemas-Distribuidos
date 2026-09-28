"""Formatação das mensagens exibidas pelos clientes e pelo servidor."""

from datetime import datetime

from processamento import ProcessingEvent
from protocolo import STATUS_DUPLICATE, STATUS_OUT_OF_ORDER, STATUS_PROCESSED


def now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def client_server_addr(server_ip: str) -> str:
    return f"{now()} server_addr {server_ip}"


def invalid_value(value: object) -> str:
    return f"{now()} invalid_value {value}"


def request_sent(server_ip: str, req_id: int, value: int) -> str:
    return f"{now()} send server {server_ip} id_req {req_id} value {value}"


def request_resent(req_id: int, value: int) -> str:
    return f"{now()} resend id_req {req_id} value {value}"


def ack_received(
    server_ip: str,
    req_id: int,
    value: int,
    num_reqs: int,
    total_sum: int,
) -> str:
    return (
        f"{now()} server {server_ip} id_req {req_id} value {value} "
        f"num_reqs {num_reqs} total_sum {total_sum}"
    )


def server_started(num_reqs: int, total_sum: int) -> str:
    return f"{now()} num_reqs {num_reqs} total_sum {total_sum}"


def server_processing_event(event: ProcessingEvent) -> str:
    if event.kind == STATUS_PROCESSED:
        return (
            f"{now()} client {event.client_ip} id_req {event.req_id} "
            f"value {event.value} num_reqs {event.num_reqs} "
            f"total_sum {event.total_sum}"
        )
    if event.kind == STATUS_DUPLICATE:
        return (
            f"{now()} client {event.client_ip} DUP!! id_req {event.req_id} "
            f"value {event.value} num_reqs {event.num_reqs} "
            f"total_sum {event.total_sum}"
        )
    if event.kind == STATUS_OUT_OF_ORDER:
        return (
            f"{now()} client {event.client_ip} OUT-OF-ORDER!! "
            f"id_req {event.req_id} value {event.value} expected {event.expected} "
            f"last_req {event.last_req} num_reqs {event.num_reqs} "
            f"total_sum {event.total_sum}"
        )
    raise ValueError(f"tipo de evento desconhecido: {event.kind}")