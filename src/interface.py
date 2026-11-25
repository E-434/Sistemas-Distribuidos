"""Formatação padronizada das mensagens exibidas pelos clientes, FE e RMs."""

from datetime import datetime

from processamento import ProcessingEvent
from protocolo import STATUS_DUPLICATE, STATUS_OUT_OF_ORDER, STATUS_PROCESSED


def now() -> str:
    return datetime.now().strftime("%H:%M:%S")


def format_fe_log(message: str) -> str:
    return f"[FE] {now()} {message}"


def format_rm_log(rm_id: str, message: str) -> str:
    return f"[RM {rm_id}] {now()} {message}"


def format_client_log(client_endpoint: str, message: str) -> str:
    return f"[Cliente {client_endpoint}] {now()} {message}"


# --- Logs do Cliente ---

def client_discovery_attempt(
    client_endpoint: str,
    attempt: int,
    max_attempts: int,
    port: int,
) -> str:
    return format_client_log(
        client_endpoint,
        f"discovery_attempt attempt {attempt}/{max_attempts} port {port}",
    )


def client_discovery_result(
    client_endpoint: str,
    attempt: int,
    status: str,
    elapsed: float,
    frontend_id: str = "",
    frontend_addr: str = "",
) -> str:
    base = f"discovery_result attempt {attempt} status {status}"
    if status == "found":
        base += f" frontend_id {frontend_id} frontend_addr {frontend_addr}"
    return format_client_log(client_endpoint, f"{base} elapsed {elapsed:.3f}s")


def client_server_addr(
    client_endpoint: str,
    server_addr: str,
    frontend_id: str,
) -> str:
    return format_client_log(
        client_endpoint,
        f"frontend_connected frontend_addr {server_addr} frontend_id {frontend_id}",
    )


def invalid_value(client_endpoint: str, value: object) -> str:
    return format_client_log(client_endpoint, f"invalid_value {value}")


def request_sent(
    client_endpoint: str,
    server_addr: str,
    req_id: int,
    value: int,
) -> str:
    return format_client_log(
        client_endpoint,
        f"send frontend {server_addr} request_id {req_id} value {value}",
    )


def request_resent(
    client_endpoint: str,
    server_addr: str,
    req_id: int,
    value: int,
) -> str:
    return format_client_log(
        client_endpoint,
        f"resend frontend {server_addr} request_id {req_id} value {value}",
    )


def ack_received(
    client_endpoint: str,
    server_addr: str,
    req_id: int,
    value: int,
    seq_id: int,
    num_reqs: int,
    total_sum: int,
) -> str:
    return format_client_log(
        client_endpoint,
        f"ack_received frontend {server_addr} request_id {req_id} "
        f"seq_id {seq_id} value {value} num_reqs {num_reqs} total_sum {total_sum}",
    )


def client_frontend_error(
    client_endpoint: str,
    req_id: int,
    status: object,
) -> str:
    return format_client_log(
        client_endpoint,
        f"frontend_error request_id {req_id} status {status}",
    )


# --- Logs do Front-End (FE) ---

def server_started(num_reqs: int, total_sum: int, frontend_id: str) -> str:
    return format_fe_log(
        f"started frontend_id {frontend_id} "
        f"num_reqs {num_reqs} total_sum {total_sum}"
    )


def server_processing_event(event: ProcessingEvent) -> str:
    if event.kind == STATUS_PROCESSED:
        return format_fe_log(
            f"client {event.client_ip} request_id {event.req_id} "
            f"seq_id {event.seq_id} value {event.value} num_reqs {event.num_reqs} "
            f"total_sum {event.total_sum}"
        )
    if event.kind == STATUS_DUPLICATE:
        return format_fe_log(
            f"client {event.client_ip} DUP!! request_id {event.req_id} "
            f"seq_id {event.seq_id} value {event.value} num_reqs {event.num_reqs} "
            f"total_sum {event.total_sum}"
        )
    if event.kind == STATUS_OUT_OF_ORDER:
        return format_fe_log(
            f"client {event.client_ip} OUT-OF-ORDER!! "
            f"request_id {event.req_id} seq_id {event.seq_id} value {event.value} "
            f"expected {event.expected} num_reqs {event.num_reqs} "
            f"total_sum {event.total_sum}"
        )
    raise ValueError(f"tipo de evento desconhecido: {event.kind}")