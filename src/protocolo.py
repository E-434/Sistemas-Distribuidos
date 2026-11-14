#!/usr/bin/env python3
"""Protocolo UDP cliente/front-end e contratos reservados para o cluster."""

from __future__ import annotations

import json
from typing import Any

PROTOCOL_VERSION = 2
MAX_DATAGRAM_SIZE = 4096

# Canal cliente-front-end
TYPE_DISCOVER_FRONTEND = "DISCOVER_FRONTEND"
TYPE_FRONTEND_INFO = "FRONTEND_INFO"
TYPE_SUM_REQUEST = "SUM_REQUEST"
TYPE_SUM_RESULT = "SUM_RESULT"

# O registro RM↔FE é funcional. Os demais tipos são reservados para o canal de
# dados e controle futuro: ordenação replicada, ACKs, heartbeat e eleição.
TYPE_ORDERED_REQUEST = "ORDERED_REQUEST"
TYPE_REPLICA_ACK = "REPLICA_ACK"
TYPE_REPLICA_REGISTER = "REPLICA_REGISTER"
TYPE_REPLICA_REGISTERED = "REPLICA_REGISTERED"
TYPE_REPLICA_HEARTBEAT = "REPLICA_HEARTBEAT"
TYPE_REPLICA_RECEIPT_ACK = "REPLICA_RECEIPT_ACK"
TYPE_HEARTBEAT = "HEARTBEAT"
TYPE_ELECTION = "ELECTION"

STATUS_PROCESSED = "PROCESSED"
STATUS_DUPLICATE = "DUPLICATE"
STATUS_OUT_OF_ORDER = "OUT_OF_ORDER"
STATUS_RECEIVED = "RECEIVED"
STATUS_DUPLICATE_RECEIPT = "DUPLICATE_RECEIPT"


def encode_message(message: dict[str, Any]) -> bytes:
    """Serializa uma mensagem para JSON UTF-8."""
    payload = dict(message)
    payload.setdefault("version", PROTOCOL_VERSION)
    encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )
    if len(encoded) > MAX_DATAGRAM_SIZE:
        raise ValueError("mensagem excede o tamanho máximo do protocolo")
    return encoded


def decode_message(data: bytes) -> dict[str, Any] | None:
    """Desserializa e valida a estrutura básica de um datagrama."""
    if not data or len(data) > MAX_DATAGRAM_SIZE:
        return None

    try:
        message = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None

    if not isinstance(message, dict):
        return None
    if message.get("version") != PROTOCOL_VERSION:
        return None
    if not isinstance(message.get("type"), str):
        return None
    return message


def make_discover_frontend(client_id: str) -> bytes:
    return encode_message(
        {"type": TYPE_DISCOVER_FRONTEND, "client_id": client_id}
    )


def make_frontend_info(frontend_id: str, frontend_addr: str) -> bytes:
    return encode_message(
        {
            "type": TYPE_FRONTEND_INFO,
            "frontend_id": frontend_id,
            "frontend_addr": frontend_addr,
        }
    )


def make_sum_request(client_id: str, request_id: int, value: int) -> bytes:
    return encode_message(
        {
            "type": TYPE_SUM_REQUEST,
            "client_id": client_id,
            "request_id": request_id,
            "value": value,
        }
    )


def make_sum_result(
    client_id: str,
    request_id: int,
    seq_id: int,
    num_reqs: int,
    total_sum: int,
    status: str,
) -> bytes:
    return encode_message(
        {
            "type": TYPE_SUM_RESULT,
            "client_id": client_id,
            "request_id": request_id,
            "seq_id": seq_id,
            "num_reqs": num_reqs,
            "total_sum": total_sum,
            "status": status,
        }
    )


def make_replica_register(node_id: str, listen_port: int) -> bytes:
    return encode_message(
        {
            "type": TYPE_REPLICA_REGISTER,
            "node_id": node_id,
            "listen_port": listen_port,
        }
    )


def make_replica_registered(
    node_id: str,
    frontend_id: str,
    accepted: bool,
) -> bytes:
    return encode_message(
        {
            "type": TYPE_REPLICA_REGISTERED,
            "node_id": node_id,
            "frontend_id": frontend_id,
            "accepted": accepted,
        }
    )


def make_replica_heartbeat(node_id: str, frontend_id: str) -> bytes:
    return encode_message(
        {
            "type": TYPE_REPLICA_HEARTBEAT,
            "node_id": node_id,
            "frontend_id": frontend_id,
        }
    )


def make_ordered_request(
    frontend_id: str,
    seq_id: int,
    client_id: str,
    request_id: int,
    value: int,
) -> bytes:
    return encode_message(
        {
            "type": TYPE_ORDERED_REQUEST,
            "frontend_id": frontend_id,
            "seq_id": seq_id,
            "client_id": client_id,
            "request_id": request_id,
            "value": value,
        }
    )


def make_replica_receipt_ack(
    node_id: str,
    frontend_id: str,
    seq_id: int,
    status: str,
) -> bytes:
    return encode_message(
        {
            "type": TYPE_REPLICA_RECEIPT_ACK,
            "node_id": node_id,
            "frontend_id": frontend_id,
            "seq_id": seq_id,
            "status": status,
        }
    )
