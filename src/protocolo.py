#!/usr/bin/env python3
"""Protocolo UDP cliente/front-end, replicação ativa e eleição em anel."""

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

# Canal intra-cluster (FE <-> Replica Managers)
TYPE_ORDERED_REQUEST = "ORDERED_REQUEST"
TYPE_REPLICA_ACK = "REPLICA_ACK"
TYPE_REPLICA_REGISTER = "REPLICA_REGISTER"
TYPE_REPLICA_REGISTERED = "REPLICA_REGISTERED"
TYPE_REPLICA_HEARTBEAT = "REPLICA_HEARTBEAT"
TYPE_REPLICA_HEARTBEAT_ACK = "REPLICA_HEARTBEAT_ACK"
TYPE_REPLICA_RECEIPT_ACK = "REPLICA_RECEIPT_ACK"
TYPE_REPLICA_APPLIED_ACK = "REPLICA_APPLIED_ACK"
TYPE_REPLICA_NACK = "REPLICA_NACK"
TYPE_HEARTBEAT = "HEARTBEAT"

# Eleição em anel (critério: maior IP; desempate por porta/node_id)
TYPE_ELECTION = "ELECTION"
TYPE_ELECTION_ACK = "ELECTION_ACK"
TYPE_COORDINATOR = "COORDINATOR"
TYPE_RING_MEMBERS = "RING_MEMBERS"

STATUS_PROCESSED = "PROCESSED"
STATUS_DUPLICATE = "DUPLICATE"
STATUS_OUT_OF_ORDER = "OUT_OF_ORDER"
STATUS_RECEIVED = "RECEIVED"
STATUS_DUPLICATE_RECEIPT = "DUPLICATE_RECEIPT"
STATUS_APPLIED = "APPLIED"


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


def make_replica_register(
    node_id: str,
    listen_port: int,
    ring_port: int | None = None,
) -> bytes:
    payload: dict[str, Any] = {
        "type": TYPE_REPLICA_REGISTER,
        "node_id": node_id,
        "listen_port": listen_port,
    }
    if ring_port is not None:
        payload["ring_port"] = ring_port
    return encode_message(payload)


def make_replica_registered(
    node_id: str,
    frontend_id: str,
    accepted: bool,
    members: list[dict[str, Any]] | None = None,
    state_snapshot: dict[str, Any] | None = None,
    history: list[dict[str, Any]] | None = None,
) -> bytes:
    payload: dict[str, Any] = {
        "type": TYPE_REPLICA_REGISTERED,
        "node_id": node_id,
        "frontend_id": frontend_id,
        "accepted": accepted,
    }
    if members is not None:
        payload["members"] = members
    if state_snapshot is not None:
        payload["state_snapshot"] = state_snapshot

    if history:
        bounded_history: list[dict[str, Any]] = []
        for entry in reversed(history):
            candidate_history = list(reversed(bounded_history)) + [entry]
            payload["history"] = candidate_history
            try:
                encode_message(payload)
                bounded_history.append(entry)
            except ValueError:
                payload["history"] = list(reversed(bounded_history))
                break
        if not payload.get("history"):
            payload.pop("history", None)

    return encode_message(payload)


def make_replica_heartbeat(node_id: str, frontend_id: str) -> bytes:
    return encode_message(
        {
            "type": TYPE_REPLICA_HEARTBEAT,
            "node_id": node_id,
            "frontend_id": frontend_id,
        }
    )


def make_replica_heartbeat_ack(node_id: str, frontend_id: str) -> bytes:
    return encode_message(
        {
            "type": TYPE_REPLICA_HEARTBEAT_ACK,
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


def make_replica_applied_ack(
    node_id: str,
    frontend_id: str,
    last_applied_seq_id: int,
    num_reqs: int,
    total_sum: int,
) -> bytes:
    return encode_message(
        {
            "type": TYPE_REPLICA_APPLIED_ACK,
            "node_id": node_id,
            "frontend_id": frontend_id,
            "last_applied_seq_id": last_applied_seq_id,
            "num_reqs": num_reqs,
            "total_sum": total_sum,
        }
    )


def make_replica_nack(
    node_id: str,
    frontend_id: str,
    missing_seq_ids: list[int],
) -> bytes:
    return encode_message(
        {
            "type": TYPE_REPLICA_NACK,
            "node_id": node_id,
            "frontend_id": frontend_id,
            "missing": missing_seq_ids,
        }
    )


def make_election(
    sender_id: str,
    candidate_id: str,
    candidate_ip: str,
    candidate_port: int,
    origin_id: str,
    hop_index: int,
) -> bytes:
    return encode_message(
        {
            "type": TYPE_ELECTION,
            "sender_id": sender_id,
            "candidate_id": candidate_id,
            "candidate_ip": candidate_ip,
            "candidate_port": candidate_port,
            "origin_id": origin_id,
            "hop_index": hop_index,
        }
    )


def make_election_ack(
    sender_id: str,
    origin_id: str,
    hop_index: int,
) -> bytes:
    return encode_message(
        {
            "type": TYPE_ELECTION_ACK,
            "sender_id": sender_id,
            "origin_id": origin_id,
            "hop_index": hop_index,
        }
    )


def make_coordinator(
    sender_id: str,
    leader_id: str,
    leader_ip: str,
    leader_port: int,
    origin_id: str,
) -> bytes:
    return encode_message(
        {
            "type": TYPE_COORDINATOR,
            "sender_id": sender_id,
            "leader_id": leader_id,
            "leader_ip": leader_ip,
            "leader_port": leader_port,
            "origin_id": origin_id,
        }
    )


def make_ring_members(
    sender_id: str,
    members: list[dict[str, Any]],
) -> bytes:
    return encode_message(
        {
            "type": TYPE_RING_MEMBERS,
            "sender_id": sender_id,
            "members": members,
        }
    )