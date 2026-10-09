#!/usr/bin/env python3
"""
Protocolo de aplicação do trabalho prático.

As quatro mensagens são transportadas exclusivamente por UDP:
  1. DESCOBERTA      (cliente -> broadcast)
  2. DESCOBERTA_ACK  (servidor -> unicast)
  3. REQUISICAO       (cliente -> servidor)
  4. ACK              (servidor -> cliente)

Formato: JSON UTF-8.
"""

import json

PROTOCOL_VERSION = 1
MAX_DATAGRAM_SIZE = 4096

# Os tipos identificam as etapas de descoberta e de envio de valores.
TYPE_DISCOVER = "DISCOVER"
TYPE_DISCOVER_ACK = "DISCOVER_ACK"
TYPE_REQUEST = "REQUEST"
TYPE_ACK = "ACK"

STATUS_PROCESSED = "PROCESSED"
STATUS_DUPLICATE = "DUPLICATE"
STATUS_OUT_OF_ORDER = "OUT_OF_ORDER"


def encode_message(message: dict) -> bytes:
    """Serializa uma mensagem para JSON UTF-8."""
    message = dict(message)
    # Inclui a versão por padrão para que o receptor possa validar o formato.
    message.setdefault("version", PROTOCOL_VERSION)
    return json.dumps(
        message,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def decode_message(data: bytes) -> dict | None:
    """Desserializa e valida minimamente uma mensagem recebida."""
    # Descarta datagramas vazios ou maiores que o limite do protocolo.
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


def make_discover() -> bytes:
    return encode_message({"type": TYPE_DISCOVER})


def make_discover_ack(server_addr: str) -> bytes:
    return encode_message(
        {
            "type": TYPE_DISCOVER_ACK,
            "server_addr": server_addr,
        }
    )


def make_request(req_id: int, value: int) -> bytes:
    return encode_message(
        {
            "type": TYPE_REQUEST,
            "id_req": req_id,
            "value": value,
        }
    )


def make_ack(
    req_id: int,
    num_reqs: int,
    total_sum: int,
    status: str,
) -> bytes:
    return encode_message(
        {
            "type": TYPE_ACK,
            "id_req": req_id,
            "num_reqs": num_reqs,
            "total_sum": total_sum,
            "status": status,
        }
    )

