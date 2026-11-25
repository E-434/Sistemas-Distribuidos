#!/usr/bin/env python3
"""Benchmark do serviço distribuído de soma sobre UDP usando números de arquivo de texto."""

from __future__ import annotations

import os
import socket
import sys
import time
import uuid
from typing import Final, List, Optional

from descoberta import discover_frontend
from interface import client_server_addr, format_client_log
from protocolo import (
    TYPE_SUM_RESULT,
    decode_message,
    make_sum_request,
)

BUFFER_SIZE: Final[int] = 4096
REQUEST_TIMEOUT: Final[float] = 0.100
REDISCOVER_AFTER_RETRIES: Final[int] = 5


def load_numbers_from_file(filepath: str) -> List[int]:
    """Carrega os números do arquivo de texto linha a linha."""
    if not os.path.exists(filepath):
        print(f"Erro: Arquivo '{filepath}' não encontrado.", file=sys.stderr)
        raise SystemExit(1)

    numbers: List[int] = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                numbers.append(int(line))
            except ValueError:
                print(
                    f"Aviso: Linha {line_num} ignorada (não é inteiro válido): {line!r}",
                    file=sys.stderr,
                )

    if not numbers:
        print(f"Erro: Nenhum número válido encontrado em '{filepath}'.", file=sys.stderr)
        raise SystemExit(1)

    return numbers


def wait_for_ack(
    sock: socket.socket,
    frontend_addr: tuple[str, int],
    client_id: str,
    request_id: int,
) -> Optional[dict]:
    """Espera o resultado correspondente ao request_id especificado."""
    loopback_ips = {"127.0.0.1", "0.0.0.0", "localhost"}
    while True:
        try:
            data, sender = sock.recvfrom(BUFFER_SIZE)
        except ConnectionResetError:
            continue
        except socket.timeout:
            return None

        if sender[0] != frontend_addr[0] and not (
            sender[0] in loopback_ips and frontend_addr[0] in loopback_ips
        ):
            continue

        message = decode_message(data)
        if message is None or message.get("type") != TYPE_SUM_RESULT:
            continue

        if (
            message.get("client_id") == client_id
            and message.get("request_id") == request_id
        ):
            return message

        continue


def main() -> None:
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print(
            f"Uso: python3 {sys.argv[0]} <porta_udp> [caminho_arquivo.txt]",
            file=sys.stderr,
        )
        raise SystemExit(2)

    try:
        port = int(sys.argv[1])
    except ValueError:
        print("A porta deve ser um número inteiro.", file=sys.stderr)
        raise SystemExit(2)

    if not 1 <= port <= 65535:
        print("A porta deve estar no intervalo de 1 a 65535.", file=sys.stderr)
        raise SystemExit(2)

    file_path = sys.argv[2] if len(sys.argv) == 3 else "RAND_NUM_1.txt"

    numbers = load_numbers_from_file(file_path)
    total_requests = len(numbers)
    expected_total_sum = sum(numbers)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.bind(("0.0.0.0", 0))

    local_ip, local_port = sock.getsockname()
    if local_ip == "0.0.0.0":
        local_ip = "127.0.0.1"
    client_endpoint = f"{local_ip}:{local_port}"

    client_id = uuid.uuid4().hex

    try:
        server_addr, frontend_id = discover_frontend(sock, port, client_id)
        fe_endpoint = f"{server_addr[0]}:{server_addr[1]}"

        print(client_server_addr(client_endpoint, fe_endpoint, frontend_id))
        print(
            format_client_log(
                client_endpoint,
                f"benchmark_file_start file {file_path} requests {total_requests:,} "
                f"expected_sum {expected_total_sum:,}",
            )
        )

        start = time.perf_counter()
        last_ack = None
        retransmissions = 0
        log_interval = max(1000, total_requests // 10)

        for req_id, value in enumerate(numbers, start=1):
            packet = make_sum_request(client_id, req_id, value)
            retries_without_ack = 0

            while True:
                sock.settimeout(REQUEST_TIMEOUT)
                sock.sendto(packet, server_addr)

                ack = wait_for_ack(sock, server_addr, client_id, req_id)

                if ack is not None:
                    last_ack = ack
                    break

                retransmissions += 1
                retries_without_ack += 1
                if retries_without_ack >= REDISCOVER_AFTER_RETRIES:
                    retries_without_ack = 0
                    try:
                        new_addr, new_fe_id = discover_frontend(
                            sock, port, client_id
                        )
                        if new_addr != server_addr or new_fe_id != frontend_id:
                            server_addr = new_addr
                            frontend_id = new_fe_id
                            fe_endpoint = f"{server_addr[0]}:{server_addr[1]}"
                            print(
                                client_server_addr(
                                    client_endpoint, fe_endpoint, frontend_id
                                ),
                                flush=True,
                            )
                    except RuntimeError:
                        pass

            if req_id % log_interval == 0 or req_id == total_requests:
                elapsed = time.perf_counter() - start
                rate = req_id / elapsed if elapsed > 0 else 0.0
                print(
                    format_client_log(
                        client_endpoint,
                        f"progresso {req_id:,}/{total_requests:,} "
                        f"({(req_id / total_requests) * 100:.1f}%) "
                        f"taxa {rate:,.0f} req/s retransmissoes {retransmissions}",
                    ),
                    flush=True,
                )

        elapsed = time.perf_counter() - start
        rate = total_requests / elapsed if elapsed > 0 else 0.0

        received_num_reqs = last_ack.get("num_reqs") if last_ack else None
        received_total_sum = last_ack.get("total_sum") if last_ack else None

        ok = (
            received_num_reqs == total_requests
            and received_total_sum == expected_total_sum
        )
        status_str = "OK" if ok else "FALHA"
        print(
            format_client_log(
                client_endpoint,
                f"benchmark_file_end status {status_str} elapsed {elapsed:.3f}s "
                f"rate {rate:,.2f} req/s retransmissoes {retransmissions} "
                f"num_reqs {received_num_reqs}/{total_requests} "
                f"total_sum {received_total_sum}/{expected_total_sum}",
            )
        )
        if not ok:
            raise SystemExit(1)

    finally:
        sock.close()


if __name__ == "__main__":
    main()