#!/usr/bin/env python3
"""Benchmark do serviço distribuído de soma sobre UDP.

Envia 1 milhão de requisições automaticamente, respeitando o protocolo
stop-and-wait: a próxima requisição só é enviada após o ACK da anterior.

IMPORTANTE:
    Para a comparação final ser direta, inicie o servidor zerado antes do
    benchmark:

        python3 servidor.py 4000
        python3 benchmark.py 4000

O benchmark usa value=1 em todas as requisições, portanto o resultado
esperado é:
    num_reqs  = 1_000_000
    total_sum = 1_000_000

Se um ACK for perdido, a mesma requisição é retransmitida. Como o servidor
faz deduplicação por id_req, ela não será somada duas vezes.
"""

from __future__ import annotations

import socket
import sys
import time
from typing import Final

from protocolo import (
    TYPE_ACK,
    TYPE_DISCOVER_ACK,
    decode_message,
    make_discover,
    make_request,
)

BUFFER_SIZE: Final[int] = 4096
BROADCAST_ADDRESS: Final[str] = "255.255.255.255"
DISCOVERY_TIMEOUT: Final[float] = 0.5
REQUEST_TIMEOUT: Final[float] = 0.100

# Um valor fixo facilita comparar execuções do benchmark.
NUM_REQUESTS: Final[int] = 1_000_000
VALUE: Final[int] = 1


def now() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def discover_server(sock: socket.socket, port: int) -> str:
    """Descobre o servidor via broadcast."""
    sock.settimeout(DISCOVERY_TIMEOUT)
    packet = make_discover()

    for _ in range(5):
        sock.sendto(packet, (BROADCAST_ADDRESS, port))

        while True:
            try:
                data, sender = sock.recvfrom(BUFFER_SIZE)
            except socket.timeout:
                break

            message = decode_message(data)
            if message is None:
                continue

            if message.get("type") != TYPE_DISCOVER_ACK:
                continue

            # Usa o IP real de origem do ACK.
            return sender[0]

    raise RuntimeError("nenhum servidor encontrado via broadcast")


def wait_for_ack(
    sock: socket.socket,
    server_ip: str,
    port: int,
    req_id: int,
) -> dict:
    """Espera o ACK correspondente ao req_id."""
    while True:
        try:
            data, sender = sock.recvfrom(BUFFER_SIZE)
        except socket.timeout:
            return None

        if sender[0] != server_ip:
            continue

        message = decode_message(data)
        if message is None or message.get("type") != TYPE_ACK:
            continue

        ack_id = message.get("id_req")
        if not isinstance(ack_id, int):
            continue

        if ack_id == req_id:
            return message

        # ACKs antigos podem chegar atrasados.
        # ACK com id maior não deve ocorrer em um benchmark stop-and-wait
        # normal; ele também não deve ser usado para confirmar a requisição.
        continue


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Uso: python3 {sys.argv[0]} <porta_udp>", file=sys.stderr)
        raise SystemExit(2)

    try:
        port = int(sys.argv[1])
    except ValueError:
        print("A porta deve ser um inteiro.", file=sys.stderr)
        raise SystemExit(2)

    if not 1 <= port <= 65535:
        print("A porta deve estar entre 1 e 65535.", file=sys.stderr)
        raise SystemExit(2)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    try:
        server_ip = discover_server(sock, port)
        server_addr = (server_ip, port)

        print(f"{now()} server_addr {server_ip}")
        print(f"Benchmark: {NUM_REQUESTS:,} requisições, value={VALUE}")
        print("O servidor deve ter sido iniciado zerado.")
        print()

        expected_num_reqs = NUM_REQUESTS
        expected_total_sum = NUM_REQUESTS * VALUE

        start = time.perf_counter()
        last_ack = None
        retransmissions = 0

        for req_id in range(1, NUM_REQUESTS + 1):
            packet = make_request(req_id, VALUE)

            while True:
                sock.settimeout(REQUEST_TIMEOUT)
                sock.sendto(packet, server_addr)

                ack = wait_for_ack(sock, server_ip, port, req_id)

                if ack is not None:
                    last_ack = ack
                    break

                retransmissions += 1

            # Mostra o progresso em intervalos para evitar milhões de linhas.
            if req_id % 100_000 == 0 or req_id == NUM_REQUESTS:
                elapsed = time.perf_counter() - start
                rate = req_id / elapsed if elapsed > 0 else 0.0
                print(
                    f"{now()} progresso {req_id:,}/{NUM_REQUESTS:,} "
                    f"taxa {rate:,.0f} req/s "
                    f"retransmissoes {retransmissions}",
                    flush=True,
                )

        elapsed = time.perf_counter() - start
        rate = NUM_REQUESTS / elapsed if elapsed > 0 else 0.0

        received_num_reqs = last_ack.get("num_reqs")
        received_total_sum = last_ack.get("total_sum")

        print()
        print("========== RESULTADO ==========")
        print(f"Tempo total:          {elapsed:.3f} s")
        print(f"Taxa média:           {rate:,.2f} req/s")
        print(f"Retransmissões:       {retransmissions}")
        print()
        print(f"Esperado num_reqs:    {expected_num_reqs}")
        print(f"Servidor num_reqs:    {received_num_reqs}")
        print()
        print(f"Esperado total_sum:   {expected_total_sum}")
        print(f"Servidor total_sum:   {received_total_sum}")
        print()

        ok = (
            received_num_reqs == expected_num_reqs
            and received_total_sum == expected_total_sum
        )

        if ok:
            print("RESULTADO: OK")
            print("O servidor processou corretamente 1 milhão de requisições.")
        else:
            print("RESULTADO: FALHA")
            print("O resultado do servidor não corresponde ao esperado.")
            raise SystemExit(1)

    finally:
        sock.close()


if __name__ == "__main__":
    main()
