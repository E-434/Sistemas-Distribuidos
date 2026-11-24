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

Se a resposta for perdida ou o front-end falhar, a mesma requisição é
retransmitida e o cliente redescobre o novo front-end após falhas consecutivas.
O front-end deduplica por client_id/request_id para não somá-la duas vezes.
"""

from __future__ import annotations

import socket
import sys
import time
import uuid
from typing import Final

from descoberta import discover_frontend
from protocolo import (
    TYPE_SUM_RESULT,
    decode_message,
    make_sum_request,
)

BUFFER_SIZE: Final[int] = 4096
REQUEST_TIMEOUT: Final[float] = 0.100
REDISCOVER_AFTER_RETRIES: Final[int] = 5

# Um valor fixo facilita comparar execuções do benchmark.
NUM_REQUESTS: Final[int] = 1_000_000
VALUE: Final[int] = 1


def now() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def wait_for_ack(
    sock: socket.socket,
    frontend_addr: tuple[str, int],
    client_id: str,
    request_id: int,
) -> dict | None:
    """Espera o resultado correspondente ao request_id."""
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
    client_id = uuid.uuid4().hex

    try:
        server_addr, frontend_id = discover_frontend(sock, port, client_id)
        server_ip = server_addr[0]

        print(f"{now()} frontend_addr {server_ip} frontend_id {frontend_id}")
        print(f"Benchmark: {NUM_REQUESTS:,} requisições, value={VALUE}")
        print("O servidor deve ter sido iniciado zerado.")
        print()

        expected_num_reqs = NUM_REQUESTS
        expected_total_sum = NUM_REQUESTS * VALUE

        start = time.perf_counter()
        last_ack = None
        retransmissions = 0

        for req_id in range(1, NUM_REQUESTS + 1):
            packet = make_sum_request(client_id, req_id, VALUE)
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
                            print(
                                f"{now()} frontend_addr {server_addr[0]} "
                                f"frontend_id {frontend_id}",
                                flush=True,
                            )
                    except RuntimeError:
                        pass

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

        received_num_reqs = last_ack.get("num_reqs") if last_ack else None
        received_total_sum = last_ack.get("total_sum") if last_ack else None

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