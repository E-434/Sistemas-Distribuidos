#!/usr/bin/env python3
"""Benchmark do serviço distribuído de soma sobre UDP usando números de arquivo de texto.

Lê os valores a serem somados de um arquivo de texto (por padrão 'RAND_NUM_1.txt'),
envia cada requisição respeitando o protocolo stop-and-wait (esperando o ACK correspondente)
e valida o resultado final contra a soma acumulada esperada.

Uso:
    python3 benchmark_file.py <porta_udp> [caminho_arquivo]

Exemplo:
    python3 benchmark_file.py 4000 RAND_NUM_1.txt
"""

from __future__ import annotations

import os
import socket
import sys
import time
from typing import Final, List, Optional

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


def now() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


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
                print(f"Aviso: Linha {line_num} ignorada (não é inteiro válido): {line!r}", file=sys.stderr)

    if not numbers:
        print(f"Erro: Nenhum número válido encontrado em '{filepath}'.", file=sys.stderr)
        raise SystemExit(1)

    return numbers


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

            # Retorna o IP real de origem do servidor
            return sender[0]

    raise RuntimeError("Nenhum servidor encontrado via broadcast.")


def wait_for_ack(
    sock: socket.socket,
    server_ip: str,
    port: int,
    req_id: int,
) -> Optional[dict]:
    """Espera o ACK correspondente ao req_id especificado."""
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

        # Ignora ACKs antigos/atrasados
        continue


def main() -> None:
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print(f"Uso: python3 {sys.argv[0]} <porta_udp> [caminho_arquivo.txt]", file=sys.stderr)
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

    print(f"Carregando números do arquivo: {file_path}")
    numbers = load_numbers_from_file(file_path)
    total_requests = len(numbers)
    expected_total_sum = sum(numbers)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    try:
        server_ip = discover_server(sock, port)
        server_addr = (server_ip, port)

        print(f"{now()} Servidor encontrado em {server_ip}:{port}")
        print(f"Benchmark: {total_requests:,} requisições a serem enviadas.")
        print(f"Soma esperada no conjunto de números: {expected_total_sum:,}")
        print("Nota: O servidor deve ter sido iniciado zerado para validação exata.")
        print("-" * 50)

        start = time.perf_counter()
        last_ack = None
        retransmissions = 0

        # Define frequência de logs de progresso (a cada 1.000 requisições ou a cada 10%)
        log_interval = max(1000, total_requests // 10)

        for req_id, value in enumerate(numbers, start=1):
            packet = make_request(req_id, value)

            while True:
                sock.settimeout(REQUEST_TIMEOUT)
                sock.sendto(packet, server_addr)

                ack = wait_for_ack(sock, server_ip, port, req_id)

                if ack is not None:
                    last_ack = ack
                    break

                retransmissions += 1

            if req_id % log_interval == 0 or req_id == total_requests:
                elapsed = time.perf_counter() - start
                rate = req_id / elapsed if elapsed > 0 else 0.0
                print(
                    f"{now()} Progresso {req_id:,}/{total_requests:,} "
                    f"({(req_id/total_requests)*100:.1f}%) | "
                    f"Taxa: {rate:,.0f} req/s | "
                    f"Retransmissões: {retransmissions}",
                    flush=True,
                )

        elapsed = time.perf_counter() - start
        rate = total_requests / elapsed if elapsed > 0 else 0.0

        received_num_reqs = last_ack.get("num_reqs") if last_ack else None
        received_total_sum = last_ack.get("total_sum") if last_ack else None

        print()
        print("=" * 20 + " RESULTADO DO BENCHMARK " + "=" * 20)
        print(f"Tempo total decorrido: {elapsed:.3f} s")
        print(f"Taxa média:            {rate:,.2f} req/s")
        print(f"Retransmissões:        {retransmissions}")
        print()
        print(f"Esperado num_reqs:     {total_requests:,}")
        print(f"Servidor num_reqs:     {received_num_reqs}")
        print()
        print(f"Esperado total_sum:    {expected_total_sum:,}")
        print(f"Servidor total_sum:    {received_total_sum}")
        print("=" * 64)

        if received_num_reqs == total_requests and received_total_sum == expected_total_sum:
            print("RESULTADO: OK! Todos os números foram processados e somados corretamente.")
        else:
            print("RESULTADO: DIVERGÊNCIA! Os valores retornados pelo servidor diferem do esperado.")
            raise SystemExit(1)

    finally:
        sock.close()


if __name__ == "__main__":
    main()
