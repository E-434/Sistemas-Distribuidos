#!/usr/bin/env python3
"""Cliente do serviço distribuído de soma sobre UDP."""

from __future__ import annotations

import queue
import socket
import sys
import threading
from typing import Final

from descoberta import discover_server
from interface import (
    ack_received,
    client_server_addr,
    invalid_value,
    request_resent,
    request_sent,
)
from processamento import UINT64_MAX, send_request, wait_for_ack

DEFAULT_TIMEOUT_SECONDS: Final[float] = 0.010

_SENTINEL = object()


class DistributedClient:
    def __init__(self, port: int, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> None:
        self.port = port
        self.timeout = timeout

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # O cliente usa uma porta efêmera. A porta recebida na linha de comando
        # é a porta UDP na qual o servidor está escutando.
        self.server_addr: tuple[str, int] | None = None

        # As filas separam leitura/escrita do terminal do trabalho de rede.
        self.stop_event = threading.Event()
        self.input_queue: queue.Queue[object] = queue.Queue()
        self.output_queue: queue.Queue[object] = queue.Queue()

        self.output_thread = threading.Thread(
            target=self._output_worker,
            name="output",
            daemon=True,
        )
        self.input_thread = threading.Thread(
            target=self._input_worker,
            name="input",
            daemon=True,
        )
        self.network_thread = threading.Thread(
            target=self._network_worker,
            name="network",
            daemon=True,
        )

    def _output_worker(self) -> None:
        while True:
            item = self.output_queue.get()
            if item is _SENTINEL:
                return
            print(item, flush=True)

    def _input_worker(self) -> None:
        while not self.stop_event.is_set():
            try:
                line = sys.stdin.readline()
            except (KeyboardInterrupt, OSError):
                self.stop_event.set()
                return

            if line == "":
                # Ctrl+D / EOF. Um sentinel é colocado depois dos valores já
                # lidos, permitindo ao manager terminar a fila sem perder dados.
                self.input_queue.put(_SENTINEL)
                return

            line = line.strip()
            if not line:
                continue

            try:
                value = int(line)
            except ValueError:
                self.output_queue.put(invalid_value(line))
                continue

            if value <= 0 or value > UINT64_MAX:
                self.output_queue.put(invalid_value(value))
                continue

            self.input_queue.put(value)

    def _network_worker(self) -> None:
        assert self.server_addr is not None
        req_id = 1

        try:
            while not self.stop_event.is_set():
                try:
                    item = self.input_queue.get(timeout=0.050)
                except queue.Empty:
                    continue

                if item is _SENTINEL:
                    return

                value = int(item)
                current_id = req_id
                first_attempt = True

                # Stop-and-wait: só avança o id depois de receber o ACK atual.
                while not self.stop_event.is_set():
                    try:
                        self.sock.settimeout(self.timeout)
                        assert self.server_addr is not None
                        send_request(self.sock, self.server_addr, current_id, value)
                    except OSError:
                        if self.stop_event.is_set():
                            return
                        raise

                    if first_attempt:
                        self.output_queue.put(
                            request_sent(self.server_addr[0], current_id, value)
                        )
                        first_attempt = False
                    else:
                        self.output_queue.put(request_resent(current_id, value))

                    ack = wait_for_ack(
                        self.sock,
                        self.server_addr,
                        current_id,
                        self.stop_event,
                    )
                    if ack is None:
                        # Timeout: reenvia exatamente a mesma requisição.
                        continue

                    self.output_queue.put(
                        ack_received(
                            self.server_addr[0],
                            current_id,
                            value,
                            ack["num_reqs"],
                            ack["total_sum"],
                        )
                    )

                    req_id += 1
                    break
        except OSError:
            self.stop_event.set()

    def run(self) -> None:
        try:
            self.server_addr = discover_server(self.sock, self.port)
        except RuntimeError as exc:
            print(f"Erro: {exc}", file=sys.stderr)
            self.sock.close()
            return

        server_ip = self.server_addr[0]

        # Primeiro publica server_addr; só depois inicia as threads que podem
        # produzir mensagens de envio. Assim a ordem exigida pela interface é
        # preservada.
        self.output_thread.start()
        self.output_queue.put(client_server_addr(server_ip))

        self.input_thread.start()
        self.network_thread.start()

        try:
            while not self.stop_event.wait(0.050):
                # Em Ctrl+D, a thread de rede termina depois de drenar a fila.
                if not self.network_thread.is_alive():
                    break
        except KeyboardInterrupt:
            self.stop_event.set()
        finally:
            self.stop_event.set()

            # Fecha o socket depois que a thread de rede terminou. Em Ctrl+C,
            # o timeout curto garante que ela deixe recvfrom rapidamente.
            self.network_thread.join(timeout=max(1.0, self.timeout * 20))
            self.sock.close()

            # A thread de input pode estar bloqueada em readline após Ctrl+C;
            # ela é daemon e não impede o encerramento do processo.
            self.input_thread.join(timeout=0.2)

            # Só encerramos a thread de saída depois que todas as mensagens do
            # manager foram colocadas na fila.
            self.output_queue.put(_SENTINEL)
            self.output_thread.join(timeout=1.0)


def parse_args(argv: list[str]) -> tuple[int, float]:
    if len(argv) not in (2, 3):
        print(f"Uso: python3 {argv[0]} <porta_udp> [timeout_s]", file=sys.stderr)
        raise SystemExit(2)

    try:
        port = int(argv[1])
    except ValueError:
        print("A porta deve ser um inteiro.", file=sys.stderr)
        raise SystemExit(2)

    if not 1 <= port <= 65535:
        print("A porta deve estar entre 1 e 65535.", file=sys.stderr)
        raise SystemExit(2)

    timeout = DEFAULT_TIMEOUT_SECONDS
    if len(argv) == 3:
        try:
            timeout = float(argv[2])
        except ValueError:
            print("O timeout deve ser um número em segundos.", file=sys.stderr)
            raise SystemExit(2)
        if timeout <= 0:
            print("O timeout deve ser positivo.", file=sys.stderr)
            raise SystemExit(2)

    return port, timeout


def main() -> None:
    port, timeout = parse_args(sys.argv)
    client = DistributedClient(port=port, timeout=timeout)
    client.run()


if __name__ == "__main__":
    main()

