#!/usr/bin/env python3
"""Cliente do serviço distribuído de soma sobre UDP."""

from __future__ import annotations

import queue
import socket
import sys
import threading
from datetime import datetime
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
DEFAULT_TIMEOUT_SECONDS: Final[float] = 0.010
DISCOVERY_TIMEOUT_SECONDS: Final[float] = 0.500
DISCOVERY_RETRIES: Final[int] = 5
UINT64_MAX: Final[int] = (1 << 64) - 1

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

    @staticmethod
    def now() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def discover_server(self) -> str:
        """Descobre o servidor via broadcast e retorna seu endereço IPv4."""
        self.sock.settimeout(DISCOVERY_TIMEOUT_SECONDS)
        discovery = make_discover()

        for _ in range(DISCOVERY_RETRIES):
            try:
                self.sock.sendto(discovery, (BROADCAST_ADDRESS, self.port))
            except OSError as exc:
                raise RuntimeError(f"falha ao enviar descoberta: {exc}") from exc

            while True:
                try:
                    data, sender = self.sock.recvfrom(BUFFER_SIZE)
                except socket.timeout:
                    break

                message = decode_message(data)
                if message is None:
                    continue

                if message.get("type") != TYPE_DISCOVER_ACK:
                    continue

                # O IP de origem do datagrama é o endereço que deve ser usado
                # para enviar as requisições subsequentes.
                server_ip = sender[0]
                self.server_addr = (server_ip, self.port)
                return server_ip

        raise RuntimeError("nenhum servidor encontrado via broadcast")

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
                self.output_queue.put(f"{self.now()} invalid_value {line}")
                continue

            if value <= 0 or value > UINT64_MAX:
                self.output_queue.put(f"{self.now()} invalid_value {value}")
                continue

            self.input_queue.put(value)

    def _send_request(self, req_id: int, value: int) -> None:
        assert self.server_addr is not None
        packet = make_request(req_id=req_id, value=value)
        self.sock.sendto(packet, self.server_addr)

    def _wait_for_ack(self, req_id: int) -> dict | None:
        """Espera o ACK da requisição atual, ignorando ACKs atrasados."""
        assert self.server_addr is not None

        while not self.stop_event.is_set():
            try:
                data, sender = self.sock.recvfrom(BUFFER_SIZE)
            except socket.timeout:
                return None
            except OSError:
                if self.stop_event.is_set():
                    return None
                raise

            if sender[0] != self.server_addr[0]:
                continue

            message = decode_message(data)
            if message is None or message.get("type") != TYPE_ACK:
                continue

            ack_id = message.get("id_req")
            num_reqs = message.get("num_reqs")
            total_sum = message.get("total_sum")

            if not isinstance(ack_id, int):
                continue
            if not isinstance(num_reqs, int):
                continue
            if not isinstance(total_sum, int):
                continue

            # ACK menor significa que é uma resposta atrasada referente a uma
            # requisição anterior. ACK maior é inconsistente com stop-and-wait.
            if ack_id != req_id:
                continue

            return message

        return None

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
                        self._send_request(current_id, value)
                    except OSError:
                        if self.stop_event.is_set():
                            return
                        raise

                    if first_attempt:
                        self.output_queue.put(
                            f"{self.now()} send server {self.server_addr[0]} "
                            f"id_req {current_id} value {value}"
                        )
                        first_attempt = False
                    else:
                        self.output_queue.put(
                            f"{self.now()} resend id_req {current_id} value {value}"
                        )

                    ack = self._wait_for_ack(current_id)
                    if ack is None:
                        # Timeout: reenvia exatamente a mesma requisição.
                        continue

                    self.output_queue.put(
                        f"{self.now()} server {self.server_addr[0]} "
                        f"id_req {current_id} value {value} "
                        f"num_reqs {ack['num_reqs']} total_sum {ack['total_sum']}"
                    )

                    req_id += 1
                    break
        except OSError:
            self.stop_event.set()

    def run(self) -> None:
        try:
            server_ip = self.discover_server()
        except RuntimeError as exc:
            print(f"Erro: {exc}", file=sys.stderr)
            self.sock.close()
            return

        # Primeiro publica server_addr; só depois inicia as threads que podem
        # produzir mensagens de envio. Assim a ordem exigida pela interface é
        # preservada.
        self.output_thread.start()
        self.output_queue.put(f"{self.now()} server_addr {server_ip}")

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

