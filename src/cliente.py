#!/usr/bin/env python3
"""Cliente do serviço distribuído de soma sobre UDP com redescobrir de FE."""

from __future__ import annotations

import queue
import socket
import sys
import threading
import time
import uuid
from typing import Final

from descoberta import discover_frontend
from interface import (
    ack_received,
    client_discovery_attempt,
    client_discovery_result,
    client_frontend_error,
    client_server_addr,
    format_client_log,
    invalid_value,
    request_resent,
    request_sent,
)
from processamento import UINT64_MAX, send_request, wait_for_ack
from protocolo import STATUS_DUPLICATE, STATUS_PROCESSED

DEFAULT_TIMEOUT_SECONDS: Final[float] = 0.010
REDISCOVER_AFTER_RETRIES: Final[int] = 5
STARTUP_DISCOVERY_ATTEMPTS: Final[int] = 25
STARTUP_DISCOVERY_RETRY_DELAY_SECONDS: Final[float] = 0.2

_SENTINEL = object()


class DistributedClient:
    def __init__(self, port: int, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> None:
        self.port = port
        self.timeout = timeout
        self.client_id = uuid.uuid4().hex

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("0.0.0.0", 0))

        local_ip, local_port = self.sock.getsockname()
        if local_ip == "0.0.0.0":
            local_ip = "127.0.0.1"
        self.client_endpoint = f"{local_ip}:{local_port}"

        self.server_addr: tuple[str, int] | None = None
        self.frontend_id = ""

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
                self.input_queue.put(_SENTINEL)
                return

            line = line.strip()
            if not line:
                continue

            try:
                value = int(line)
            except ValueError:
                self.output_queue.put(invalid_value(self.client_endpoint, line))
                continue

            if value <= 0 or value > UINT64_MAX:
                self.output_queue.put(invalid_value(self.client_endpoint, value))
                continue

            self.input_queue.put(value)

    def _try_rediscover_frontend(self) -> None:
        try:
            new_addr, new_fe_id = discover_frontend(
                self.sock,
                self.port,
                self.client_id,
            )
        except RuntimeError:
            return
        if new_addr != self.server_addr or new_fe_id != self.frontend_id:
            self.server_addr = new_addr
            self.frontend_id = new_fe_id
            fe_endpoint = f"{new_addr[0]}:{new_addr[1]}"
            self.output_queue.put(
                client_server_addr(self.client_endpoint, fe_endpoint, new_fe_id)
            )

    def _network_worker(self) -> None:
        assert self.server_addr is not None
        request_id = 1

        try:
            while not self.stop_event.is_set():
                try:
                    item = self.input_queue.get(timeout=0.050)
                except queue.Empty:
                    continue

                if item is _SENTINEL:
                    return

                value = int(item)
                current_id = request_id
                first_attempt = True
                retries_without_ack = 0

                while not self.stop_event.is_set():
                    try:
                        self.sock.settimeout(self.timeout)
                        assert self.server_addr is not None
                        send_request(
                            self.sock,
                            self.server_addr,
                            self.client_id,
                            current_id,
                            value,
                        )
                    except OSError:
                        if self.stop_event.is_set():
                            return
                        raise

                    fe_endpoint = f"{self.server_addr[0]}:{self.server_addr[1]}"
                    if first_attempt:
                        self.output_queue.put(
                            request_sent(
                                self.client_endpoint,
                                fe_endpoint,
                                current_id,
                                value,
                            )
                        )
                        first_attempt = False
                    else:
                        self.output_queue.put(
                            request_resent(
                                self.client_endpoint,
                                fe_endpoint,
                                current_id,
                                value,
                            )
                        )

                    ack = wait_for_ack(
                        self.sock,
                        self.server_addr,
                        self.client_id,
                        current_id,
                        self.stop_event,
                    )
                    if ack is None:
                        retries_without_ack += 1
                        if retries_without_ack >= REDISCOVER_AFTER_RETRIES:
                            retries_without_ack = 0
                            self._try_rediscover_frontend()
                        continue

                    if ack.get("status") not in (STATUS_PROCESSED, STATUS_DUPLICATE):
                        self.output_queue.put(
                            client_frontend_error(
                                self.client_endpoint,
                                current_id,
                                ack.get("status"),
                            )
                        )
                        self.stop_event.set()
                        return

                    self.output_queue.put(
                        ack_received(
                            self.client_endpoint,
                            fe_endpoint,
                            current_id,
                            value,
                            ack["seq_id"],
                            ack["num_reqs"],
                            ack["total_sum"],
                        )
                    )

                    request_id += 1
                    break
        except OSError:
            self.stop_event.set()

    def _discover_initial_frontend(self) -> tuple[tuple[str, int], str]:
        last_exc: RuntimeError | None = None
        for attempt in range(1, STARTUP_DISCOVERY_ATTEMPTS + 1):
            t0 = time.monotonic()
            print(
                client_discovery_attempt(
                    self.client_endpoint,
                    attempt,
                    STARTUP_DISCOVERY_ATTEMPTS,
                    self.port,
                ),
                flush=True,
            )
            try:
                addr, fid = discover_frontend(
                    self.sock,
                    self.port,
                    self.client_id,
                )
                elapsed = time.monotonic() - t0
                print(
                    client_discovery_result(
                        self.client_endpoint,
                        attempt,
                        "found",
                        elapsed,
                        frontend_id=fid,
                        frontend_addr=f"{addr[0]}:{addr[1]}",
                    ),
                    flush=True,
                )
                return addr, fid
            except RuntimeError as exc:
                last_exc = exc
                elapsed = time.monotonic() - t0
                print(
                    client_discovery_result(
                        self.client_endpoint,
                        attempt,
                        "not_found",
                        elapsed,
                    ),
                    flush=True,
                )
                if attempt < STARTUP_DISCOVERY_ATTEMPTS:
                    time.sleep(STARTUP_DISCOVERY_RETRY_DELAY_SECONDS)
        assert last_exc is not None
        raise last_exc

    def run(self) -> None:
        try:
            self.server_addr, self.frontend_id = self._discover_initial_frontend()
        except RuntimeError as exc:
            print(
                format_client_log(self.client_endpoint, f"error {exc}"),
                file=sys.stderr,
                flush=True,
            )
            self.sock.close()
            return

        self.output_thread.start()
        self.input_thread.start()
        self.network_thread.start()

        try:
            while not self.stop_event.wait(0.050):
                if not self.network_thread.is_alive():
                    break
        except KeyboardInterrupt:
            self.stop_event.set()
        finally:
            self.stop_event.set()
            self.network_thread.join(timeout=max(1.0, self.timeout * 20))
            self.sock.close()
            self.input_thread.join(timeout=0.2)
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