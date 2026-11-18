#!/usr/bin/env python3
"""Teste de integração da replicação ativa.

Sobe um cluster real (front-end + réplicas) em subprocessos sobre a interface
de loopback e comprova as duas propriedades centrais:

  Cenário A — Convergência + quórum:
      FE + 2 RMs desde o início processam um lote de operações. Ao final, o
      estado replicado (num_reqs, total_sum) de TODAS as réplicas deve ser
      idêntico ao do front-end.

  Cenário B — Recuperação por NACK (entrega confiável):
      Uma terceira réplica entra ATRASADA, depois que centenas de operações já
      foram aplicadas. Ela detecta a lacuna, pede retransmissão (NACK) e deve
      alcançar exatamente o mesmo estado final — provando que nenhuma operação
      é perdida mesmo com o multicast UDP não confiável.

Uso:  python3 tests/test_replicacao_ativa.py [n_por_lote]
"""

from __future__ import annotations

import pathlib
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from descoberta import discover_frontend  # noqa: E402
from protocolo import (  # noqa: E402
    TYPE_SUM_RESULT,
    decode_message,
    make_sum_request,
)

PORT = 4100

SERVER_ARGS = [
    "--heartbeat-interval", "0.5",
    "--heartbeat-timeout", "3",
    "--receipt-ack-timeout", "0.5",
]


class Node:
    def __init__(self, name: str, logdir: pathlib.Path) -> None:
        self.name = name
        self.logpath = logdir / f"{name}.log"
        self._log = open(self.logpath, "w")
        self.proc = subprocess.Popen(
            [sys.executable, str(SRC / "servidor.py"), str(PORT), *SERVER_ARGS],
            stdout=self._log,
            stderr=subprocess.STDOUT,
            cwd=str(ROOT),
        )

    def text(self) -> str:
        return self.logpath.read_text(errors="replace")

    def wait_for(self, needle: str, timeout: float) -> bool:
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if needle in self.text():
                return True
            if self.proc.poll() is not None:
                return needle in self.text()
            time.sleep(0.05)
        return False

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGINT)
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
        self._log.flush()
        self._log.close()

    def final_state(self) -> tuple[int, int, int]:
        """Retorna (last_applied, num_reqs, total_sum) da linha final_state."""
        m = re.search(
            r"final_state .*?(?:last_applied (\d+) )?num_reqs (\d+) total_sum (\d+)",
            self.text(),
        )
        if not m:
            raise AssertionError(f"{self.name}: linha final_state não encontrada")
        last_applied = int(m.group(1)) if m.group(1) else int(m.group(2))
        return last_applied, int(m.group(2)), int(m.group(3))


def send_batch(values: list[int], timeout: float = 0.3) -> tuple[int, int, int]:
    """Envia um lote de operações stop-and-wait direto ao FE (estilo benchmark).

    Retorna (num_enviadas, num_reqs_no_ultimo_ack, total_sum_no_ultimo_ack).
    Reenvia em caso de timeout; o FE deduplica por client_id/request_id.
    """
    disc = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    disc.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    client_id = uuid.uuid4().hex
    server_addr, _ = discover_frontend(disc, PORT, client_id)
    disc.close()

    # Socket limpo (sem SO_BROADCAST) para o tráfego de dados unicast, evitando
    # o erro assíncrono de broadcast do macOS vazar no próximo sendto.
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    last: dict | None = None
    try:
        for req_id, value in enumerate(values, start=1):
            packet = make_sum_request(client_id, req_id, value)
            while True:
                sock.settimeout(timeout)
                sock.sendto(packet, server_addr)
                ack = _await_ack(sock, server_addr, client_id, req_id)
                if ack is not None:
                    last = ack
                    break
    finally:
        sock.close()
    assert last is not None, "nenhum ACK recebido do front-end"
    return len(values), int(last["num_reqs"]), int(last["total_sum"])


def _await_ack(sock, server_addr, client_id, req_id) -> dict | None:
    while True:
        try:
            data, sender = sock.recvfrom(4096)
        except socket.timeout:
            return None
        except (ConnectionResetError, OSError):
            return None
        if sender[0] != server_addr[0]:
            continue
        message = decode_message(data)
        if message is None or message.get("type") != TYPE_SUM_RESULT:
            continue
        if message.get("client_id") == client_id and message.get("request_id") == req_id:
            return message


def banner(title: str) -> None:
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def main() -> None:
    per_batch = int(sys.argv[1]) if len(sys.argv) > 1 else 1500
    batch1 = list(range(1, per_batch + 1))
    batch2 = list(range(per_batch + 1, 2 * per_batch + 1))
    total_ops = 2 * per_batch
    expected_sum = total_ops * (total_ops + 1) // 2

    logdir = pathlib.Path(tempfile.mkdtemp(prefix="replicacao_ativa_"))
    print(f"logs em: {logdir}")
    nodes: list[Node] = []

    try:
        banner("SETUP: front-end + 1 réplica")
        fe = Node("frontend", logdir); nodes.append(fe)
        assert fe.wait_for("role frontend", 5), "front-end não iniciou"
        print("front-end ativo")

        rm1 = Node("replica-1", logdir); nodes.append(rm1)
        assert rm1.wait_for("status registered", 5), "réplica 1 não registrou"
        print("réplica 1 registrada")
        time.sleep(0.5)  # deixa o heartbeat/membership estabilizar

        banner(f"CENÁRIO A: {per_batch} operações com FE + 2 RMs")
        rm2 = Node("replica-2", logdir); nodes.append(rm2)
        assert rm2.wait_for("status registered", 5), "réplica 2 não registrou"
        print("réplica 2 registrada")
        time.sleep(0.5)

        sent1, nr1, ts1 = send_batch(batch1)
        print(f"cliente 1 enviou {sent1} ops — último ACK num_reqs={nr1} total_sum={ts1}")
        # espera as duas réplicas aplicarem até a última operação do lote 1
        assert rm1.wait_for(f"seq_id {per_batch} ", 10), "RM1 não aplicou o lote 1"
        assert rm2.wait_for(f"seq_id {per_batch} ", 10), "RM2 não aplicou o lote 1"
        print(f"ambas as réplicas aplicaram até seq_id {per_batch}")

        banner("CENÁRIO B: réplica ATRASADA entra e recupera via NACK")
        rm3 = Node("replica-3-atrasada", logdir); nodes.append(rm3)
        assert rm3.wait_for("status registered", 5), "réplica 3 não registrou"
        print("réplica 3 (atrasada) registrada — está 0 operações à frente")
        time.sleep(0.5)

        sent2, nr2, ts2 = send_batch(batch2)
        print(f"cliente 2 enviou {sent2} ops — último ACK num_reqs={nr2} total_sum={ts2}")

        # espera TODAS as réplicas alcançarem a última operação global
        for rm in (rm1, rm2, rm3):
            ok = rm.wait_for(f"seq_id {total_ops} ", 20)
            assert ok, f"{rm.name} não alcançou seq_id {total_ops} (recuperação falhou)"
        print(f"todas as réplicas alcançaram seq_id {total_ops}")
        time.sleep(1.0)  # drena ACKs finais

    finally:
        for node in reversed(nodes):
            node.stop()

    # ---------------------------------------------------------------- #
    # Verificação                                                      #
    # ---------------------------------------------------------------- #
    banner("RESULTADO: estado final de cada nó")
    print(f"{'nó':<22} {'last_applied':>12} {'num_reqs':>10} {'total_sum':>14}")
    states = {}
    for node in nodes:
        la, nr, ts = node.final_state()
        states[node.name] = (la, nr, ts)
        print(f"{node.name:<22} {la:>12} {nr:>10} {ts:>14}")

    print(f"\nesperado:              {total_ops:>12} {total_ops:>10} {expected_sum:>14}")

    # Evidências de mecanismo nos logs
    fe_txt = nodes[0].text()
    rm3_txt = nodes[-1].text()
    banner("EVIDÊNCIAS DE MECANISMO (trechos dos logs)")
    print("front-end — amostra de decisões de quórum:")
    for line in re.findall(r".*quorum_decision.*", fe_txt)[:3]:
        print("   ", line.strip())
    print(f"front-end — total de decisões de quórum: "
          f"{len(re.findall(r'quorum_decision', fe_txt))}")
    print("\nréplica atrasada — recuperação por NACK:")
    for line in re.findall(r".*(?:gap_detected|nack).*", rm3_txt)[:3]:
        print("   ", line.strip())
    for line in re.findall(r".*nack_handled.*", fe_txt)[:3]:
        print("    FE:", line.strip())

    # Asserções finais
    ok = True
    for name, (la, nr, ts) in states.items():
        if nr != total_ops or ts != expected_sum:
            ok = False
    fe_state = states[nodes[0].name]
    all_match = all(s == fe_state for s in states.values())

    banner("VEREDITO")
    if ok and all_match:
        print(f"PASSOU: front-end e as {len(nodes) - 1} réplicas convergiram para")
        print(f"        num_reqs={total_ops} total_sum={expected_sum} (idêntico em todos).")
        print("        A réplica atrasada recuperou as operações perdidas via NACK.")
    else:
        print("FALHOU: há divergência entre os nós ou o total está incorreto.")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
