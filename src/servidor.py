#!/usr/bin/env python3
"""Servidor UDP com replicação ativa (state-machine replication).

O front-end (FE) sequencia as operações (`seq_id` global) e as dissemina por
UNICAST, em laço, para cada Replica Manager (RM) ativo. Cada RM mantém sua
própria cópia da máquina de estados e APLICA cada operação na ordem do
`seq_id`; como todos aplicam a mesma sequência, convergem para o mesmo estado
(replicação ativa). O FE só confirma a operação ao cliente após um QUÓRUM de
réplicas confirmar a aplicação. Entregas perdidas pelo UDP são recuperadas por
hold-back + NACK/retransmissão a partir de um log de operações no FE.
"""

from __future__ import annotations



import socket
import sys
import argparse
import time

import uuid
from collections import deque
from typing import Final, Any, cast

from arquitetura import ClusterConfig, MembershipState, NodeRole
from arquitetura import QuorumTracker, ReplicaDescriptor







from descoberta import discover_frontend, respond_to_discovery
from interface import server_processing_event, server_started
from processamento import ReplicatedSumState, RequestProcessor
from protocolo import (


    TYPE_DISCOVER_FRONTEND,


    TYPE_ORDERED_REQUEST,
    TYPE_REPLICA_APPLIED_ACK,
    TYPE_REPLICA_HEARTBEAT,

    TYPE_REPLICA_NACK,
    TYPE_REPLICA_REGISTER,
    TYPE_REPLICA_REGISTERED,

    TYPE_SUM_REQUEST,
    STATUS_PROCESSED,
    decode_message,



    make_ordered_request,
    make_replica_applied_ack,
    make_replica_heartbeat,

    make_replica_nack,
    make_replica_registered,
    make_replica_register,


)

BUFFER_SIZE: Final[int] = 4096
MEMBERSHIP_SWEEP_INTERVAL_SECONDS: Final[float] = 0.2
OP_LOG_MAX: Final[int] = 200_000
NACK_MAX_SEQS: Final[int] = 256
NACK_MIN_INTERVAL_SECONDS: Final[float] = 0.1



REGISTRATION_TIMEOUT_SECONDS: Final[float] = 1.0
REGISTRATION_ATTEMPTS: Final[int] = 5

UINT64_MAX: Final[int] = (1 << 64) - 1















class FrontendServer:
    """FE: sequenciador, disseminador unicast e coletor de quórum de aplicação."""

    def __init__(
        self,
        port: int,
        frontend_id: str | None = None,
        cluster_config: ClusterConfig | None = None,



    ) -> None:
        self.cluster_config = cluster_config or ClusterConfig()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock.bind(("", port))






        self.sock.settimeout(MEMBERSHIP_SWEEP_INTERVAL_SECONDS)
        self.port = self.sock.getsockname()[1]
        self.frontend_id = frontend_id or f"frontend-{self.port}"
        self.membership = MembershipState(
            node_id=self.frontend_id,
            role=NodeRole.FRONTEND,

        )
        self.processor = RequestProcessor()
        self.registered_replicas: dict[str, ReplicaDescriptor] = {}













        # Log de operações para retransmissão (resposta a NACK) e para que
        # réplicas atrasadas recuperem o que perderam.
        self.op_log: dict[int, dict[str, Any]] = {}
        self.op_log_order: deque[int] = deque()

        # Quórum de aplicação: operações aguardando confirmação das réplicas.
        self.quorum_trackers: dict[int, QuorumTracker] = {}
        self.pending_order: deque[int] = deque()
        # Progresso contíguo aplicado por cada réplica (node_id -> last_applied).
        self.replica_progress: dict[str, int] = {}
















































































    def process_discovery(self, sender: tuple[str, int]) -> None:
        respond_to_discovery(self.sock, sender, self.frontend_id)

@@ -97,13 +219,11 @@ def active_replicas(self) -> dict[str, ReplicaDescriptor]:
        return {
            node_id: replica
            for node_id, replica in self.registered_replicas.items()
            if replica.status == "active"

            and now - replica.last_seen < self.cluster_config.heartbeat_timeout_seconds
        }

    # ------------------------------------------------------------------ #
    # Canal do cliente + disseminação                                    #
    # ------------------------------------------------------------------ #
    def process_request(
        self,
        message: dict[str, Any],
@@ -116,7 +236,6 @@ def process_request(
                self.replicate(message, event.seq_id, response, sender)
                return
        if response is not None:
            # Duplicata / fora de ordem: responde na hora (nada a replicar).
            self.sock.sendto(response, sender)

    def record_op(self, message: dict[str, Any], seq_id: int) -> None:
@@ -132,7 +251,6 @@ def record_op(self, message: dict[str, Any], seq_id: int) -> None:
            self.op_log.pop(self.op_log_order.popleft(), None)

    def disseminate(self, seq_id: int, message: dict[str, Any]) -> int:
        """Envia a operação ordenada por unicast a cada réplica ativa."""
        packet = make_ordered_request(
            self.frontend_id,
            seq_id,
@@ -169,7 +287,6 @@ def replicate(
        )

        if target <= 0:
            # Sem réplicas: o FE é o único membro, responde imediatamente.
            self.sock.sendto(response, client_addr)
            return

@@ -186,9 +303,6 @@ def replicate(
        self.pending_order.append(seq_id)
        self.drain_quorum()

    # ------------------------------------------------------------------ #
    # Quórum de aplicação                                                 #
    # ------------------------------------------------------------------ #
    def progress_count(self, seq_id: int) -> int:
        return sum(1 for applied in self.replica_progress.values() if applied >= seq_id)

@@ -205,12 +319,6 @@ def finalize_tracker(self, tracker: QuorumTracker, reason: str) -> None:
        )

    def drain_quorum(self) -> None:
        """Confirma ao cliente, em ordem, as operações que atingiram quórum.

        O progresso de cada réplica é monotônico, então o tracker de menor
        `seq_id` é sempre o mais fácil de satisfazer: se o da frente ainda não
        tem quórum, nenhum posterior tem.
        """
        while self.pending_order:
            seq_id = self.pending_order[0]
            tracker = self.quorum_trackers.get(seq_id)
@@ -220,7 +328,9 @@ def drain_quorum(self) -> None:
                continue
            count = self.progress_count(seq_id)
            if count >= tracker.target:
                self.finalize_tracker(tracker, f"quorum_reached acks {count}/{tracker.target}")


                self.pending_order.popleft()
                self.quorum_trackers.pop(seq_id, None)
                continue
@@ -235,8 +345,6 @@ def process_replica_applied_ack(
        last_applied = message.get("last_applied_seq_id")
        num_reqs = message.get("num_reqs")
        total_sum = message.get("total_sum")
        if message.get("frontend_id") != self.frontend_id:
            return
        if not isinstance(node_id, str):
            return
        if not _is_uint(last_applied) or not _is_uint(num_reqs) or not _is_uint(total_sum):
@@ -246,21 +354,14 @@ def process_replica_applied_ack(
        total_sum = cast(int, total_sum)
        replica = self.registered_replicas.get(node_id)
        if replica is None or replica.host != sender[0]:
            print(
                f"applied_ack_rejected node_id {node_id} "
                "reason unregistered_or_wrong_host",
                flush=True,
            )
            return

        # Um ACK também prova atividade: atualiza membership.
        replica.last_seen = time.monotonic()
        if replica.status != "active":
            replica.status = "active"
            print(f"replica_recovered node_id {node_id}", flush=True)


        # Verificação de divergência: compara o estado da réplica com o estado
        # correto do FE no mesmo ponto da sequência.
        entry = self.op_log.get(last_applied)
        if entry is not None and (
            num_reqs != entry["num_reqs"] or total_sum != entry["total_sum"]
@@ -284,8 +385,6 @@ def process_replica_nack(
    ) -> None:
        node_id = message.get("node_id")
        missing = message.get("missing")
        if message.get("frontend_id") != self.frontend_id:
            return
        if not isinstance(node_id, str):
            return
        replica = self.registered_replicas.get(node_id)
@@ -310,59 +409,57 @@ def process_replica_nack(
                entry["request_id"],
                entry["value"],
            )
            # Retransmissão unicast direta para a réplica que pediu.
            self.sock.sendto(packet, sender)
            resent += 1
        print(
            f"nack_handled node_id {node_id} resent {resent} "
            f"unsatisfiable {unsatisfiable}",
            flush=True,
        )


    # ------------------------------------------------------------------ #
    # Membership                                                          #
    # ------------------------------------------------------------------ #
    def process_replica_heartbeat(
        self,
        message: dict[str, Any],
        sender: tuple[str, int],
    ) -> None:
        node_id = message.get("node_id")
        if message.get("frontend_id") != self.frontend_id:
            return
        if not isinstance(node_id, str):
            return
        replica = self.registered_replicas.get(node_id)
        if replica is None or replica.host != sender[0]:
            print(
                f"heartbeat_rejected node_id {node_id} "
                "reason unregistered_or_wrong_host",
                flush=True,
            )
            return
        was_suspect = replica.status != "active"
        replica.last_seen = time.monotonic()
        replica.status = "active"




        if was_suspect:
            print(f"replica_recovered node_id {node_id}", flush=True)


    def maintain(self) -> None:
        now = time.monotonic()

        for replica in self.registered_replicas.values():
            if (
                replica.status == "active"
                and now - replica.last_seen
                >= self.cluster_config.heartbeat_timeout_seconds
            ):
                replica.status = "suspect"

                print(
                    f"replica_suspect node_id {replica.node_id} "
                    f"last_seen_seconds {now - replica.last_seen:.3f}",
                    flush=True,
                )

        # Operações sem quórum dentro do prazo: confirma assim mesmo (o
        # resultado do FE é correto pela ordem total) e registra o timeout.

        for tracker in list(self.quorum_trackers.values()):
            if not tracker.finalized and now >= tracker.deadline:
                self.finalize_tracker(
@@ -379,20 +476,26 @@ def process_replica_registration(
    ) -> None:
        node_id = message.get("node_id")
        listen_port = message.get("listen_port")

        accepted = (
            isinstance(node_id, str)
            and bool(node_id)
            and len(node_id) <= 128

            and isinstance(listen_port, int)
            and not isinstance(listen_port, bool)
            and 1 <= listen_port <= 65535



        )
        if accepted:
            previous = self.registered_replicas.get(node_id)
            replica = ReplicaDescriptor(
                node_id=node_id,
                host=sender[0],
                port=listen_port,

                last_seen=time.monotonic(),
                status="active",
            )
@@ -413,10 +516,22 @@ def process_replica_registration(
        else:
            node_id = node_id if isinstance(node_id, str) else "invalid"




        self.sock.sendto(
            make_replica_registered(node_id, self.frontend_id, accepted),







            sender,
        )



    def run(self) -> None:
        print(
@@ -427,6 +542,7 @@ def run(self) -> None:
            ),
            flush=True,
        )

        try:
            while True:
                try:
@@ -452,6 +568,25 @@ def run(self) -> None:
                        self.process_replica_applied_ack(message, sender)
                    elif message_type == TYPE_REPLICA_NACK:
                        self.process_replica_nack(message, sender)



















                self.maintain()
        except KeyboardInterrupt:
            pass
@@ -466,32 +601,22 @@ def run(self) -> None:


class ReplicaManagerNode:
    """RM: réplica ativa que aplica a sequência de operações e confirma o quórum."""

    def __init__(
        self,
        node_id: str,
        frontend_addr: tuple[str, int],
        frontend_id: str,
        cluster_config: ClusterConfig,


    ) -> None:
        self.node_id = node_id
        self.frontend_addr = frontend_addr
        self.frontend_id = frontend_id
        self.cluster_config = cluster_config
        self.membership = MembershipState(
            node_id=node_id,
            role=NodeRole.REPLICA_MANAGER,
            leader_id=frontend_id,
        )
        self.state = ReplicatedSumState()
        self.holdback: dict[int, int] = {}
        self._last_nack = 0.0

        # Um único socket unicast (porta efêmera) para TODO o diálogo com o FE:
        # recebe ORDERED_REQUEST e retransmissões; envia registro, heartbeat,
        # ACK de aplicação e NACK. A porta efêmera evita colisão quando vários
        # RMs rodam no mesmo host.
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
@@ -501,8 +626,135 @@ def __init__(
            self.sock.close()
            raise




























































































































    def register(self) -> None:
        packet = make_replica_register(self.node_id, self.listen_port)




        self.sock.settimeout(REGISTRATION_TIMEOUT_SECONDS)
        for _ in range(REGISTRATION_ATTEMPTS):
            self.sock.sendto(packet, self.frontend_addr)
@@ -524,13 +776,24 @@ def register(self) -> None:
                    continue
                if message.get("accepted") is not True:
                    raise RuntimeError("o front-end rejeitou o registro do Replica Manager")









                return
        raise RuntimeError(
            f"não foi possível registrar {self.node_id} no front-end "
            f"{self.frontend_addr[0]}:{self.frontend_addr[1]}"
        )

    def send_applied_ack(self) -> None:


        self.sock.sendto(
            make_replica_applied_ack(
                self.node_id,
@@ -543,20 +806,22 @@ def send_applied_ack(self) -> None:
        )

    def send_heartbeat(self) -> None:


        self.sock.sendto(
            make_replica_heartbeat(self.node_id, self.frontend_id),
            self.frontend_addr,
        )

    def drain_apply(self) -> None:
        """Aplica do buffer todas as operações contíguas disponíveis."""
        progressed = False
        while True:
            nxt = self.state.last_applied_seq_id + 1
            if nxt not in self.holdback:
                break
            value = self.holdback.pop(nxt)
            self.state.apply(nxt, value)

            progressed = True
            print(
                f"applied node_id {self.node_id} seq_id {nxt} value {value} "
@@ -567,8 +832,11 @@ def drain_apply(self) -> None:
            self.send_applied_ack()

    def request_missing(self, force: bool = False) -> None:
        """Se há lacuna na cabeça do buffer, pede retransmissão (NACK)."""
        if not self.holdback:



            return
        head = self.state.last_applied_seq_id + 1
        if head in self.holdback:
@@ -577,10 +845,16 @@ def request_missing(self, force: bool = False) -> None:
        if not force and now - self._last_nack < NACK_MIN_INTERVAL_SECONDS:
            return
        smallest_buffered = min(self.holdback)
        missing = list(range(head, smallest_buffered))[:NACK_MAX_SEQS]




        if not missing:
            return
        self._last_nack = now


        self.sock.sendto(
            make_replica_nack(self.node_id, self.frontend_id, missing),
            self.frontend_addr,
@@ -597,8 +871,6 @@ def handle_ordered_request(self, message: dict[str, Any]) -> None:
        client_id = message.get("client_id")
        request_id = message.get("request_id")
        value = message.get("value")
        if message.get("frontend_id") != self.frontend_id:
            return
        if (
            not _is_uint(seq_id)
            or seq_id == 0
@@ -608,24 +880,278 @@ def handle_ordered_request(self, message: dict[str, Any]) -> None:
            or request_id == 0
            or not _is_uint(value)
        ):
            print(
                f"ordered_request_rejected node_id {self.node_id} reason invalid_fields",
                flush=True,
            )
            return


        seq_id = cast(int, seq_id)

        value = cast(int, value)

        if seq_id <= self.state.last_applied_seq_id:
            # Retransmissão de algo já aplicado: reconfirma o progresso ao FE
            # (o ACK anterior pode ter se perdido) e descarta idempotentemente.
            self.send_applied_ack()
            return

        self.holdback[seq_id] = value

        self.drain_apply()
        self.request_missing()
































































































































































































































































    def run(self) -> None:
        try:
            self.register()
@@ -636,77 +1162,151 @@ def run(self) -> None:
                flush=True,
            )
            interval = self.cluster_config.heartbeat_interval_seconds
            self.sock.settimeout(min(0.2, interval))
            next_heartbeat = time.monotonic()
            while True:




                now = time.monotonic()
                if now >= next_heartbeat:
                    self.send_heartbeat()
                    next_heartbeat = now + interval
                # Reenvia NACK se uma lacuna persiste (retransmissão também
                # pode se perder).





















                self.request_missing()

                try:
                    data, _sender = self.sock.recvfrom(BUFFER_SIZE)
                except ConnectionResetError:
                    continue
                except socket.timeout:
                    continue






                message = decode_message(data)
                if message is None:
                    continue
                if message.get("type") == TYPE_ORDERED_REQUEST:

                    self.handle_ordered_request(message)


















        except KeyboardInterrupt:
            pass
        finally:
            print(
                f"final_state node_id {self.node_id} "
                f"last_applied {self.state.last_applied_seq_id} "
                f"num_reqs {self.state.num_reqs} total_sum {self.state.total_sum}",
                flush=True,
            )
            self.sock.close()



class DistributedServer:
    """Descobre o FE ativo e inicia como RM ou se promove via eleição dummy."""

    def __init__(self, port: int, cluster_config: ClusterConfig) -> None:
        self.port = port
        self.cluster_config = cluster_config
        self.node_id = f"server-{uuid.uuid4().hex[:12]}"

    def elect_frontend_dummy(self) -> bool:
        """Placeholder: sem coordenador ou votação, o próprio nó vira FE."""
        print(
            f"election_started node_id {self.node_id} algorithm dummy",

            flush=True,
        )
        print(
            f"election_result node_id {self.node_id} "
            "role frontend decision self_promotion",
            flush=True,
        )
        return True

    def run(self) -> None:
        discovery_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        discovery_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        discovery_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:






            try:
                frontend_addr, frontend_id = discover_frontend(
                    discovery_sock,
                    self.port,
                    self.node_id,
                )








            except RuntimeError:
                frontend_addr = None
                frontend_id = ""
        finally:
            discovery_sock.close()









        if frontend_addr is not None:
            print(
@@ -722,18 +1322,41 @@ def run(self) -> None:
            ).run()
            return

        if not self.elect_frontend_dummy():
            raise RuntimeError("a eleição dummy não selecionou um front-end")
        frontend_id = f"frontend-{self.port}-{self.node_id}"





























        print(
            f"frontend_starting frontend_id {frontend_id} port {self.port}",
            flush=True,
        )
        FrontendServer(
            self.port,
            frontend_id=frontend_id,
            cluster_config=self.cluster_config,
        ).run()


def _is_uint(value: Any) -> bool:
@@ -747,14 +1370,15 @@ def _is_uint(value: Any) -> bool:
def parse_args(argv: list[str]) -> tuple[int, ClusterConfig]:
    parser = argparse.ArgumentParser(
        description=(
            "Servidor UDP unificado: descobre o front-end ativo e inicia como "
            "Replica Manager; se não encontrar um, usa a eleição dummy e inicia como FE."
        )
    )
    parser.add_argument("port", type=int)
    parser.add_argument("--heartbeat-interval", type=float, default=1.0)
    parser.add_argument("--heartbeat-timeout", type=float, default=4.0)
    parser.add_argument("--receipt-ack-timeout", type=float, default=0.5)

    parser.add_argument(
        "--expected-replicas",
        type=int,
@@ -775,6 +1399,7 @@ def parse_args(argv: list[str]) -> tuple[int, ClusterConfig]:
            heartbeat_interval_seconds=args.heartbeat_interval,
            heartbeat_timeout_seconds=args.heartbeat_timeout,
            receipt_ack_timeout_seconds=args.receipt_ack_timeout,

            expected_replicas=args.expected_replicas,
            receipt_ack_quorum=args.receipt_ack_quorum,
        )
@@ -789,4 +1414,4 @@ def main() -> None:


if __name__ == "__main__":
    main()
