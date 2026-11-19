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
import asyncio

from collections import deque
from typing import Final, Any, cast, Optional, Set
from enum import Enum, auto

from arquitetura import ClusterConfig, MembershipState, NodeRole, RingState
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

class Role(Enum):
    FRONTEND = auto()
    REPLICA_MANAGER = auto()

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

    def active_replicas(self) -> dict[str, ReplicaDescriptor]:
        now = time.monotonic()
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
        sender: tuple[str, int],
    ) -> None:
        response, event = self.processor.process_request(message, sender)
        if event is not None:
            print(server_processing_event(event), flush=True)
            if event.kind == STATUS_PROCESSED and response is not None:
                self.replicate(message, event.seq_id, response, sender)
                return
        if response is not None:
            # Duplicata / fora de ordem: responde na hora (nada a replicar).
            self.sock.sendto(response, sender)

    def record_op(self, message: dict[str, Any], seq_id: int) -> None:
        self.op_log[seq_id] = {
            "client_id": message["client_id"],
            "request_id": message["request_id"],
            "value": message["value"],
            "num_reqs": self.processor.num_reqs,
            "total_sum": self.processor.total_sum,
        }
        self.op_log_order.append(seq_id)
        while len(self.op_log_order) > OP_LOG_MAX:
            self.op_log.pop(self.op_log_order.popleft(), None)

    def disseminate(self, seq_id: int, message: dict[str, Any]) -> int:
        """Envia a operação ordenada por unicast a cada réplica ativa."""
        packet = make_ordered_request(
            self.frontend_id,
            seq_id,
            message["client_id"],
            message["request_id"],
            message["value"],
        )
        replicas = self.active_replicas()
        for replica in replicas.values():
            try:
                self.sock.sendto(packet, (replica.host, replica.port))
            except OSError as exc:
                print(
                    f"disseminate_failed seq_id {seq_id} node_id {replica.node_id} "
                    f"error {exc}",
                    flush=True,
                )
        return len(replicas)

    def replicate(
        self,
        message: dict[str, Any],
        seq_id: int,
        response: bytes,
        client_addr: tuple[str, int],
    ) -> None:
        self.record_op(message, seq_id)
        active = self.disseminate(seq_id, message)
        target = self.cluster_config.quorum_target(active)
        print(
            f"disseminated seq_id {seq_id} active_replicas {active} "
            f"quorum_target {target}",
            flush=True,
        )

        if target <= 0:
            # Sem réplicas: o FE é o único membro, responde imediatamente.
            self.sock.sendto(response, client_addr)
            return

        tracker = QuorumTracker(
            seq_id=seq_id,
            expected_num_reqs=self.processor.num_reqs,
            expected_total_sum=self.processor.total_sum,
            target=target,
            deadline=time.monotonic() + self.cluster_config.receipt_ack_timeout_seconds,
            client_addr=client_addr,
            response=response,
        )
        self.quorum_trackers[seq_id] = tracker
        self.pending_order.append(seq_id)
        self.drain_quorum()

    # ------------------------------------------------------------------ #
    # Quórum de aplicação                                                 #
    # ------------------------------------------------------------------ #
    def progress_count(self, seq_id: int) -> int:
        return sum(1 for applied in self.replica_progress.values() if applied >= seq_id)

    def finalize_tracker(self, tracker: QuorumTracker, reason: str) -> None:
        tracker.finalized = True
        if not tracker.replied:
            self.sock.sendto(tracker.response, tracker.client_addr)
            tracker.replied = True
        print(
            f"quorum_decision seq_id {tracker.seq_id} {reason} "
            f"num_reqs {tracker.expected_num_reqs} "
            f"total_sum {tracker.expected_total_sum}",
            flush=True,
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
            if tracker is None or tracker.finalized:
                self.pending_order.popleft()
                self.quorum_trackers.pop(seq_id, None)
                continue
            count = self.progress_count(seq_id)
            if count >= tracker.target:
                self.finalize_tracker(tracker, f"quorum_reached acks {count}/{tracker.target}")
                self.pending_order.popleft()
                self.quorum_trackers.pop(seq_id, None)
                continue
            break

    def process_replica_applied_ack(
        self,
        message: dict[str, Any],
        sender: tuple[str, int],
    ) -> None:
        node_id = message.get("node_id")
        last_applied = message.get("last_applied_seq_id")
        num_reqs = message.get("num_reqs")
        total_sum = message.get("total_sum")
        if message.get("frontend_id") != self.frontend_id:
            return
        if not isinstance(node_id, str):
            return
        if not _is_uint(last_applied) or not _is_uint(num_reqs) or not _is_uint(total_sum):
            return
        last_applied = cast(int, last_applied)
        num_reqs = cast(int, num_reqs)
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
        ):
            print(
                f"replica_divergence node_id {node_id} seq_id {last_applied} "
                f"replica num_reqs {num_reqs} total_sum {total_sum} "
                f"expected num_reqs {entry['num_reqs']} total_sum {entry['total_sum']}",
                flush=True,
            )

        previous = self.replica_progress.get(node_id, 0)
        if last_applied > previous:
            self.replica_progress[node_id] = last_applied
            self.drain_quorum()

    def process_replica_nack(
        self,
        message: dict[str, Any],
        sender: tuple[str, int],
    ) -> None:
        node_id = message.get("node_id")
        missing = message.get("missing")
        if message.get("frontend_id") != self.frontend_id:
            return
        if not isinstance(node_id, str):
            return
        replica = self.registered_replicas.get(node_id)
        if replica is None or replica.host != sender[0]:
            return
        if not isinstance(missing, list):
            return

        resent = 0
        unsatisfiable = 0
        for seq_id in missing[:NACK_MAX_SEQS]:
            if not _is_uint(seq_id) or seq_id == 0:
                continue
            entry = self.op_log.get(seq_id)
            if entry is None:
                unsatisfiable += 1
                continue
            packet = make_ordered_request(
                self.frontend_id,
                seq_id,
                entry["client_id"],
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
                    tracker,
                    f"quorum_timeout acks {self.progress_count(tracker.seq_id)}"
                    f"/{tracker.target}",
                )
        self.drain_quorum()

    def process_replica_registration(
        self,
        message: dict[str, Any],
        sender: tuple[str, int],
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
            is_new = node_id not in self.registered_replicas
            self.registered_replicas[node_id] = replica
            action = "registered" if is_new else "registration_refreshed"
            print(
                f"{action} replica_manager {node_id} "
                f"addr {replica.host}:{replica.port}",
                flush=True,
            )
            if previous is not None and previous.host != sender[0]:
                print(
                    f"replica_endpoint_changed node_id {node_id} "
                    f"old_host {previous.host} new_host {sender[0]}",
                    flush=True,
                )
        else:
            node_id = node_id if isinstance(node_id, str) else "invalid"

        self.sock.sendto(
            make_replica_registered(node_id, self.frontend_id, accepted),
            sender,
        )

    def run(self) -> None:
        print(
            server_started(
                self.processor.num_reqs,
                self.processor.total_sum,
                self.frontend_id,
            ),
            flush=True,
        )
        try:
            while True:
                try:
                    data, sender = self.sock.recvfrom(BUFFER_SIZE)
                except ConnectionResetError:
                    self.maintain()
                    continue
                except socket.timeout:
                    self.maintain()
                    continue
                message = decode_message(data)
                if message is not None:
                    message_type = message.get("type")
                    if message_type == TYPE_DISCOVER_FRONTEND:
                        self.process_discovery(sender)
                    elif message_type == TYPE_SUM_REQUEST:
                        self.process_request(message, sender)
                    elif message_type == TYPE_REPLICA_REGISTER:
                        self.process_replica_registration(message, sender)
                    elif message_type == TYPE_REPLICA_HEARTBEAT:
                        self.process_replica_heartbeat(message, sender)
                    elif message_type == TYPE_REPLICA_APPLIED_ACK:
                        self.process_replica_applied_ack(message, sender)
                    elif message_type == TYPE_REPLICA_NACK:
                        self.process_replica_nack(message, sender)
                self.maintain()
        except KeyboardInterrupt:
            pass
        finally:
            print(
                f"final_state frontend_id {self.frontend_id} "
                f"num_reqs {self.processor.num_reqs} "
                f"total_sum {self.processor.total_sum}",
                flush=True,
            )
            self.sock.close()


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
            self.sock.bind(("", 0))
            self.listen_port = self.sock.getsockname()[1]
        except OSError:
            self.sock.close()
            raise

    def register(self) -> None:
        packet = make_replica_register(self.node_id, self.listen_port)
        self.sock.settimeout(REGISTRATION_TIMEOUT_SECONDS)
        for _ in range(REGISTRATION_ATTEMPTS):
            self.sock.sendto(packet, self.frontend_addr)
            while True:
                try:
                    data, sender = self.sock.recvfrom(BUFFER_SIZE)
                except ConnectionResetError:
                    continue
                except socket.timeout:
                    break
                if sender != self.frontend_addr:
                    continue
                message = decode_message(data)
                if (
                    message is None
                    or message.get("type") != TYPE_REPLICA_REGISTERED
                    or message.get("node_id") != self.node_id
                ):
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
                self.frontend_id,
                self.state.last_applied_seq_id,
                self.state.num_reqs,
                self.state.total_sum,
            ),
            self.frontend_addr,
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
                f"num_reqs {self.state.num_reqs} total_sum {self.state.total_sum}",
                flush=True,
            )
        if progressed:
            self.send_applied_ack()

    def request_missing(self, force: bool = False) -> None:
        """Se há lacuna na cabeça do buffer, pede retransmissão (NACK)."""
        if not self.holdback:
            return
        head = self.state.last_applied_seq_id + 1
        if head in self.holdback:
            return
        now = time.monotonic()
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
        )
        print(
            f"gap_detected node_id {self.node_id} last_applied "
            f"{self.state.last_applied_seq_id} nack_missing "
            f"{missing[0]}..{missing[-1]} count {len(missing)}",
            flush=True,
        )

    def handle_ordered_request(self, message: dict[str, Any]) -> None:
        seq_id = message.get("seq_id")
        client_id = message.get("client_id")
        request_id = message.get("request_id")
        value = message.get("value")
        if message.get("frontend_id") != self.frontend_id:
            return
        if (
            not _is_uint(seq_id)
            or seq_id == 0
            or not isinstance(client_id, str)
            or not client_id
            or not _is_uint(request_id)
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
            print(
                f"role replica_manager node_id {self.node_id} "
                f"frontend_id {self.frontend_id} listen_port {self.listen_port} "
                "status registered data_plane active_replication",
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
                f"frontend_discovered frontend_id {frontend_id} "
                f"addr {frontend_addr[0]}:{frontend_addr[1]}",
                flush=True,
            )
            ReplicaManagerNode(
                self.node_id,
                frontend_addr,
                frontend_id,
                self.cluster_config,
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
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and 0 <= value <= UINT64_MAX
    )


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
        default=0,
        help="quantidade esperada para métricas; 0 usa o membership ativo",
    )
    parser.add_argument(
        "--receipt-ack-quorum",
        type=int,
        default=0,
        help="meta de ACKs de aplicação; 0 deriva a maioria dos membros",
    )
    args = parser.parse_args(argv[1:])
    if not 1 <= args.port <= 65535:
        parser.error("port deve estar entre 1 e 65535")
    try:
        config = ClusterConfig(
            heartbeat_interval_seconds=args.heartbeat_interval,
            heartbeat_timeout_seconds=args.heartbeat_timeout,
            receipt_ack_timeout_seconds=args.receipt_ack_timeout,
            expected_replicas=args.expected_replicas,
            receipt_ack_quorum=args.receipt_ack_quorum,
        )
    except ValueError as exc:
        parser.error(str(exc))
    return args.port, config


def main() -> None:
    port, cluster_config = parse_args(sys.argv)
    DistributedServer(port, cluster_config).run()

class RingCreator:
    """Cria um anel de descoberta de front-end e réplicas."""

    def __init__(self, nodes: list[str]):
        if len(nodes) < 2:
            raise ValueError("é necessário pelo menos 2 nós para formar um anel")
        self.nodes = sorted(nodes)

    def create_ring(self) -> list[RingState]:
        ring_states = []
        num_nodes = len(self.nodes)
        for i, node in enumerate(self.nodes):
            first_node = self.nodes[0]
            previous_node = self.nodes[i - 1] if i > 0 else self.nodes[-1]
            next_node = self.nodes[(i + 1) % num_nodes]
            ring_states.append(
                RingState(
                    first_node=first_node,
                    previous_node=previous_node,
                    next_node=next_node,
                )
            )
        return ring_states

if __name__ == "__main__":
    main()
