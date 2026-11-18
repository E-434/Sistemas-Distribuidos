#!/usr/bin/env python3
"""Front-end UDP com sequenciamento, multicast e métricas de recebimento."""

from __future__ import annotations

import socket
import sys
import argparse
import struct
import time
import uuid
from collections import deque
from typing import Final, Any

from arquitetura import ClusterConfig, MembershipState, NodeRole
from arquitetura import ReceiptAckTracker, ReplicaDescriptor
from descoberta import discover_frontend, respond_to_discovery
from interface import server_processing_event, server_started
from processamento import RequestProcessor
from protocolo import (
    TYPE_DISCOVER_FRONTEND,
    TYPE_ORDERED_REQUEST,
    TYPE_REPLICA_HEARTBEAT,
    TYPE_REPLICA_RECEIPT_ACK,
    TYPE_REPLICA_REGISTER,
    TYPE_REPLICA_REGISTERED,
    TYPE_SUM_REQUEST,
    STATUS_PROCESSED,
    STATUS_RECEIVED,
    decode_message,
    make_ordered_request,
    make_replica_heartbeat,
    make_replica_receipt_ack,
    make_replica_registered,
    make_replica_register,
)

BUFFER_SIZE: Final[int] = 4096
MEMBERSHIP_SWEEP_INTERVAL_SECONDS: Final[float] = 0.2
MAX_COMPLETED_ACK_TRACKERS: Final[int] = 1024
REGISTRATION_TIMEOUT_SECONDS: Final[float] = 1.0
REGISTRATION_ATTEMPTS: Final[int] = 5
UINT64_MAX: Final[int] = (1 << 64) - 1


class FrontendServer:
    """Front-end com multicast/ACKs de recebimento, sem replicação ou eleição."""

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
        self.multicast_addr = (
            self.cluster_config.multicast_group,
            self.cluster_config.multicast_port,
        )
        self.multicast_sock = socket.socket(
            socket.AF_INET,
            socket.SOCK_DGRAM,
            socket.IPPROTO_UDP,
        )
        try:
            self.multicast_sock.setsockopt(
                socket.IPPROTO_IP,
                socket.IP_MULTICAST_TTL,
                1,
            )
            self.multicast_sock.setsockopt(
                socket.IPPROTO_IP,
                socket.IP_MULTICAST_LOOP,
                1,
            )
        except OSError:
            self.multicast_sock.close()
            self.sock.close()
            raise
        self.membership = MembershipState(
            node_id=self.frontend_id,
            role=NodeRole.FRONTEND,
        )
        self.processor = RequestProcessor()
        self.registered_replicas: dict[str, ReplicaDescriptor] = {}
        self.receipt_ack_trackers: dict[int, ReceiptAckTracker] = {}
        self.completed_ack_sequences: deque[int] = deque()

    def process_discovery(self, sender: tuple[str, int]) -> None:
        respond_to_discovery(self.sock, sender, self.frontend_id)

    def process_request(
        self,
        message: dict[str, Any],
        sender: tuple[str, int],
    ) -> None:
        response, event = self.processor.process_request(message, sender)
        if event is not None:
            print(server_processing_event(event), flush=True)
            if event.kind == STATUS_PROCESSED:
                self.multicast_request(message, event.seq_id)
        if response is not None:
            self.sock.sendto(response, sender)

    def active_replicas(self) -> dict[str, ReplicaDescriptor]:
        now = time.monotonic()
        return {
            node_id: replica
            for node_id, replica in self.registered_replicas.items()
            if replica.status == "active"
            and now - replica.last_seen < self.cluster_config.heartbeat_timeout_seconds
        }

    def multicast_request(self, message: dict[str, Any], seq_id: int) -> None:
        replicas = self.active_replicas()
        expected_nodes = frozenset(replicas)
        target = self.cluster_config.receipt_ack_target(len(expected_nodes))
        tracker = ReceiptAckTracker(
            seq_id=seq_id,
            expected_nodes=expected_nodes,
            target=target,
            deadline=time.monotonic()
            + self.cluster_config.receipt_ack_timeout_seconds,
        )
        self.receipt_ack_trackers[seq_id] = tracker
        try:
            packet = make_ordered_request(
                self.frontend_id,
                seq_id,
                message["client_id"],
                message["request_id"],
                message["value"],
            )
            self.multicast_sock.sendto(packet, self.multicast_addr)
        except OSError as exc:
            self.receipt_ack_trackers.pop(seq_id, None)
            print(
                f"multicast_send_failed seq_id {seq_id} error {exc}",
                flush=True,
            )
            return

        configured_expected = self.cluster_config.expected_replicas
        if configured_expected and configured_expected != len(expected_nodes):
            print(
                f"replica_membership_mismatch seq_id {seq_id} "
                f"configured_expected {configured_expected} "
                f"active_registered {len(expected_nodes)}",
                flush=True,
            )
        print(
            f"multicast_sent seq_id {seq_id} active_replicas {len(expected_nodes)} "
            f"receipt_ack_target {target}",
            flush=True,
        )
        if not expected_nodes:
            self.finalize_receipt_tracker(tracker, "no_active_replicas")

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

    def process_replica_receipt_ack(
        self,
        message: dict[str, Any],
        sender: tuple[str, int],
    ) -> None:
        node_id = message.get("node_id")
        seq_id = message.get("seq_id")
        if message.get("frontend_id") != self.frontend_id:
            return
        if message.get("status") != STATUS_RECEIVED:
            return
        if not isinstance(node_id, str):
            return
        if not isinstance(seq_id, int) or isinstance(seq_id, bool) or seq_id <= 0:
            return
        replica = self.registered_replicas.get(node_id)
        if replica is None or replica.host != sender[0]:
            print(
                f"receipt_ack_rejected node_id {node_id} seq_id {seq_id} "
                "reason unregistered_or_wrong_host",
                flush=True,
            )
            return
        tracker = self.receipt_ack_trackers.get(seq_id)
        if tracker is None:
            print(
                f"receipt_ack_ignored node_id {node_id} seq_id {seq_id} "
                "reason unknown_or_expired_sequence",
                flush=True,
            )
            return
        if not tracker.record(node_id):
            print(
                f"receipt_ack_ignored node_id {node_id} seq_id {seq_id} "
                "reason duplicate_or_finalized",
                flush=True,
            )
            return
        print(
            f"receipt_ack_received node_id {node_id} seq_id {seq_id} "
            f"acks {len(tracker.received_nodes)}/{len(tracker.expected_nodes)}",
            flush=True,
        )

    def finalize_receipt_tracker(
        self,
        tracker: ReceiptAckTracker,
        reason: str,
    ) -> None:
        if tracker.finalized:
            return
        tracker.finalized = True
        print(
            f"receipt_ack_summary seq_id {tracker.seq_id} "
            f"received {len(tracker.received_nodes)} "
            f"expected {len(tracker.expected_nodes)} target {tracker.target} "
            f"target_met {str(tracker.target_met).lower()} reason {reason} "
            "ack_type receipt_only",
            flush=True,
        )
        self.completed_ack_sequences.append(tracker.seq_id)
        while len(self.completed_ack_sequences) > MAX_COMPLETED_ACK_TRACKERS:
            old_seq_id = self.completed_ack_sequences.popleft()
            self.receipt_ack_trackers.pop(old_seq_id, None)

    def maintain_membership_and_ack_trackers(self) -> None:
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

        for tracker in tuple(self.receipt_ack_trackers.values()):
            if tracker.finalized:
                continue
            all_expected_acked = (
                len(tracker.received_nodes) == len(tracker.expected_nodes)
            )
            if all_expected_acked:
                self.finalize_receipt_tracker(tracker, "all_active_replicas_acked")
            elif now >= tracker.deadline:
                self.finalize_receipt_tracker(tracker, "ack_timeout")

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
                    self.maintain_membership_and_ack_trackers()
                    continue
                except socket.timeout:
                    self.maintain_membership_and_ack_trackers()
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
                    elif message_type == TYPE_REPLICA_RECEIPT_ACK:
                        self.process_replica_receipt_ack(message, sender)
                self.maintain_membership_and_ack_trackers()
        except KeyboardInterrupt:
            pass
        finally:
            self.sock.close()
            self.multicast_sock.close()


class ReplicaManagerNode:
    """Componente RM executado pelo mesmo servidor quando outro FE foi encontrado."""

    def __init__(
        self,
        node_id: str,
        listen_port: int,
        frontend_addr: tuple[str, int],
        frontend_id: str,
        cluster_config: ClusterConfig,
    ) -> None:
        self.node_id = node_id
        self.listen_port = listen_port
        self.frontend_addr = frontend_addr
        self.frontend_id = frontend_id
        self.cluster_config = cluster_config
        self.membership = MembershipState(
            node_id=node_id,
            role=NodeRole.REPLICA_MANAGER,
            leader_id=frontend_id,
        )
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind(("", listen_port))
            group = socket.inet_aton(cluster_config.multicast_group)
            membership_request = struct.pack(
                "4s4s",
                group,
                socket.inet_aton("0.0.0.0"),
            )
            self.sock.setsockopt(
                socket.IPPROTO_IP,
                socket.IP_ADD_MEMBERSHIP,
                membership_request,
            )
            self.sock.settimeout(0.2)
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

    def process_ordered_request(self, message: dict[str, Any]) -> None:
        seq_id = message.get("seq_id")
        client_id = message.get("client_id")
        request_id = message.get("request_id")
        value = message.get("value")
        if message.get("frontend_id") != self.frontend_id:
            print(
                f"ordered_request_rejected node_id {self.node_id} "
                "reason frontend_id_mismatch",
                flush=True,
            )
            return
        if (
            not isinstance(seq_id, int)
            or isinstance(seq_id, bool)
            or not 1 <= seq_id <= UINT64_MAX
            or not isinstance(client_id, str)
            or not client_id
            or len(client_id) > 128
            or not isinstance(request_id, int)
            or isinstance(request_id, bool)
            or not 1 <= request_id <= UINT64_MAX
            or not isinstance(value, int)
            or isinstance(value, bool)
            or not 0 <= value <= UINT64_MAX
        ):
            print(
                f"ordered_request_rejected node_id {self.node_id} "
                "reason invalid_fields",
                flush=True,
            )
            return

        previous_seq_id = self.membership.last_received_seq_id
        if seq_id == previous_seq_id:
            sequence_status = "duplicate"
        elif seq_id < previous_seq_id:
            sequence_status = "out_of_order"
        elif previous_seq_id and seq_id > previous_seq_id + 1:
            sequence_status = f"gap_after_{previous_seq_id}"
            self.membership.last_received_seq_id = seq_id
        else:
            sequence_status = "in_order"
            self.membership.last_received_seq_id = seq_id

        print(
            f"ordered_request_received node_id {self.node_id} "
            f"seq_id {seq_id} client_id {client_id} request_id {request_id} "
            f"value {value} sequence_status {sequence_status} "
            "operation_applied false",
            flush=True,
        )
        self.sock.sendto(
            make_replica_receipt_ack(
                self.node_id,
                self.frontend_id,
                seq_id,
                STATUS_RECEIVED,
            ),
            self.frontend_addr,
        )

    def send_heartbeat(self) -> None:
        self.sock.sendto(
            make_replica_heartbeat(self.node_id, self.frontend_id),
            self.frontend_addr,
        )

    def run(self) -> None:
        try:
            self.register()
            print(
                f"role replica_manager node_id {self.node_id} "
                f"frontend_id {self.frontend_id} "
                f"listen_port {self.listen_port} status registered "
                "data_plane receipt_only",
                flush=True,
            )
            self.sock.settimeout(
                min(0.2, self.cluster_config.heartbeat_interval_seconds)
            )
            next_heartbeat = time.monotonic()
            while True:
                now = time.monotonic()
                if now >= next_heartbeat:
                    self.send_heartbeat()
                    next_heartbeat = now + self.cluster_config.heartbeat_interval_seconds

                try:
                    data, _sender = self.sock.recvfrom(BUFFER_SIZE)
                except ConnectionResetError:
                    continue
                except socket.timeout:
                    continue
                message = decode_message(data)
                if message is None:
                    print(
                        f"datagram_rejected node_id {self.node_id} "
                        "reason malformed_or_wrong_version",
                        flush=True,
                    )
                elif message.get("type") == TYPE_ORDERED_REQUEST:
                    self.process_ordered_request(message)
        except KeyboardInterrupt:
            pass
        finally:
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
                self.cluster_config.multicast_port,
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


def parse_args(argv: list[str]) -> tuple[int, ClusterConfig]:
    parser = argparse.ArgumentParser(
        description=(
            "Servidor UDP unificado: descobre o front-end ativo e inicia como "
            "Replica Manager; se não encontrar um, usa a eleição dummy e inicia como FE."
        )
    )
    parser.add_argument("port", type=int)
    parser.add_argument("--multicast-group", default="239.10.10.10")
    parser.add_argument("--multicast-port", type=int, default=5001)
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
        help="meta de ACKs de recebimento; 0 deriva maioria, sem validar operações",
    )
    args = parser.parse_args(argv[1:])
    if not 1 <= args.port <= 65535:
        parser.error("port deve estar entre 1 e 65535")
    if not 1 <= args.multicast_port <= 65535:
        parser.error("multicast-port deve estar entre 1 e 65535")
    try:
        config = ClusterConfig(
            multicast_group=args.multicast_group,
            multicast_port=args.multicast_port,
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


if __name__ == "__main__":
    main()
