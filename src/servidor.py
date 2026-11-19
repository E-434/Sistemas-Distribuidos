#!/usr/bin/env python3
"""Servidor UDP com replicação ativa e eleição de líder em anel (maior IP)."""

from __future__ import annotations

import argparse
from collections import deque
import socket
import sys
import time
from typing import Any, Final, cast
import uuid

from arquitetura import (
    ClusterConfig,
    MembershipState,
    NodeRole,
    QuorumTracker,
    ReplicaDescriptor,
    RingNodeInfo,
    RingTopology,
)
from descoberta import discover_frontend, respond_to_discovery
from interface import server_processing_event, server_started
from processamento import ClientState, ReplicatedSumState, RequestProcessor
from protocolo import (
    STATUS_PROCESSED,
    TYPE_COORDINATOR,
    TYPE_DISCOVER_FRONTEND,
    TYPE_ELECTION,
    TYPE_ELECTION_ACK,
    TYPE_ORDERED_REQUEST,
    TYPE_REPLICA_APPLIED_ACK,
    TYPE_REPLICA_HEARTBEAT,
    TYPE_REPLICA_HEARTBEAT_ACK,
    TYPE_REPLICA_NACK,
    TYPE_REPLICA_REGISTER,
    TYPE_REPLICA_REGISTERED,
    TYPE_RING_MEMBERS,
    TYPE_SUM_REQUEST,
    decode_message,
    make_coordinator,
    make_election,
    make_election_ack,
    make_ordered_request,
    make_replica_applied_ack,
    make_replica_heartbeat,
    make_replica_heartbeat_ack,
    make_replica_nack,
    make_replica_register,
    make_replica_registered,
    make_ring_members,
)

BUFFER_SIZE: Final[int] = 4096
MEMBERSHIP_SWEEP_INTERVAL_SECONDS: Final[float] = 0.2
OP_LOG_MAX: Final[int] = 200_000
NACK_MAX_SEQS: Final[int] = 256
NACK_MIN_INTERVAL_SECONDS: Final[float] = 0.1
REGISTRATION_TIMEOUT_SECONDS: Final[float] = 1.0
REGISTRATION_ATTEMPTS: Final[int] = 5
UINT64_MAX: Final[int] = (1 << 64) - 1


def _local_ip_for(peer_ip: str = "127.0.0.1") -> str:
    if peer_ip in ("127.0.0.1", "localhost"):
        return "127.0.0.1"
    temp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        temp.connect((peer_ip, 1))
        return temp.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        temp.close()


class FrontendServer:
    """FE: sequenciador global, disseminador e coletor de quórum."""

    def __init__(
        self,
        port: int,
        frontend_id: str | None = None,
        cluster_config: ClusterConfig | None = None,
        initial_state: ReplicatedSumState | None = None,
        initial_members: list[dict[str, Any]] | None = None,
        existing_sock: socket.socket | None = None,
    ) -> None:
        self.cluster_config = cluster_config or ClusterConfig()
        if existing_sock is not None:
            self.sock = existing_sock
        else:
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
            leader_id=self.frontend_id,
        )
        self.processor = RequestProcessor()
        if initial_state is not None:
            self.processor.num_reqs = initial_state.num_reqs
            self.processor.total_sum = initial_state.total_sum
            self.processor.next_seq_id = initial_state.last_applied_seq_id + 1
            for cid, cstate in initial_state.clients.items():
                self.processor.clients[cid] = ClientState(
                    last_request_id=cstate.last_request_id,
                    last_seq_id=cstate.last_seq_id,
                    last_num_reqs=cstate.last_num_reqs,
                    last_total_sum=cstate.last_total_sum,
                )

        self.registered_replicas: dict[str, ReplicaDescriptor] = {}
        self.op_log: dict[int, dict[str, Any]] = {}
        self.op_log_order: deque[int] = deque()
        self.quorum_trackers: dict[int, QuorumTracker] = {}
        self.pending_order: deque[int] = deque()
        self.replica_progress: dict[str, int] = {}

        if initial_members:
            now = time.monotonic()
            for item in initial_members:
                nid = item.get("node_id")
                host = item.get("ip")
                rport = item.get("ring_port")
                if (
                    isinstance(nid, str)
                    and nid != self.frontend_id
                    and isinstance(host, str)
                    and isinstance(rport, int)
                    and rport > 0
                ):
                    self.registered_replicas[nid] = ReplicaDescriptor(
                        node_id=nid,
                        host=host,
                        port=rport,
                        ring_port=rport,
                        last_seen=now,
                        status="active",
                    )

    def _ring_members_payload(self) -> list[dict[str, Any]]:
        payload: list[dict[str, Any]] = []
        for replica in self.active_replicas().values():
            payload.append(
                {
                    "node_id": replica.node_id,
                    "ip": replica.host,
                    "ring_port": replica.ring_port or replica.port,
                }
            )
        return payload

    def broadcast_ring_members(self) -> None:
        members = self._ring_members_payload()
        packet = make_ring_members(self.frontend_id, members)
        for replica in self.active_replicas().values():
            target_port = replica.ring_port or replica.port
            try:
                self.sock.sendto(packet, (replica.host, target_port))
            except OSError:
                continue

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
        while self.pending_order:
            seq_id = self.pending_order[0]
            tracker = self.quorum_trackers.get(seq_id)
            if tracker is None or tracker.finalized:
                self.pending_order.popleft()
                self.quorum_trackers.pop(seq_id, None)
                continue
            count = self.progress_count(seq_id)
            if count >= tracker.target:
                self.finalize_tracker(
                    tracker, f"quorum_reached acks {count}/{tracker.target}"
                )
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
        if not isinstance(node_id, str):
            return
        if not _is_uint(last_applied) or not _is_uint(num_reqs) or not _is_uint(total_sum):
            return
        last_applied = cast(int, last_applied)
        num_reqs = cast(int, num_reqs)
        total_sum = cast(int, total_sum)
        replica = self.registered_replicas.get(node_id)
        if replica is None or replica.host != sender[0]:
            return

        replica.last_seen = time.monotonic()
        if replica.status != "active":
            replica.status = "active"
            print(f"replica_recovered node_id {node_id}", flush=True)
            self.broadcast_ring_members()

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
            self.sock.sendto(packet, sender)
            resent += 1
        print(
            f"nack_handled node_id {node_id} resent {resent} "
            f"unsatisfiable {unsatisfiable}",
            flush=True,
        )

    def process_replica_heartbeat(
        self,
        message: dict[str, Any],
        sender: tuple[str, int],
    ) -> None:
        node_id = message.get("node_id")
        if not isinstance(node_id, str):
            return
        replica = self.registered_replicas.get(node_id)
        if replica is None or replica.host != sender[0]:
            return
        was_suspect = replica.status != "active"
        replica.last_seen = time.monotonic()
        replica.status = "active"
        self.sock.sendto(
            make_replica_heartbeat_ack(node_id, self.frontend_id),
            sender,
        )
        if was_suspect:
            print(f"replica_recovered node_id {node_id}", flush=True)
            self.broadcast_ring_members()

    def maintain(self) -> None:
        now = time.monotonic()
        changed = False
        for replica in self.registered_replicas.values():
            if (
                replica.status == "active"
                and now - replica.last_seen
                >= self.cluster_config.heartbeat_timeout_seconds
            ):
                replica.status = "suspect"
                changed = True
                print(
                    f"replica_suspect node_id {replica.node_id} "
                    f"last_seen_seconds {now - replica.last_seen:.3f}",
                    flush=True,
                )

        if changed:
            self.broadcast_ring_members()

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
        ring_port = message.get("ring_port", listen_port)
        accepted = (
            isinstance(node_id, str)
            and bool(node_id)
            and len(node_id) <= 128
            and isinstance(listen_port, int)
            and not isinstance(listen_port, bool)
            and 1 <= listen_port <= 65535
            and isinstance(ring_port, int)
            and not isinstance(ring_port, bool)
            and 1 <= ring_port <= 65535
        )
        if accepted:
            previous = self.registered_replicas.get(node_id)
            replica = ReplicaDescriptor(
                node_id=node_id,
                host=sender[0],
                port=listen_port,
                ring_port=ring_port,
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

        members_payload = self._ring_members_payload() if accepted else None
        self.sock.sendto(
            make_replica_registered(
                node_id, self.frontend_id, accepted, members=members_payload
            ),
            sender,
        )
        if accepted:
            self.broadcast_ring_members()

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
                    elif message_type == TYPE_ELECTION:
                        origin_id = message.get("origin_id")
                        hop_index = message.get("hop_index")
                        if isinstance(origin_id, str) and isinstance(hop_index, int):
                            self.sock.sendto(
                                make_election_ack(self.frontend_id, origin_id, hop_index),
                                sender,
                            )
                        local_ip = _local_ip_for(sender[0])
                        self.sock.sendto(
                            make_coordinator(
                                self.frontend_id,
                                self.frontend_id,
                                local_ip,
                                self.port,
                                self.frontend_id,
                            ),
                            sender,
                        )
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
    """RM em anel: aplica operações ordenadas e elege novo FE (maior IP)."""

    def __init__(
        self,
        node_id: str,
        frontend_addr: tuple[str, int],
        frontend_id: str,
        cluster_config: ClusterConfig,
        initial_state: ReplicatedSumState | None = None,
        initial_members: list[dict[str, Any]] | None = None,
    ) -> None:
        self.node_id = node_id
        self.frontend_addr = frontend_addr
        self.frontend_id = frontend_id
        self.cluster_config = cluster_config

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind(("", 0))
            self.listen_port = self.sock.getsockname()[1]
        except OSError:
            self.sock.close()
            raise

        self.local_ip = _local_ip_for(frontend_addr[0])
        self.ring = RingTopology(
            self_info=RingNodeInfo(
                node_id=self.node_id,
                ip=self.local_ip,
                port=self.listen_port,
            )
        )
        if initial_members:
            self._ingest_members(initial_members)

        self.membership = MembershipState(
            node_id=node_id,
            role=NodeRole.REPLICA_MANAGER,
            leader_id=frontend_id,
            ring=self.ring,
        )
        self.state = initial_state or ReplicatedSumState()
        self.holdback: dict[int, tuple[int, str, int]] = {}
        self._last_nack = 0.0
        self._last_fe_contact = time.monotonic()
        self._fe_failed_reported = False
        self._hop_seq = 0
        self._promoted_frontend: FrontendServer | None = None

    def _ingest_members(self, members: list[Any]) -> None:
        rebuilt: list[RingNodeInfo] = [self.ring.self_info]
        for item in members:
            if not isinstance(item, dict):
                continue
            nid = item.get("node_id")
            ip = item.get("ip")
            rport = item.get("ring_port")
            if (
                isinstance(nid, str)
                and nid
                and isinstance(ip, str)
                and ip
                and isinstance(rport, int)
                and not isinstance(rport, bool)
                and 1 <= rport <= 65535
            ):
                rebuilt.append(RingNodeInfo(node_id=nid, ip=ip, port=rport))
        by_id = {m.node_id: m for m in rebuilt}
        self.ring.members = sorted(
            by_id.values(), key=lambda m: (m.node_id, m.ip, m.port)
        )

    def register(self) -> None:
        packet = make_replica_register(
            self.node_id,
            self.listen_port,
            ring_port=self.listen_port,
        )
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
                self._last_fe_contact = time.monotonic()
                self._fe_failed_reported = False
                members = message.get("members")
                if isinstance(members, list):
                    self._ingest_members(members)
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
        progressed = False
        while True:
            nxt = self.state.last_applied_seq_id + 1
            if nxt not in self.holdback:
                break
            value, client_id, request_id = self.holdback.pop(nxt)
            self.state.apply(nxt, value, client_id=client_id, request_id=request_id)
            progressed = True
            print(
                f"applied node_id {self.node_id} seq_id {nxt} value {value} "
                f"num_reqs {self.state.num_reqs} total_sum {self.state.total_sum}",
                flush=True,
            )
        if progressed:
            self.send_applied_ack()

    def request_missing(self, force: bool = False) -> None:
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
        if (
            not _is_uint(seq_id)
            or seq_id == 0
            or not isinstance(client_id, str)
            or not client_id
            or not _is_uint(request_id)
            or request_id == 0
            or not _is_uint(value)
        ):
            return
        self._last_fe_contact = time.monotonic()
        self._fe_failed_reported = False
        seq_id = cast(int, seq_id)
        request_id = cast(int, request_id)
        value = cast(int, value)

        if seq_id <= self.state.last_applied_seq_id:
            self.send_applied_ack()
            return

        self.holdback[seq_id] = (value, client_id, request_id)
        self.drain_apply()
        self.request_missing()

    def _send_election_to_hop(self, candidate: RingNodeInfo, origin_id: str) -> None:
        self._hop_seq += 1
        target = self.ring.next_hop()
        self.ring.pending_target = target
        self.ring.ack_deadline = (
            time.monotonic() + self.cluster_config.election_ack_timeout_seconds
        )
        packet = make_election(
            sender_id=self.node_id,
            candidate_id=candidate.node_id,
            candidate_ip=candidate.ip,
            candidate_port=candidate.port,
            origin_id=origin_id,
            hop_index=self._hop_seq,
        )
        self.sock.sendto(packet, (target.ip, target.port))
        print(
            f"election_send node_id {self.node_id} to {target.node_id} "
            f"({target.ip}:{target.port}) candidate {candidate.node_id} "
            f"ip {candidate.ip} hop_offset {self.ring.hop_offset}",
            flush=True,
        )

    def start_election(self) -> None:
        self.ring.election_active = True
        self.ring.candidate = self.ring.self_info
        self.ring.origin_id = self.node_id
        self.ring.hop_offset = 1
        print(
            f"election_started node_id {self.node_id} algorithm ring_highest_ip "
            f"ip {self.local_ip}",
            flush=True,
        )
        self._send_election_to_hop(self.ring.candidate, self.node_id)

    def _promote_to_frontend(self, leader_id: str, leader_port: int) -> None:
        members_data = [
            {
                "node_id": m.node_id,
                "ip": m.ip,
                "ring_port": m.port,
            }
            for m in self.ring.members
            if m.node_id != self.node_id
        ]
        self.sock.close()
        fe_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        fe_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        fe_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        fe_sock.bind(("", leader_port))
        self._promoted_frontend = FrontendServer(
            port=leader_port,
            frontend_id=leader_id,
            cluster_config=self.cluster_config,
            initial_state=self.state,
            initial_members=members_data,
            existing_sock=fe_sock,
        )
        print(
            f"promoted_to_frontend node_id {self.node_id} frontend_id {leader_id} "
            f"port {leader_port} num_reqs {self.state.num_reqs} "
            f"total_sum {self.state.total_sum}",
            flush=True,
        )

    def _announce_coordinator(self, winner: RingNodeInfo) -> None:
        self.ring.election_active = False
        self.ring.pending_target = None
        target = self.ring.next_hop(offset=1)
        packet = make_coordinator(
            sender_id=self.node_id,
            leader_id=winner.node_id,
            leader_ip=winner.ip,
            leader_port=self.frontend_addr[1],
            origin_id=self.node_id,
        )
        print(
            f"new_leader_elected leader_id {winner.node_id} ip {winner.ip} "
            f"port {self.frontend_addr[1]}",
            flush=True,
        )
        if winner.node_id == self.node_id:
            self._promote_to_frontend(self.node_id, self.frontend_addr[1])
            if target.node_id != self.node_id and self._promoted_frontend is not None:
                try:
                    self._promoted_frontend.sock.sendto(packet, (target.ip, target.port))
                except OSError:
                    pass
        else:
            self.frontend_id = winner.node_id
            self.frontend_addr = (winner.ip, self.frontend_addr[1])
            self.membership.leader_id = winner.node_id
            self._last_fe_contact = time.monotonic()
            self._fe_failed_reported = False
            self.ring.remove_member(winner.node_id)
            if target.node_id != self.node_id:
                self.sock.sendto(packet, (target.ip, target.port))

    def _handle_election_msg(
        self,
        message: dict[str, Any],
        sender: tuple[str, int],
    ) -> None:
        sender_id = message.get("sender_id")
        candidate_id = message.get("candidate_id")
        candidate_ip = message.get("candidate_ip")
        candidate_port = message.get("candidate_port")
        origin_id = message.get("origin_id")
        hop_index = message.get("hop_index")
        if not (
            isinstance(sender_id, str)
            and isinstance(candidate_id, str)
            and isinstance(candidate_ip, str)
            and isinstance(candidate_port, int)
            and isinstance(origin_id, str)
            and isinstance(hop_index, int)
        ):
            return

        self.sock.sendto(
            make_election_ack(self.node_id, origin_id, hop_index),
            sender,
        )
        print(
            f"election_received node_id {self.node_id} from {sender_id} "
            f"candidate {candidate_id} ip {candidate_ip} origin {origin_id}",
            flush=True,
        )

        incoming_candidate = RingNodeInfo(
            node_id=candidate_id,
            ip=candidate_ip,
            port=candidate_port,
        )
        if incoming_candidate.priority > self.ring.self_info.priority:
            chosen = incoming_candidate
        else:
            chosen = self.ring.self_info

        if origin_id == self.node_id:
            self._announce_coordinator(chosen)
            return

        self.ring.election_active = True
        self.ring.candidate = chosen
        self.ring.origin_id = origin_id
        self.ring.hop_offset = 1
        self._send_election_to_hop(chosen, origin_id)

    def _handle_election_ack(self, message: dict[str, Any]) -> None:
        hop_index = message.get("hop_index")
        if isinstance(hop_index, int) and hop_index == self._hop_seq:
            self.ring.pending_target = None

    def _handle_coordinator_msg(self, message: dict[str, Any]) -> None:
        leader_id = message.get("leader_id")
        leader_ip = message.get("leader_ip")
        leader_port = message.get("leader_port")
        origin_id = message.get("origin_id")
        if not (
            isinstance(leader_id, str)
            and isinstance(leader_ip, str)
            and isinstance(leader_port, int)
            and isinstance(origin_id, str)
        ):
            return

        self.ring.election_active = False
        self.ring.pending_target = None
        self.ring.remove_member(leader_id)
        self.frontend_id = leader_id
        self.frontend_addr = (leader_ip, leader_port)
        self.membership.leader_id = leader_id
        self._last_fe_contact = time.monotonic()
        self._fe_failed_reported = False
        print(
            f"coordinator_recognized node_id {self.node_id} leader_id {leader_id} "
            f"addr {leader_ip}:{leader_port}",
            flush=True,
        )

        if origin_id != self.node_id:
            target = self.ring.next_hop(offset=1)
            if target.node_id != self.node_id:
                self.sock.sendto(
                    make_coordinator(
                        self.node_id,
                        leader_id,
                        leader_ip,
                        leader_port,
                        origin_id,
                    ),
                    (target.ip, target.port),
                )

    def _check_election_ack_timeout(self, now: float) -> None:
        if not self.ring.election_active or self.ring.pending_target is None:
            return
        if now < self.ring.ack_deadline:
            return
        failed_node = self.ring.pending_target
        print(
            f"election_hop_timeout node_id {self.node_id} "
            f"unreachable {failed_node.node_id} ({failed_node.ip}:{failed_node.port})",
            flush=True,
        )
        self.ring.remove_member(failed_node.node_id)
        next_node = self.ring.skip_unresponsive_hop()
        candidate = self.ring.candidate or self.ring.self_info
        origin = self.ring.origin_id or self.node_id
        if next_node is None or next_node.node_id == self.node_id:
            self._announce_coordinator(candidate)
        else:
            self._send_election_to_hop(candidate, origin)

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
            self.sock.settimeout(min(0.1, interval))
            next_heartbeat = time.monotonic()
            while True:
                now = time.monotonic()
                if not self.ring.election_active:
                    if now >= next_heartbeat:
                        self.send_heartbeat()
                        next_heartbeat = now + interval
                    if (
                        now - self._last_fe_contact
                        >= self.cluster_config.heartbeat_timeout_seconds
                    ):
                        if not self._fe_failed_reported:
                            self._fe_failed_reported = True
                            print(
                                f"frontend_failure_detected node_id {self.node_id} "
                                f"frontend_id {self.frontend_id} "
                                f"elapsed {now - self._last_fe_contact:.3f}s",
                                flush=True,
                            )
                        self.start_election()

                self._check_election_ack_timeout(now)
                self.request_missing()

                try:
                    data, sender = self.sock.recvfrom(BUFFER_SIZE)
                except ConnectionResetError:
                    continue
                except socket.timeout:
                    continue

                message = decode_message(data)
                if message is None:
                    continue
                msg_type = message.get("type")
                if msg_type == TYPE_ORDERED_REQUEST:
                    self.handle_ordered_request(message)
                elif msg_type == TYPE_REPLICA_HEARTBEAT_ACK:
                    if message.get("frontend_id") == self.frontend_id:
                        self._last_fe_contact = time.monotonic()
                        self._fe_failed_reported = False
                elif msg_type == TYPE_RING_MEMBERS:
                    members = message.get("members")
                    if isinstance(members, list):
                        self._ingest_members(members)
                elif msg_type == TYPE_ELECTION:
                    self._handle_election_msg(message, sender)
                elif msg_type == TYPE_ELECTION_ACK:
                    self._handle_election_ack(message)
                elif msg_type == TYPE_COORDINATOR:
                    self._handle_coordinator_msg(message)

                if self._promoted_frontend is not None:
                    self._promoted_frontend.run()
                    return
        except KeyboardInterrupt:
            pass
        finally:
            if self._promoted_frontend is None:
                print(
                    f"final_state node_id {self.node_id} "
                    f"last_applied {self.state.last_applied_seq_id} "
                    f"num_reqs {self.state.num_reqs} total_sum {self.state.total_sum}",
                    flush=True,
                )
                self.sock.close()


class DistributedServer:
    """Descobre o FE ativo e inicia como RM ou promove via eleição em anel."""

    def __init__(self, port: int, cluster_config: ClusterConfig) -> None:
        self.port = port
        self.cluster_config = cluster_config
        self.node_id = f"server-{uuid.uuid4().hex[:12]}"

    def elect_frontend_ring_initial(self) -> bool:
        local_ip = _local_ip_for("127.0.0.1")
        print(
            f"election_started node_id {self.node_id} algorithm ring_highest_ip "
            f"ip {local_ip}",
            flush=True,
        )
        print(
            f"election_result node_id {self.node_id} "
            "role frontend decision ring_sole_candidate",
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

        self.elect_frontend_ring_initial()
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
            "Servidor UDP unificado: descobre o FE ativo e inicia como RM "
            "em anel; se não houver FE, assume como FE inicial."
        )
    )
    parser.add_argument("port", type=int)
    parser.add_argument("--heartbeat-interval", type=float, default=1.0)
    parser.add_argument("--heartbeat-timeout", type=float, default=4.0)
    parser.add_argument("--receipt-ack-timeout", type=float, default=0.5)
    parser.add_argument("--election-ack-timeout", type=float, default=0.25)
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
            election_ack_timeout_seconds=args.election_ack_timeout,
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