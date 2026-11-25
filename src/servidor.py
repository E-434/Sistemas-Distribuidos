#!/usr/bin/env python3
"""Servidor UDP com replicação ativa, threads e eleição de líder em anel (maior IP)."""

from __future__ import annotations

import argparse
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import socket
import sys
import threading
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
from interface import (
    format_fe_log,
    format_rm_log,
    now as now_str,
    server_processing_event,
    server_started,
)
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
NACK_UNSATISFIABLE_COOLDOWN_SECONDS: Final[float] = 5.0
PROMOTE_BIND_ATTEMPTS: Final[int] = 8
PROMOTE_BIND_RETRY_DELAY_SECONDS: Final[float] = 0.05
REGISTRATION_TIMEOUT_SECONDS: Final[float] = 1.0
REGISTRATION_ATTEMPTS: Final[int] = 5
STARTUP_DISCOVERY_ATTEMPTS: Final[int] = 2
ELECTION_HOP_MAX_RETRIES: Final[int] = 1
UINT64_MAX: Final[int] = (1 << 64) - 1


def _log(message: str) -> None:
    print(f"{now_str()} {message}", flush=True)


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
    """FE: sequenciador global concorrente (threads), disseminador e coletor de quórum."""

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
            try:
                self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                self.sock.bind(("", port))
            except OSError:
                self.sock.close()
                raise
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
            self.processor.next_seq_id = (
                max(initial_state.last_applied_seq_id, initial_state.num_reqs) + 1
            )
            for cid, cstate in initial_state.clients.items():
                self.processor.clients[cid] = ClientState(
                    last_request_id=cstate.last_request_id,
                    last_seq_id=cstate.last_seq_id,
                    last_num_reqs=cstate.last_num_reqs,
                    last_total_sum=cstate.last_total_sum,
                )

        self._lock = threading.RLock()
        self._executor = ThreadPoolExecutor(
            max_workers=self.cluster_config.worker_threads,
            thread_name_prefix="fe-worker",
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

    def _fe_log(self, message: str) -> None:
        print(format_fe_log(message), flush=True)

    def _state_snapshot_payload(self) -> dict[str, Any]:
        clients_payload: dict[str, dict[str, int]] = {}
        for cid, cstate in self.processor.clients.items():
            clients_payload[cid] = {
                "last_request_id": cstate.last_request_id,
                "last_seq_id": cstate.last_seq_id,
                "last_num_reqs": cstate.last_num_reqs,
                "last_total_sum": cstate.last_total_sum,
            }
        return {
            "num_reqs": self.processor.num_reqs,
            "total_sum": self.processor.total_sum,
            "last_applied_seq_id": max(0, self.processor.next_seq_id - 1),
            "clients": clients_payload,
        }

    def _history_payload(self) -> list[dict[str, Any]]:
        entries: list[dict[str, Any]] = []
        for seq_id in self.op_log_order:
            item = self.op_log.get(seq_id)
            if item is not None:
                entries.append(
                    {
                        "seq_id": seq_id,
                        "client_id": item["client_id"],
                        "request_id": item["request_id"],
                        "value": item["value"],
                    }
                )
        return entries

    def _ring_members_payload(self) -> list[dict[str, Any]]:
        payload: list[dict[str, Any]] = []
        for replica in self.active_replicas().values():
            if replica.node_id == self.frontend_id:
                continue
            payload.append(
                {
                    "node_id": replica.node_id,
                    "ip": replica.host,
                    "ring_port": replica.ring_port or replica.port,
                }
            )
        return payload

    def broadcast_ring_members(self) -> None:
        with self._lock:
            members = self._ring_members_payload()
            replicas = list(self.active_replicas().values())
        packet = make_ring_members(self.frontend_id, members)
        for replica in replicas:
            if replica.node_id == self.frontend_id:
                continue
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
            if node_id != self.frontend_id
            and replica.status == "active"
            and now - replica.last_seen < self.cluster_config.heartbeat_timeout_seconds
        }

    def process_request(
        self,
        message: dict[str, Any],
        sender: tuple[str, int],
    ) -> None:
        with self._lock:
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
                self._fe_log(
                    f"disseminate_failed seq_id {seq_id} node_id {replica.node_id} "
                    f"error {exc}"
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
        self._fe_log(
            f"disseminated seq_id {seq_id} active_replicas {active} "
            f"quorum_target {target}"
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
        active_ids = set(self.active_replicas().keys())
        return sum(
            1
            for nid, applied in self.replica_progress.items()
            if nid in active_ids and applied >= seq_id
        )

    def finalize_tracker(self, tracker: QuorumTracker, reason: str) -> None:
        tracker.finalized = True
        if not tracker.replied:
            self.sock.sendto(tracker.response, tracker.client_addr)
            tracker.replied = True
        self._fe_log(
            f"quorum_decision seq_id {tracker.seq_id} {reason} "
            f"num_reqs {tracker.expected_num_reqs} "
            f"total_sum {tracker.expected_total_sum}"
        )

    def drain_quorum(self) -> None:
        while self.pending_order:
            seq_id = self.pending_order[0]
            tracker = self.quorum_trackers.get(seq_id)
            if tracker is None or tracker.finalized:
                self.pending_order.popleft()
                self.quorum_trackers.pop(seq_id, None)
                continue
            active_count = len(self.active_replicas())
            effective_target = min(
                tracker.target,
                self.cluster_config.quorum_target(active_count),
            )
            count = self.progress_count(seq_id)
            if count >= effective_target:
                self.finalize_tracker(
                    tracker, f"quorum_reached acks {count}/{effective_target}"
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
        should_broadcast = False
        with self._lock:
            replica = self.registered_replicas.get(node_id)
            if replica is None or replica.host != sender[0]:
                return

            replica.last_seen = time.monotonic()
            if replica.status != "active":
                replica.status = "active"
                should_broadcast = True
                self._fe_log(
                    f"replica_recovered node_id {node_id} "
                    f"active_replicas {len(self.active_replicas())}"
                )

            entry = self.op_log.get(last_applied)
            if entry is not None and (
                num_reqs != entry["num_reqs"] or total_sum != entry["total_sum"]
            ):
                self._fe_log(
                    f"replica_divergence node_id {node_id} seq_id {last_applied} "
                    f"replica num_reqs {num_reqs} total_sum {total_sum} "
                    f"expected num_reqs {entry['num_reqs']} total_sum {entry['total_sum']}"
                )

            previous = self.replica_progress.get(node_id, 0)
            if last_applied > previous:
                self.replica_progress[node_id] = last_applied
                self.drain_quorum()
        if should_broadcast:
            self.broadcast_ring_members()

    def process_replica_nack(
        self,
        message: dict[str, Any],
        sender: tuple[str, int],
    ) -> None:
        node_id = message.get("node_id")
        missing = message.get("missing")
        if not isinstance(node_id, str):
            return
        if not isinstance(missing, list):
            return

        with self._lock:
            replica = self.registered_replicas.get(node_id)
            if replica is None or replica.host != sender[0]:
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
            if resent > 0 or unsatisfiable == 0:
                self._fe_log(
                    f"nack_handled node_id {node_id} resent {resent} "
                    f"unsatisfiable {unsatisfiable}"
                )

    def process_replica_heartbeat(
        self,
        message: dict[str, Any],
        sender: tuple[str, int],
    ) -> None:
        node_id = message.get("node_id")
        if not isinstance(node_id, str):
            return
        should_broadcast = False
        with self._lock:
            replica = self.registered_replicas.get(node_id)
            if replica is None:
                replica = ReplicaDescriptor(
                    node_id=node_id,
                    host=sender[0],
                    port=sender[1],
                    ring_port=sender[1],
                    last_seen=time.monotonic(),
                    status="active",
                )
                self.registered_replicas[node_id] = replica
                should_broadcast = True
                self._fe_log(
                    f"replica_rejoined node_id {node_id} addr {sender[0]}:{sender[1]} "
                    f"active_replicas {len(self.active_replicas())}"
                )
            elif replica.host != sender[0]:
                return
            else:
                was_suspect = replica.status != "active"
                replica.last_seen = time.monotonic()
                replica.port = sender[1]
                replica.ring_port = sender[1]
                replica.status = "active"
                if was_suspect:
                    should_broadcast = True
                    self._fe_log(
                        f"replica_recovered node_id {node_id} "
                        f"active_replicas {len(self.active_replicas())}"
                    )

        self.sock.sendto(
            make_replica_heartbeat_ack(node_id, self.frontend_id),
            sender,
        )
        if should_broadcast:
            self.broadcast_ring_members()

    def maintain(self) -> None:
        now = time.monotonic()
        changed = False
        with self._lock:
            for replica in self.registered_replicas.values():
                if (
                    replica.status == "active"
                    and now - replica.last_seen
                    >= self.cluster_config.heartbeat_timeout_seconds
                ):
                    replica.status = "suspect"
                    self.replica_progress.pop(replica.node_id, None)
                    changed = True
                    active_now = len(self.active_replicas())
                    self._fe_log(
                        f"replica_failure_detected node_id {replica.node_id} "
                        f"last_seen_seconds {now - replica.last_seen:.3f} "
                        f"active_replicas {active_now} "
                        f"quorum_target {self.cluster_config.quorum_target(active_now)}"
                    )

            for tracker in list(self.quorum_trackers.values()):
                if not tracker.finalized and now >= tracker.deadline:
                    self.finalize_tracker(
                        tracker,
                        f"quorum_timeout acks {self.progress_count(tracker.seq_id)}"
                        f"/{tracker.target}",
                    )
            self.drain_quorum()

        if changed:
            self.broadcast_ring_members()

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
            and node_id != self.frontend_id
            and isinstance(listen_port, int)
            and not isinstance(listen_port, bool)
            and 1 <= listen_port <= 65535
            and isinstance(ring_port, int)
            and not isinstance(ring_port, bool)
            and 1 <= ring_port <= 65535
        )
        with self._lock:
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
                active_now = len(self.active_replicas())
                action = "registered" if is_new else "registration_refreshed"
                self._fe_log(
                    f"{action} replica_manager {node_id} "
                    f"addr {replica.host}:{replica.port} "
                    f"active_replicas {active_now} "
                    f"quorum_target {self.cluster_config.quorum_target(active_now)}"
                )
                if previous is not None and previous.host != sender[0]:
                    self._fe_log(
                        f"replica_endpoint_changed node_id {node_id} "
                        f"old_host {previous.host} new_host {sender[0]}"
                    )
            else:
                node_id = node_id if isinstance(node_id, str) else "invalid"

            members_payload = self._ring_members_payload() if accepted else None
            snapshot_payload = self._state_snapshot_payload() if accepted else None
            history_payload = self._history_payload() if accepted else None

        self.sock.sendto(
            make_replica_registered(
                node_id,
                self.frontend_id,
                accepted,
                members=members_payload,
                state_snapshot=snapshot_payload,
                history=history_payload,
            ),
            sender,
        )
        if accepted:
            self.broadcast_ring_members()

    def _dispatch_datagram(self, data: bytes, sender: tuple[str, int]) -> None:
        message = decode_message(data)
        if message is None:
            return
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

    def run(self) -> None:
        print(
            server_started(
                self.processor.num_reqs,
                self.processor.total_sum,
                self.frontend_id,
            ),
            flush=True,
        )
        self.broadcast_ring_members()
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
                self._executor.submit(self._dispatch_datagram, data, sender)
                self.maintain()
        except KeyboardInterrupt:
            pass
        finally:
            self._executor.shutdown(wait=True)
            with self._lock:
                self._fe_log(
                    f"final_state frontend_id {self.frontend_id} "
                    f"num_reqs {self.processor.num_reqs} "
                    f"total_sum {self.processor.total_sum}"
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
        self._unsatisfiable_until: dict[int, float] = {}
        self._last_fe_contact = time.monotonic()
        self._fe_failed_reported = False
        self._hop_seq = 0
        self._promoted_frontend: FrontendServer | None = None

    def _rm_log(self, message: str) -> None:
        print(format_rm_log(self.node_id, message), flush=True)

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
                and nid != self.frontend_id
                and isinstance(ip, str)
                and ip
                and isinstance(rport, int)
                and not isinstance(rport, bool)
                and 1 <= rport <= 65535
            ):
                rebuilt.append(RingNodeInfo(node_id=nid, ip=ip, port=rport))
        self.ring.replace_members(rebuilt)

    def _apply_snapshot_and_history(
        self,
        snapshot: Any,
        history: Any,
    ) -> None:
        if isinstance(snapshot, dict):
            num_reqs = snapshot.get("num_reqs")
            total_sum = snapshot.get("total_sum")
            last_applied = snapshot.get("last_applied_seq_id")
            if (
                _is_uint(num_reqs)
                and _is_uint(total_sum)
                and _is_uint(last_applied)
                and cast(int, last_applied) > self.state.last_applied_seq_id
            ):
                self.state.num_reqs = cast(int, num_reqs)
                self.state.total_sum = cast(int, total_sum)
                self.state.last_applied_seq_id = cast(int, last_applied)

            clients_data = snapshot.get("clients")
            if isinstance(clients_data, dict):
                for cid, cinfo in clients_data.items():
                    if not isinstance(cid, str) or not isinstance(cinfo, dict):
                        continue
                    l_req = cinfo.get("last_request_id")
                    l_seq = cinfo.get("last_seq_id")
                    l_num = cinfo.get("last_num_reqs")
                    l_sum = cinfo.get("last_total_sum")
                    if (
                        _is_uint(l_req)
                        and _is_uint(l_seq)
                        and _is_uint(l_num)
                        and _is_uint(l_sum)
                    ):
                        existing = self.state.clients.get(cid)
                        if existing is None or cast(int, l_req) > existing.last_request_id:
                            self.state.clients[cid] = ClientState(
                                last_request_id=cast(int, l_req),
                                last_seq_id=cast(int, l_seq),
                                last_num_reqs=cast(int, l_num),
                                last_total_sum=cast(int, l_sum),
                            )

        if isinstance(history, list):
            for item in history:
                if not isinstance(item, dict):
                    continue
                seq_id = item.get("seq_id")
                client_id = item.get("client_id")
                request_id = item.get("request_id")
                value = item.get("value")
                if (
                    _is_uint(seq_id)
                    and cast(int, seq_id) > 0
                    and isinstance(client_id, str)
                    and client_id
                    and _is_uint(request_id)
                    and cast(int, request_id) > 0
                    and _is_uint(value)
                ):
                    sid = cast(int, seq_id)
                    if sid > self.state.last_applied_seq_id:
                        self.holdback[sid] = (
                            cast(int, value),
                            client_id,
                            cast(int, request_id),
                        )
            self.drain_apply()

        if self.state.last_applied_seq_id > 0:
            self.send_applied_ack()

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
                self._apply_snapshot_and_history(
                    message.get("state_snapshot"),
                    message.get("history"),
                )
                return
        raise RuntimeError(
            f"não foi possível registrar {self.node_id} no front-end "
            f"{self.frontend_addr[0]}:{self.frontend_addr[1]}"
        )

    def _send_reregister_nonblocking(self) -> None:
        if self._promoted_frontend is not None or self.frontend_id == self.node_id:
            return
        packet = make_replica_register(
            self.node_id,
            self.listen_port,
            ring_port=self.listen_port,
        )
        try:
            self.sock.sendto(packet, self.frontend_addr)
        except OSError:
            pass

    def send_applied_ack(self) -> None:
        if self._promoted_frontend is not None:
            return
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
        if self._promoted_frontend is not None:
            return
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
            self._unsatisfiable_until.pop(nxt, None)
            self.state.apply(nxt, value, client_id=client_id, request_id=request_id)
            progressed = True
            self._rm_log(
                f"applied seq_id {nxt} value {value} "
                f"num_reqs {self.state.num_reqs} total_sum {self.state.total_sum}"
            )
        if progressed:
            self.send_applied_ack()

    def request_missing(self, force: bool = False) -> None:
        if (
            self._promoted_frontend is not None
            or self.ring.election_active
            or not self.holdback
        ):
            return
        head = self.state.last_applied_seq_id + 1
        if head in self.holdback:
            return
        now = time.monotonic()
        if not force and now - self._last_nack < NACK_MIN_INTERVAL_SECONDS:
            return
        smallest_buffered = min(self.holdback)
        missing = [
            seq
            for seq in range(head, smallest_buffered)
            if now >= self._unsatisfiable_until.get(seq, 0.0)
        ][:NACK_MAX_SEQS]
        if not missing:
            return
        self._last_nack = now
        for seq in missing:
            self._unsatisfiable_until[seq] = now + NACK_UNSATISFIABLE_COOLDOWN_SECONDS
        self.sock.sendto(
            make_replica_nack(self.node_id, self.frontend_id, missing),
            self.frontend_addr,
        )
        self._rm_log(
            f"gap_detected last_applied {self.state.last_applied_seq_id} "
            f"nack_missing {missing[0]}..{missing[-1]} count {len(missing)}"
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
            self._unsatisfiable_until.pop(seq_id, None)
            self.send_applied_ack()
            return

        self.holdback[seq_id] = (value, client_id, request_id)
        self._unsatisfiable_until.pop(seq_id, None)
        self.drain_apply()
        self.request_missing()

    def _send_election_to_hop(self, candidate: RingNodeInfo, origin_id: str) -> None:
        target = self.ring.next_hop()
        if target.node_id == self.node_id:
            self._announce_coordinator(candidate)
            return

        self._hop_seq += 1
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
        self._rm_log(
            f"election_send to {target.node_id} ({target.ip}:{target.port}) "
            f"candidate {candidate.node_id} ip {candidate.ip} "
            f"hop_offset {self.ring.hop_offset}"
        )

    def start_election(self) -> None:
        self.ring.election_active = True
        self.ring.candidate = self.ring.self_info
        self.ring.origin_id = self.node_id
        self.ring.hop_offset = 1
        self.ring.hop_retries = 0
        self._rm_log(f"election_started algorithm ring_highest_ip ip {self.local_ip}")
        self._send_election_to_hop(self.ring.candidate, self.node_id)

    def _promote_to_frontend(self, leader_id: str, leader_port: int) -> bool:
        members_data = [
            {
                "node_id": m.node_id,
                "ip": m.ip,
                "ring_port": m.port,
            }
            for m in self.ring.members
            if m.node_id != self.node_id and m.node_id != leader_id
        ]
        fe_sock: socket.socket | None = None
        last_exc: OSError | None = None
        for _ in range(PROMOTE_BIND_ATTEMPTS):
            candidate_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                candidate_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                candidate_sock.bind(("", leader_port))
                fe_sock = candidate_sock
                break
            except OSError as exc:
                last_exc = exc
                candidate_sock.close()
                time.sleep(PROMOTE_BIND_RETRY_DELAY_SECONDS)

        if fe_sock is None:
            self._rm_log(
                f"promote_bind_failed port {leader_port} error {last_exc}"
            )
            return False

        self.sock.close()
        self._promoted_frontend = FrontendServer(
            port=leader_port,
            frontend_id=leader_id,
            cluster_config=self.cluster_config,
            initial_state=self.state,
            initial_members=members_data,
            existing_sock=fe_sock,
        )
        print(
            format_fe_log(
                f"promoted_to_frontend frontend_id {leader_id} port {leader_port} "
                f"num_reqs {self.state.num_reqs} total_sum {self.state.total_sum}"
            ),
            flush=True,
        )
        return True

    def _announce_coordinator(self, winner: RingNodeInfo) -> None:
        self.ring.election_active = False
        self.ring.pending_target = None
        self.ring.hop_retries = 0
        self.ring.remove_member(winner.node_id)
        self.ring.hop_offset = 1
        target = self.ring.next_hop(offset=1)
        packet = make_coordinator(
            sender_id=self.node_id,
            leader_id=winner.node_id,
            leader_ip=winner.ip,
            leader_port=self.frontend_addr[1],
            origin_id=self.node_id,
        )
        self._rm_log(
            f"new_leader_elected leader_id {winner.node_id} ip {winner.ip} "
            f"port {self.frontend_addr[1]}"
        )
        if winner.node_id == self.node_id:
            promoted = self._promote_to_frontend(self.node_id, self.frontend_addr[1])
            if promoted and self._promoted_frontend is not None:
                for member in self.ring.members:
                    if member.node_id != self.node_id:
                        try:
                            self._promoted_frontend.sock.sendto(
                                packet, (member.ip, member.port)
                            )
                        except OSError:
                            pass
                return
            self.frontend_id = f"frontend-{self.frontend_addr[1]}-busy"
            self._last_fe_contact = (
                time.monotonic() - self.cluster_config.heartbeat_timeout_seconds
            )
            self._fe_failed_reported = False
            return

        self.frontend_id = winner.node_id
        self.frontend_addr = (winner.ip, self.frontend_addr[1])
        self.membership.leader_id = winner.node_id
        self._last_fe_contact = time.monotonic()
        self._fe_failed_reported = False
        if target.node_id != self.node_id:
            self.sock.sendto(packet, (target.ip, target.port))
        self._send_reregister_nonblocking()

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
        self._rm_log(
            f"election_received from {sender_id} candidate {candidate_id} "
            f"ip {candidate_ip} origin {origin_id}"
        )

        if sender_id != self.frontend_id:
            self.ring.upsert_member(sender_id, sender[0], sender[1])
        if candidate_id != self.frontend_id:
            self.ring.upsert_member(candidate_id, candidate_ip, candidate_port)

        incoming_candidate = RingNodeInfo(
            node_id=candidate_id,
            ip=candidate_ip,
            port=candidate_port,
        )
        if incoming_candidate.priority > self.ring.self_info.priority:
            chosen = incoming_candidate
            next_origin = origin_id
        elif incoming_candidate.priority < self.ring.self_info.priority:
            chosen = self.ring.self_info
            next_origin = self.node_id
        else:
            chosen = self.ring.self_info
            next_origin = origin_id

        if origin_id == self.node_id and chosen.node_id == self.node_id:
            self._announce_coordinator(chosen)
            return

        self.ring.election_active = True
        self.ring.candidate = chosen
        self.ring.origin_id = next_origin
        self.ring.hop_offset = 1
        self.ring.hop_retries = 0
        self._send_election_to_hop(chosen, next_origin)

    def _handle_election_ack(self, message: dict[str, Any]) -> None:
        hop_index = message.get("hop_index")
        if isinstance(hop_index, int) and hop_index == self._hop_seq:
            self.ring.pending_target = None
            self.ring.hop_retries = 0

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

        if leader_id == self.node_id:
            return

        already_known = (
            not self.ring.election_active
            and self.frontend_id == leader_id
            and self.frontend_addr == (leader_ip, leader_port)
        )
        self.ring.election_active = False
        self.ring.pending_target = None
        self.ring.hop_retries = 0
        self.ring.remove_member(leader_id)
        self.ring.hop_offset = 1
        self.frontend_id = leader_id
        self.frontend_addr = (leader_ip, leader_port)
        self.membership.leader_id = leader_id
        self._last_fe_contact = time.monotonic()
        self._fe_failed_reported = False

        if already_known:
            return

        self._rm_log(
            f"coordinator_recognized leader_id {leader_id} addr {leader_ip}:{leader_port}"
        )

        self._send_reregister_nonblocking()

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
        candidate = self.ring.candidate or self.ring.self_info
        origin = self.ring.origin_id or self.node_id

        if self.ring.hop_retries < ELECTION_HOP_MAX_RETRIES:
            self.ring.hop_retries += 1
            self._send_election_to_hop(candidate, origin)
            return

        self._rm_log(
            f"election_hop_timeout unreachable {failed_node.node_id} "
            f"({failed_node.ip}:{failed_node.port})"
        )
        self.ring.remove_member(failed_node.node_id)
        next_node = self.ring.skip_unresponsive_hop()
        if next_node is None or next_node.node_id == self.node_id:
            self._announce_coordinator(self.ring.self_info)
        else:
            self._send_election_to_hop(candidate, origin)

    def run(self) -> None:
        try:
            self.register()
            self._rm_log(
                f"registered frontend_id {self.frontend_id} "
                f"listen_port {self.listen_port}"
            )
            interval = self.cluster_config.heartbeat_interval_seconds
            self.sock.settimeout(min(0.1, interval))
            next_heartbeat = time.monotonic()
            while True:
                if self._promoted_frontend is not None:
                    self._promoted_frontend.run()
                    return

                now = time.monotonic()
                if (
                    not self.ring.election_active
                    and self.frontend_id != self.node_id
                ):
                    if now >= next_heartbeat:
                        self.send_heartbeat()
                        next_heartbeat = now + interval
                    if (
                        now - self._last_fe_contact
                        >= self.cluster_config.heartbeat_timeout_seconds
                    ):
                        if not self._fe_failed_reported:
                            self._fe_failed_reported = True
                            self._rm_log(
                                f"frontend_failure_detected frontend_id {self.frontend_id} "
                                f"elapsed {now - self._last_fe_contact:.3f}s"
                            )
                        self.start_election()

                self._check_election_ack_timeout(now)
                if self._promoted_frontend is not None:
                    self._promoted_frontend.run()
                    return

                self.request_missing()

                try:
                    data, sender = self.sock.recvfrom(BUFFER_SIZE)
                except ConnectionResetError:
                    continue
                except socket.timeout:
                    continue
                except OSError:
                    if self._promoted_frontend is not None:
                        self._promoted_frontend.run()
                        return
                    raise

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
                elif msg_type == TYPE_REPLICA_REGISTERED:
                    if (
                        message.get("node_id") == self.node_id
                        and message.get("accepted") is True
                    ):
                        self._last_fe_contact = time.monotonic()
                        self._fe_failed_reported = False
                        members = message.get("members")
                        if isinstance(members, list):
                            self._ingest_members(members)
                        self._apply_snapshot_and_history(
                            message.get("state_snapshot"),
                            message.get("history"),
                        )
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
                self._rm_log(
                    f"final_state last_applied {self.state.last_applied_seq_id} "
                    f"num_reqs {self.state.num_reqs} total_sum {self.state.total_sum}"
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
        _log(f"election_started algorithm ring_highest_ip ip {local_ip}")
        _log("election_result role frontend decision ring_sole_candidate")
        return True

    def _try_discover_frontend(self) -> tuple[tuple[str, int] | None, str]:
        for attempt in range(1, STARTUP_DISCOVERY_ATTEMPTS + 1):
            t0 = time.monotonic()
            _log(
                f"discovery_attempt attempt {attempt}/{STARTUP_DISCOVERY_ATTEMPTS} "
                f"port {self.port}"
            )
            discovery_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            discovery_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            discovery_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                addr, fid = discover_frontend(
                    discovery_sock,
                    self.port,
                    self.node_id,
                )
                elapsed = time.monotonic() - t0
                _log(
                    f"discovery_result attempt {attempt} status found "
                    f"frontend_id {fid} addr {addr[0]}:{addr[1]} elapsed {elapsed:.3f}s"
                )
                return addr, fid
            except RuntimeError:
                elapsed = time.monotonic() - t0
                _log(
                    f"discovery_result attempt {attempt} status not_found "
                    f"elapsed {elapsed:.3f}s"
                )
            finally:
                discovery_sock.close()
        return None, ""

    def run(self) -> None:
        frontend_addr, frontend_id = self._try_discover_frontend()

        if frontend_addr is not None:
            ReplicaManagerNode(
                self.node_id,
                frontend_addr,
                frontend_id,
                self.cluster_config,
            ).run()
            return

        frontend_id = f"frontend-{self.port}-{self.node_id}"
        try:
            fe_server = FrontendServer(
                self.port,
                frontend_id=frontend_id,
                cluster_config=self.cluster_config,
            )
        except OSError as exc:
            _log(f"frontend_bind_busy port {self.port} error {exc}")
            frontend_addr, frontend_id = self._try_discover_frontend()
            if frontend_addr is not None:
                ReplicaManagerNode(
                    self.node_id,
                    frontend_addr,
                    frontend_id,
                    self.cluster_config,
                ).run()
                return
            raise

        self.elect_frontend_ring_initial()
        _log(f"frontend_starting frontend_id {frontend_id} port {self.port}")
        fe_server.run()


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
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="número de threads worker para processar requisições no front-end",
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
            worker_threads=args.workers,
        )
    except ValueError as exc:
        parser.error(str(exc))
    return args.port, config


def main() -> None:
    port, cluster_config = parse_args(sys.argv)
    DistributedServer(port, cluster_config).run()


if __name__ == "__main__":
    main()