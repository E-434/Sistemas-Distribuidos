#!/usr/bin/env python3
"""Processo-base de um Replica Manager, sem execução de operações replicadas."""

from __future__ import annotations

import socket
import sys
import argparse
import struct
import time
from typing import Final

from arquitetura import ClusterConfig, MembershipState, NodeRole
from protocolo import (
    TYPE_ORDERED_REQUEST,
    TYPE_REPLICA_REGISTERED,
    decode_message,
    make_replica_heartbeat,
    make_replica_receipt_ack,
    make_replica_register,
    STATUS_RECEIVED,
)

BUFFER_SIZE: Final[int] = 4096
REGISTRATION_TIMEOUT_SECONDS: Final[float] = 1.0
REGISTRATION_ATTEMPTS: Final[int] = 5
UINT64_MAX: Final[int] = (1 << 64) - 1


class ReplicaManagerServer:
    """Registra um RM no FE e mantém seu endpoint preparado para expansão."""

    def __init__(
        self,
        node_id: str,
        listen_port: int,
        frontend_host: str,
        frontend_port: int,
        cluster_config: ClusterConfig | None = None,
    ) -> None:
        self.node_id = node_id
        self.cluster_config = cluster_config or ClusterConfig()
        frontend_addr = (socket.gethostbyname(frontend_host), frontend_port)
        if listen_port not in (0, self.cluster_config.multicast_port):
            raise ValueError(
                "listen_port deve ser 0 ou igual a multicast_port para receber multicast"
            )
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind(("", self.cluster_config.multicast_port))
            self.listen_port = self.sock.getsockname()[1]
            group = socket.inet_aton(self.cluster_config.multicast_group)
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
        self.frontend_addr = frontend_addr
        self.frontend_id = ""
        self.membership = MembershipState(
            node_id=node_id,
            role=NodeRole.REPLICA_MANAGER,
        )

    def register(self) -> None:
        """Tenta registrar-se no FE; falha explicitamente sem resposta válida."""
        packet = make_replica_register(self.node_id, self.listen_port)
        self.sock.settimeout(REGISTRATION_TIMEOUT_SECONDS)
        for _ in range(REGISTRATION_ATTEMPTS):
            self.sock.sendto(packet, self.frontend_addr)
            while True:
                try:
                    data, sender = self.sock.recvfrom(BUFFER_SIZE)
                except socket.timeout:
                    break

                if sender != self.frontend_addr:
                    continue
                message = decode_message(data)
                if message is None or message.get("type") != TYPE_REPLICA_REGISTERED:
                    continue
                if message.get("node_id") != self.node_id:
                    continue
                if message.get("accepted") is not True:
                    raise RuntimeError("o front-end rejeitou o registro do Replica Manager")
                frontend_id = message.get("frontend_id")
                if not isinstance(frontend_id, str) or not frontend_id:
                    raise RuntimeError("resposta de registro sem frontend_id válido")
                self.frontend_id = frontend_id
                self.membership.leader_id = frontend_id
                return

        raise RuntimeError(
            f"não foi possível registrar {self.node_id} no front-end "
            f"{self.frontend_addr[0]}:{self.frontend_addr[1]}"
        )

    def process_ordered_request(
        self,
        message: dict[str, object],
    ) -> None:
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
        try:
            self.sock.sendto(
                make_replica_receipt_ack(
                    self.node_id,
                    self.frontend_id,
                    seq_id,
                    STATUS_RECEIVED,
                ),
                self.frontend_addr,
            )
        except OSError as exc:
            print(
                f"receipt_ack_send_failed node_id {self.node_id} "
                f"seq_id {seq_id} error {exc}",
                flush=True,
            )

    def send_heartbeat(self) -> None:
        try:
            self.sock.sendto(
                make_replica_heartbeat(self.node_id, self.frontend_id),
                self.frontend_addr,
            )
        except OSError as exc:
            print(
                f"heartbeat_send_failed node_id {self.node_id} error {exc}",
                flush=True,
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
            self.sock.settimeout(min(0.2, self.cluster_config.heartbeat_interval_seconds))
            next_heartbeat = time.monotonic()
            while True:
                now = time.monotonic()
                if now >= next_heartbeat:
                    self.send_heartbeat()
                    next_heartbeat = now + self.cluster_config.heartbeat_interval_seconds

                try:
                    data, sender = self.sock.recvfrom(BUFFER_SIZE)
                except socket.timeout:
                    continue
                message = decode_message(data)
                if message is None:
                    print(
                        f"datagram_rejected node_id {self.node_id} "
                        f"source {sender[0]}:{sender[1]} reason malformed_or_wrong_version",
                        flush=True,
                    )
                elif message.get("type") == TYPE_ORDERED_REQUEST:
                    self.process_ordered_request(message)
                else:
                    print(
                        f"message_ignored node_id {self.node_id} "
                        f"type {message.get('type')} reason unexpected_message_type",
                        flush=True,
                    )
        except KeyboardInterrupt:
            pass
        finally:
            self.sock.close()


def parse_args(argv: list[str]) -> tuple[str, int, str, int, ClusterConfig]:
    parser = argparse.ArgumentParser(
        description="Replica Manager UDP (recebimento de operações apenas)."
    )
    parser.add_argument("node_id")
    parser.add_argument("listen_port", type=int)
    parser.add_argument("frontend_host")
    parser.add_argument("frontend_port", type=int)
    parser.add_argument("--multicast-group", default="239.10.10.10")
    parser.add_argument("--multicast-port", type=int, default=5001)
    parser.add_argument("--heartbeat-interval", type=float, default=1.0)
    args = parser.parse_args(argv[1:])

    node_id = args.node_id
    if not node_id or len(node_id) > 128:
        parser.error("node_id deve conter entre 1 e 128 caracteres")

    if not 0 <= args.listen_port <= 65535:
        parser.error("listen_port deve estar entre 0 e 65535")
    if not 1 <= args.frontend_port <= 65535:
        parser.error("frontend_port deve estar entre 1 e 65535")
    if not 1 <= args.multicast_port <= 65535:
        parser.error("multicast_port deve estar entre 1 e 65535")

    try:
        config = ClusterConfig(
            multicast_group=args.multicast_group,
            multicast_port=args.multicast_port,
            heartbeat_interval_seconds=args.heartbeat_interval,
        )
    except ValueError as exc:
        parser.error(str(exc))
    return (
        node_id,
        args.listen_port,
        args.frontend_host,
        args.frontend_port,
        config,
    )


def main() -> None:
    node_id, listen_port, frontend_host, frontend_port, cluster_config = parse_args(
        sys.argv
    )
    manager = ReplicaManagerServer(
        node_id,
        listen_port,
        frontend_host,
        frontend_port,
        cluster_config,
    )
    try:
        manager.run()
    except RuntimeError as exc:
        print(f"Erro: {exc}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
