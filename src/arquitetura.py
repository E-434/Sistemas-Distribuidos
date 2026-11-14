"""Configuração de transporte, membership e métricas de ACKs de recebimento."""

from __future__ import annotations

from dataclasses import dataclass, field
import ipaddress
from enum import Enum
from time import monotonic


class NodeRole(str, Enum):
    FRONTEND = "frontend"
    REPLICA_MANAGER = "replica_manager"


@dataclass
class ReplicaDescriptor:
    """Endpoint anunciado e saúde observada de um Replica Manager."""

    node_id: str
    host: str
    port: int
    registered: bool = True
    last_seen: float = field(default_factory=monotonic)
    status: str = "active"


@dataclass(frozen=True)
class ClusterConfig:
    """Parâmetros de rede e de observação (não de quórum de processamento)."""

    multicast_group: str = "239.10.10.10"
    multicast_port: int = 5001
    heartbeat_interval_seconds: float = 1.0
    heartbeat_timeout_seconds: float = 4.0
    receipt_ack_timeout_seconds: float = 0.5
    expected_replicas: int = 0
    receipt_ack_quorum: int = 0
    replicas: tuple[ReplicaDescriptor, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        group = ipaddress.ip_address(self.multicast_group)
        if group.version != 4 or not group.is_multicast:
            raise ValueError("multicast_group deve ser um endereço IPv4 multicast")
        if not 1 <= self.multicast_port <= 65535:
            raise ValueError("multicast_port deve estar entre 1 e 65535")
        if not 0 < self.heartbeat_interval_seconds < self.heartbeat_timeout_seconds:
            raise ValueError("heartbeat interval deve ser positivo e menor que o timeout")
        if self.receipt_ack_timeout_seconds <= 0:
            raise ValueError("receipt ACK timeout deve ser positivo")
        if self.expected_replicas < 0 or self.receipt_ack_quorum < 0:
            raise ValueError("parâmetros de réplicas e ACK quorum não podem ser negativos")
        if self.expected_replicas and self.receipt_ack_quorum > self.expected_replicas:
            raise ValueError("receipt ACK quorum não pode exceder expected_replicas")

    def receipt_ack_target(self, active_replicas: int) -> int:
        """Calcula somente uma meta de confirmações de recebimento."""
        expected = self.expected_replicas or active_replicas
        if expected == 0:
            return 0
        return self.receipt_ack_quorum or (expected // 2 + 1)


@dataclass
class ReceiptAckTracker:
    """Contagem temporária de ACKs de transporte de uma sequência multicast."""

    seq_id: int
    expected_nodes: frozenset[str]
    target: int
    deadline: float
    received_nodes: set[str] = field(default_factory=set)
    finalized: bool = False

    def record(self, node_id: str) -> bool:
        if self.finalized or node_id not in self.expected_nodes:
            return False
        previous_count = len(self.received_nodes)
        self.received_nodes.add(node_id)
        return len(self.received_nodes) != previous_count

    @property
    def target_met(self) -> bool:
        return self.target > 0 and len(self.received_nodes) >= self.target


@dataclass
class MembershipState:
    """Identidade/papel e sequência recebida/aplicada do nó."""

    node_id: str
    role: NodeRole
    leader_id: str | None = None
    epoch: int = 0
    last_applied_seq_id: int = 0
    last_received_seq_id: int = 0
