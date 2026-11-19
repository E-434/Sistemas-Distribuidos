"""Configuração de transporte, membership, anel e métricas de ACKs de recebimento."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import ipaddress
from time import monotonic


def ip_priority_key(ip: str, port: int, node_id: str = "") -> tuple[int, int, int, str]:
    """Chave de ordenação para eleição por maior IP (desempate por porta e node_id)."""
    parsed = ipaddress.ip_address(ip)
    return (parsed.version, int(parsed), int(port), node_id)


class NodeRole(str, Enum):
    FRONTEND = "frontend"
    REPLICA_MANAGER = "replica_manager"


@dataclass
class ReplicaDescriptor:
    """Endpoint anunciado e saúde observada de um Replica Manager."""

    node_id: str
    host: str
    port: int
    ring_port: int = 0
    registered: bool = True
    last_seen: float = field(default_factory=monotonic)
    status: str = "active"


@dataclass
class RingNodeInfo:
    """Participante do anel lógico de eleição."""

    node_id: str
    ip: str
    port: int

    @property
    def priority(self) -> tuple[int, int, int, str]:
        return ip_priority_key(self.ip, self.port, self.node_id)


@dataclass
class RingTopology:
    """Mantém o anel lógico ordenado e o salto em caso de sucessor inativo."""

    self_info: RingNodeInfo
    members: list[RingNodeInfo] = field(default_factory=list)
    election_active: bool = False
    candidate: RingNodeInfo | None = None
    origin_id: str | None = None
    hop_offset: int = 1
    pending_target: RingNodeInfo | None = None
    ack_deadline: float = 0.0

    def upsert_member(self, node_id: str, ip: str, port: int) -> None:
        by_id = {m.node_id: m for m in self.members}
        by_id[node_id] = RingNodeInfo(node_id=node_id, ip=ip, port=port)
        by_id[self.self_info.node_id] = self.self_info
        self.members = sorted(by_id.values(), key=lambda m: (m.node_id, m.ip, m.port))

    def remove_member(self, node_id: str) -> None:
        if node_id == self.self_info.node_id:
            return
        self.members = [m for m in self.members if m.node_id != node_id]

    def next_hop(self, offset: int | None = None) -> RingNodeInfo:
        if not self.members:
            return self.self_info
        idx = next(
            (i for i, m in enumerate(self.members) if m.node_id == self.self_info.node_id),
            0,
        )
        step = self.hop_offset if offset is None else offset
        return self.members[(idx + step) % len(self.members)]

    def skip_unresponsive_hop(self) -> RingNodeInfo | None:
        if len(self.members) <= 1:
            return self.self_info
        if self.hop_offset < len(self.members) - 1:
            self.hop_offset += 1
            return self.next_hop()
        return None


@dataclass(frozen=True)
class ClusterConfig:
    """Parâmetros de rede e de quórum de processamento da replicação ativa.

    A disseminação é feita por UNICAST em laço sobre as réplicas registradas
    (não há multicast): o front-end envia cada operação ordenada diretamente a
    cada Replica Manager ativo.
    """

    heartbeat_interval_seconds: float = 1.0
    heartbeat_timeout_seconds: float = 4.0
    receipt_ack_timeout_seconds: float = 0.5
    election_ack_timeout_seconds: float = 0.25
    expected_replicas: int = 0
    receipt_ack_quorum: int = 0
    replicas: tuple[ReplicaDescriptor, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if not 0 < self.heartbeat_interval_seconds < self.heartbeat_timeout_seconds:
            raise ValueError("heartbeat interval deve ser positivo e menor que o timeout")
        if self.receipt_ack_timeout_seconds <= 0:
            raise ValueError("receipt ACK timeout deve ser positivo")
        if self.election_ack_timeout_seconds <= 0:
            raise ValueError("election ACK timeout deve ser positivo")
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

    def quorum_target(self, active_replicas: int) -> int:
        """Quantos ACKs de APLICAÇÃO de réplicas são exigidos para confirmar.

        O quórum é a maioria dos membros do grupo contando o próprio front-end
        (que já aplicou a operação antes de disseminá-la). Assim, exigimos
        `maioria - 1` ACKs de réplicas. Sem réplicas ativas o alvo é 0 e o
        front-end responde imediatamente, mantendo o serviço utilizável.

        `--receipt-ack-quorum`, se informado, sobrescreve a meta (limitada ao
        número de réplicas ativas, para permanecer alcançável).
        """
        if active_replicas <= 0:
            return 0
        if self.receipt_ack_quorum:
            return min(self.receipt_ack_quorum, active_replicas)
        total_members = active_replicas + 1  # inclui o front-end
        majority = total_members // 2 + 1
        return max(0, majority - 1)


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
class QuorumTracker:
    """Operação disseminada aguardando quórum de APLICAÇÃO das réplicas.

    O front-end já aplicou a operação `seq_id` e calculou a resposta final ao
    cliente (`response`). Ele só a envia quando `target` réplicas confirmarem
    que aplicaram até pelo menos `seq_id` (ou quando o prazo expira, para
    preservar a disponibilidade). `expected_num_reqs`/`expected_total_sum` são
    o estado correto esperado, usados para detectar divergência entre réplicas.
    """

    seq_id: int
    expected_num_reqs: int
    expected_total_sum: int
    target: int
    deadline: float
    client_addr: tuple[str, int]
    response: bytes
    replied: bool = False
    finalized: bool = False


@dataclass
class MembershipState:
    """Identidade/papel e sequência recebida/aplicada do nó."""

    node_id: str
    role: NodeRole
    leader_id: str | None = None
    epoch: int = 0
    last_applied_seq_id: int = 0
    last_received_seq_id: int = 0
    ring: RingTopology | None = None