# Serviço distribuído de soma sobre UDP com replicação ativa

Implementação em Python 3.10+ de um serviço distribuído de soma sobre UDP com
**replicação ativa**, **processamento concorrente por threads** e **eleição de
líder em anel por maior IP**. Todos os nós são iniciados pelo mesmo programa:
cada servidor procura um front-end ativo; se encontrar, inicia como Replica
Manager (RM); se não encontrar, inicia eleição em anel (promovendo o vencedor
a front-end quando o anel se fecha, ou assumindo diretamente se for o único
nó).

O FE atribui um `seq_id` global a cada operação, aplica a soma ao seu próprio
estado e **dissemina a operação para cada RM ativo** (excluindo o próprio FE do
conjunto de réplicas ativas enquanto estiver no papel de líder). Cada RM mantém
a sua própria cópia da máquina de estados e **aplica** cada operação na ordem do
`seq_id`; como todos aplicam a mesma sequência de operações na mesma ordem,
convergem para o mesmo estado. O FE só **confirma a operação ao cliente depois
que um quórum de réplicas confirma a aplicação** (ou após o prazo de
disponibilidade). Entregas perdidas pelo UDP são recuperadas por hold-back +
NACK/retransmissão. Se o FE falhar, os RMs detectam a falha por heartbeat e
elegem um novo líder no anel por maior IP (com retentativa e pulo de nós
inativos via `ELECTION_ACK`), promovendo-o a FE com preservação de estado e
histórico de clientes, enquanto clientes e benchmarks redescobrem o novo FE após
timeouts.

O projeto usa somente a biblioteca padrão do Python (sockets UDP) e é
executável em ambientes Unix/Linux e macOS.

## Arquivos

- `Makefile`: automação de verificação de sintaxe (`make check`), testes
  (`make test`), execução de servidores, clientes e benchmarks.
- `src/cliente.py`: lê inteiros positivos da entrada padrão (`stdin`), gera um
  `client_id`, realiza múltiplas tentativas na descoberta inicial do front-end
  (com redescoberta automática após timeouts repetidos) e envia operações com
  `request_id`.
- `src/servidor.py`: servidor UDP unificado. Contém o **FrontendServer**
  (sequenciador + pool de threads worker + disseminador + coletor de quórum +
  gestão do anel + envio de estado/histórico no registro de RMs) e o
  **ReplicaManagerNode** (réplica ativa que sincroniza estado inicial ao
  registrar, aplica a sequência + detecção de falha do FE + eleição em anel +
  promoção a FE), além da descoberta inicial e logs de comportamento.
- `src/descoberta.py`: descoberta do front-end via broadcast/loopback e
  resposta unicast.
- `src/processamento.py`: sequenciamento global, idempotência e histórico por
  cliente no FE (`RequestProcessor`) e a máquina de estados replicada
  (`ReplicatedSumState`, preservando estado por cliente).
- `src/protocolo.py`: mensagens versionadas dos canais externo, intra-cluster e
  eleição em anel (incluindo `state_snapshot` e `history` limitados ao
  tamanho máximo do datagrama em `REPLICA_REGISTERED`).
- `src/arquitetura.py`: identidades, descritores, membership, anel
  (`RingTopology` e `RingNodeInfo`), chave por IP (`ip_priority_key`), quórum
  e trackers.
- `src/interface.py`: formatação padronizada dos logs com horário (`HH:MM:SS`)
  e prefixos de papel (`[FE]`, `[RM <id>]`, `[Cliente <endpoint>]`).
- `src/benchmark.py` e `src/Benchmark_file.py`: benchmarks cliente↔FE com
  redescoberta automática de FE durante falhas e suporte a arquivo de entrada.
- `tests/test_replicacao_ativa.py`: teste de integração da replicação ativa
  (convergência, quórum e recuperação por NACK).

## Execução

Você pode usar os alvos do `Makefile` a partir da raiz do repositório ou invocar
os scripts diretamente:

```bash
make check                      # verifica sintaxe dos arquivos
python3 src/servidor.py 4000      # inicia como FE (ou RM se já houver FE)
python3 src/servidor.py 4000      # entra como RM (descobre o FE)
python3 src/servidor.py 4000      # entra como RM
