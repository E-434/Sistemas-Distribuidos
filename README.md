# Serviço distribuído de soma sobre UDP com replicação ativa

Implementação em Python 3.10+ de um serviço distribuído de soma sobre UDP com
**replicação ativa**, **processamento concorrente por threads** e **eleição de
líder em anel por maior IP**. Todos os nós são iniciados pelo mesmo programa:
cada servidor procura um front-end ativo; se encontrar, inicia como Replica
Manager (RM); se não encontrar, inicia eleição em anel (promovendo o vencedor
a front-end quando o anel se fecha, ou assumindo diretamente se for o único
nó).

O FE atribui um `seq_id` global a cada operação, aplica a soma ao seu próprio
estado e **dissemina a operação por unicast, em laço, para cada RM ativo**. Cada
RM mantém a sua própria cópia da máquina de estados e **aplica** cada operação
na ordem do `seq_id`; como todos aplicam a mesma sequência de operações na mesma
ordem, convergem para o mesmo estado. O FE só **confirma a operação ao cliente
depois que um quórum de réplicas confirma a aplicação** (ou após o prazo de
disponibilidade). Entregas perdidas pelo UDP são recuperadas por hold-back +
NACK/retransmissão. Se o FE falhar, os RMs detectam a falha e elegem um novo
líder no anel por maior IP (com retentativa e pulo de nós inativos via
`ELECTION_ACK`), promovendo-o a FE com preservação de estado e histórico de
clientes, enquanto clientes e benchmarks redescobrem o novo FE após timeouts.

O projeto usa somente a biblioteca padrão do Python e pode ser executado em Linux/Unix e
macOS. Não há dependência de APIs específicas do Windows.

## Arquivos

- `Makefile`: automação de verificação de sintaxe (`make check`), testes
  (`make test`), execução de servidores, clientes e benchmarks.
- `src/cliente.py`: gera um `client_id`, realiza múltiplas tentativas na
  descoberta inicial do front-end (com redescoberta automática após timeouts
  repetidos) e envia operações stop-and-wait com `request_id`.
- `src/servidor.py`: servidor UDP unificado. Contém o **FrontendServer**
  (sequenciador + pool de threads worker + disseminador + coletor de quórum +
  gestão do anel + envio de estado/histórico no registro de RMs) e o
  **ReplicaManagerNode** (réplica ativa que sincroniza estado inicial ao
  registrar, aplica a sequência + detecção de falha do FE + eleição em anel +
  promoção a FE), além da descoberta/eleição e logs detalhados de tentativas
  (`discovery_attempt` e `discovery_result`).
- `src/replica_manager.py`: entrypoint legado desativado; a implementação do RM
  foi incorporada a `src/servidor.py`.
- `src/descoberta.py`: descoberta do front-end por broadcast + loopback e
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
- `src/interface.py`: formatação dos logs.
- `src/benchmark.py` e `src/Benchmark_file.py`: benchmarks cliente↔FE com
  redescoberta automática de FE durante falhas e suporte a arquivo de entrada.
- `tests/test_replicacao_ativa.py`: teste de integração da replicação ativa
  (convergência, quórum e recuperação por NACK).

## Execução

Você pode usar os alvos do `Makefile` a partir da raiz do repositório ou invocar
os scripts diretamente:

```bash
make check                      # verifica sintaxe dos arquivos
python3 src/servidor.py 4000      # vira FE
python3 src/servidor.py 4000      # vira RM (descobre o FE)
python3 src/servidor.py 4000      # vira RM
```

Em outro terminal, inicie o cliente e digite inteiros positivos, um por linha
(Ctrl+D encerra após processar a fila; Ctrl+C interrompe):

```bash
make client PORT=4000
# ou: python3 src/cliente.py 4000
```

Parâmetros opcionais de cluster (iguais em todos os nós):

```bash
python3 src/servidor.py 4000 \
  --heartbeat-interval 1 --heartbeat-timeout 4 \
  --receipt-ack-timeout 0.5 \
  --election-ack-timeout 0.25 \
  --expected-replicas 2 --receipt-ack-quorum 0 \
  --workers 4
```

- `--receipt-ack-timeout`: prazo para o FE esperar o quórum antes de responder
  mesmo assim (disponibilidade).
- `--election-ack-timeout`: prazo para aguardar `ELECTION_ACK` do sucessor no
  anel antes de repetir/pular para o próximo nó.
- `--receipt-ack-quorum`: nº de ACKs de aplicação exigidos; `0` deriva a maioria
  dos membros do grupo (contando o FE).
- `--expected-replicas`: usado apenas na métrica de membership.
- `--workers`: número de threads worker no `ThreadPoolExecutor` do front-end.

## Protocolo e estado

Datagramas são objetos JSON UTF-8 com `version: 2`.

**Canal Cliente<->FE**

1. `DISCOVER_FRONTEND` / `FRONTEND_INFO`: descoberta do FE.
2. `SUM_REQUEST` (`client_id`, `request_id`, `value`).
3. `SUM_RESULT` (`seq_id`, `num_reqs`, `total_sum`, `status`): enviado **após o
   quórum** (ou imediatamente se não houver réplicas).

**Canal FE<->RM e Eleição em Anel**

4. `REPLICA_REGISTER` / `REPLICA_REGISTERED`: o RM registra a sua porta efêmera
   de escuta (e porta do anel); o FE confirma e retorna os membros do anel,
   além de `state_snapshot` (`num_reqs`, `total_sum`, `last_applied_seq_id` e
   `clients`) e operações recentes de `history` que caibam no limite de
   datagrama.
5. `ORDERED_REQUEST` (`seq_id`, `client_id`, `request_id`, `value`): operação
   ordenada enviada a cada RM ativo.
6. `REPLICA_APPLIED_ACK` (`last_applied_seq_id`, `num_reqs`, `total_sum`): o RM
   confirma **até onde o seu estado está aplicado** e reporta o estado (usado
   para o quórum e para detectar divergência).
7. `REPLICA_NACK` (`missing`): o RM pede retransmissão dos `seq_id` que faltam.
8. `REPLICA_HEARTBEAT` / `REPLICA_HEARTBEAT_ACK`: o RM informa atividade ao FE
   e recebe confirmação.
9. `RING_MEMBERS`: atualização da topologia do anel disseminada pelo FE.
10. `ELECTION` / `ELECTION_ACK` / `COORDINATOR`: eleição em anel por maior IP
    (com retentativa e salto de nós inativos via `ELECTION_ACK`) e anúncio do
    novo líder/FE.

**Observações**

- Assim que um novo RM se conecta e é aceito via `REPLICA_REGISTERED`, ele
  incorpora o `state_snapshot` e aplica em ordem as operações recebidas em
  `history`, enviando `REPLICA_APPLIED_ACK` para alinhar seu progresso com o
  FE.
- O FE só incrementa `seq_id` ao aceitar uma requisição na ordem esperada do
  cliente; reenvios de `request_id` já processados são tratados como duplicatas
  e respondidos imediatamente, sem alterar a soma.
- Cada RM aplica estritamente `last_applied_seq_id + 1` e registra o histórico
  por `client_id`. `ORDERED_REQUEST` fora de ordem vai para o **hold-back
  buffer**; lacunas disparam **NACK** (pausadas durante eleição ativa e com
  *cooldown* para sequências ainda indisponíveis no log do FE), e o FE retransmite
  a partir do seu **log de operações**. Reentregas são ignoradas de forma
  idempotente.
- O FE confirma a operação ao cliente quando `quórum` réplicas têm
  `last_applied_seq_id >= seq_id` (considerando apenas réplicas ativas). Se o
  prazo expira sem quórum, o FE responde assim mesmo (o resultado é correto
  pela ordem total) e registra `quorum_timeout`.
- Na promoção de um RM a FE, `num_reqs`, `total_sum`, `next_seq_id` e o
  histórico de `clients` são preservados a partir do estado replicado, e o anel
  é atualizado para os RMs remanescentes.

## Estado da implementação

Implementado: sequenciamento global concorrente via threads, disseminação
unicast, **aplicação da soma em todos os RMs**, **quórum de processamento**,
recuperação de operações perdidas por hold-back/NACK (com prevenção de loops de
NACK), detecção de divergência, **eleição em anel por maior IP com retentativa
e pulo de nó inativo**, **failover/promoção do novo FE preservando estado**,
sincronização de estado na conexão de novos RMs e redescoberta de FE em
clientes e benchmarks. O serviço permanece utilizável sem réplicas (o FE responde
imediatamente).

## Teste de integração

O teste sobe um cluster real em subprocessos (loopback) e comprova (1) a
convergência de FE + réplicas, (2) o quórum, e (3) a recuperação de uma réplica
**atrasada** via NACK:

```bash
make test                               # executa suíte de testes
python3 tests/test_replicacao_ativa.py          # 2 lotes de 1500 ops = 3000
python3 tests/test_replicacao_ativa.py 50        # execução rápida
```

Ao final, FE, `replica-1`, `replica-2` e `replica-3-atrasada` devem apresentar
`num_reqs` e `total_sum` idênticos.

> Nota (macOS): o broadcast não é entregue na interface de loopback e um erro
> pode aparecer na operação seguinte do socket. A
> descoberta inclui a loopback (`127.0.0.1`) por unicast e tolera esse erro, de
> modo que o cluster de teste rode em um host macOS.

## Benchmark

```bash
make benchmark PORT=4000
# ou:
python3 src/servidor.py 4000
python3 src/benchmark.py 4000
```

Para rodar com arquivo de números (`src/RAND_NUM_1.txt` por padrão):

```bash
make benchmark-file PORT=4000 FILE=src/RAND_NUM_1.txt
```

Com um FE sem réplicas, o resultado esperado é `num_reqs 1000000` e
`total_sum 1000000`. Com réplicas, cada operação espera o quórum, então a taxa é
menor, e ambos os benchmarks agora redescobrem automaticamente um novo FE em caso
de falha durante a execução.
