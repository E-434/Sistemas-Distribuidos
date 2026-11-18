# Serviço distribuído de soma sobre UDP com replicação ativa

Implementação em Python 3.10+ de um serviço distribuído de soma sobre UDP com
**replicação ativa** (state-machine replication). Todos os nós são iniciados
pelo mesmo programa: cada servidor procura um front-end ativo; se encontrar,
inicia como Replica Manager (RM); se não encontrar, usa uma eleição dummy que
promove o próprio nó a front-end (FE).

O FE atribui um `seq_id` global a cada operação, aplica a soma ao seu próprio
estado e **dissemina a operação por unicast, em laço, para cada RM ativo**. Cada
RM mantém a sua própria cópia da máquina de estados e **aplica** cada operação
na ordem do `seq_id`; como todos aplicam a mesma sequência de operações na mesma
ordem, convergem para o mesmo estado. O FE só **confirma a operação ao cliente
depois que um quórum de réplicas confirma a aplicação**. Entregas perdidas pelo
UDP são recuperadas por hold-back + NACK/retransmissão.

> A disseminação é por **unicast em laço**, não por multicast — conforme a
> orientação de que não é necessário multicast para esta tarefa.

O projeto usa somente a biblioteca padrão do Python e roda em Linux/Unix e
macOS.

## Arquivos

- `src/cliente.py`: gera um `client_id`, descobre o front-end e envia operações
  stop-and-wait com `request_id`.
- `src/servidor.py`: servidor UDP unificado. Contém o **FrontendServer**
  (sequenciador + disseminador + coletor de quórum) e o **ReplicaManagerNode**
  (réplica ativa que aplica a sequência), além da descoberta/eleição.
- `src/replica_manager.py`: entrypoint legado desativado; a implementação do RM
  foi incorporada a `src/servidor.py`.
- `src/descoberta.py`: descoberta do front-end por broadcast + loopback e
  resposta unicast.
- `src/processamento.py`: sequenciamento global e idempotência por cliente no FE
  (`RequestProcessor`) e a máquina de estados replicada (`ReplicatedSumState`).
- `src/protocolo.py`: mensagens versionadas dos canais externo e intra-cluster.
- `src/arquitetura.py`: identidades, descritores, membership, quórum e trackers.
- `src/interface.py`: formatação dos logs.
- `src/benchmark.py` e `src/Benchmark_file.py`: benchmarks cliente↔FE.
- `tests/test_replicacao_ativa.py`: teste de integração da replicação ativa
  (convergência, quórum e recuperação por NACK).

## Execução

Inicie todos os servidores com o mesmo comando e a mesma porta externa. O
primeiro nó sem FE detectável se promove a FE; os próximos se registram como RMs:

```bash
python3 src/servidor.py 4000      # vira FE
python3 src/servidor.py 4000      # vira RM (descobre o FE)
python3 src/servidor.py 4000      # vira RM
```

Em outro terminal, inicie o cliente e digite inteiros positivos, um por linha
(Ctrl+D encerra após processar a fila; Ctrl+C interrompe):

```bash
python3 src/cliente.py 4000
```

Parâmetros opcionais de cluster (iguais em todos os nós):

```bash
python3 src/servidor.py 4000 \
  --heartbeat-interval 1 --heartbeat-timeout 4 \
  --receipt-ack-timeout 0.5 \
  --expected-replicas 2 --receipt-ack-quorum 0
```

- `--receipt-ack-timeout`: prazo para o FE esperar o quórum antes de responder
  mesmo assim (disponibilidade).
- `--receipt-ack-quorum`: nº de ACKs de aplicação exigidos; `0` deriva a maioria
  dos membros do grupo (contando o FE).
- `--expected-replicas`: usado apenas na métrica de membership.

## Protocolo e estado

Datagramas são objetos JSON UTF-8 com `version: 2`.

**Canal cliente↔FE**

1. `DISCOVER_FRONTEND` / `FRONTEND_INFO`: descoberta do FE.
2. `SUM_REQUEST` (`client_id`, `request_id`, `value`).
3. `SUM_RESULT` (`seq_id`, `num_reqs`, `total_sum`, `status`): enviado **após o
   quórum** (ou imediatamente se não houver réplicas).

**Canal FE↔RM (intra-cluster, unicast)**

4. `REPLICA_REGISTER` / `REPLICA_REGISTERED`: o RM registra a sua porta efêmera
   de escuta; o FE confirma e passa a enviar operações para esse endereço.
5. `ORDERED_REQUEST` (`seq_id`, `client_id`, `request_id`, `value`): operação
   ordenada enviada a cada RM ativo.
6. `REPLICA_APPLIED_ACK` (`last_applied_seq_id`, `num_reqs`, `total_sum`): o RM
   confirma **até onde o seu estado está aplicado** e reporta o estado (usado
   para o quórum e para detectar divergência).
7. `REPLICA_NACK` (`missing`): o RM pede retransmissão dos `seq_id` que faltam.
8. `REPLICA_HEARTBEAT`: o RM informa atividade ao FE.

**Ordenação, idempotência e quórum**

- O FE só incrementa `seq_id` ao aceitar uma requisição na ordem esperada do
  cliente; reenvios de `request_id` já processados são tratados como duplicatas
  e respondidos imediatamente, sem alterar a soma.
- Cada RM aplica estritamente `last_applied_seq_id + 1`. `ORDERED_REQUEST` fora
  de ordem vai para o **hold-back buffer**; lacunas disparam **NACK**, e o FE
  retransmite a partir do seu **log de operações**. Reentregas são ignoradas de
  forma idempotente.
- O FE confirma a operação ao cliente quando `quórum` réplicas têm
  `last_applied_seq_id >= seq_id`. Se o prazo expira sem quórum, o FE responde
  assim mesmo (o resultado é correto pela ordem total) e registra `quorum_timeout`.

## Estado da implementação

Implementado: sequenciamento global, disseminação unicast, **aplicação da soma
em todos os RMs**, **quórum de processamento**, recuperação de operações perdidas
por hold-back/NACK e detecção de divergência. O serviço permanece utilizável sem
réplicas (o FE responde imediatamente).

**Fora do escopo desta etapa** (próximos passos): eleição coordenada real (a
atual é dummy, só na inicialização), failover do FE e transferência de estado
por snapshot para um RM que entra depois que o log de operações já foi truncado.

## Teste de integração

O teste sobe um cluster real em subprocessos (loopback) e comprova (1) a
convergência de FE + réplicas, (2) o quórum, e (3) a recuperação de uma réplica
**atrasada** via NACK:

```bash
python3 tests/test_replicacao_ativa.py          # 2 lotes de 1500 ops = 3000
python3 tests/test_replicacao_ativa.py 50        # execução rápida
```

Ao final, FE, `replica-1`, `replica-2` e `replica-3-atrasada` devem apresentar
`num_reqs` e `total_sum` idênticos.

> Nota (macOS): o broadcast não é entregue na interface de loopback e um erro
> assíncrono de broadcast pode aparecer na operação seguinte do socket. A
> descoberta inclui a loopback (`127.0.0.1`) por unicast e tolera esse erro, de
> modo que o cluster roda em um único host macOS.

## Benchmark

```bash
python3 src/servidor.py 4000
python3 src/benchmark.py 4000
```

Com um FE sem réplicas, o resultado esperado é `num_reqs 1000000` e
`total_sum 1000000`. Com réplicas, cada operação espera o quórum, então a taxa é
menor.
