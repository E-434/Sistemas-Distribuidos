# Serviço distribuído de soma sobre UDP

Implementação em Python 3.10+ do canal externo cliente-front-end para um
serviço de soma. O front-end atende os clientes, atribui um `seq_id` global e
responde com o total acumulado. O front-end dissemina cada operação aceita
por multicast para os Replica Managers, que validam e confirmam apenas o
recebimento. **Não há aplicação da soma nos RMs, replicação ativa, quórum de
processamento ou eleição em anel**. O FE continua calculando a resposta com
seu acumulador local, sem tolerância a falhas.

O projeto usa somente a biblioteca padrão do Python e pode ser executado em
Linux/Unix. Não há dependência de APIs específicas do Windows.

## Arquivos

- `src/cliente.py`: gera um `client_id`, descobre o front-end e envia operações
  stop-and-wait com `request_id`.
- `src/servidor.py`: front-end UDP; sequencia operações, dissemina multicast,
  monitora membership e agrega métricas de ACKs de recebimento.
- `src/replica_manager.py`: processo UDP independente de Replica Manager;
  registra-se no FE, recebe multicast, valida sequências e confirma recebimento
  sem executar operações.
- `src/descoberta.py`: descoberta do front-end por broadcast e resposta unicast.
- `src/processamento.py`: sequenciamento global, idempotência por cliente e
  acumulador temporário da configuração sem réplicas.
- `src/protocolo.py`: mensagens versionadas para os canais externo e
  intra-cluster, incluindo operações multicast, ACKs de recebimento e heartbeat.
- `src/arquitetura.py`: identidades, descritores e estado de membership da
  topologia do cluster.
- `src/interface.py`: formatação dos logs com identificação do front-end e
  `seq_id`.
- `src/benchmark.py` e `src/Benchmark_file.py`: benchmarks usando o mesmo
  protocolo cliente-front-end.
- `RELATORIO_EVOLUCAO_ARQUITETURAL.md`: mapeamento detalhado da refatoração da
  Parte 1 para esta etapa da Parte 2.

## Execução

Inicie o front-end:

```bash
python3 src/servidor.py 4000
```

Inicie um ou mais Replica Managers em terminais separados. Cada processo
precisa de um identificador único e deve escutar na porta multicast
configurada. No mesmo host, o socket UDP reutiliza a porta do grupo:

```bash
python3 src/replica_manager.py rm-1 5001 127.0.0.1 4000
python3 src/replica_manager.py rm-2 5001 127.0.0.1 4000
```

O FE registra cada RM e acompanha seus heartbeats. O RM recebe operações por
multicast e envia ACK confirmando o recebimento válido; ele não calcula nem
mantém cópia do acumulador.

Em outro terminal, inicie o cliente:

```bash
python3 src/cliente.py 4000
```

Digite inteiros positivos, um por linha. Ctrl+D encerra a entrada depois de
processar a fila; Ctrl+C interrompe o cliente. Para encerrar o front-end, use
Ctrl+C no terminal correspondente. O timeout do cliente pode ser configurado
em segundos:

```bash
python3 src/cliente.py 4000 0.05
```

## Protocolo e estado

Os datagramas são objetos JSON UTF-8 com `version: 2`.

1. `DISCOVER_FRONTEND`: cliente anuncia seu `client_id` por broadcast.
2. `FRONTEND_INFO`: o front-end responde unicast com identidade e endereço.
3. `SUM_REQUEST`: inclui `client_id`, `request_id` e `value`.
4. `SUM_RESULT`: inclui o `seq_id` global, estado acumulado e status.
5. `ORDERED_REQUEST`: o FE envia por multicast o `seq_id` e os dados da
   operação aos RMs.
6. `REPLICA_RECEIPT_ACK`: RM confirma recebimento e validação estrutural; não
   confirma execução da soma.
7. `REPLICA_HEARTBEAT`: RM informa atividade ao FE para atualizar membership.

O front-end só incrementa `seq_id` ao aceitar uma nova requisição na ordem
esperada daquele cliente. Reenvios de `request_id` já processados são
identificados como duplicatas e não alteram a soma. O `seq_id` é monotônico
durante a vida do FE e é incluído no datagrama multicast.

O processo RM registra `node_id` e porta de escuta com `REPLICA_REGISTER`;
o front-end confirma com `REPLICA_REGISTERED` e mantém membership em memória.
`REPLICA_HEARTBEAT` atualiza a última atividade; nós sem heartbeat dentro do
timeout configurado ficam `suspect` e deixam de ser alvos do multicast.

Cada `ORDERED_REQUEST` inclui `seq_id`, `client_id`, `request_id` e `value`.
O RM valida os campos e reporta lacunas, duplicatas ou sequências fora de
ordem para observabilidade, mas não aplica a operação. `REPLICA_RECEIPT_ACK`
confirma somente que o datagrama válido foi recebido. O FE conta ACKs por
sequência e registra se a meta de recebimento foi atingida; essa métrica não
condiciona nem valida o resultado enviado ao cliente.

## Estado da implementação

A versão atual fornece os dois canais UDP e processos RM recebendo operações.
O estado da soma continua local ao FE e é respondido sem esperar ACKs; não há
quorum de processamento, recuperação de estado, failover ou eleição. O
heartbeat apenas classifica membership e não promove líderes.

### Parâmetros de cluster

FE e RMs precisam usar o mesmo endereço e porta multicast:

```bash
python3 src/servidor.py 4000 \
  --multicast-group 239.10.10.10 --multicast-port 5001 \
  --heartbeat-interval 1 --heartbeat-timeout 4 \
  --receipt-ack-timeout 0.5 --expected-replicas 2
python3 src/replica_manager.py rm-1 5001 127.0.0.1 4000 \
  --multicast-group 239.10.10.10 --multicast-port 5001 \
  --heartbeat-interval 1
```

`--receipt-ack-quorum` configura uma meta de ACKs de **recebimento**. Se não
informada (ou 0), a meta é maioria do número esperado; `--expected-replicas 0`
usa o membership ativo no momento do envio. É apenas instrumentação e não
representa um quórum de resultados computados.

## Benchmark

O benchmark padrão envia 1 milhão de requisições de valor 1:

```bash
python3 src/servidor.py 4000
python3 src/benchmark.py 4000
```

O resultado esperado em um front-end recém-iniciado é `num_reqs 1000000` e
`total_sum 1000000`. O benchmark de arquivo aceita um caminho de números:

```bash
python3 src/Benchmark_file.py 4000 src/RAND_NUM_1.txt
```
