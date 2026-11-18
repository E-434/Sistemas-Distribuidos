# Serviço distribuído de soma sobre UDP

Implementação em Python 3.10+ de um serviço distribuído de soma sobre UDP.
Todos os nós são iniciados pelo mesmo programa: cada servidor procura um
front-end ativo; se encontrar, inicia como Replica Manager, e se não encontrar
um, usa uma eleição dummy que promove o próprio nó a front-end. O FE atribui
um `seq_id` global, calcula o total local e dissemina cada operação por
multicast. Os RMs validam e confirmam apenas o recebimento. **A promoção atual
é apenas um placeholder: não há eleição coordenada, aplicação da soma nos
RMs, quórum de processamento, failover ou recuperação de estado.**

O projeto usa somente a biblioteca padrão do Python e pode ser executado em
Linux/Unix. Não há dependência de APIs específicas do Windows.

## Arquivos

- `src/cliente.py`: gera um `client_id`, descobre o front-end e envia operações
  stop-and-wait com `request_id`.
- `src/servidor.py`: servidor UDP unificado. Descobre o FE ativo e executa
  como RM, ou inicia como FE pelo placeholder de eleição; contém ambos os
  papéis e seus canais de comunicação.
- `src/replica_manager.py`: entrypoint legado desativado; a implementação do
  Replica Manager foi incorporada a `src/servidor.py`.
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

## Execução

Inicie todos os servidores usando o mesmo comando e a porta externa do serviço.
O primeiro nó sem FE detectável se promove a FE; os próximos se registram como
RMs:

```bash
python3 src/servidor.py 4000
python3 src/servidor.py 4000
```

O processo que encontrar o FE ativo junta-se ao multicast, registra-se e envia
heartbeats. Use o mesmo valor de porta externa em todos os nós do cluster;
apenas o FE ocupa essa porta para atender descoberta e clientes. A porta de
multicast permanece configurável e, por padrão, é `5001`.

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

A versão atual fornece os dois canais UDP e nós RM recebendo operações.
O estado da soma continua local ao FE e é respondido sem esperar ACKs; não há
quorum de processamento, recuperação de estado, failover ou eleição. O
heartbeat apenas classifica membership e não promove líderes. A eleição dummy
ocorre somente na inicialização: se a descoberta não encontrar um FE, o próprio
processo se promove. Ela não coordena candidatos, então inícios simultâneos
podem criar mais de um FE.

### Parâmetros de cluster

FE e RMs precisam usar o mesmo endereço e porta multicast:

```bash
python3 src/servidor.py 4000 \
  --multicast-group 239.10.10.10 --multicast-port 5001 \
  --heartbeat-interval 1 --heartbeat-timeout 4 \
  --receipt-ack-timeout 0.5 --expected-replicas 2
python3 src/servidor.py 4000 \
  --multicast-group 239.10.10.10 --multicast-port 5001 \
  --heartbeat-interval 1
```

As opções de cluster devem ser iguais em todos os servidores. Para testar em
máquinas distintas, use a mesma porta externa e verifique se broadcast e
multicast UDP estão permitidos na rede.

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
