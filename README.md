# Serviço distribuído de soma sobre UDP

Implementação em Python 3 de um serviço de soma distribuído. O servidor recebe valores de clientes, soma cada requisição aceita uma única vez e mantém os totais em memória.

## Requisitos

- Python 3.10 ou superior.
- Rede IPv4 com suporte a broadcast para a descoberta automática.
- Cliente e servidor devem conseguir trocar datagramas UDP. Para a descoberta por broadcast, execute-os no mesmo segmento de rede.

O projeto usa somente a biblioteca padrão do Python; não é necessário instalar dependências externas. Em uma máquina Windows, os comandos abaixo podem ser executados em uma distribuição WSL, como Ubuntu.

## Arquivos

- `servidor.py`: recebe descoberta e requisições UDP, mantém os totais globais e o último estado processado de cada cliente identificado pelo IP.
- `cliente.py`: descobre o servidor, lê inteiros positivos da entrada padrão e envia uma requisição por vez. A leitura, a rede e a saída são tratadas por threads separadas.
- `protocolo.py`: codifica e decodifica as mensagens JSON UTF-8, incluindo a versão do protocolo.
- `benchmark.py`: executa um benchmark stop-and-wait de 1 milhão de requisições, cada uma com valor 1.

## Como executar

Abra um terminal na pasta do projeto. Inicie o servidor informando a porta UDP:

```bash
python3 servidor.py 4000
```

O servidor inicia com `num_reqs 0 total_sum 0`. Mantenha esse terminal aberto e, em outro terminal, inicie o cliente na mesma porta:

```bash
python3 cliente.py 4000
```

Após descobrir o servidor, digite um inteiro positivo por linha. Não é necessário pressionar Ctrl+D entre valores. Para encerrar a entrada e deixar o cliente concluir as requisições já enfileiradas, use Ctrl+D em Linux/WSL. Ctrl+C interrompe o cliente. Para parar o servidor, use Ctrl+C no terminal dele.

O cliente também permite configurar o timeout em segundos. O padrão é 0,010 segundo:

```bash
python3 cliente.py 4000 0.05
```

O servidor e o cliente devem usar a mesma porta. A porta representa a porta UDP de escuta do servidor; o cliente usa uma porta local efêmera.

## Protocolo e processamento

As mensagens são objetos JSON UTF-8 transportados por UDP:

1. `DISCOVER`: o cliente envia a descoberta por broadcast.
2. `DISCOVER_ACK`: o servidor responde por unicast e registra o IP do cliente.
3. `REQUEST`: contém `id_req` e `value`.
4. `ACK`: informa `id_req`, `num_reqs`, `total_sum` e o status (`PROCESSED`, `DUPLICATE` ou `OUT_OF_ORDER`).

Cada cliente começa com o ID 1. O cliente opera em stop-and-wait: só envia o próximo valor após receber o ACK da requisição atual. Se o timeout expirar, retransmite a mesma requisição. O servidor usa o IP de origem para manter a sequência de cada cliente, descarta duplicatas sem somá-las novamente e informa o último estado processado quando recebe uma requisição repetida ou fora de ordem.

O acumulador global e os estados dos clientes existem somente na memória do processo servidor. Reiniciar o servidor zera esses dados.

## Saída

O servidor imprime o estado inicial e, para cada requisição nova, exibe data/hora, IP do cliente, ID, valor e totais atualizados. Requisições duplicadas e fora de ordem também são sinalizadas.

O cliente exibe o IP descoberto, as mensagens de envio e reenvio e, após cada ACK aceito, os totais informados pelo servidor. Valores inválidos ou não positivos são rejeitados pelo cliente.

## Benchmark

O benchmark atual executa **1 milhão de requisições de um único cliente**. Como é stop-and-wait, ele aguarda um ACK antes de enviar cada requisição seguinte; não simula clientes concorrentes nem IPs diferentes.

Inicie um servidor novo e zerado em um terminal:

```bash
python3 servidor.py 4000
```

Em outro terminal, execute:

```bash
python3 benchmark.py 4000
```

Como cada valor é 1, o resultado esperado ao final é `num_reqs 1000000` e `total_sum 1000000`. O benchmark informa progresso, taxa média, retransmissões e compara o ACK final com esses valores. Execute-o contra um servidor dedicado e zerado; um servidor que já processou requisições fará a comparação falhar.

O tempo medido não inclui a descoberta do servidor. A taxa também inclui os custos de comunicação UDP e processamento do servidor, inclusive a impressão e o flush de cada requisição.