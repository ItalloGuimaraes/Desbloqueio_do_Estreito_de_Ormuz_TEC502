# 🚢 Desbloqueio do Estreito de Ormuz - Sistemas Distribuídos

![Go Version](https://img.shields.io/badge/Go-1.21-00ADD8?style=for-the-badge&logo=go)
![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?style=for-the-badge&logo=docker)
![Architecture](https://img.shields.io/badge/Arquitetura-P2P-8A2BE2?style=for-the-badge)
![Status](https://img.shields.io/badge/Status-Concluído-4CAF50?style=for-the-badge)

Este projeto é a solução para o **Problema 2** da disciplina **TEC502: MI - Concorrência e Conectividade** da Universidade Estadual de Feira de Santana (UEFS). 

O objetivo é desenvolver uma infraestrutura distribuída, **sem ponto único de falha (No Single Point of Failure)**, para coordenar uma frota de drones autônomos de monitoramento marítimo no Estreito de Ormuz. O sistema gerencia eventos de alta criticidade utilizando algoritmos clássicos de consenso distribuído, roteamento inteligente e resiliência a quedas de nós.

---

## 🏗️ Arquitetura do Sistema

O sistema abandona o modelo Cliente-Servidor tradicional em favor de uma **Arquitetura Peer-to-Peer (P2P)** dividida em três camadas funcionais.

1. **Camada de Orquestração (Brokers):** Nós P2P que mantêm o estado global da rede (Fila Distribuída) via *Gossip Protocol* e negociam o despacho de drones utilizando o algoritmo de **Ricart-Agrawala**.
2. **Camada de Telemetria (Sensores):** Produtores de eventos (anomalias marítimas) com roteamento estocástico e **Node Affinity** (preferência de conexão ao broker do seu setor geográfico, com *Failover* dinâmico para os demais).
3. **Camada de Execução (Drones):** *Workers* autônomos que realizam *Polling* à procura de missões e emitem *Heartbeats* periódicos para provar vitalidade.

---

## ⚙️ Características Técnicas e Algoritmos

* **Exclusão Mútua Distribuída (Ricart-Agrawala):** Evita que dois brokers despachem drones para a mesma missão. O Broker solicitante faz *Multicast* e só entra na **Secção Crítica** quando recebe `REPLY_OK` do quórum de *peers*. Conta com proteção interna contra concorrência reentrante.
* **Ordenação Causal (Relógio Lógico de Lamport):** Como relógios físicos sofrem *clock drift*, utilizamos Timestamps de Lamport. Em caso de empates concorrentes pelo mesmo drone, a tupla estruturada `(TS_Lamport, BrokerID)` atua como critério de desempate determinístico.
* **Watchdog de Drones:** Se um drone for abatido e o *Heartbeat* não for recebido por mais de 15 segundos, a missão sofre *Rollback* automático para a fila e a malha P2P é atualizada para replanejamento.
* **Prevenção de Inanição (Aging):** Uma *Goroutine* varre continuamente a fila de pendências. Missões de baixa prioridade esquecidas sofrem incremento automático de criticidade (*Starvation Prevention*).

---

## 🔌 API de Comunicação (Protocolo P2P)

Toda a comunicação do sistema utiliza **TCP Sockets puros** com serialização JSON, encapsulada num envelope único (`MensagemDistribuida`). O sistema não utiliza HTTP/REST.

| Tipo de Mensagem | Emissor | Destinatário | Descrição Funcional |
| --- | --- | --- | --- |
| `MsgJoin` | Novo Broker | Seed | Solicita entrada dinâmica na topologia P2P. |
| `MsgJoinACK` | Seed | Novo Broker | Confirma entrada e retorna metadados para conexão. |
| `MsgFullSync` | Seed | Novo Broker | Snapshot completo: transfere a Fila Distribuída atual para o novato. |
| `MsgSyncNew` | Sensor / Broker | Brokers | Propaga um novo alerta (missão) para a malha via *Gossip*. |
| `MsgSyncUpdate` | Broker | Peers | Sincroniza mudanças de estado atômicas (ex: `PENDENTE` $\rightarrow$ `EM_ATENDIMENTO`). |
| `MsgReqDrone` | Drone / Broker | Broker / Peers | O Drone usa para *Polling*. O Broker usa para solicitar permissão de Secção Crítica (R-A). |
| `MsgReplyOK` | Broker | Broker | Concede o voto de permissão no algoritmo de Ricart-Agrawala. |
| `MsgDroneHeartbeat` | Drone | Broker | Atualiza a propriedade `UltimoHeartbeat` (TTL) evitando o desarme do *Watchdog*. |
| `MsgDroneConcluido` | Drone | Broker | Notifica o fim da missão; libera o Drone para nova alocação. |
| `MsgConsultaFila` | Cliente CLI | Broker | Requisita o *snapshot* de memória local do Broker para leitura humana. |

---

## 🗂️ Estrutura do Projeto

```text
├── cmd/
│   ├── broker/        # Nó orquestrador (Lamport, Consenso R-A, Watchdog)
│   ├── drone/         # Worker autônomo (Polling TCP, Heartbeat)
│   ├── sensor/        # Produtor de anomalias (Geração estocástica, Fail-Fast)
│   ├── cliente/       # CLI interativa para consulta manual do estado global
│   └── testes/        # Testes Automatizados 
├── internal/
│   └── models/        # Modelagem estrita da API e Serializadores
├── docker-compose.yml # Orquestração do laboratório virtual
└── .env               # Mapeamento de IPs físicos/virtuais da malha

```

---

## 🛠️ Configuração de Rede (`.env`)

Por ser um sistema verdadeiramente distribuído, ele pode rodar em **uma única máquina** ou em **vários computadores físicos em um laboratório**. Para isso, é crucial configurar corretamente o arquivo `.env` na raiz do projeto:

```env
# IP do computador atual rodando este container
IP_MAQUINA=172.16.201.12

# Endereço "Semente" (Seed) de entrada na malha P2P
SEED_ADDR=172.16.201.11:9000

# Mapa estático de afinidade de nós e failover da malha
ADDR_BROKER_1=172.16.201.11:9000
ADDR_BROKER_2=172.16.201.12:9001
ADDR_BROKER_3=172.16.201.10:9002
ADDR_BROKER_4=172.16.201.10:9003

```

* **Nota:** O `SEED_ADDR` é o primeiro IP que um Broker recém-ligado vai procurar. Se o sistema estiver rodando numa única máquina para testes, você pode usar um IP único.

---

## 🚀 Como Executar

### Cenário 1: Tudo na mesma máquina (Ambiente de Teste)

Certifique-se de ter o Docker e o Docker Compose instalados.

```bash
# Sobe a malha completa (4 Brokers, 4 Sensores, 3 Drones) em background
docker compose up --build -d

# Para acompanhar os logs de comunicação P2P de um nó específico
docker logs -f broker-1

```

### Cenário 2: Laboratório com Múltiplas Máquinas

Se estiver executando em máquinas separadas (ex: `Maquina A` roda o Broker 1 e Drones; `Maquina B` roda Broker 2 e Sensores), configure o `.env` com os IPs da LAN e suba os serviços individualmente:

```bash
# Na Máquina A (IP final .11)
docker compose up -d broker1 drone_alpha drone_bravo

# Na Máquina B (IP final .12)
docker compose up -d broker2 sensor_setor_1 sensor_setor_2

```

---

## 🧪 Engenharia do Caos e Monitoramento

### Testes Automatizados (Chaos Engineering)

A infraestrutura inclui um contêiner de testes com acesso mapeado ao `docker.sock` do hospedeiro. Ele injeta falhas ativas na rede (ex: `docker kill broker-1`) em tempo de execução para validar se o *Failover* estocástico dos sensores e o *Watchdog* dos drones operam conforme o modelo teórico:

```bash
docker compose run --rm testes

```

### Cliente Operador (CLI)

Para monitorar a "saúde" da fila distribuída, aprovar ordens manuais ou visualizar a ordem de Lamport sem precisar ler logs, utilize o terminal interativo:

```bash
docker compose run --rm cliente

```

---

## 👨‍💻 Autor

Desenvolvido por **Ítallo de Santana Guimarães**  
Engenharia de Computação - Universidade Estadual de Feira de Santana (UEFS).

## 📄 Licença

Este projeto está licenciado sob a Licença MIT - veja o arquivo [LICENSE](LICENSE) para mais detalhes.
