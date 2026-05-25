# 🚢 Desbloqueio do Estreito de Ormuz - Sistemas Distribuídos

![Go Version](https://img.shields.io/badge/Go-1.21-00ADD8?style=for-the-badge&logo=go)
![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?style=for-the-badge&logo=docker)
![Architecture](https://img.shields.io/badge/Arquitetura-P2P-8A2BE2?style=for-the-badge)
![Status](https://img.shields.io/badge/Status-Concluído-4CAF50?style=for-the-badge)

Este projeto é a solução para o **Problema 2** da disciplina **TEC502 - Sistemas Distribuídos** da Universidade Estadual de Feira de Santana (UEFS). 

O objetivo é desenvolver uma infraestrutura distribuída, **sem ponto único de falha (No Single Point of Failure)**, para coordenar uma frota de drones autônomos de monitoramento marítimo no Estreito de Ormuz. O sistema gerencia eventos de alta criticidade utilizando algoritmos clássicos de consenso distribuído, roteamento inteligente e resiliência a quedas de nós.


## 🏗️ Arquitetura do Sistema

O sistema abandona o modelo Cliente-Servidor tradicional em favor de uma **Arquitetura Peer-to-Peer (P2P)** dividida em três camadas funcionais:

1. **Camada de Orquestração (Brokers):** Nós P2P que mantêm o estado global da rede (Fila Distribuída) via *Gossip Protocol* e negociam o despacho de drones utilizando o algoritmo de **Ricart-Agrawala**.
2. **Camada de Telemetria (Sensores):** Produtores de eventos (anomalias marítimas) com roteamento estocástico e **Node Affinity** (preferência de conexão ao broker do seu setor geográfico, com *Failover* para os demais).
3. **Camada de Execução (Drones):** *Workers* autônomos que realizam *Polling* à procura de missões e emitem *Heartbeats* periódicos para provar vitalidade.

---

## ⚙️ Características Técnicas e Algoritmos

Este projeto implementa conceitos avançados de computação distribuída:

### 1. Exclusão Mútua Distribuída (Ricart-Agrawala)
Para evitar que dois brokers despachem drones diferentes para a mesma anomalia simultaneamente, foi implementado o algoritmo de Ricart-Agrawala. 
* O Broker solicitante faz *Multicast* de um pedido de permissão (`MsgReqDrone`) para a malha.
* Só entra na **Secção Crítica** (despacho) quando recebe a resposta (`MsgReplyOK`) de todos os *peers* ativos, garantindo segurança na alocação de recursos compartilhados.

### 2. Ordenação Causal (Relógio Lógico de Lamport)
Como relógios físicos sofrem de *clock drift*, o sistema utiliza os **Relógios de Lamport** para ordenar eventos de forma determinística. Em caso de pedidos concorrentes pelo mesmo drone, o Timestamp de Lamport atua como critério de desempate, evitando *deadlocks*.

### 3. Tolerância a Falhas e Auto-Cura
* **Failover TCP nos Sensores:** Os sensores tentam conectar-se ao seu Broker Primário (via lista no `.env`). Se a porta estiver fechada, assumem a falha instantaneamente e tentam o próximo nó da malha (*Fail-Fast*).
* **Watchdog de Drones:** Se um drone for "abatido" ou perder sinal, o Broker que gerencia a missão nota a ausência de *Heartbeats* por mais de 15 segundos. A missão sofre *Rollback* automático para a fila e a malha P2P é atualizada para que outro drone assuma a tarefa.
* **Limpeza de Topologia:** Brokers que falham ao responder 3 vezes consecutivas são removidos dinamicamente da lista de *peers*, impedindo que o quórum de consenso fique travado à espera de nós mortos.

### 4. Prevenção de Inanição (Aging/Envelhecimento)
Uma *Goroutine* dedicada varre continuamente a fila de pendências. Missões de baixa prioridade esquecidas por muito tempo sofrem um acréscimo automático de prioridade para garantir que não sofram *Starvation* perante o fluxo de missões críticas.

---

## 🗂️ Estrutura do Projeto

A base de código segue as melhores práticas de modularidade do Go:

```text
├── cmd/
│   ├── broker/       # Lógica do nó orquestrador (Consenso, Lamport, Watchdog)
│   ├── drone/        # Agente worker autônomo (Polling, Heartbeat)
│   ├── sensor/       # Produtor de anomalias (Geração estocástica, Failover)
│   ├── cliente/      # Interface interativa (CLI) para consulta de fila
│   └── testes/       # Suite de Engenharia do Caos (Teste automatizado)
├── internal/
│   └── models/       # Definição das APIs (MensagemDistribuida, Tipos de Payload)
├── docker-compose.yml # Topologia de rede e injeção de dependências
├── Dockerfile         # Imagem multi-stage build para os serviços Go
└── README.md

```

---

## 🚀 Como Executar o Projeto

A infraestrutura foi desenhada para emulação realista utilizando **Docker**. Todos os nós (4 Brokers, 4 Sensores e 3 Drones) correm em contentores isolados na mesma rede `bridge`.

### Pré-requisitos

* [Docker](https://docs.docker.com/get-docker/) e [Docker Compose](https://docs.docker.com/compose/install/) instalados.

### Inicializando a Malha P2P

Abra o terminal na raiz do projeto e execute:

```bash
docker compose up --build -d

```

O sistema subirá autonomamente. Os *Brokers* farão o *Handshake* para formar a malha, os *Sensores* começarão a injetar alertas aleatórios e os *Drones* processarão a fila dinamicamente.

Para acompanhar os logs em tempo real (exemplo do Broker 1):

```bash
docker logs -f broker-1

```

---

## 🧪 Testes Automatizados e Engenharia do Caos

O projeto inclui uma suíte robusta de testes focada em validação sob estresse. O container de testes tem o `docker.sock` mapeado, o que lhe dá permissões para **derrubar brokers e drones ativamente** durante a execução para testar a resiliência da arquitetura.

Para rodar os testes:

```bash
docker compose run --rm testes

```

O script testará conectividade, sincronização da fila, ordenação por prioridade, failover de broker, resgate de missões perdidas (watchdog) e consistência eventual sob carga.

---

## 💻 Interface do Operador (Cliente)

Para monitorar a saúde da fila distribuída manualmente sem olhar os logs, você pode utilizar o terminal interativo do operador:

```bash
docker compose run --rm cliente

```

Isso abrirá um menu onde você pode consultar um *snapshot* da Fila Global ou injetar missões personalizadas em um setor específico.

---

## 👨‍💻 Autor

Desenvolvido por **Ítallo de Santana Guimarães** - Engenharia de Computação - Universidade Estadual de Feira de Santana (UEFS)
