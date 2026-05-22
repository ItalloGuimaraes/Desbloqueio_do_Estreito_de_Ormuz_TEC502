package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"ormuz_distribuido/internal/models"
	"os"
	"strings"
	"time"
)

// =========================================================
// Ponto de Entrada (Nó Emissor / Sensor)
// =========================================================

// main inicializa a instância do sensor autônomo, realizando o parse da topologia
// de rede (brokers) e estabelecendo a rotina infinita de injeção estocástica de eventos na malha.
func main() {
	// Carregamento da topologia alvo via variáveis de ambiente
	brokerAddrsRaw := os.Getenv("BROKER_ADDRS")
	if brokerAddrsRaw == "" {
		brokerAddrsRaw = "localhost:9000"
	}
	brokerAddrs := strings.Split(brokerAddrsRaw, ",")

	// Identidade do nó na matriz distribuída
	setorID := 1
	fmt.Sscanf(os.Getenv("SETOR_ID"), "%d", &setorID)

	fmt.Printf(">>> [SETOR %d] Sensor Marítimo Online\n", setorID)

	// Inicialização da semente de entropia para geração de eventos não determinísticos
	rand.Seed(time.Now().UnixNano())

	alertas := []string{
		"Embarcação à deriva detectada",
		"Objeto não identificado no canal",
		"Bloqueio parcial de rota comercial",
		"Sinal de socorro (SOS) captado",
		"Mancha de óleo identificada",
	}

	// Loop de Telemetria: geração contínua de anomalias simuladas
	for {
		prioridade := rand.Intn(5) + 1
		descricao := alertas[rand.Intn(len(alertas))]

		// Empacotamento do objeto de requisição (Payload)
		req := models.Requisicao{
			ID:         fmt.Sprintf("REQ-%d-%04d", setorID, rand.Intn(10000)),
			Setor:      setorID,
			Prioridade: prioridade,
			Descricao:  descricao,
			Status:     models.StatusPendente,
			CreatedAt:  time.Now(),
		}

		sucesso := false
		// Rotina de Roteamento com Failover:
		// Itera sobre a lista de endpoints disponíveis e aborta a iteração no primeiro sucesso.
		for _, addr := range brokerAddrs {
			addr = strings.TrimSpace(addr)
			if err := enviarAlerta(addr, req); err == nil {
				sucesso = true
				break
			}
		}

		if sucesso {
			// Renderização visual de prioridade para a CLI local
			prioLabel := strings.Repeat("★", prioridade) + strings.Repeat("☆", 5-prioridade)
			fmt.Printf("[SETOR %d] %-42s | %s | ID: %s\n",
				setorID, descricao, prioLabel, req.ID)
		} else {
			fmt.Printf("[SETOR %d] ✗ Nenhum broker disponível na malha!\n", setorID)
		}

		// Backoff Randômico (Jitter) para evitar saturação (Thundering Herd Problem) na rede
		proximoEnvio := rand.Intn(10) + 5
		time.Sleep(time.Duration(proximoEnvio) * time.Second)
	}
}

// =========================================================
// Roteamento e Transporte
// =========================================================

// enviarAlerta estabelece uma conexão TCP síncrona com timeout (Fail-Fast) para um nó alvo.
// Realiza o marshalling do alerta encapsulando-o no protocolo de MensagemDistribuida.
// Retorna erro em caso de Timeout/Refused, acionando a heurística de Failover do chamador.
func enviarAlerta(addr string, req models.Requisicao) error {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	// O Timestamp 0 indica que a mensagem é de origem externa (Client/Sensor),
	// delegando ao Broker receptor a responsabilidade de assinar o relógio de Lamport inicial.
	return json.NewEncoder(conn).Encode(models.MensagemDistribuida{
		Tipo:      models.MsgSyncNew,
		SenderID:  req.Setor,
		Timestamp: 0,
		Payload:   req,
	})
}
