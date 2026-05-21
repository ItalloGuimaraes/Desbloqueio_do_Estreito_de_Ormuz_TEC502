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

func main() {
	brokerAddrsRaw := os.Getenv("BROKER_ADDRS")
	if brokerAddrsRaw == "" {
		brokerAddrsRaw = "localhost:9000"
	}
	brokerAddrs := strings.Split(brokerAddrsRaw, ",")

	setorID := 1
	fmt.Sscanf(os.Getenv("SETOR_ID"), "%d", &setorID)

	fmt.Printf(">>> [SETOR %d] Sensor Marítimo Online\n", setorID)

	rand.Seed(time.Now().UnixNano())

	alertas := []string{
		"Embarcação à deriva detectada",
		"Objeto não identificado no canal",
		"Bloqueio parcial de rota comercial",
		"Sinal de socorro (SOS) captado",
		"Mancha de óleo identificada",
	}

	for {
		prioridade := rand.Intn(5) + 1
		descricao := alertas[rand.Intn(len(alertas))]

		req := models.Requisicao{
			ID:         fmt.Sprintf("REQ-%d-%04d", setorID, rand.Intn(10000)),
			Setor:      setorID,
			Prioridade: prioridade,
			Descricao:  descricao,
			Status:     models.StatusPendente,
			CreatedAt:  time.Now(),
		}

		sucesso := false
		for _, addr := range brokerAddrs {
			addr = strings.TrimSpace(addr)
			if err := enviarAlerta(addr, req); err == nil {
				sucesso = true
				break
			}
		}

		if sucesso {
			// Log claro mostrando o que o sensor enviou
			prioLabel := strings.Repeat("★", prioridade) + strings.Repeat("☆", 5-prioridade)
			fmt.Printf("[SETOR %d] %-42s | %s | ID: %s\n",
				setorID, descricao, prioLabel, req.ID)
		} else {
			fmt.Printf("[SETOR %d] ✗ Nenhum broker disponível!\n", setorID)
		}

		proximoEnvio := rand.Intn(10) + 5
		time.Sleep(time.Duration(proximoEnvio) * time.Second)
	}
}

// enviarAlerta envia a requisição para um broker via TCP.
// Retorna erro se o broker estiver offline, para que o loop tente o próximo.
func enviarAlerta(addr string, req models.Requisicao) error {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	return json.NewEncoder(conn).Encode(models.MensagemDistribuida{
		Tipo:      models.MsgSyncNew,
		SenderID:  req.Setor,
		Timestamp: 0, // broker atribuirá o timestamp de Lamport
		Payload:   req,
	})
}
