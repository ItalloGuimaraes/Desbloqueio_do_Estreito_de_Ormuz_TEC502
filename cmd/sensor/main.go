package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"ormuz_distribuido/internal/models" // Caminho baseado no seu go.mod
	"os"
	"strings"
	"time"
)

func main() {
	// 1. Configuração de múltiplos Brokers para alta disponibilidade
	// Lê uma lista separada por vírgulas (ex: "broker1:9000,broker2:9000")
	brokerAddrsRaw := os.Getenv("BROKER_ADDRS")
	if brokerAddrsRaw == "" {
		brokerAddrsRaw = "localhost:9000"
	}
	brokerAddrs := strings.Split(brokerAddrsRaw, ",")

	setorID := 1
	fmt.Sscanf(os.Getenv("SETOR_ID"), "%d", &setorID)

	fmt.Printf(">>> [SETOR %d] Sensor Marítimo Online\n", setorID)

	rand.Seed(time.Now().UnixNano())

	for {
		// 2. Gerar dados aleatórios para simular carga (Requisito 2)
		prioridade := rand.Intn(5) + 1
		alertas := []string{
			"Embarcação à deriva detectada",
			"Objeto não identificado no canal",
			"Bloqueio parcial de rota comercial",
			"Sinal de socorro (SOS) captado",
			"Mancha de óleo identificada",
		}
		descricao := alertas[rand.Intn(len(alertas))]

		req := models.Requisicao{
			ID:         fmt.Sprintf("REQ-%d-%d", setorID, rand.Intn(10000)),
			Setor:      setorID,
			Prioridade: prioridade,
			Descricao:  descricao,
			Status:     models.StatusPendente,
			CreatedAt:  time.Now(),
		}

		// 3. Tentativa de envio para qualquer Broker disponível na malha
		sucesso := false
		for _, addr := range brokerAddrs {
			addr = strings.TrimSpace(addr)
			err := enviarAlerta(addr, req)
			if err == nil {
				sucesso = true
				break // Sai do loop se conseguir enviar para um broker
			}
		}

		if !sucesso {
			fmt.Printf("[ALERTA] Falha crítica: nenhum broker disponível para o Setor %d\n", setorID)
		}

		// 4. Intervalo aleatório para simulação dinâmica [cite: 25]
		proximoEnvio := rand.Intn(10) + 5
		time.Sleep(time.Duration(proximoEnvio) * time.Second)
	}
}

// Retorna erro para que o loop principal saiba se deve tentar outro broker
func enviarAlerta(addr string, req models.Requisicao) error {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	envelope := models.MensagemDistribuida{
		Tipo:      models.MsgSyncNew,
		SenderID:  req.Setor,
		Timestamp: 0, // O Broker atribuirá o tempo de Lamport
		Payload:   req,
	}

	err = json.NewEncoder(conn).Encode(envelope)
	if err != nil {
		return err
	}

	fmt.Printf("[%s] Alerta enviado para %s: %s (Prioridade %d)\n",
		time.Now().Format("15:04:05"), addr, req.Descricao, req.Prioridade)
	return nil
}
