package main

import (
	"encoding/json"
	"fmt"
	"net"
	"ormuz_distribuido/internal/models"
	"os"
	"strings"
	"time"
)

func main() {
	droneID := os.Getenv("DRONE_ID")
	if droneID == "" {
		droneID = "DRONE-01"
	}

	// Lista de brokers configurados (ex: "localhost:9000,localhost:9001")
	brokerAddrs := strings.Split(os.Getenv("BROKER_ADDRS"), ",")

	for {
		sucesso := false
		for _, addr := range brokerAddrs {
			fmt.Printf("[%s] Tentando Broker: %s\n", droneID, addr)

			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
			if err != nil {
				continue
			}

			// 1. Notifica que está pronto para receber missão
			envelope := models.MensagemDistribuida{
				Tipo:     models.MsgReqDrone,
				SenderID: 0, // Drone não participa na malha P2P
				Payload:  droneID,
			}
			json.NewEncoder(conn).Encode(envelope)

			// 2. Espera a decisão do algoritmo Ricart-Agrawala (feita pelo Broker)
			var tarefa models.Requisicao
			err = json.NewDecoder(conn).Decode(&tarefa)
			conn.Close()

			if err == nil && tarefa.ID != "" {
				executarMissao(droneID, tarefa)
				sucesso = true
				break
			}
		}

		if !sucesso {
			time.Sleep(5 * time.Second)
		}
	}
}

func executarMissao(id string, t models.Requisicao) {
	fmt.Printf(">>> [%s] ASSUMIU: %s (Prioridade %d)\n", id, t.Descricao, t.Prioridade)
	time.Sleep(10 * time.Second) // Simula o tempo de voo e resolução
	fmt.Printf("--- [%s] MISSÃO %s CONCLUÍDA ---\n", id, t.ID)
}
