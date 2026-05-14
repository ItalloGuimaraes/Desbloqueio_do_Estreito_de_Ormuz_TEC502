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

	brokerAddrs := strings.Split(os.Getenv("BROKER_ADDRS"), ",")

	fmt.Printf(">>> [%s] Drone online\n", droneID)

	for {
		sucesso := false
		for _, addr := range brokerAddrs {
			addr = strings.TrimSpace(addr)
			fmt.Printf("[%s] Tentando broker: %s\n", droneID, addr)

			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
			if err != nil {
				continue
			}

			// Solicita missão ao broker
			envelope := models.MensagemDistribuida{
				Tipo:     models.MsgReqDrone,
				SenderID: 0,
				Payload:  droneID,
			}
			json.NewEncoder(conn).Encode(envelope)

			// Aguarda decisão do Ricart-Agrawala (o broker pode demorar até 5s)
			conn.SetReadDeadline(time.Now().Add(10 * time.Second))
			var tarefa models.Requisicao
			err = json.NewDecoder(conn).Decode(&tarefa)
			conn.Close()

			if err == nil && tarefa.ID != "" {
				executarMissao(droneID, tarefa, brokerAddrs)
				sucesso = true
				break
			}
		}

		if !sucesso {
			time.Sleep(5 * time.Second)
		}
	}
}

// executarMissao simula o trabalho do drone e envia heartbeats periódicos ao broker.
// Se o broker detectar ausência de heartbeat, devolve a missão à fila.
func executarMissao(id string, t models.Requisicao, brokerAddrs []string) {
	tempoTotal := time.Duration(10+(t.Prioridade*3)) * time.Second
	fmt.Printf(">>> [%s] ASSUMIU: %s | Prioridade %d | Tempo estimado: %v\n",
		id, t.Descricao, t.Prioridade, tempoTotal)

	inicio := time.Now()

	// Goroutine de heartbeat: avisa o broker que o drone ainda está vivo
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				enviarHeartbeat(id, t.ID, brokerAddrs)
			}
		}
	}()

	// Simula o trabalho
	time.Sleep(tempoTotal)
	close(done)

	fmt.Printf("--- [%s] MISSÃO %s CONCLUÍDA em %v ---\n", id, t.ID, time.Since(inicio))

	// Notifica conclusão ao broker
	enviarConclusao(id, t.ID, brokerAddrs)
}

func enviarHeartbeat(droneID, missionID string, brokerAddrs []string) {
	for _, addr := range brokerAddrs {
		addr = strings.TrimSpace(addr)
		conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
		if err != nil {
			continue
		}
		json.NewEncoder(conn).Encode(models.MensagemDistribuida{
			Tipo:     models.MsgDroneHeartbeat,
			SenderID: 0,
			Payload: models.DroneStatus{
				DroneID:   droneID,
				MissionID: missionID,
			},
		})
		conn.Close()
		return // Basta um broker receber
	}
}

func enviarConclusao(droneID, missionID string, brokerAddrs []string) {
	for _, addr := range brokerAddrs {
		addr = strings.TrimSpace(addr)
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			continue
		}
		json.NewEncoder(conn).Encode(models.MensagemDistribuida{
			Tipo:     models.MsgDroneConcluido,
			SenderID: 0,
			Payload: models.DroneStatus{
				DroneID:   droneID,
				MissionID: missionID,
			},
		})
		conn.Close()
		return
	}
}
