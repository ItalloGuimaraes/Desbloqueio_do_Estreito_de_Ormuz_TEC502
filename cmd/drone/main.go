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

	fmt.Printf(">>> [DRONE %s] Online\n", droneID)

	for {
		sucesso := false
		for _, addr := range brokerAddrs {
			addr = strings.TrimSpace(addr)

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

			// Aguarda decisão do Ricart-Agrawala (broker pode demorar até 5s + margem)
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

// executarMissao simula o trabalho do drone enviando heartbeats periódicos ao broker.
// Se o broker não receber heartbeat por 15s, devolve a missão à fila automaticamente.
func executarMissao(id string, t models.Requisicao, brokerAddrs []string) {
	tempoTotal := time.Duration(10+(t.Prioridade*3)) * time.Second

	fmt.Printf("\n╔═══════════════════════════════════════════════════════╗\n")
	fmt.Printf("║ DRONE %-6s ASSUMIU MISSÃO                           ║\n", id)
	fmt.Printf("╠═══════════════════════════════════════════════════════╣\n")
	fmt.Printf("║ ID       : %-42s ║\n", t.ID)
	fmt.Printf("║ Alerta   : %-42s ║\n", t.Descricao)
	fmt.Printf("║ Setor    : %-42d ║\n", t.Setor)
	fmt.Printf("║ Prioridade: %-41d ║\n", t.Prioridade)
	fmt.Printf("║ Duração  : %-42s ║\n", tempoTotal)
	fmt.Printf("╚═══════════════════════════════════════════════════════╝\n\n")

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

	time.Sleep(tempoTotal)
	close(done)

	fmt.Printf("[DRONE %s] ✓ Missão concluída: \"%s\" (Setor %d) em %v\n",
		id, t.Descricao, t.Setor, time.Since(inicio).Round(time.Second))

	enviarConclusao(id, t.ID, brokerAddrs)
}

// enviarHeartbeat sinaliza ao broker que o drone ainda está ativo na missão.
// Tenta cada broker na lista até conseguir enviar para pelo menos um.
func enviarHeartbeat(droneID, missionID string, brokerAddrs []string) {
	for _, addr := range brokerAddrs {
		conn, err := net.DialTimeout("tcp", strings.TrimSpace(addr), 1*time.Second)
		if err != nil {
			continue
		}
		json.NewEncoder(conn).Encode(models.MensagemDistribuida{
			Tipo:     models.MsgDroneHeartbeat,
			SenderID: 0,
			Payload:  models.DroneStatus{DroneID: droneID, MissionID: missionID},
		})
		conn.Close()
		// O 'return' que estava aqui foi REMOVIDO para ele avisar toda a malha!
	}
}

// enviarConclusao notifica o broker que a missão foi concluída com sucesso,
// para que o status seja atualizado na fila distribuída de todos os brokers.
func enviarConclusao(droneID, missionID string, brokerAddrs []string) {
	for _, addr := range brokerAddrs {
		conn, err := net.DialTimeout("tcp", strings.TrimSpace(addr), 2*time.Second)
		if err != nil {
			continue
		}
		json.NewEncoder(conn).Encode(models.MensagemDistribuida{
			Tipo:     models.MsgDroneConcluido,
			SenderID: 0,
			Payload:  models.DroneStatus{DroneID: droneID, MissionID: missionID},
		})
		conn.Close()
		// O 'return' que estava aqui foi REMOVIDO também!
	}
}
