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

// =========================================================
// Ponto de Entrada (Agente Autônomo)
// =========================================================

// main inicializa o nó Worker (Drone), realiza o bind das variáveis de ambiente
// e inicia o loop principal de Polling para requisição de carga de trabalho (Missões).
func main() {
	droneID := os.Getenv("DRONE_ID")
	if droneID == "" {
		droneID = "DRONE-01"
	}

	// Extração da topologia do cluster para roteamento de requisições
	brokerAddrs := strings.Split(os.Getenv("BROKER_ADDRS"), ",")

	fmt.Printf(">>> [DRONE %s] Online\n", droneID)

	// Loop infinito de Polling: O drone busca continuamente por recursos liberados
	for {
		sucesso := false
		for _, addr := range brokerAddrs {
			addr = strings.TrimSpace(addr)

			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
			if err != nil {
				continue
			}

			// Transmissão da intenção de alocação de missão ao cluster
			envelope := models.MensagemDistribuida{
				Tipo:     models.MsgReqDrone,
				SenderID: 0, // Sender 0 identifica agentes externos à malha P2P
				Payload:  droneID,
			}
			json.NewEncoder(conn).Encode(envelope)

			// Aguarda a resolução do consenso distribuído (Ricart-Agrawala).
			// O Timeout reflete a latência esperada para o fechamento do quórum entre os brokers.
			conn.SetReadDeadline(time.Now().Add(10 * time.Second))
			var tarefa models.Requisicao
			err = json.NewDecoder(conn).Decode(&tarefa)
			conn.Close()

			// Validação da obtenção do Lock (Missão concedida)
			if err == nil && tarefa.ID != "" {
				executarMissao(droneID, tarefa, brokerAddrs)
				sucesso = true
				break
			}
		}

		// Backoff delay caso a fila esteja vazia ou a rede indisponível
		if !sucesso {
			time.Sleep(5 * time.Second)
		}
	}
}

// =========================================================
// Máquina de Estado e Execução
// =========================================================

// executarMissao simula o tempo de processamento da tarefa baseando-se no nível de prioridade.
// Instancia rotinas concorrentes para manutenção do estado de sessão (Keep-Alive) no cluster.
func executarMissao(id string, t models.Requisicao, brokerAddrs []string) {
	// Cálculo heurístico de duração baseado na urgência (Prioridade)
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

	// Contexto de cancelamento via Channel para a rotina de sinalização
	done := make(chan struct{})

	// Goroutine dedicada ao envio de sinais Keep-Alive (Heartbeats)
	// Previne que os mecanismos Watchdog dos brokers efetuem o Rollback da missão
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

	// Bloqueio síncrono simulando a operação física
	time.Sleep(tempoTotal)

	// Sinalização de término para encerramento da Goroutine de Heartbeat
	close(done)

	fmt.Printf("[DRONE %s] ✓ Missão concluída: \"%s\" (Setor %d) em %v\n",
		id, t.Descricao, t.Setor, time.Since(inicio).Round(time.Second))

	enviarConclusao(id, t.ID, brokerAddrs)
}

// =========================================================
// Comunicação de Rede e Resiliência
// =========================================================

// enviarHeartbeat transmite pacotes de vitalidade (TTL Refresh) para a topologia conhecida.
// Utiliza padrão de Broadcast redundante: tenta notificar todos os nós da lista
// para garantir a sincronização do estado global mesmo em cenários de partição de rede (NAT/Firewall).
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
		// Transmissão mantida em malha aberta (sem return precoce) para máxima resiliência
	}
}

// enviarConclusao injeta a alteração atômica de status (StatusConcluido) no sistema.
// Segue a mesma diretriz de tolerância a falhas do Heartbeat, notificando todos os endpoints alcançáveis.
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
		// Transmissão mantida em malha aberta para máxima resiliência
	}
}
