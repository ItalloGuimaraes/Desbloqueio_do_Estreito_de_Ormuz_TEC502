package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"ormuz_distribuido/internal/models" // Certifique-se de que o path bate com seu go.mod
	"os"
	"time"
)

func main() {
	// 1. Configurações via variáveis de ambiente para facilitar o Docker futuramente
	brokerAddr := os.Getenv("BROKER_ADDR")
	if brokerAddr == "" {
		brokerAddr = "localhost:9000"
	}
	setorID := 1
	fmt.Sscanf(os.Getenv("SETOR_ID"), "%d", &setorID)

	fmt.Printf(">>> [SETOR %d] Sensor Marítimo Online (Conectando em %s via TCP)\n", setorID, brokerAddr)

	// Semente para geração de dados aleatórios
	rand.Seed(time.Now().UnixNano())

	for {
		// 2. Gerar dados aleatórios (Requisito 2 do PBL)
		prioridade := rand.Intn(5) + 1 // 1 a 5
		alertas := []string{
			"Embarcação à deriva detectada",
			"Objeto não identificado no canal",
			"Bloqueio parcial de rota comercial",
			"Sinal de socorro (SOS) captado",
			"Mancha de óleo identificada",
		}
		descricao := alertas[rand.Intn(len(alertas))]

		// 3. Criar a estrutura da Requisição
		req := models.Requisicao{
			ID:         fmt.Sprintf("REQ-%d-%d", setorID, rand.Intn(10000)),
			Setor:      setorID,
			Prioridade: prioridade,
			Descricao:  descricao,
			Status:     models.StatusPendente,
			CreatedAt:  time.Now(),
		}

		// 4. Enviar via TCP (Garantia de entrega)
		enviarAlerta(brokerAddr, req)

		// 5. Intervalo aleatório para simular carga dinâmica
		proximoEnvio := rand.Intn(10) + 5
		time.Sleep(time.Duration(proximoEnvio) * time.Second)
	}
}

func enviarAlerta(addr string, req models.Requisicao) {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		fmt.Printf("[ERRO] Não foi possível conectar ao Broker: %v\n", err)
		return
	}
	defer conn.Close()

	// Como o Broker espera uma MensagemDistribuida para atualizar o relógio de Lamport
	envelope := models.MensagemDistribuida{
		Tipo:      models.MsgSyncNew,
		SenderID:  req.Setor, // No caso do sensor, usamos o ID do setor
		Timestamp: 0,         // Sensores não mantêm relógio de Lamport, o Broker atribuirá
		Payload:   req,
	}

	err = json.NewEncoder(conn).Encode(envelope)
	if err != nil {
		fmt.Printf("[ERRO] Falha ao codificar alerta JSON: %v\n", err)
	} else {
		fmt.Printf("[%s] Alerta enviado: %s (Prioridade %d)\n", time.Now().Format("15:04:05"), req.Descricao, req.Prioridade)
	}
}
