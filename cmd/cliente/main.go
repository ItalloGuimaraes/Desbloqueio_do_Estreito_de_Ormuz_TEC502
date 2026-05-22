package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"ormuz_distribuido/internal/models"
	"os"
	"strconv"
	"strings"
	"time"
)

// brokerAddrs armazena a lista de endereços dos nós (brokers) disponíveis
// na malha P2P para roteamento de requisições e consultas de estado.
var brokerAddrs []string

// main inicializa o nó cliente (Operador).
// Realiza o parsing das variáveis de ambiente para descoberta dos brokers,
// inicializa o gerador de entropia para IDs aleatórios e inicia o loop da CLI (REPL).
func main() {
	addrsRaw := os.Getenv("BROKER_ADDRS")
	if addrsRaw == "" {
		addrsRaw = "broker-1:9000,broker-2:9000,broker-3:9000"
	}

	// Popula a lista de nós disponíveis para a rotina de failover
	for _, a := range strings.Split(addrsRaw, ",") {
		brokerAddrs = append(brokerAddrs, strings.TrimSpace(a))
	}

	rand.Seed(time.Now().UnixNano())

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║         SISTEMA DE MONITORAMENTO — ESTREITO DE ORMUZ         ║")
	fmt.Println("║                   Terminal do Operador                       ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		exibirMenu()
		fmt.Print("  Opção: ")
		if !scanner.Scan() {
			break
		}
		opcao := strings.TrimSpace(scanner.Text())

		switch opcao {
		case "1":
			enviarRequisicaoManual(scanner)
		case "2":
			enviarRequisicaoAleatoria()
		case "3":
			consultarFila()
		case "4":
			enviarMultiplasRequisicoes(scanner)
		case "0":
			fmt.Println("\n  Encerrando terminal do operador.")
			return
		default:
			fmt.Println("\n  ✗ Opção inválida.")
		}
	}
}

// exibirMenu renderiza a interface de texto (CLI) com as rotinas operacionais
// disponíveis para a administração da malha.
func exibirMenu() {
	fmt.Println()
	fmt.Println("  ┌──────────────────────────────────────┐")
	fmt.Println("  │            MENU PRINCIPAL            │")
	fmt.Println("  ├──────────────────────────────────────┤")
	fmt.Println("  │  1. Enviar requisição manual         │")
	fmt.Println("  │  2. Enviar requisição aleatória      │")
	fmt.Println("  │  3. Consultar fila de requisições    │")
	fmt.Println("  │  4. Enviar múltiplas requisições     │")
	fmt.Println("  │  0. Sair                             │")
	fmt.Println("  └──────────────────────────────────────┘")
}

// enviarRequisicaoManual coleta parâmetros via stdin (tipo, prioridade e setor),
// instancia a estrutura models.Requisicao e invoca o despachante de rede.
func enviarRequisicaoManual(scanner *bufio.Scanner) {
	fmt.Println()
	fmt.Println("  ── NOVA REQUISIÇÃO MANUAL ──────────────────────────────────")

	alertas := []string{
		"1. Embarcação à deriva detectada",
		"2. Objeto não identificado no canal",
		"3. Bloqueio parcial de rota comercial",
		"4. Sinal de socorro (SOS) captado",
		"5. Mancha de óleo identificada",
	}
	for _, a := range alertas {
		fmt.Printf("     %s\n", a)
	}

	// Coleta e validação do payload da missão
	fmt.Print("  Escolha o tipo de alerta (1-5): ")
	scanner.Scan()
	tipoStr := strings.TrimSpace(scanner.Text())
	tipo, err := strconv.Atoi(tipoStr)
	if err != nil || tipo < 1 || tipo > 5 {
		fmt.Println("  ✗ Tipo inválido.")
		return
	}
	descricoes := []string{
		"Embarcação à deriva detectada",
		"Objeto não identificado no canal",
		"Bloqueio parcial de rota comercial",
		"Sinal de socorro (SOS) captado",
		"Mancha de óleo identificada",
	}

	fmt.Print("  Prioridade (1=baixa … 5=crítica): ")
	scanner.Scan()
	prioStr := strings.TrimSpace(scanner.Text())
	prioridade, err := strconv.Atoi(prioStr)
	if err != nil || prioridade < 1 || prioridade > 5 {
		fmt.Println("  ✗ Prioridade inválida.")
		return
	}

	fmt.Print("  Setor: ")
	scanner.Scan()
	setorStr := strings.TrimSpace(scanner.Text())
	setor, err := strconv.Atoi(setorStr)
	if err != nil || setor < 1 || setor > 9 {
		fmt.Println("  ✗ Setor inválido.")
		return
	}

	// Instanciação da requisição com status primário
	req := models.Requisicao{
		ID:         fmt.Sprintf("REQ-%d-%04d", setor, rand.Intn(10000)),
		Setor:      setor,
		Prioridade: prioridade,
		Descricao:  descricoes[tipo-1],
		Status:     models.StatusPendente,
		CreatedAt:  time.Now(),
	}

	enviar(req)
}

// enviarRequisicaoAleatoria gera um payload estocástico de models.Requisicao.
// Utilizado primariamente para simulação de eventos em testes unitários ou de integração.
func enviarRequisicaoAleatoria() {
	descricoes := []string{
		"Embarcação à deriva detectada",
		"Objeto não identificado no canal",
		"Bloqueio parcial de rota comercial",
		"Sinal de socorro (SOS) captado",
		"Mancha de óleo identificada",
	}
	setor := rand.Intn(2) + 1
	req := models.Requisicao{
		ID:         fmt.Sprintf("REQ-%d-%04d", setor, rand.Intn(10000)),
		Setor:      setor,
		Prioridade: rand.Intn(5) + 1,
		Descricao:  descricoes[rand.Intn(len(descricoes))],
		Status:     models.StatusPendente,
		CreatedAt:  time.Now(),
	}
	enviar(req)
}

// enviarMultiplasRequisicoes executa uma rotina de injeção sequencial de pacotes
// na malha (Load Testing) com controle de vazão (delay) para evitar saturação TCP.
func enviarMultiplasRequisicoes(scanner *bufio.Scanner) {
	fmt.Print("\n  Quantas requisições enviar? ")
	scanner.Scan()
	n, err := strconv.Atoi(strings.TrimSpace(scanner.Text()))
	if err != nil || n < 1 {
		fmt.Println("  ✗ Quantidade inválida.")
		return
	}
	fmt.Printf("\n  Enviando %d requisições...\n", n)
	for i := 0; i < n; i++ {
		enviarRequisicaoAleatoria()
		time.Sleep(300 * time.Millisecond)
	}
	fmt.Printf("  ✓ %d requisições enviadas.\n", n)
}

// enviar executa a rotina de Failover iterando sobre os nós registrados (brokerAddrs).
// Ao estabelecer a conexão TCP, realiza o marshalling do payload para JSON e transmite
// sob o protocolo interno MsgSyncNew.
func enviar(req models.Requisicao) {
	prioLabel := strings.Repeat("★", req.Prioridade) + strings.Repeat("☆", 5-req.Prioridade)
	fmt.Println()
	fmt.Println("  ┌─────────────────────────────────────────────────────────┐")
	fmt.Printf("  │ ENVIANDO: %-48s│\n", req.Descricao)
	fmt.Printf("  │ ID: %-52s│\n", req.ID)
	fmt.Printf("  │ Setor: %-2d  Prioridade: %s                         │\n", req.Setor, prioLabel)
	fmt.Println("  └─────────────────────────────────────────────────────────┘")

	// Lógica de resiliência: tenta conexão com o primeiro nó responsivo da lista
	for _, addr := range brokerAddrs {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			fmt.Printf("  ✗ Broker %s indisponível\n", addr)
			continue
		}

		// Empacotamento para o padrão de mensagens distribuídas
		envelope := models.MensagemDistribuida{
			Tipo:      models.MsgSyncNew,
			SenderID:  req.Setor,
			Timestamp: 0,
			Payload:   req,
		}

		// Transmissão via socket
		if err := json.NewEncoder(conn).Encode(envelope); err != nil {
			conn.Close()
			fmt.Printf("  ✗ Falha ao enviar para %s\n", addr)
			continue
		}
		conn.Close()
		fmt.Printf("  ✓ Requisição enviada para broker %s\n", addr)
		return
	}
	fmt.Println("  ✗ ERRO: nenhum broker disponível para recebimento da requisição!")
}

// consultarFila executa uma chamada síncrona (RPC-like) ao broker disponível
// visando obter o snapshot atualizado da ListaDistribuida (estado global).
func consultarFila() {
	fmt.Println()
	fmt.Println("  Consultando fila no broker...")

	for _, addr := range brokerAddrs {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			continue
		}

		msg := models.MensagemDistribuida{
			Tipo:      models.MsgConsultaFila,
			SenderID:  0,
			Timestamp: 0,
		}

		// Transmite a flag de requisição de estado
		if err := json.NewEncoder(conn).Encode(msg); err != nil {
			conn.Close()
			continue
		}

		// Aguarda o processamento remoto e unmarshalling do payload de resposta
		conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		var resposta models.RespostaFila
		if err := json.NewDecoder(conn).Decode(&resposta); err != nil {
			conn.Close()
			fmt.Println("  ✗ Falha ao decodificar a estrutura de resposta do broker.")
			return
		}
		conn.Close()

		exibirFila(resposta.Requisicoes, addr)
		return
	}
	fmt.Println("  ✗ Nenhum broker disponível para consulta do estado global.")
}

// exibirFila processa o array de requisições retornadas pelo nó,
// classificando-as e renderizando os agrupamentos com base no
// ciclo de vida atual (Pendente, Em Atendimento, Concluído).
func exibirFila(lista []models.Requisicao, broker string) {
	fmt.Printf("\n  ╔══ FILA DE REQUISIÇÕES — %s (%d missões) ══╗\n", broker, len(lista))

	// Indexação das requisições por status
	grupos := map[models.StatusRequisicao][]models.Requisicao{
		models.StatusPendente:      {},
		models.StatusEmAtendimento: {},
		models.StatusConcluido:     {},
	}
	for _, r := range lista {
		grupos[r.Status] = append(grupos[r.Status], r)
	}

	ordemStatus := []models.StatusRequisicao{
		models.StatusEmAtendimento,
		models.StatusPendente,
		models.StatusConcluido,
	}
	icones := map[models.StatusRequisicao]string{
		models.StatusEmAtendimento: "🚁",
		models.StatusPendente:      "⏳",
		models.StatusConcluido:     "✓",
	}

	// Renderização tabular dos status particionados
	for _, status := range ordemStatus {
		reqs := grupos[status]
		if len(reqs) == 0 {
			continue
		}
		fmt.Printf("\n  %s %s (%d)\n", icones[status], status, len(reqs))
		fmt.Println("  ─────────────────────────────────────────────────────────")
		for _, r := range reqs {
			prioLabel := strings.Repeat("★", r.Prioridade) + strings.Repeat("☆", 5-r.Prioridade)
			drone := r.DroneID
			if drone == "" {
				drone = "—"
			}
			fmt.Printf("  │ %-12s │ %s │ Setor %d │ Drone: %-6s │ %s\n",
				r.ID, prioLabel, r.Setor, drone, r.Descricao)
		}
	}

	if len(lista) == 0 {
		fmt.Println("  │ Fila vazia — nenhuma requisição processada ou pendente no cluster.")
	}
	fmt.Println("\n  ╚══════════════════════════════════════════════════════╝")
}
