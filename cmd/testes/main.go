package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"ormuz_distribuido/internal/models"
	"os"
	"os/exec"
	"strings"
	"time"
)

// brokerAddrs é a lista de brokers disponíveis para os testes.
var brokerAddrs []string

// resultado de um caso de teste
type resultado struct {
	nome    string
	passou  bool
	detalhe string
}

func main() {
	addrsRaw := os.Getenv("BROKER_ADDRS")
	if addrsRaw == "" {
		addrsRaw = "broker-1:9000,broker-2:9000,broker-3:9000"
	}
	for _, a := range strings.Split(addrsRaw, ",") {
		brokerAddrs = append(brokerAddrs, strings.TrimSpace(a))
	}

	rand.Seed(time.Now().UnixNano())

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║         SUITE DE TESTES — ESTREITO DE ORMUZ                 ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	resultados := []resultado{}

	resultados = append(resultados, testeConectividadeBrokers())
	resultados = append(resultados, testeEnvioRequisicao())
	resultados = append(resultados, testePrioridade())
	resultados = append(resultados, testeRequisicaoNaFila())
	resultados = append(resultados, testeFalhaBroker())
	resultados = append(resultados, testeFalhaDrone())
	resultados = append(resultados, testeConsistenciaFila())

	// Relatório final
	fmt.Println()
	fmt.Println("══════════════════════════════════════════════════════════════")
	fmt.Println("  RELATÓRIO FINAL")
	fmt.Println("══════════════════════════════════════════════════════════════")
	passou := 0
	for _, r := range resultados {
		icone := "✓"
		if !r.passou {
			icone = "✗"
		} else {
			passou++
		}
		fmt.Printf("  %s %-45s %s\n", icone, r.nome, r.detalhe)
	}
	fmt.Printf("\n  Resultado: %d/%d testes passaram\n", passou, len(resultados))
	fmt.Println("══════════════════════════════════════════════════════════════")
}

// ─────────────────────────────────────────────────────────────────
// TESTE 1: Conectividade com os brokers
// ─────────────────────────────────────────────────────────────────
func testeConectividadeBrokers() resultado {
	nome := "Conectividade com brokers"
	fmt.Printf("\n[TESTE 1] %s...\n", nome)

	conectados := 0
	for _, addr := range brokerAddrs {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			fmt.Printf("  ✗ Broker %s indisponível: %v\n", addr, err)
			continue
		}
		conn.Close()
		fmt.Printf("  ✓ Broker %s acessível\n", addr)
		conectados++
	}

	if conectados == 0 {
		return resultado{nome, false, "nenhum broker acessível"}
	}
	return resultado{nome, true, fmt.Sprintf("%d/%d brokers acessíveis", conectados, len(brokerAddrs))}
}

// ─────────────────────────────────────────────────────────────────
// TESTE 2: Envio de requisição e sincronização
// ─────────────────────────────────────────────────────────────────
func testeEnvioRequisicao() resultado {
	nome := "Envio de requisição e sincronização"
	fmt.Printf("\n[TESTE 2] %s...\n", nome)

	req := novaRequisicao(1, 3, "Sinal de socorro (SOS) captado")
	if err := enviarRequisicao(req); err != nil {
		return resultado{nome, false, "falha ao enviar: " + err.Error()}
	}
	fmt.Printf("  ✓ Requisição %s enviada\n", req.ID)

	// Aguarda propagação via broadcast
	time.Sleep(2 * time.Second)

	// Verifica se a requisição aparece na fila de TODOS os brokers
	for _, addr := range brokerAddrs {
		fila, err := consultarFila(addr)
		if err != nil {
			fmt.Printf("  ✗ Broker %s: falha ao consultar fila\n", addr)
			continue
		}
		encontrou := false
		for _, r := range fila {
			if r.ID == req.ID {
				encontrou = true
				break
			}
		}
		if encontrou {
			fmt.Printf("  ✓ Broker %s: requisição %s na fila\n", addr, req.ID)
		} else {
			fmt.Printf("  ✗ Broker %s: requisição %s NÃO encontrada\n", addr, req.ID)
			return resultado{nome, false, "fila não sincronizada em " + addr}
		}
	}
	return resultado{nome, true, "requisição presente em todos os brokers"}
}

// ─────────────────────────────────────────────────────────────────
// TESTE 3: Prioridade na fila
// ─────────────────────────────────────────────────────────────────
func testePrioridade() resultado {
	nome := "Ordenação por prioridade na fila"
	fmt.Printf("\n[TESTE 3] %s...\n", nome)

	// Envia requisição de baixa prioridade primeiro
	req1 := novaRequisicao(1, 1, "Mancha de óleo identificada")
	req2 := novaRequisicao(1, 5, "Sinal de socorro (SOS) captado") // prioridade máxima

	enviarRequisicao(req1)
	time.Sleep(100 * time.Millisecond)
	enviarRequisicao(req2)
	time.Sleep(2 * time.Second)

	fila, err := consultarFila(brokerAddrs[0])
	if err != nil {
		return resultado{nome, false, "falha ao consultar fila"}
	}

	// Busca posições das duas requisições
	pos1, pos2 := -1, -1
	for i, r := range fila {
		if r.ID == req1.ID {
			pos1 = i
		}
		if r.ID == req2.ID {
			pos2 = i
		}
	}

	fmt.Printf("  Req baixa prioridade (P1): posição %d\n", pos1)
	fmt.Printf("  Req alta prioridade  (P5): posição %d\n", pos2)

	if pos1 == -1 || pos2 == -1 {
		return resultado{nome, false, "requisições não encontradas na fila"}
	}
	if pos2 < pos1 {
		return resultado{nome, true, "P5 está antes de P1 na fila ✓"}
	}
	return resultado{nome, false, fmt.Sprintf("P5 (pos %d) deveria estar antes de P1 (pos %d)", pos2, pos1)}
}

// ─────────────────────────────────────────────────────────────────
// TESTE 4: Requisição permanece na fila sem drones disponíveis
// ─────────────────────────────────────────────────────────────────
func testeRequisicaoNaFila() resultado {
	nome := "Requisição aguarda na fila sem drones"
	fmt.Printf("\n[TESTE 4] %s...\n", nome)

	req := novaRequisicao(2, 2, "Objeto não identificado no canal")
	if err := enviarRequisicao(req); err != nil {
		return resultado{nome, false, "falha ao enviar"}
	}

	time.Sleep(2 * time.Second)

	fila, err := consultarFila(brokerAddrs[0])
	if err != nil {
		return resultado{nome, false, "falha ao consultar fila"}
	}

	for _, r := range fila {
		if r.ID == req.ID && r.Status == models.StatusPendente {
			fmt.Printf("  ✓ Requisição %s está PENDENTE na fila\n", req.ID)
			return resultado{nome, true, "requisição aguardando drone disponível"}
		}
	}
	return resultado{nome, false, "requisição não encontrada como PENDENTE"}
}

// ─────────────────────────────────────────────────────────────────
// TESTE 5: Falha de broker — sistema continua operando
// ─────────────────────────────────────────────────────────────────
func testeFalhaBroker() resultado {
	nome := "Tolerância à falha de broker"
	fmt.Printf("\n[TESTE 5] %s...\n", nome)

	if len(brokerAddrs) < 2 {
		return resultado{nome, false, "necessário ao menos 2 brokers para este teste"}
	}

	// Verifica que broker-1 está acessível
	conn, err := net.DialTimeout("tcp", brokerAddrs[0], 2*time.Second)
	if err != nil {
		return resultado{nome, false, "broker-1 já está inacessível antes do teste"}
	}
	conn.Close()

	fmt.Println("  Derrubando broker-1 (docker kill broker-1)...")
	cmd := exec.Command("docker", "kill", "broker-1")
	if err := cmd.Run(); err != nil {
		fmt.Printf("  ✗ Falha ao derrubar broker-1: %v\n", err)
		fmt.Println("  (Se não estiver em Docker, pare o broker-1 manualmente e pressione Enter)")
		fmt.Scanln()
	}

	time.Sleep(2 * time.Second)

	// Tenta enviar requisição para broker-2
	req := novaRequisicao(1, 4, "Bloqueio parcial de rota comercial")
	var brokerFallback string
	for _, addr := range brokerAddrs[1:] {
		if err := enviarRequisicaoParaAddr(addr, req); err == nil {
			brokerFallback = addr
			break
		}
	}

	if brokerFallback == "" {
		return resultado{nome, false, "nenhum broker de fallback respondeu"}
	}
	fmt.Printf("  ✓ Requisição enviada para broker de fallback: %s\n", brokerFallback)

	time.Sleep(1 * time.Second)
	fila, err := consultarFila(brokerFallback)
	if err != nil {
		return resultado{nome, false, "falha ao consultar fila no broker de fallback"}
	}
	for _, r := range fila {
		if r.ID == req.ID {
			fmt.Println("  ✓ Sistema operando normalmente sem broker-1")
			fmt.Println("\n  Reiniciando broker-1 (docker compose up broker1 -d)...")
			exec.Command("docker", "compose", "up", "broker1", "-d").Run()
			return resultado{nome, true, "sistema continuou sem broker-1"}
		}
	}
	return resultado{nome, false, "requisição não encontrada no broker de fallback"}
}

// ─────────────────────────────────────────────────────────────────
// TESTE 6: Falha de drone — requisição volta à fila
// ─────────────────────────────────────────────────────────────────
func testeFalhaDrone() resultado {
	nome := "Falha de drone — requisição volta à fila"
	fmt.Printf("\n[TESTE 6] %s...\n", nome)

	// Envia uma requisição e aguarda ser assumida por um drone
	req := novaRequisicao(1, 5, "Sinal de socorro (SOS) captado")
	if err := enviarRequisicao(req); err != nil {
		return resultado{nome, false, "falha ao enviar requisição"}
	}
	fmt.Printf("  ✓ Requisição %s enviada (P5)\n", req.ID)

	// Aguarda drone assumir (até 30s)
	fmt.Println("  Aguardando drone assumir a missão...")
	assumiu := false
	for i := 0; i < 30; i++ {
		time.Sleep(1 * time.Second)
		fila, _ := consultarFila(brokerAddrs[0])
		for _, r := range fila {
			if r.ID == req.ID && r.Status == models.StatusEmAtendimento {
				fmt.Printf("  ✓ Drone %s assumiu a missão\n", r.DroneID)
				assumiu = true
				break
			}
		}
		if assumiu {
			break
		}
	}

	if !assumiu {
		return resultado{nome, false, "nenhum drone assumiu a missão em 15s (drones rodando?)"}
	}

	// Derruba o drone que assumiu
	fmt.Println("  Derrubando drone-alpha (docker kill drone-alpha)...")
	exec.Command("docker", "kill", "drone-alpha").Run()
	time.Sleep(500 * time.Millisecond)
	exec.Command("docker", "kill", "drone-bravo").Run()

	// Aguarda watchdog detectar a falha e devolver à fila (até 25s)
	fmt.Println("  Aguardando watchdog devolver missão à fila (até 25s)...")
	voltou := false
	for i := 0; i < 25; i++ {
		time.Sleep(1 * time.Second)
		fila, _ := consultarFila(brokerAddrs[0])
		for _, r := range fila {
			if r.ID == req.ID && r.Status == models.StatusPendente {
				fmt.Printf("  ✓ Missão %s voltou para PENDENTE após %ds\n", req.ID, i+1)
				voltou = true
				break
			}
		}
		if voltou {
			break
		}
	}

	// Reinicia os drones
	fmt.Println("  Reiniciando drones...")
	exec.Command("docker", "compose", "up", "drone_alpha", "drone_bravo", "-d").Run()

	if voltou {
		return resultado{nome, true, "requisição devolvida à fila após falha do drone"}
	}
	return resultado{nome, false, "missão não voltou à fila em 25s"}
}

// ─────────────────────────────────────────────────────────────────
// TESTE 7: Consistência da fila sob carga
// ─────────────────────────────────────────────────────────────────
func testeConsistenciaFila() resultado {
	nome := "Consistência da fila sob carga"
	fmt.Printf("\n[TESTE 7] %s...\n", nome)

	// Envia 10 requisições rapidamente
	ids := map[string]bool{}
	for i := 0; i < 10; i++ {
		setor := (i % 2) + 1
		req := novaRequisicao(setor, rand.Intn(5)+1, "Teste de carga")
		ids[req.ID] = true
		enviarRequisicao(req)
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Printf("  ✓ 10 requisições enviadas\n")

	time.Sleep(3 * time.Second)

	// Verifica que todos os brokers têm as mesmas requisições
	filas := map[string]map[string]bool{}
	for _, addr := range brokerAddrs {
		fila, err := consultarFila(addr)
		if err != nil {
			fmt.Printf("  ✗ Broker %s: falha ao consultar\n", addr)
			continue
		}
		filas[addr] = map[string]bool{}
		for _, r := range fila {
			filas[addr][r.ID] = true
		}
		fmt.Printf("  Broker %s: %d requisições na fila\n", addr, len(fila))
	}

	// Verifica que as filas são iguais entre os brokers
	addrs := []string{}
	for a := range filas {
		addrs = append(addrs, a)
	}
	if len(addrs) < 2 {
		return resultado{nome, true, "apenas 1 broker disponível para comparar"}
	}

	for id := range filas[addrs[0]] {
		for _, addr := range addrs[1:] {
			if !filas[addr][id] {
				return resultado{nome, false, fmt.Sprintf("ID %s ausente em %s — filas inconsistentes", id, addr)}
			}
		}
	}

	return resultado{nome, true, "todas as filas consistentes entre os brokers"}
}

// ─────────────────────────────────────────────────────────────────
// Funções auxiliares
// ─────────────────────────────────────────────────────────────────

func novaRequisicao(setor, prioridade int, descricao string) models.Requisicao {
	return models.Requisicao{
		ID:         fmt.Sprintf("TST-%d-%04d", setor, rand.Intn(10000)),
		Setor:      setor,
		Prioridade: prioridade,
		Descricao:  descricao,
		Status:     models.StatusPendente,
		CreatedAt:  time.Now(),
	}
}

func enviarRequisicao(req models.Requisicao) error {
	for _, addr := range brokerAddrs {
		if err := enviarRequisicaoParaAddr(addr, req); err == nil {
			return nil
		}
	}
	return fmt.Errorf("nenhum broker disponível")
}

func enviarRequisicaoParaAddr(addr string, req models.Requisicao) error {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	return json.NewEncoder(conn).Encode(models.MensagemDistribuida{
		Tipo:      models.MsgSyncNew,
		SenderID:  req.Setor,
		Timestamp: 0,
		Payload:   req,
	})
}

func consultarFila(addr string) ([]models.Requisicao, error) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	json.NewEncoder(conn).Encode(models.MensagemDistribuida{
		Tipo: models.MsgConsultaFila,
	})

	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	var resposta models.RespostaFila
	if err := json.NewDecoder(conn).Decode(&resposta); err != nil {
		return nil, err
	}
	return resposta.Requisicoes, nil
}
