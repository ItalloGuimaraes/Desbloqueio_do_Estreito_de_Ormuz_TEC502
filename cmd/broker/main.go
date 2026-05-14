package main

import (
	"encoding/json"
	"fmt"
	"net"
	"ormuz_distribuido/internal/models"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// =========================================================
// Estruturas de dados
// =========================================================

type Broker struct {
	ID               int
	Porta            string
	MeuEndereco      string // IP:Porta externamente acessível nesta máquina
	Relogio          int64
	ListaDistribuida []models.Requisicao
	Peers            map[int]string // map[brokerID]"ip:porta"
	mu               sync.Mutex
}

// PendenciaAgrawala representa uma solicitação de CS em andamento (Ricart-Agrawala).
// Armazena o timestamp LOCAL da requisição para comparação correta com remotas.
type PendenciaAgrawala struct {
	MissionID      string
	TimestampLocal int64 // CORRIGIDO: timestamp desta requisição (não o relógio atual)
	Respostas      int
	TotalEsperado  int // captura len(Peers) no momento da requisição
	DroneConn      net.Conn
	DroneID        string
}

// pendencias é global pois é acessada em múltiplas goroutines; protegida por broker.mu
var pendencias = make(map[string]*PendenciaAgrawala)

// =========================================================
// Relógio de Lamport
// =========================================================

func (b *Broker) tickRelogio() int64 {
	b.Relogio++
	return b.Relogio
}

// atualizarRelogio implementa a regra de Lamport: max(local, remoto) + 1.
// Deve ser chamado FORA de b.mu (adquire o lock internamente).
func (b *Broker) atualizarRelogio(remoteTimestamp int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if remoteTimestamp > b.Relogio {
		b.Relogio = remoteTimestamp
	}
	b.Relogio++
}

// =========================================================
// Ordenação da fila
// =========================================================

// ordenarLista ordena por: prioridade desc → timestamp asc → brokerID asc.
// Deve ser chamado com b.mu já adquirido.
func (b *Broker) ordenarLista() {
	sort.Slice(b.ListaDistribuida, func(i, j int) bool {
		r1, r2 := b.ListaDistribuida[i], b.ListaDistribuida[j]
		if r1.Prioridade != r2.Prioridade {
			return r1.Prioridade > r2.Prioridade
		}
		if r1.Timestamp != r2.Timestamp {
			return r1.Timestamp < r2.Timestamp
		}
		return r1.BrokerID < r2.BrokerID
	})
}

// =========================================================
// Comunicação P2P
// =========================================================

// broadcast envia msg para todos os peers conhecidos, exceto si mesmo.
// Usa uma cópia dos peers para não segurar o lock durante I/O de rede.
func (b *Broker) broadcast(msg models.MensagemDistribuida) {
	b.mu.Lock()
	peersCopia := make(map[int]string, len(b.Peers))
	for id, addr := range b.Peers {
		peersCopia[id] = addr
	}
	b.mu.Unlock()

	for id, addr := range peersCopia {
		if id == b.ID {
			continue
		}
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			fmt.Printf("[P2P] Falha ao contatar Broker %d (%s): %v\n", id, addr, err)
			continue
		}
		if err := json.NewEncoder(conn).Encode(msg); err != nil {
			fmt.Printf("[P2P] Erro ao enviar para Broker %d: %v\n", id, err)
		}
		conn.Close()
	}
}

// =========================================================
// Servidor TCP
// =========================================================

func (b *Broker) Iniciar() {
	ln, err := net.Listen("tcp", b.Porta)
	if err != nil {
		fmt.Printf("[ERRO] Falha ao abrir porta %s: %v\n", b.Porta, err)
		return
	}
	defer ln.Close()

	b.processoEnvelhecimento()
	b.processoWatchdogDrones()

	fmt.Printf(">>> Broker %d escutando em %s (externo: %s)...\n", b.ID, b.Porta, b.MeuEndereco)

	for {
		conn, err := ln.Accept()
		if err == nil {
			go b.handleConnection(conn)
		}
	}
}

func (b *Broker) handleConnection(conn net.Conn) {
	// IMPORTANTE: não fechar a conn aqui; cada case decide quando fechar.
	var msg models.MensagemDistribuida
	if err := json.NewDecoder(conn).Decode(&msg); err != nil {
		conn.Close()
		return
	}

	fmt.Printf("[REDE] Tipo=%-14s | De=%d | TS=%d\n", msg.Tipo, msg.SenderID, msg.Timestamp)
	b.atualizarRelogio(msg.Timestamp)

	switch msg.Tipo {
	case models.MsgJoin:
		// CORRIGIDO: passa conn para que processarJoin possa responder antes de fechar
		b.processarJoin(msg, conn)

	case models.MsgJoinACK:
		// NOVO: resposta do seed com seus dados (ID + addr)
		b.processarJoinACK(msg)
		conn.Close()

	case models.MsgSyncNew:
		b.processarNovoAlerta(msg)
		conn.Close()

	case models.MsgSyncUpdate:
		b.processarUpdateStatus(msg)
		conn.Close()

	case models.MsgFullSync:
		b.receberSincronizacaoCompleta(msg)
		conn.Close()

	case models.MsgReqDrone:
		if msg.SenderID == 0 {
			// Veio de um drone (não participa da malha P2P)
			droneID, _ := msg.Payload.(string)
			b.solicitarMissao(conn, droneID)
			// conn será fechada dentro de solicitarMissao / confirmarMissaoAoDrone
		} else {
			// Veio de outro broker — protocolo Ricart-Agrawala
			b.responderRicartAgrawala(msg)
			conn.Close()
		}

	case models.MsgReplyOK:
		b.processarReplyOK(msg)
		conn.Close()

	case models.MsgDroneHeartbeat:
		b.processarHeartbeat(msg)
		conn.Close()

	case models.MsgDroneConcluido:
		b.processarConclusaoDrone(msg)
		conn.Close()

	default:
		conn.Close()
	}
}

// =========================================================
// Heartbeat e recuperação de falha de drone
// =========================================================

// processarHeartbeat atualiza o timestamp do último sinal do drone na missão.
func (b *Broker) processarHeartbeat(msg models.MensagemDistribuida) {
	pBytes, _ := json.Marshal(msg.Payload)
	var status models.DroneStatus
	json.Unmarshal(pBytes, &status)

	b.mu.Lock()
	for i := range b.ListaDistribuida {
		if b.ListaDistribuida[i].ID == status.MissionID {
			b.ListaDistribuida[i].UltimoHeartbeat = time.Now()
			break
		}
	}
	b.mu.Unlock()
}

// processarConclusaoDrone marca a missão como concluída e propaga o update.
func (b *Broker) processarConclusaoDrone(msg models.MensagemDistribuida) {
	pBytes, _ := json.Marshal(msg.Payload)
	var status models.DroneStatus
	json.Unmarshal(pBytes, &status)

	b.mu.Lock()
	for i := range b.ListaDistribuida {
		if b.ListaDistribuida[i].ID == status.MissionID {
			b.ListaDistribuida[i].Status = models.StatusConcluido
			tarefa := b.ListaDistribuida[i]
			fmt.Printf("[MISSÃO] %s CONCLUÍDA pelo drone %s\n", status.MissionID, status.DroneID)
			go b.broadcast(models.MensagemDistribuida{
				Tipo:     models.MsgSyncUpdate,
				SenderID: b.ID,
				Payload:  tarefa,
			})
			break
		}
	}
	b.mu.Unlock()
}

// processoWatchdogDrones monitora missões em andamento.
// Se um drone não enviar heartbeat por mais de 10s, a missão volta para PENDENTE.
func (b *Broker) processoWatchdogDrones() {
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		for range ticker.C {
			b.mu.Lock()
			for i := range b.ListaDistribuida {
				req := &b.ListaDistribuida[i]
				if req.Status != models.StatusEmAtendimento {
					continue
				}
				// Missão sem heartbeat por mais de 10s → drone caiu
				semHeartbeat := req.UltimoHeartbeat.IsZero() ||
					time.Since(req.UltimoHeartbeat) > 10*time.Second
				// Mas só considera falha se a missão já tem mais de 10s
				// (evita falso positivo logo após despacho)
				missaoAntiga := time.Since(req.CreatedAt) > 10*time.Second
				if semHeartbeat && missaoAntiga {
					fmt.Printf("[WATCHDOG-DRONE] Drone %s não responde! Missão %s devolvida à fila.\n",
						req.DroneID, req.ID)
					req.Status = models.StatusPendente
					req.DroneID = ""
					req.UltimoHeartbeat = time.Time{}
					go b.broadcast(models.MensagemDistribuida{
						Tipo:     models.MsgSyncUpdate,
						SenderID: b.ID,
						Payload:  *req,
					})
				}
			}
			b.mu.Unlock()
		}
	}()
}

// =========================================================
// DINAMISMO: Entrada de novos Brokers (Join P2P)
// =========================================================

// solicitarEntrada é chamado por brokers com SEED_ADDR configurado.
// Envia MsgJoin ao seed e aguarda MsgJoinACK com os dados do seed,
// adicionando-o como peer conhecido.
//
// CORRIGIDO: agora lê a resposta do seed (MsgJoinACK) para descobrir
//
//	o ID e endereço do seed e adicioná-lo aos próprios Peers.
func (b *Broker) solicitarEntrada(seedAddr string) {
	fmt.Printf("[JOIN] Tentando entrar na malha via semente: %s\n", seedAddr)

	conn, err := net.DialTimeout("tcp", seedAddr, 5*time.Second)
	if err != nil {
		fmt.Printf("[ERRO] Falha ao conectar na semente %s: %v\n", seedAddr, err)
		return
	}
	defer conn.Close()

	b.mu.Lock()
	b.Relogio++
	ts := b.Relogio
	b.mu.Unlock()

	joinMsg := models.MensagemDistribuida{
		Tipo:      models.MsgJoin,
		SenderID:  b.ID,
		Timestamp: ts,
		Payload: models.JoinRequest{
			ID:   b.ID,
			Addr: b.MeuEndereco,
		},
	}

	if err := json.NewEncoder(conn).Encode(joinMsg); err != nil {
		fmt.Printf("[ERRO] Falha ao enviar MsgJoin: %v\n", err)
		return
	}
	fmt.Printf("[JOIN] MsgJoin enviada para %s\n", seedAddr)

	// CORRIGIDO: aguarda ACK do seed com seus dados (ID + addr)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var ack models.MensagemDistribuida
	if err := json.NewDecoder(conn).Decode(&ack); err != nil {
		fmt.Printf("[JOIN] Sem ACK do seed (pode ser broker legado): %v\n", err)
		return
	}

	if ack.Tipo == models.MsgJoinACK {
		var seedInfo models.JoinRequest
		pBytes, _ := json.Marshal(ack.Payload)
		json.Unmarshal(pBytes, &seedInfo)

		b.mu.Lock()
		b.Peers[seedInfo.ID] = seedInfo.Addr
		fmt.Printf("[JOIN] Seed %d (%s) adicionado aos Peers\n", seedInfo.ID, seedInfo.Addr)
		b.mu.Unlock()
	}
}

// processarJoin é chamado quando recebemos MsgJoin de um broker novato.
//
// CORRIGIDO:
//  1. Responde ao novato com MsgJoinACK antes de fechar a conn.
//  2. Só envia FullSync se este broker foi o receptor direto (é o seed).
//     Identificado verificando se a conn veio de fora (não via broadcast).
//     Como o broadcast repropaga o Join, usamos uma flag no payload para
//     distinguir: quem fecha a conn com o novato é o receptor direto.
func (b *Broker) processarJoin(msg models.MensagemDistribuida, conn net.Conn) {
	pBytes, _ := json.Marshal(msg.Payload)
	var reqJoin models.JoinRequest
	json.Unmarshal(pBytes, &reqJoin)

	b.mu.Lock()
	_, jaConhece := b.Peers[reqJoin.ID]
	if !jaConhece {
		b.Peers[reqJoin.ID] = reqJoin.Addr
		fmt.Printf("[P2P] Novo Broker %d adicionado: %s\n", reqJoin.ID, reqJoin.Addr)
	}
	b.mu.Unlock()

	// CORRIGIDO: responde com MsgJoinACK contendo nossos próprios dados,
	// para que o novato possa nos adicionar como peer.
	ack := models.MensagemDistribuida{
		Tipo:      models.MsgJoinACK,
		SenderID:  b.ID,
		Timestamp: b.Relogio,
		Payload: models.JoinRequest{
			ID:   b.ID,
			Addr: b.MeuEndereco,
		},
	}
	json.NewEncoder(conn).Encode(ack)
	conn.Close() // podemos fechar após responder

	if !jaConhece {
		// Propaga o Join para que o resto da malha também conheça o novato.
		// CORRIGIDO: broadcast NÃO envia de volta para o novato (ele não está nos
		// Peers de ninguém ainda além do seed); os outros brokers processarão o
		// Join, adicionarão o novato, e responderão com seu próprio ACK.
		// O novato acumulará os peers via FullSync do seed.
		go b.broadcast(msg)

		// Envia o estado atual (lista de missões) para o novato via FullSync.
		// Somente o receptor direto do Join faz isso (o seed), pois a conn
		// acima fechou após o ACK. Para os demais brokers que receberem o
		// Join via broadcast, o recipient será o próprio broker que originou
		// o broadcast (msg.SenderID != reqJoin.ID pois o SenderID será o
		// broker que reencaminhou). CORRIGIDO: verificamos se quem enviou
		// a msg é o próprio novato (SenderID == reqJoin.ID), ou seja, se
		// é a mensagem original e não um reencaminhamento.
		if msg.SenderID == reqJoin.ID {
			go b.enviarEstadoParaNovato(reqJoin.Addr)
		}
	}
}

// processarJoinACK lida com a resposta do seed.
// Adicionamos o seed (e futuros peers que se anunciarem) à nossa lista.
func (b *Broker) processarJoinACK(msg models.MensagemDistribuida) {
	pBytes, _ := json.Marshal(msg.Payload)
	var info models.JoinRequest
	json.Unmarshal(pBytes, &info)

	b.mu.Lock()
	if _, existe := b.Peers[info.ID]; !existe {
		b.Peers[info.ID] = info.Addr
		fmt.Printf("[P2P] Peer %d (%s) adicionado via ACK\n", info.ID, info.Addr)
	}
	b.mu.Unlock()
}

// enviarEstadoParaNovato envia a lista completa de missões (FullSync) ao novo broker.
func (b *Broker) enviarEstadoParaNovato(addr string) {
	// Pequena pausa para garantir que o novato já está ouvindo após receber o ACK
	time.Sleep(500 * time.Millisecond)

	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		fmt.Printf("[SYNC] Falha ao enviar FullSync para %s: %v\n", addr, err)
		return
	}
	defer conn.Close()

	b.mu.Lock()
	msg := models.MensagemDistribuida{
		Tipo:      models.MsgFullSync,
		SenderID:  b.ID,
		Timestamp: b.Relogio,
		Payload:   b.ListaDistribuida,
	}
	b.mu.Unlock()

	json.NewEncoder(conn).Encode(msg)
	fmt.Printf("[SYNC] FullSync enviado para %s\n", addr)
}

// receberSincronizacaoCompleta substitui a lista local pela recebida do seed.
func (b *Broker) receberSincronizacaoCompleta(msg models.MensagemDistribuida) {
	b.mu.Lock()
	defer b.mu.Unlock()

	pBytes, _ := json.Marshal(msg.Payload)
	var listaNova []models.Requisicao
	json.Unmarshal(pBytes, &listaNova)

	b.ListaDistribuida = listaNova
	b.ordenarLista()
	fmt.Printf("[SYNC] Lista sincronizada: %d missões\n", len(listaNova))
}

// =========================================================
// Processamento de alertas (sensores → brokers)
// =========================================================

func (b *Broker) processarNovoAlerta(msg models.MensagemDistribuida) {
	var req models.Requisicao
	pBytes, _ := json.Marshal(msg.Payload)
	json.Unmarshal(pBytes, &req)

	b.mu.Lock()
	origemLocal := msg.Timestamp == 0 // Timestamp 0 = veio diretamente de um sensor
	if origemLocal {
		b.Relogio++
		req.Timestamp = b.Relogio
		req.BrokerID = b.ID
	} else {
		req.Timestamp = msg.Timestamp
		req.BrokerID = msg.SenderID
	}
	req.Status = models.StatusPendente
	b.ListaDistribuida = append(b.ListaDistribuida, req)
	b.ordenarLista()
	b.mu.Unlock()

	// Propaga apenas se originou aqui (evita loop de broadcast)
	if origemLocal {
		msg.Timestamp = req.Timestamp
		msg.SenderID = b.ID
		go b.broadcast(msg)
	}
}

func (b *Broker) processarUpdateStatus(msg models.MensagemDistribuida) {
	var update models.Requisicao
	pBytes, _ := json.Marshal(msg.Payload)
	json.Unmarshal(pBytes, &update)

	b.mu.Lock()
	defer b.mu.Unlock()
	for i, r := range b.ListaDistribuida {
		if r.ID == update.ID {
			b.ListaDistribuida[i].Status = update.Status
			b.ListaDistribuida[i].DroneID = update.DroneID
			break
		}
	}
}

// =========================================================
// Ricart-Agrawala — exclusão mútua distribuída
// =========================================================

// solicitarMissao é chamado quando um drone pede uma missão.
// Inicia o protocolo Ricart-Agrawala com os peers e aguarda respostas.
func (b *Broker) solicitarMissao(conn net.Conn, droneID string) {
	b.mu.Lock()
	var alvo *models.Requisicao
	for i := range b.ListaDistribuida {
		if b.ListaDistribuida[i].Status == models.StatusPendente {
			alvo = &b.ListaDistribuida[i]
			break
		}
	}

	if alvo == nil {
		b.mu.Unlock()
		json.NewEncoder(conn).Encode(models.Requisicao{})
		conn.Close()
		return
	}

	b.Relogio++
	timestampLocal := b.Relogio

	// CORRIGIDO: guarda o timestamp da requisição e o total de peers no momento
	totalPeers := len(b.Peers)
	pendencia := &PendenciaAgrawala{
		MissionID:      alvo.ID,
		TimestampLocal: timestampLocal,
		Respostas:      0,
		TotalEsperado:  totalPeers,
		DroneConn:      conn,
		DroneID:        droneID,
	}
	pendencias[alvo.ID] = pendencia

	if totalPeers == 0 {
		fmt.Printf("[R-A] Sem peers. Missão %s autorizada de imediato.\n", alvo.ID)
		go b.confirmarMissaoAoDrone(pendencia)
		delete(pendencias, alvo.ID)
		b.mu.Unlock()
		return
	}
	b.mu.Unlock()

	// Watchdog: se após 5s não chegaram todas as respostas, prossegue mesmo assim
	// (tolerância a falhas em ambiente de redes instáveis como o descrito no PBL)
	go func(p *PendenciaAgrawala, idMissao string) {
		time.Sleep(5 * time.Second)
		b.mu.Lock()
		if pend, existe := pendencias[idMissao]; existe {
			fmt.Printf("[WATCHDOG] Timeout com %d/%d respostas. Confirmando missão %s.\n",
				pend.Respostas, pend.TotalEsperado, idMissao)
			go b.confirmarMissaoAoDrone(pend)
			delete(pendencias, idMissao)
		}
		b.mu.Unlock()
	}(pendencia, alvo.ID)

	// Broadcast do pedido de CS para todos os peers
	reqMsg := models.MensagemDistribuida{
		Tipo:      models.MsgReqDrone,
		SenderID:  b.ID,
		Timestamp: timestampLocal,
		Payload:   alvo.ID,
	}
	go b.broadcast(reqMsg)
}

// responderRicartAgrawala avalia se devemos conceder ou negar o OK ao broker solicitante.
//
// CORRIGIDO: compara o timestamp da PENDÊNCIA LOCAL (se existir) com o remoto,
// não o relógio atual. Isso implementa corretamente o algoritmo R-A:
// "Eu nego o OK apenas se eu também quero a CS E minha requisição tem prioridade."
func (b *Broker) responderRicartAgrawala(msg models.MensagemDistribuida) {
	missionID, ok := msg.Payload.(string)
	if !ok {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	okParaEnviar := true

	if pendLocal, existe := pendencias[missionID]; existe {
		// Temos uma pendência para a mesma missão.
		// Regra R-A: nega OK se nosso timestamp for menor (mais antigo = prioridade)
		// ou se empate no timestamp, nosso ID for menor.
		meTemPrioridade := pendLocal.TimestampLocal < msg.Timestamp ||
			(pendLocal.TimestampLocal == msg.Timestamp && b.ID < msg.SenderID)
		if meTemPrioridade {
			okParaEnviar = false
			fmt.Printf("[R-A] Negando OK para Broker %d (nossa TS=%d, remota=%d)\n",
				msg.SenderID, pendLocal.TimestampLocal, msg.Timestamp)
		}
	}

	if okParaEnviar {
		b.enviarOK(msg.SenderID, missionID)
	}
}

func (b *Broker) enviarOK(destID int, missionID string) {
	b.mu.Lock()
	addr, ok := b.Peers[destID]
	ts := b.Relogio
	b.mu.Unlock()

	if !ok {
		return
	}

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return
	}
	defer conn.Close()

	json.NewEncoder(conn).Encode(models.MensagemDistribuida{
		Tipo:      models.MsgReplyOK,
		SenderID:  b.ID,
		Timestamp: ts,
		Payload:   missionID,
	})
}

// processarReplyOK contabiliza os OKs recebidos.
// Quando todos os peers responderam, confirma a missão ao drone.
func (b *Broker) processarReplyOK(msg models.MensagemDistribuida) {
	b.mu.Lock()
	missionID, ok := msg.Payload.(string)
	if ok {
		if p, existe := pendencias[missionID]; existe {
			p.Respostas++
			fmt.Printf("[R-A] OK de Broker %d para missão %s (%d/%d)\n",
				msg.SenderID, missionID, p.Respostas, p.TotalEsperado)
			if p.Respostas >= p.TotalEsperado {
				go b.confirmarMissaoAoDrone(p)
				delete(pendencias, missionID)
			}
		}
	}
	b.mu.Unlock()
}

// confirmarMissaoAoDrone atualiza o status da missão e notifica o drone.
func (b *Broker) confirmarMissaoAoDrone(p *PendenciaAgrawala) {
	defer p.DroneConn.Close()
	b.mu.Lock()
	for i, r := range b.ListaDistribuida {
		if r.ID == p.MissionID {
			b.ListaDistribuida[i].Status = models.StatusEmAtendimento
			b.ListaDistribuida[i].DroneID = p.DroneID
			tarefa := b.ListaDistribuida[i]

			if err := json.NewEncoder(p.DroneConn).Encode(tarefa); err != nil {
				fmt.Printf("[DRONE] Erro ao enviar missão ao drone %s: %v\n", p.DroneID, err)
			} else {
				fmt.Printf("[DRONE] Missão %s despachada para drone %s\n", p.MissionID, p.DroneID)
			}

			go b.broadcast(models.MensagemDistribuida{
				Tipo:      models.MsgSyncUpdate,
				SenderID:  b.ID,
				Timestamp: b.Relogio,
				Payload:   tarefa,
			})
			break
		}
	}
	b.mu.Unlock()
}

// =========================================================
// Envelhecimento de requisições (aging)
// =========================================================

func (b *Broker) processoEnvelhecimento() {
	ticker := time.NewTicker(20 * time.Second)
	go func() {
		for range ticker.C {
			b.mu.Lock()
			houveMudanca := false
			for i := range b.ListaDistribuida {
				req := &b.ListaDistribuida[i]
				if req.Status == models.StatusPendente && req.Prioridade < 5 {
					if time.Since(req.CreatedAt) > 45*time.Second {
						req.Prioridade++
						houveMudanca = true
						fmt.Printf("[AGING] Requisição %s → Prioridade %d\n", req.ID, req.Prioridade)
					}
				}
			}
			if houveMudanca {
				b.ordenarLista()
			}
			b.mu.Unlock()
		}
	}()
}

// =========================================================
// Ponto de entrada
// =========================================================

func main() {
	id, _ := strconv.Atoi(os.Getenv("BROKER_ID"))
	porta := os.Getenv("BROKER_PORT")
	peersStr := os.Getenv("PEERS")
	seedAddr := os.Getenv("SEED_ADDR")
	meuEndereco := os.Getenv("MY_ADDR")

	if porta == "" {
		porta = ":9000"
	}

	broker := &Broker{
		ID:          id,
		Porta:       porta,
		MeuEndereco: meuEndereco,
		Peers:       make(map[int]string),
	}

	// Parsing de peers estáticos (opcional, para topologias fixas)
	if peersStr != "" {
		for i, p := range strings.Split(peersStr, ",") {
			p = strings.TrimSpace(p)
			parts := strings.Split(p, "=")
			if len(parts) == 2 {
				pID, _ := strconv.Atoi(parts[0])
				broker.Peers[pID] = parts[1]
			} else if p != "" {
				broker.Peers[i+2] = p
			}
		}
	}

	// DINAMISMO: solicita entrada na malha via seed após a porta estar aberta
	if seedAddr != "" && meuEndereco != "" {
		go func() {
			time.Sleep(2 * time.Second) // aguarda Iniciar() abrir a porta
			broker.solicitarEntrada(seedAddr)
		}()
	}

	fmt.Printf(">>> Broker %d iniciando na porta %s | externo: %s\n", broker.ID, porta, meuEndereco)
	broker.Iniciar()
}
