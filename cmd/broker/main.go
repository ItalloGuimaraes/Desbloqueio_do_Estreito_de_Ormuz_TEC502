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
	ID                 int
	Porta              string
	MeuEndereco        string
	Relogio            int64
	ListaDistribuida   []models.Requisicao
	Peers              map[int]string
	FalhasConsecutivas map[int]int
	mu                 sync.Mutex
}

type PendenciaAgrawala struct {
	MissionID      string
	TimestampLocal int64
	Respostas      map[int]bool // Previne duplicação de OKs do mesmo broker
	TotalEsperado  int
	DroneConn      net.Conn
	DroneID        string
}

var pendencias = make(map[string]*PendenciaAgrawala)

// =========================================================
// Relógio de Lamport
// =========================================================

func (b *Broker) tickRelogio() int64 {
	b.Relogio++
	return b.Relogio
}

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
// Comunicação P2P (PARALELA / FAN-OUT)
// =========================================================

const limiteFalhas = 3

func (b *Broker) broadcast(msg models.MensagemDistribuida) (enviados int, removidos []int) {
	b.mu.Lock()
	peersCopia := make(map[int]string, len(b.Peers))
	for id, addr := range b.Peers {
		peersCopia[id] = addr
	}
	b.mu.Unlock()

	var wg sync.WaitGroup
	var envMu sync.Mutex
	removidos = []int{}

	for id, addr := range peersCopia {
		if id == b.ID {
			continue
		}
		wg.Add(1)

		go func(peerID int, peerAddr string) {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", peerAddr, 2*time.Second)
			if err != nil {
				b.mu.Lock()
				b.FalhasConsecutivas[peerID]++
				falhas := b.FalhasConsecutivas[peerID]
				b.mu.Unlock()

				if falhas >= limiteFalhas {
					b.mu.Lock()
					delete(b.Peers, peerID)
					delete(b.FalhasConsecutivas, peerID)
					b.mu.Unlock()

					envMu.Lock()
					removidos = append(removidos, peerID)
					envMu.Unlock()
					fmt.Printf("[P2P] Broker %d removido após %d falhas.\n", peerID, falhas)
				}
				return
			}

			b.mu.Lock()
			b.FalhasConsecutivas[peerID] = 0
			b.mu.Unlock()

			if err := json.NewEncoder(conn).Encode(msg); err == nil {
				envMu.Lock()
				enviados++
				envMu.Unlock()
			}
			conn.Close()
		}(id, addr)
	}

	wg.Wait()
	return enviados, removidos
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
	var msg models.MensagemDistribuida
	if err := json.NewDecoder(conn).Decode(&msg); err != nil {
		conn.Close()
		return
	}

	fmt.Printf("[REDE] Tipo=%-14s | De=%d | TS=%d\n", msg.Tipo, msg.SenderID, msg.Timestamp)
	b.atualizarRelogio(msg.Timestamp)

	switch msg.Tipo {
	case models.MsgJoin:
		b.processarJoin(msg, conn)
	case models.MsgJoinACK:
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
			var droneID string
			if s, ok := msg.Payload.(string); ok {
				droneID = s
			} else {
				pBytes, _ := json.Marshal(msg.Payload)
				json.Unmarshal(pBytes, &droneID)
			}
			b.solicitarMissao(conn, droneID)
		} else {
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
	case models.MsgConsultaFila:
		b.responderConsultaFila(conn)
	default:
		conn.Close()
	}
}

func (b *Broker) responderConsultaFila(conn net.Conn) {
	defer conn.Close()
	b.mu.Lock()
	copia := make([]models.Requisicao, len(b.ListaDistribuida))
	copy(copia, b.ListaDistribuida)
	b.mu.Unlock()

	resposta := models.RespostaFila{Requisicoes: copia}
	json.NewEncoder(conn).Encode(resposta)
}

// =========================================================
// Heartbeat e recuperação de falha de drone
// =========================================================

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

func (b *Broker) processarConclusaoDrone(msg models.MensagemDistribuida) {
	pBytes, _ := json.Marshal(msg.Payload)
	var status models.DroneStatus
	json.Unmarshal(pBytes, &status)

	b.mu.Lock()
	var broadcastMsg *models.MensagemDistribuida
	for i := range b.ListaDistribuida {
		if b.ListaDistribuida[i].ID == status.MissionID {
			b.ListaDistribuida[i].Status = models.StatusConcluido
			tarefa := b.ListaDistribuida[i]
			fmt.Printf("[CONCLUÍDO] Drone %-6s → %s | Alerta: \"%s\" | Setor %d\n",
				tarefa.DroneID, tarefa.ID, tarefa.Descricao, tarefa.Setor)
			m := models.MensagemDistribuida{
				Tipo:     models.MsgSyncUpdate,
				SenderID: b.ID,
				Payload:  tarefa,
			}
			broadcastMsg = &m
			break
		}
	}
	b.mu.Unlock()

	if broadcastMsg != nil {
		go b.broadcast(*broadcastMsg)
	}
}

func (b *Broker) processoWatchdogDrones() {
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		for range ticker.C {
			var parabroadcast []models.MensagemDistribuida
			b.mu.Lock()
			for i := range b.ListaDistribuida {
				req := &b.ListaDistribuida[i]
				if req.Status != models.StatusEmAtendimento {
					continue
				}
				semHeartbeat := req.UltimoHeartbeat.IsZero() ||
					time.Since(req.UltimoHeartbeat) > 15*time.Second
				droneJaAssumiu := !req.IniciadoEm.IsZero() &&
					time.Since(req.IniciadoEm) > 15*time.Second

				if semHeartbeat && droneJaAssumiu {
					fmt.Printf("[WATCHDOG] Drone %-6s falhou | %s devolvida à fila | Alerta: \"%s\"\n",
						req.DroneID, req.ID, req.Descricao)
					req.Status = models.StatusPendente
					req.DroneID = ""
					req.IniciadoEm = time.Time{}
					req.UltimoHeartbeat = time.Time{}
					parabroadcast = append(parabroadcast, models.MensagemDistribuida{
						Tipo:     models.MsgSyncUpdate,
						SenderID: b.ID,
						Payload:  *req,
					})
				}
			}
			b.mu.Unlock()

			for _, bMsg := range parabroadcast {
				go b.broadcast(bMsg)
			}
		}
	}()
}

// =========================================================
// DINAMISMO: Entrada de novos Brokers (Join P2P)
// =========================================================

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
		return
	}

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var ack models.MensagemDistribuida
	if err := json.NewDecoder(conn).Decode(&ack); err != nil {
		fmt.Printf("[ERRO] Sem resposta síncrona do seed %s: %v\n", seedAddr, err)
		return
	}

	if ack.Tipo == models.MsgJoinACK {
		var seedInfo models.JoinRequest
		pBytes, _ := json.Marshal(ack.Payload)
		json.Unmarshal(pBytes, &seedInfo)

		b.mu.Lock()
		b.Peers[seedInfo.ID] = seedInfo.Addr
		b.FalhasConsecutivas[seedInfo.ID] = 0
		fmt.Printf("[JOIN] Seed %d (%s) adicionado aos Peers\n", seedInfo.ID, seedInfo.Addr)
		b.mu.Unlock()
	}
}

func (b *Broker) processarJoin(msg models.MensagemDistribuida, conn net.Conn) {
	pBytes, _ := json.Marshal(msg.Payload)
	var reqJoin models.JoinRequest
	json.Unmarshal(pBytes, &reqJoin)

	if reqJoin.ID == b.ID {
		conn.Close()
		return
	}

	b.mu.Lock()
	_, jaConhece := b.Peers[reqJoin.ID]
	if !jaConhece {
		b.Peers[reqJoin.ID] = reqJoin.Addr
		b.FalhasConsecutivas[reqJoin.ID] = 0
		fmt.Printf("[P2P] Novo Broker %d adicionado: %s\n", reqJoin.ID, reqJoin.Addr)
	}
	b.mu.Unlock()

	ack := models.MensagemDistribuida{
		Tipo:      models.MsgJoinACK,
		SenderID:  b.ID,
		Timestamp: b.Relogio,
		Payload:   models.JoinRequest{ID: b.ID, Addr: b.MeuEndereco},
	}

	// CORREÇÃO CRÍTICA DO APERTO DE MÃO:
	// Se fomos contactados diretamente, respondemos na mesma conexão para garantir a entrega síncrona.
	if msg.SenderID == reqJoin.ID {
		json.NewEncoder(conn).Encode(ack)
	} else {
		// Se soubemos via fofoca (broadcast), discamos de forma assíncrona.
		go func(targetAddr string) {
			connDir, err := net.DialTimeout("tcp", targetAddr, 2*time.Second)
			if err == nil {
				json.NewEncoder(connDir).Encode(ack)
				connDir.Close()
			}
		}(reqJoin.Addr)
	}

	conn.Close()

	if !jaConhece {
		msgPropagada := msg
		msgPropagada.SenderID = b.ID
		go b.broadcast(msgPropagada)

		if msg.SenderID == reqJoin.ID {
			go b.enviarEstadoParaNovato(reqJoin.Addr)
		}
	}
}

func (b *Broker) processarJoinACK(msg models.MensagemDistribuida) {
	pBytes, _ := json.Marshal(msg.Payload)
	var info models.JoinRequest
	json.Unmarshal(pBytes, &info)

	b.mu.Lock()
	if _, existe := b.Peers[info.ID]; !existe {
		b.Peers[info.ID] = info.Addr
		b.FalhasConsecutivas[info.ID] = 0
		fmt.Printf("[P2P] Peer %d (%s) adicionado via ACK\n", info.ID, info.Addr)
	}
	b.mu.Unlock()
}

func (b *Broker) enviarEstadoParaNovato(addr string) {
	time.Sleep(500 * time.Millisecond)
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
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
}

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
	origemLocal := msg.Timestamp == 0
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

			// LÓGICA FAIL-FAST: Se outro broker assumiu a missão, desistimos dela instantaneamente!
			if update.Status == models.StatusEmAtendimento {
				b.ListaDistribuida[i].IniciadoEm = time.Now()
				b.ListaDistribuida[i].UltimoHeartbeat = time.Now()

				if pend, existe := pendencias[update.ID]; existe {
					fmt.Printf("[R-A] A missão %s foi assumida por outro. Recalculando...\n", update.ID)
					droneConn := pend.DroneConn
					droneID := pend.DroneID
					delete(pendencias, update.ID)
					go b.solicitarMissao(droneConn, droneID)
				}
			}
			break
		}
	}
}

// =========================================================
// Ricart-Agrawala — exclusão mútua distribuída
// =========================================================

func (b *Broker) solicitarMissao(conn net.Conn, droneID string) {
	b.mu.Lock()
	var alvo *models.Requisicao
	for i := range b.ListaDistribuida {
		if b.ListaDistribuida[i].Status == models.StatusPendente {
			// Não tentar negociar o que já está a ser negociado
			if _, jaNegociando := pendencias[b.ListaDistribuida[i].ID]; !jaNegociando {
				alvo = &b.ListaDistribuida[i]
				break
			}
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
	idMissao := alvo.ID

	totalPeers := 0
	for id := range b.Peers {
		if id != b.ID {
			totalPeers++
		}
	}

	pendencia := &PendenciaAgrawala{
		MissionID:      idMissao,
		TimestampLocal: timestampLocal,
		Respostas:      make(map[int]bool),
		TotalEsperado:  totalPeers,
		DroneConn:      conn,
		DroneID:        droneID,
	}
	pendencias[idMissao] = pendencia

	if totalPeers == 0 {
		fmt.Printf("[R-A] Sem peers. Missão %s autorizada de imediato.\n", idMissao)
		go b.confirmarMissaoAoDrone(pendencia)
		delete(pendencias, idMissao)
		b.mu.Unlock()
		return
	}
	b.mu.Unlock()

	go func(idM string) {
		time.Sleep(5 * time.Second)
		b.mu.Lock()
		if pend, existe := pendencias[idM]; existe {
			fmt.Printf("[WATCHDOG] Timeout com %d/%d respostas. Confirmando missão %s.\n",
				len(pend.Respostas), pend.TotalEsperado, idM)
			go b.confirmarMissaoAoDrone(pend)
			delete(pendencias, idM)
		}
		b.mu.Unlock()
	}(idMissao)

	reqMsg := models.MensagemDistribuida{
		Tipo:      models.MsgReqDrone,
		SenderID:  b.ID,
		Timestamp: timestampLocal,
		Payload:   idMissao,
	}

	entregues, _ := b.broadcast(reqMsg)
	falhasDeEnvio := totalPeers - entregues

	b.mu.Lock()
	if pend, existe := pendencias[idMissao]; existe {
		pend.TotalEsperado -= falhasDeEnvio

		if pend.TotalEsperado <= 0 {
			fmt.Printf("[R-A] Todos os peers inativos. Missão %s autorizada.\n", idMissao)
			go b.confirmarMissaoAoDrone(pend)
			delete(pendencias, idMissao)
		} else if len(pend.Respostas) >= pend.TotalEsperado {
			fmt.Printf("[R-A] Todos os OKs recebidos (%d/%d). Confirmando missão %s.\n", len(pend.Respostas), pend.TotalEsperado, idMissao)
			go b.confirmarMissaoAoDrone(pend)
			delete(pendencias, idMissao)
		}
	}
	b.mu.Unlock()
}

func (b *Broker) responderRicartAgrawala(msg models.MensagemDistribuida) {
	missionID, ok := msg.Payload.(string)
	if !ok {
		return
	}

	b.mu.Lock()
	okParaEnviar := true

	if pendLocal, existe := pendencias[missionID]; existe {
		meTemPrioridade := pendLocal.TimestampLocal < msg.Timestamp ||
			(pendLocal.TimestampLocal == msg.Timestamp && b.ID < msg.SenderID)
		if meTemPrioridade {
			okParaEnviar = false
			fmt.Printf("[R-A] Negando OK para Broker %d (nossa TS=%d, remota=%d)\n",
				msg.SenderID, pendLocal.TimestampLocal, msg.Timestamp)
		}
	}

	destAddr := ""
	if addr, existe := b.Peers[msg.SenderID]; existe {
		destAddr = addr
	}
	ts := b.Relogio
	b.mu.Unlock()

	if okParaEnviar {
		if destAddr != "" {
			b.enviarOKParaAddr(destAddr, ts, missionID)
		} else {
			// LOG DE ALERTA: Agora saberemos se alguém sofreu amnésia de IPs!
			fmt.Printf("[R-A] ALERTA: Não consigo enviar OK para Broker %d porque o IP dele sumiu da minha lista!\n", msg.SenderID)
		}
	}
}

func (b *Broker) enviarOKParaAddr(addr string, ts int64, missionID string) {
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

func (b *Broker) processarReplyOK(msg models.MensagemDistribuida) {
	b.mu.Lock()
	defer b.mu.Unlock()

	missionID, ok := msg.Payload.(string)
	if !ok {
		pBytes, _ := json.Marshal(msg.Payload)
		json.Unmarshal(pBytes, &missionID)
		if missionID == "" {
			return
		}
	}

	p, existe := pendencias[missionID]
	if !existe {
		return
	}

	p.Respostas[msg.SenderID] = true
	votosAtuais := len(p.Respostas)

	fmt.Printf("[R-A] OK de Broker %d para missão %s (%d/%d)\n",
		msg.SenderID, missionID, votosAtuais, p.TotalEsperado)

	if p.TotalEsperado > 0 && votosAtuais >= p.TotalEsperado {
		go b.confirmarMissaoAoDrone(p)
		delete(pendencias, missionID)
	}
}

func (b *Broker) confirmarMissaoAoDrone(p *PendenciaAgrawala) {
	defer p.DroneConn.Close()

	b.mu.Lock()
	var tarefa models.Requisicao
	var encontrou bool
	for i, r := range b.ListaDistribuida {
		if r.ID == p.MissionID {
			b.ListaDistribuida[i].Status = models.StatusEmAtendimento
			b.ListaDistribuida[i].DroneID = p.DroneID
			b.ListaDistribuida[i].IniciadoEm = time.Now()
			b.ListaDistribuida[i].UltimoHeartbeat = time.Now()
			tarefa = b.ListaDistribuida[i]
			encontrou = true
			break
		}
	}
	b.mu.Unlock()

	if !encontrou {
		return
	}

	if err := json.NewEncoder(p.DroneConn).Encode(tarefa); err != nil {
		fmt.Printf("[DRONE] Erro ao enviar missão ao drone %s: %v\n", p.DroneID, err)
		return
	}

	fmt.Printf("[DESPACHO] Drone %-6s <- %s | Alerta: \"%s\" | Setor %d | Prioridade %d\n",
		p.DroneID, p.MissionID, tarefa.Descricao, tarefa.Setor, tarefa.Prioridade)

	go b.broadcast(models.MensagemDistribuida{
		Tipo:      models.MsgSyncUpdate,
		SenderID:  b.ID,
		Timestamp: b.Relogio,
		Payload:   tarefa,
	})
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
// Reconexão automática à malha
// =========================================================

func (b *Broker) processoReconexao(seeds []string) {
	time.Sleep(2 * time.Second)
	for {
		b.mu.Lock()
		temPeer := len(b.Peers) > 0
		b.mu.Unlock()

		if !temPeer {
			fmt.Printf("[RECONEXAO] Sem peers. Tentando seeds: %v\n", seeds)
			for _, seed := range seeds {
				b.solicitarEntrada(seed)

				b.mu.Lock()
				temPeer = len(b.Peers) > 0
				b.mu.Unlock()

				if temPeer {
					fmt.Printf("[RECONEXAO] Conectado com sucesso via %s\n", seed)
					break
				}
			}
		}
		time.Sleep(10 * time.Second)
	}
}

// =========================================================
// Ponto de entrada
// =========================================================

func main() {
	id, _ := strconv.Atoi(os.Getenv("BROKER_ID"))
	porta := os.Getenv("BROKER_PORT")
	peersStr := os.Getenv("PEERS")
	seedAddr := os.Getenv("SEED_ADDR")
	seedAddrs := os.Getenv("SEED_ADDRS")
	meuEndereco := os.Getenv("MY_ADDR")

	if porta == "" {
		porta = ":9000"
	}

	var seeds []string
	raw := seedAddrs
	if raw == "" {
		raw = seedAddr
	}
	for _, s := range strings.Split(raw, ",") {
		s = strings.TrimSpace(s)
		if s != "" && s != meuEndereco {
			seeds = append(seeds, s)
		}
	}

	broker := &Broker{
		ID:                 id,
		Porta:              porta,
		MeuEndereco:        meuEndereco,
		Peers:              make(map[int]string),
		FalhasConsecutivas: make(map[int]int),
	}

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

	if len(seeds) > 0 && meuEndereco != "" {
		go broker.processoReconexao(seeds)
		fmt.Println(">>> Aguardando estabilização da malha P2P (3s)...")
		time.Sleep(3 * time.Second)
	}

	fmt.Printf(">>> Broker %d iniciando na porta %s | externo: %s\n", broker.ID, porta, meuEndereco)
	broker.Iniciar()
}
