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
)

type Broker struct {
	ID               int
	Porta            string
	Relogio          int64
	ListaDistribuida []models.Requisicao
	Peers            map[int]string
	mu               sync.Mutex
}

type PendenciaAgrawala struct {
	MissionID string
	Respostas int
	DroneConn net.Conn
	DroneID   string
}

var pendencias = make(map[string]*PendenciaAgrawala)

// --- Lógica de Ordenação e Relógio ---

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

func (b *Broker) atualizarRelogio(remoteTimestamp int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if remoteTimestamp > b.Relogio {
		b.Relogio = remoteTimestamp
	}
	b.Relogio++
}

// --- Comunicação e Servidor ---

func (b *Broker) broadcast(msg models.MensagemDistribuida) {
	for id, addr := range b.Peers {
		if id == b.ID {
			continue
		}
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			continue
		}
		json.NewEncoder(conn).Encode(msg)
		conn.Close()
	}
}

func (b *Broker) Iniciar() {
	ln, err := net.Listen("tcp", b.Porta)
	if err != nil {
		fmt.Printf("[ERRO] Falha ao abrir porta %s: %v\n", b.Porta, err)
		return
	}
	defer ln.Close()

	fmt.Printf(">>> Broker %d Escutando em %s...\n", b.ID, b.Porta)

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

	b.atualizarRelogio(msg.Timestamp)

	switch msg.Tipo {
	case models.MsgJoin:
		b.processarJoin(msg)
		conn.Close()
	case models.MsgSyncNew:
		b.processarNovoAlerta(msg)
		conn.Close()
	case models.MsgSyncUpdate:
		b.processarUpdateStatus(msg)
		conn.Close()
	case models.MsgFullSync:
		b.enviarListaCompleta(conn) // Não fecha a conexão aqui, o encoder faz isso
	case models.MsgReqDrone:
		// Se o SenderID for 0, é um DRONE pedindo trabalho
		if msg.SenderID == 0 {
			droneID, _ := msg.Payload.(string)
			b.solicitarMissao(conn, droneID)
		} else {
			// Se o SenderID for > 0, é outro BROKER pedindo permissão (Ricart-Agrawala)
			b.responderRicartAgrawala(msg)
			conn.Close()
		}
	case models.MsgReplyOK:
		b.processarReplyOK(msg)
		conn.Close()
	default:
		conn.Close()
	}
}

// --- Processamento de Mensagens ---

func (b *Broker) processarJoin(msg models.MensagemDistribuida) {
	b.mu.Lock()
	defer b.mu.Unlock()
	reqJoin, _ := msg.Payload.(models.JoinRequest)
	b.Peers[reqJoin.ID] = reqJoin.Addr
	fmt.Printf("[P2P] Broker %d adicionado à malha\n", reqJoin.ID)
}

func (b *Broker) processarNovoAlerta(msg models.MensagemDistribuida) {
	var req models.Requisicao
	pBytes, _ := json.Marshal(msg.Payload)
	json.Unmarshal(pBytes, &req)

	b.mu.Lock()
	if msg.Timestamp == 0 { // Veio do sensor
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

	if msg.Timestamp == 0 {
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

// --- Ricart-Agrawala e Despacho ---

func (b *Broker) solicitarMissao(conn net.Conn, droneID string) {
	b.mu.Lock()
	var alvo *models.Requisicao
	for i, r := range b.ListaDistribuida {
		if r.Status == models.StatusPendente {
			alvo = &b.ListaDistribuida[i]
			break
		}
	}
	b.mu.Unlock()

	if alvo == nil {
		json.NewEncoder(conn).Encode(models.Requisicao{})
		conn.Close()
		return
	}

	b.mu.Lock()
	b.Relogio++
	pendencias[alvo.ID] = &PendenciaAgrawala{
		MissionID: alvo.ID,
		Respostas: 0,
		DroneConn: conn,
		DroneID:   droneID,
	}
	timestampLocal := b.Relogio
	b.mu.Unlock()

	msg := models.MensagemDistribuida{
		Tipo:      models.MsgReqDrone,
		SenderID:  b.ID,
		Timestamp: timestampLocal,
		Payload:   alvo.ID,
	}
	go b.broadcast(msg)
}

func (b *Broker) responderRicartAgrawala(msg models.MensagemDistribuida) {
	missionID, ok := msg.Payload.(string)
	if !ok {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	okParaEnviar := true
	// Mudamos 'p' para '_' pois só queremos saber se a missão 'existe'
	if _, existe := pendencias[missionID]; existe {
		// Se eu também quero a missão, comparo os carimbos de tempo (Lamport)
		// Se meu tempo for menor, ou se for igual e meu ID for menor, eu ganho a prioridade
		if b.Relogio < msg.Timestamp || (b.Relogio == msg.Timestamp && b.ID < msg.SenderID) {
			okParaEnviar = false // Eu tenho prioridade, então não envio o OK agora
		}
	}

	if okParaEnviar {
		b.enviarOK(msg.SenderID, missionID)
	}
}

func (b *Broker) enviarOK(destID int, missionID string) {
	addr := b.Peers[destID]
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return
	}
	defer conn.Close()

	json.NewEncoder(conn).Encode(models.MensagemDistribuida{
		Tipo:      models.MsgReplyOK,
		SenderID:  b.ID,
		Timestamp: b.Relogio,
		Payload:   missionID,
	})
}

func (b *Broker) processarReplyOK(msg models.MensagemDistribuida) {
	b.mu.Lock()
	missionID := msg.Payload.(string)
	if p, ok := pendencias[missionID]; ok {
		p.Respostas++
		if p.Respostas >= len(b.Peers) {
			go b.confirmarMissaoAoDrone(p)
			delete(pendencias, missionID)
		}
	}
	b.mu.Unlock()
}

func (b *Broker) confirmarMissaoAoDrone(p *PendenciaAgrawala) {
	defer p.DroneConn.Close()
	b.mu.Lock()
	for i, r := range b.ListaDistribuida {
		if r.ID == p.MissionID {
			b.ListaDistribuida[i].Status = models.StatusEmAtendimento
			b.ListaDistribuida[i].DroneID = p.DroneID
			json.NewEncoder(p.DroneConn).Encode(b.ListaDistribuida[i])

			go b.broadcast(models.MensagemDistribuida{
				Tipo:     models.MsgSyncUpdate,
				SenderID: b.ID,
				Payload:  b.ListaDistribuida[i],
			})
			break
		}
	}
	b.mu.Unlock()
}

func (b *Broker) enviarListaCompleta(conn net.Conn) {
	defer conn.Close()
	b.mu.Lock()
	defer b.mu.Unlock()
	msg := models.MensagemDistribuida{
		Tipo:    models.MsgFullSync,
		Payload: b.ListaDistribuida,
	}
	json.NewEncoder(conn).Encode(msg)
}

// --- Main Atualizado ---

func main() {
	idStr := os.Getenv("BROKER_ID")
	porta := os.Getenv("BROKER_PORT")
	peersStr := os.Getenv("PEERS") // Ex: "2=broker2:9000,3=broker3:9000"

	id, _ := strconv.Atoi(idStr)

	broker := &Broker{
		ID:    id,
		Porta: porta,
		Peers: make(map[int]string),
	}

	// Parse de Peers (Opcional, mas ajuda no Docker)
	if peersStr != "" {
		pList := strings.Split(peersStr, ",")
		for _, p := range pList {
			parts := strings.Split(p, "=")
			if len(parts) == 2 {
				pID, _ := strconv.Atoi(parts[0])
				broker.Peers[pID] = parts[1]
			}
		}
	}

	fmt.Printf(">>> Broker %d iniciado na porta %s\n", broker.ID, broker.Porta)
	broker.Iniciar()
}
