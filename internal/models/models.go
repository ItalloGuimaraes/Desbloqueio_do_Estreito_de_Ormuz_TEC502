package models

import "time"

// =========================================================
// Tipos de mensagem da malha P2P
// =========================================================

type TipoMensagem string

const (
	MsgJoin           TipoMensagem = "JOIN"
	MsgJoinACK        TipoMensagem = "JOIN_ACK"
	MsgSyncNew        TipoMensagem = "SYNC_NEW"
	MsgSyncUpdate     TipoMensagem = "SYNC_UPDATE"
	MsgFullSync       TipoMensagem = "FULL_SYNC"
	MsgReqDrone       TipoMensagem = "REQ_DRONE"
	MsgReplyOK        TipoMensagem = "REPLY_OK"
	MsgDroneHeartbeat TipoMensagem = "DRONE_HEARTBEAT" // Drone sinaliza que está vivo
	MsgDroneConcluido TipoMensagem = "DRONE_CONCLUIDO" // Drone concluiu a missão
)

// =========================================================
// Status das requisições
// =========================================================

type StatusRequisicao string

const (
	StatusPendente      StatusRequisicao = "PENDENTE"
	StatusEmAtendimento StatusRequisicao = "EM_ATENDIMENTO"
	StatusConcluido     StatusRequisicao = "CONCLUIDO"
)

// =========================================================
// Estruturas de dados
// =========================================================

// MensagemDistribuida é o envelope de todas as mensagens P2P.
type MensagemDistribuida struct {
	Tipo      TipoMensagem `json:"tipo"`
	SenderID  int          `json:"sender_id"`
	Timestamp int64        `json:"timestamp"`
	Payload   interface{}  `json:"payload"`
}

// Requisicao representa uma missão de drone originada por um sensor.
type Requisicao struct {
	ID              string           `json:"id"`
	Setor           int              `json:"setor"`
	Prioridade      int              `json:"prioridade"`
	Descricao       string           `json:"descricao"`
	Status          StatusRequisicao `json:"status"`
	BrokerID        int              `json:"broker_id"`
	DroneID         string           `json:"drone_id"`
	Timestamp       int64            `json:"timestamp"`
	CreatedAt       time.Time        `json:"created_at"`
	UltimoHeartbeat time.Time        `json:"ultimo_heartbeat"` // controle de falha de drone
}

// JoinRequest é o payload de MsgJoin e MsgJoinACK.
type JoinRequest struct {
	ID   int    `json:"id"`
	Addr string `json:"addr"`
}

// DroneStatus é o payload de MsgDroneHeartbeat e MsgDroneConcluido.
type DroneStatus struct {
	DroneID   string `json:"drone_id"`
	MissionID string `json:"mission_id"`
}
