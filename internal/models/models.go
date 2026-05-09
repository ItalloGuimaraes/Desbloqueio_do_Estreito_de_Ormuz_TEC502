package models

import "time"

// Estados da Missão
const (
	StatusPendente      = "PENDENTE"
	StatusEmAtendimento = "EM_ATENDIMENTO"
	StatusConcluida     = "CONCLUIDA"
)

// Tipos de Mensagem para Coordenação
const (
	MsgSyncNew    = "SYNC_NEW"
	MsgSyncUpdate = "SYNC_UPDATE"
	MsgJoin       = "JOIN"
	MsgFullSync   = "FULL_SYNC"
	MsgReqDrone   = "REQ_DRONE"
	MsgReplyOK    = "REPLY_OK"
)

type Requisicao struct {
	ID         string    `json:"id"`
	Setor      int       `json:"setor"`
	Prioridade int       `json:"prioridade"`
	Timestamp  int64     `json:"timestamp"`
	BrokerID   int       `json:"broker_id"`
	Descricao  string    `json:"descricao"`
	Status     string    `json:"status"`
	DroneID    string    `json:"drone_id"`
	CreatedAt  time.Time `json:"created_at"`
}

type MensagemDistribuida struct {
	Tipo      string      `json:"tipo"`
	SenderID  int         `json:"sender_id"`
	Timestamp int64       `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

type JoinRequest struct {
	ID   int    `json:"id"`
	Addr string `json:"addr"`
}
