package models

import "time"

// TipoMensagem identifica o propósito de cada mensagem trocada na malha P2P.
type TipoMensagem string

const (
	MsgJoin           TipoMensagem = "JOIN"
	MsgJoinACK        TipoMensagem = "JOIN_ACK"
	MsgSyncNew        TipoMensagem = "SYNC_NEW"
	MsgSyncUpdate     TipoMensagem = "SYNC_UPDATE"
	MsgFullSync       TipoMensagem = "FULL_SYNC"
	MsgReqDrone       TipoMensagem = "REQ_DRONE"
	MsgReplyOK        TipoMensagem = "REPLY_OK"
	MsgDroneHeartbeat TipoMensagem = "DRONE_HEARTBEAT"
	MsgDroneConcluido TipoMensagem = "DRONE_CONCLUIDO"
	MsgConsultaFila   TipoMensagem = "CONSULTA_FILA" // cliente solicita lista de requisições
)

// StatusRequisicao representa o ciclo de vida de uma missão.
type StatusRequisicao string

const (
	StatusPendente      StatusRequisicao = "PENDENTE"
	StatusEmAtendimento StatusRequisicao = "EM_ATENDIMENTO"
	StatusConcluido     StatusRequisicao = "CONCLUIDO"
)

// MensagemDistribuida é o envelope JSON de toda comunicação TCP do sistema.
type MensagemDistribuida struct {
	Tipo      TipoMensagem `json:"tipo"`
	SenderID  int          `json:"sender_id"`
	Timestamp int64        `json:"timestamp"`
	Payload   interface{}  `json:"payload"`
}

// Requisicao representa uma missão criada por um sensor e gerenciada pelos brokers.
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
	IniciadoEm      time.Time        `json:"iniciado_em"`      // preenchido no despacho ao drone
	UltimoHeartbeat time.Time        `json:"ultimo_heartbeat"` // atualizado a cada heartbeat do drone
}

// JoinRequest é o payload de MsgJoin e MsgJoinACK.
// Carrega o ID e o endereço externamente acessível do broker.
type JoinRequest struct {
	ID   int    `json:"id"`
	Addr string `json:"addr"`
}

// DroneStatus é o payload de MsgDroneHeartbeat e MsgDroneConcluido.
type DroneStatus struct {
	DroneID   string `json:"drone_id"`
	MissionID string `json:"mission_id"`
}

// RespostaFila é o payload de resposta ao MsgConsultaFila.
type RespostaFila struct {
	Requisicoes []Requisicao `json:"requisicoes"`
}
