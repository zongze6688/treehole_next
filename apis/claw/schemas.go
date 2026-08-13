package claw

import "github.com/goccy/go-json"

const (
	MessageTypeAuth        string = "auth"
	MessageTypeAuthSuccess string = "auth_success"
	MessageTypeError       string = "error"
	MessageTypeMessage     string = "message"
	MessageTypePing        string = "ping"
	MessageTypePong        string = "pong"

	OpenClawOnboardEvent        string = "openclaw.onboard"
	OpenClawInstanceStatusEvent string = "openclaw.instance.status"
	OpenClawChatSendEvent       string = "openclaw.chat.send"
	OpenClawOnboardStatusEvent  string = "openclaw.onboard.status"
	OpenClawChatAcceptedEvent   string = "openclaw.chat.accepted"
	OpenClawEventError          string = "openclaw.error"
)

// BaseMessage 基础消息结构，用于初始解析路由
type BaseMessage struct {
	Type string `json:"type"`
}

type OpenClawEvent struct {
	Type      string          `json:"type"`
	RequestID string          `json:"request_id,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

type OpenClawEventErrorMessage struct {
	Type      string `json:"type"`
	RequestID string `json:"request_id,omitempty"`
	Code      string `json:"error_code"`
	Message   string `json:"message"`
}

// AuthMessage 客户端认证消息
type AuthMessage struct {
	Type      string `json:"type"`
	Token     string `json:"token"`
	Timestamp int64  `json:"timestamp,omitempty"`
	Version   string `json:"version,omitempty"`
}

// AuthSuccessMessage 认证成功响应
type AuthSuccessMessage struct {
	Type         string `json:"type"`
	Timestamp    int64  `json:"timestamp"`
	ChannelCount int    `json:"channel_count"`
	Version      string `json:"version"`
}

// ErrorMessage 错误消息
type ErrorMessage struct {
	Type      string `json:"type"`
	Code      string `json:"code"`
	ErrorMsg  string `json:"error_message"`
	MessageID string `json:"message_id,omitempty"`
	ChannelID int    `json:"channel_id,omitempty"`
	Timestamp int64  `json:"timestamp"`
}

// Media 媒体信息（暂时留空）
type Media struct{}

// ClawMessage 业务消息
// ClawMessage 在models中定义

type ListClawMessageModel struct {
	ChannelID int    `json:"channel_id" query:"channel_id" validate:"required,min=1"`
	Size      int    `json:"size" query:"size" default:"30" validate:"omitempty,min=0"`
	Offset    int    `json:"offset" query:"offset" default:"0" validate:"min=0"`
	Sort      string `json:"sort" query:"sort" default:"desc" validate:"oneof=asc desc"` // Sort order
}

// PingMessage 心跳请求
type PingMessage struct {
	Type      string `json:"type"`
	Timestamp int64  `json:"timestamp"`
	Version   string `json:"version,omitempty"`
}

// PongMessage 心跳响应
type PongMessage struct {
	Type      string `json:"type"`
	Timestamp int64  `json:"timestamp"`
	Version   string `json:"version,omitempty"`
}

// 错误码定义
const (
	ErrCodeAuthFailed    = "AUTH_001"
	ErrCodeNotAuthed     = "AUTH_002"
	ErrCodeEmptyContent  = "MSG_001"
	ErrCodeUnknownType   = "MSG_002"
	ErrCodeInternal      = "SYS_001"
	ErrCodeProcessFailed = "CLAW_001"
)

type OpenClawTest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}
