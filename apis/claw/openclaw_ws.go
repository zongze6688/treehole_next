package claw

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/goccy/go-json"
	"github.com/gofiber/contrib/websocket"
	"github.com/rs/zerolog/log"
	"gorm.io/gorm"

	. "treehole_next/models"
	"treehole_next/openclaw"
)

var openClawRequestSequence uint64

type openClawOnboardPayload struct {
	Provider string            `json:"provider"`
	Name     string            `json:"name"`
	Image    string            `json:"image"`
	Metadata map[string]string `json:"metadata"`
}

type openClawChatSendPayload struct {
	ChannelID int    `json:"channel_id"`
	SessionID string `json:"session_id"`
	Content   string `json:"content"`
	MessageID string `json:"message_id"`
	Media     any    `json:"media"`
}

type openClawStatusPayload struct {
	State      string `json:"state"`
	InstanceID uint   `json:"instance_id,omitempty"`
}

type openClawAcceptedPayload struct {
	Status    string `json:"status"`
	TaskID    string `json:"task_id"`
	SessionID string `json:"session_id,omitempty"`
	ChannelID int    `json:"channel_id"`
}

func isOpenClawEvent(messageType string) bool {
	return strings.HasPrefix(messageType, "openclaw.")
}

func handleOpenClawEvent(c *websocket.Conn, client *Client, rawMsg json.RawMessage) {
	var event OpenClawEvent
	if err := json.Unmarshal(rawMsg, &event); err != nil {
		sendOpenClawError(c, "", ErrCodeUnknownType, "消息格式错误")
		return
	}

	switch event.Type {
	case OpenClawOnboardEvent:
		handleOpenClawOnboard(c, client, event)
	case OpenClawInstanceStatusEvent:
		handleOpenClawInstanceStatus(c, client, event)
	case OpenClawChatSendEvent:
		handleOpenClawChatSend(c, client, event)
	default:
		sendOpenClawError(c, event.RequestID, ErrCodeUnknownType, "未知的 OpenClaw 事件")
	}
}

func handleOpenClawOnboard(c *websocket.Conn, client *Client, event OpenClawEvent) {
	if lifecycleService == nil {
		sendOpenClawError(c, event.RequestID, "OPENCLAW_NOT_CONFIGURED", "OpenClaw 生命周期服务未配置")
		return
	}

	var payload openClawOnboardPayload
	if err := unmarshalOpenClawPayload(event, &payload); err != nil {
		sendOpenClawError(c, event.RequestID, ErrCodeUnknownType, "onboard 参数格式错误")
		return
	}

	idempotencyKey := strings.TrimSpace(event.RequestID)
	if idempotencyKey == "" {
		idempotencyKey = newOpenClawRequestID(client.UserID, "onboard")
	}
	result, err := lifecycleService.Onboard(context.Background(), client.UserID, idempotencyKey, openclaw.OnboardRequest{
		Provider: payload.Provider,
		Name:     payload.Name,
		Image:    payload.Image,
		Metadata: payload.Metadata,
	})
	if err != nil {
		log.Err(err).Msgf("[Claw] APP OpenClaw onboard failed user=%d", client.UserID)
		sendOpenClawError(c, event.RequestID, ErrCodeProcessFailed, "OpenClaw onboard 失败")
		return
	}

	response := map[string]any{
		"type":       OpenClawOnboardStatusEvent,
		"request_id": event.RequestID,
		"payload": openClawStatusPayload{
			State:      result.Instance.State,
			InstanceID: result.Instance.ID,
		},
	}
	writeOpenClawEvent(c, response)
}

func handleOpenClawInstanceStatus(c *websocket.Conn, client *Client, event OpenClawEvent) {
	payload := openClawStatusForUser(client.UserID)
	response := map[string]any{
		"type":       OpenClawInstanceStatusEvent,
		"request_id": event.RequestID,
		"payload":    payload,
	}
	writeOpenClawEvent(c, response)
}

func handleOpenClawChatSend(c *websocket.Conn, client *Client, event OpenClawEvent) {
	var payload openClawChatSendPayload
	if err := unmarshalOpenClawPayload(event, &payload); err != nil {
		sendOpenClawError(c, event.RequestID, ErrCodeUnknownType, "chat.send 参数格式错误")
		return
	}
	if strings.TrimSpace(payload.Content) == "" {
		sendOpenClawError(c, event.RequestID, ErrCodeEmptyContent, "消息内容不能为空")
		return
	}

	instance, err := ResolveUserInstance(client.UserID)
	if err != nil || instance.State != string(openclaw.StateReady) {
		sendOpenClawError(c, event.RequestID, ErrCodeProcessFailed, "OpenClaw 实例未就绪")
		return
	}

	channelID, err := resolveOpenClawChannel(client.UserID, instance.ID, payload)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			sendOpenClawError(c, event.RequestID, ErrCodeUnknownType, "会话不存在")
		} else {
			sendOpenClawError(c, event.RequestID, ErrCodeInternal, "查询会话失败")
		}
		return
	}

	messageID := strings.TrimSpace(payload.MessageID)
	if messageID == "" {
		messageID = newOpenClawRequestID(client.UserID, "message")
	}
	message := ClawMessage{
		Type:      MessageTypeMessage,
		MessageID: messageID,
		ChannelID: channelID,
		Content:   payload.Content,
		Media:     payload.Media,
	}
	rawMessage, err := json.Marshal(message)
	if err != nil {
		sendOpenClawError(c, event.RequestID, ErrCodeInternal, "消息编码失败")
		return
	}

	handleMessage(c, client, rawMessage)

	var stored ClawMessage
	if err := DB.Where("user_id = ? AND instance_id = ? AND message_id = ?",
		client.UserID, instance.ID, messageID).Order("id DESC").First(&stored).Error; err != nil {
		// handleMessage already sent a protocol error when persistence or validation
		// failed. Do not add a misleading accepted event.
		return
	}

	writeOpenClawEvent(c, map[string]any{
		"type":       OpenClawChatAcceptedEvent,
		"request_id": event.RequestID,
		"payload": openClawAcceptedPayload{
			Status:    "queued",
			TaskID:    stored.TaskID,
			SessionID: stored.SessionID,
			ChannelID: stored.ChannelID,
		},
	})
}

func unmarshalOpenClawPayload(event OpenClawEvent, target any) error {
	if len(event.Payload) == 0 || string(event.Payload) == "null" {
		return nil
	}
	return json.Unmarshal(event.Payload, target)
}

func resolveOpenClawChannel(userID int, instanceID uint, payload openClawChatSendPayload) (int, error) {
	if payload.ChannelID > 0 {
		if _, err := GetSessionByUserInstanceAndSessionID(DB, userID, instanceID, payload.ChannelID); err != nil {
			return 0, err
		}
		return payload.ChannelID, nil
	}
	if strings.TrimSpace(payload.SessionID) != "" {
		var session ClawSession
		err := DB.Where("user_id = ? AND instance_id = ? AND oc_session_id = ?",
			userID, instanceID, payload.SessionID).First(&session).Error
		if err != nil {
			return 0, err
		}
		return session.UserSessionID, nil
	}
	return 0, nil
}

func openClawStatusForUser(userID int) openClawStatusPayload {
	if DB == nil {
		return openClawStatusPayload{State: string(openclaw.StateNotStarted)}
	}
	var instance OpenClawInstance
	if err := DB.Where("user_id = ?", userID).First(&instance).Error; err != nil {
		return openClawStatusPayload{State: string(openclaw.StateNotStarted)}
	}
	return openClawStatusPayload{State: instance.State, InstanceID: instance.ID}
}

func newOpenClawRequestID(userID int, kind string) string {
	sequence := atomic.AddUint64(&openClawRequestSequence, 1)
	return fmt.Sprintf("oc-%s-%d-%d-%d", kind, userID, time.Now().UnixNano(), sequence)
}

func writeOpenClawEvent(c *websocket.Conn, payload any) {
	if err := c.WriteJSON(payload); err != nil {
		log.Err(err).Msg("[Claw] write APP OpenClaw event failed")
	}
}

func sendOpenClawError(c *websocket.Conn, requestID, code, message string) {
	writeOpenClawEvent(c, OpenClawEventErrorMessage{
		Type:      OpenClawEventError,
		RequestID: requestID,
		Code:      code,
		Message:   message,
	})
}
