package claw

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/goccy/go-json"
	"github.com/gofiber/contrib/websocket"
	"github.com/rs/zerolog/log"
	"gorm.io/gorm"

	"treehole_next/config"
	. "treehole_next/models"
	"treehole_next/openclaw"
)

type TaskCorrelation struct {
	UserID     int
	InstanceID uint
	ChannelID  int
	SessionID  string
}

func ResolveUserInstance(userID int) (*OpenClawInstance, error) {
	if userID <= 0 || DB == nil {
		return nil, gorm.ErrRecordNotFound
	}
	var instance OpenClawInstance
	if err := DB.Where("user_id = ?", userID).First(&instance).Error; err != nil {
		return nil, err
	}
	if instance.ID == 0 || instance.State == string(openclaw.StateNotStarted) ||
		instance.State == string(openclaw.StateResetting) {
		return nil, gorm.ErrRecordNotFound
	}
	return &instance, nil
}

func ResolveTaskCorrelation(tx *gorm.DB, userID int, instanceID uint, taskID, sessionID string) (*TaskCorrelation, error) {
	if tx == nil || userID <= 0 || instanceID == 0 || strings.TrimSpace(taskID) == "" {
		return nil, errors.New("invalid task correlation")
	}
	var message ClawMessage
	query := tx.Where(
		"user_id = ? AND instance_id = ? AND task_id = ? AND from = ?",
		userID, instanceID, taskID, "user",
	)
	if err := query.Order("created_at ASC").First(&message).Error; err != nil {
		return nil, err
	}
	if sessionID != "" && message.SessionID != "" && message.SessionID != sessionID {
		return nil, errors.New("task session mismatch")
	}
	return &TaskCorrelation{
		UserID: userID, InstanceID: instanceID,
		ChannelID: message.ChannelID, SessionID: message.SessionID,
	}, nil
}

func activeOpenClawClient(userID int) (*OcClient, *OpenClawInstance, error) {
	instance, err := ResolveUserInstance(userID)
	if err != nil {
		return nil, nil, err
	}
	client, ok := ocRegistry.Get(userID, instance.ID)
	if !ok {
		return nil, instance, errors.New("OpenClaw instance is not connected")
	}
	return client, instance, nil
}

func validateOpenClawToken(token string) (int, error) {
	url := config.Config.AuthUrl + "/api/validate/oc"
	req, err := http.NewRequest(http.MethodPost, url, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("OpenClaw token validation failed: status %d", resp.StatusCode)
	}
	var result struct {
		UserID int `json:"user_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, err
	}
	if result.UserID <= 0 {
		return 0, errors.New("invalid OpenClaw user id")
	}
	return result.UserID, nil
}

func sendToOpenClaw(client *OcClient, payload any) error {
	if client == nil || !client.IsAuthed || client.Conn == nil {
		return errors.New("OpenClaw instance is not connected")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.Conn.WriteJSON(payload)
}

func writeOpenClawError(c *websocket.Conn, message string) {
	if err := c.WriteJSON(map[string]any{
		"type": MessageTypeError, "code": ErrCodeInternal, "error_message": message,
	}); err != nil {
		log.Err(err).Msg("[Claw] write OpenClaw error failed")
	}
}
