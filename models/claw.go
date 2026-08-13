package models

import (
	"time"

	"gorm.io/gorm"
)

// ClawSession OpenClaw 会话表
type ClawSession struct {
	ID            uint      `json:"id" gorm:"primaryKey"`
	UserID        int       `json:"user_id" gorm:"index:idx_claw_user_session,priority:1"`
	InstanceID    uint      `json:"instance_id" gorm:"index:idx_claw_instance_session,priority:1"`
	UserSessionID int       `json:"user_session_id" gorm:"not null;uniqueIndex:idx_claw_user_session,priority:2"`
	Conversation  string    `json:"conversation"`                                                // 用户自定义的会话名称
	OC_SessionID  string    `json:"oc_session_id" gorm:"type:varchar(191);not null;uniqueIndex"` // OpenClaw 生成的 session ID
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type ClawMessage struct {
	ID         uint           `json:"id" gorm:"primaryKey"`
	UserID     int            `json:"user_id,omitempty" gorm:"index:idx_claw_user_task,priority:1"`
	Type       string         `json:"type" gorm:"size:32;index"`
	From       string         `json:"from" gorm:"size:64"`
	Content    string         `json:"content" gorm:"type:text;not null"`
	MessageID  string         `json:"message_id" gorm:"type:varchar(191);index"`
	TaskID     string         `json:"task_id,omitempty" gorm:"type:varchar(191);index"`
	SessionID  string         `json:"session_id,omitempty" gorm:"size:191;index"`
	InstanceID uint           `json:"instance_id,omitempty" gorm:"index:idx_claw_instance_task,priority:1"`
	ChannelID  int            `json:"channel_id" gorm:"index"`
	Timestamp  int64          `json:"timestamp"`
	Media      any            `json:"media" gorm:"serializer:json"`
	Version    string         `json:"version,omitempty" gorm:"size:32"`
	CreatedAt  time.Time      `json:"created_at"`
	UpdatedAt  time.Time      `json:"updated_at"`
	DeletedAt  gorm.DeletedAt `json:"-" gorm:"index"`
}

func (ClawSession) TableName() string {
	return "claw_session"
}

func (ClawMessage) TableName() string {
	return "claw_message"
}

// GetSessionsByUserID 获取用户的所有会话
func GetSessionsByUserID(tx *gorm.DB, userID int) ([]*ClawSession, error) {
	data := make([]*ClawSession, 0)
	err := tx.Where("user_id = ?", userID).Order("created_at DESC").Find(&data).Error
	return data, err
}

// GetSessionCountByUserID 获取用户会话总数
func GetSessionCountByUserID(tx *gorm.DB, userID int) (int64, error) {
	var count int64
	err := tx.Model(&ClawSession{}).
		Where("user_id = ?", userID).
		Count(&count).Error
	return count, err
}

// GetSessionByUserAndSessionID 通过用户ID和用户内会话ID查询
func GetSessionByUserAndSessionID(tx *gorm.DB, userID int, userSessionID int) (*ClawSession, error) {
	var session ClawSession
	err := tx.Where("user_id = ? AND user_session_id = ?", userID, userSessionID).First(&session).Error
	if err != nil {
		return nil, err
	}
	return &session, nil
}

func GetSessionByUserInstanceAndSessionID(tx *gorm.DB, userID int, instanceID uint, userSessionID int) (*ClawSession, error) {
	var session ClawSession
	err := tx.Where("user_id = ? AND instance_id = ? AND user_session_id = ?", userID, instanceID, userSessionID).First(&session).Error
	if err != nil {
		return nil, err
	}
	return &session, nil
}

// GetSessionByOCID 通过 OpenClaw SessionID 查询
func GetSessionByOCID(tx *gorm.DB, ocSessionID string) (*ClawSession, error) {
	var session ClawSession
	err := tx.Where("oc_session_id = ?", ocSessionID).First(&session).Error
	if err != nil {
		return nil, err
	}
	return &session, nil
}

// CreateSession 创建新会话
func getNextUserSessionID(tx *gorm.DB, userID int) (int, error) {
	var maxID int64
	err := tx.Model(&ClawSession{}).
		Where("user_id = ?", userID).
		Select("COALESCE(MAX(user_session_id), 0)").
		Scan(&maxID).Error
	if err != nil {
		return 0, err
	}
	return int(maxID) + 1, nil
}

func CreateSession(tx *gorm.DB, userID int, conversation string, ocSessionID string) (*ClawSession, error) {
	return CreateSessionForInstance(tx, userID, 0, conversation, ocSessionID)
}

func CreateSessionForInstance(tx *gorm.DB, userID int, instanceID uint, conversation string, ocSessionID string) (*ClawSession, error) {
	userSessionID, err := getNextUserSessionID(tx, userID)
	if err != nil {
		return nil, err
	}

	session := &ClawSession{
		UserID:        userID,
		InstanceID:    instanceID,
		UserSessionID: userSessionID,
		Conversation:  conversation,
		OC_SessionID:  ocSessionID,
	}
	err = tx.Create(session).Error
	return session, err
}

// UpdateSession 更新会话名称
func UpdateSession(tx *gorm.DB, id int, conversation string) error {
	return tx.Model(&ClawSession{}).Where("id = ?", id).
		Updates(map[string]interface{}{
			"conversation": conversation,
			"updated_at":   time.Now(),
		}).Error
}

// DeleteSession 删除会话
func DeleteSession(tx *gorm.DB, id int) error {
	return tx.Where("id = ?", id).Delete(&ClawSession{}).Error
}

// GetMessagesByChannelID 获取指定频道的消息，支持分页和排序
func GetMessagesByChannelID(tx *gorm.DB, channelID int, size, offset int, sort string) ([]*ClawMessage, error) {
	data := make([]*ClawMessage, 0)
	order := "created_at DESC"
	if sort == "asc" {
		order = "created_at ASC"
	}
	query := tx.Where("channel_id = ?", channelID).Order(order)
	if size > 0 {
		query = query.Limit(size).Offset(offset)
	}
	err := query.Find(&data).Error
	return data, err
}

// CreateMessage 创建消息记录
func CreateMessage(tx *gorm.DB, msg *ClawMessage) error {
	return tx.Create(msg).Error
}

// GetOrCreateSession 获取或创建会话（你说的"不存在则新建"）
func GetOrCreateSession(tx *gorm.DB, userID int, conversation string, ocSessionID string) (*ClawSession, error) {
	// 先查是否存在
	var session ClawSession
	err := tx.Where("user_id = ? AND oc_session_id = ?", userID, ocSessionID).First(&session).Error
	if err == nil {
		return &session, nil // 已存在，直接返回
	}
	if err != gorm.ErrRecordNotFound {
		return nil, err // 其他错误
	}
	// 不存在则创建
	return CreateSession(tx, userID, conversation, ocSessionID)
}

func GetMessagesByTaskAndInstance(tx *gorm.DB, taskID string, instanceID uint) ([]*ClawMessage, error) {
	data := make([]*ClawMessage, 0)
	err := tx.Where("task_id = ? AND instance_id = ?", taskID, instanceID).
		Order("created_at ASC").Find(&data).Error
	return data, err
}
