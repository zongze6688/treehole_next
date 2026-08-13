package models

import "time"

// OpenClawInstance is the single control-plane instance record owned by a user.
type OpenClawInstance struct {
	ID                 uint      `json:"id" gorm:"primaryKey"`
	UserID             int       `json:"user_id" gorm:"not null;uniqueIndex:uidx_openclaw_instance_user"`
	Provider           string    `json:"provider" gorm:"size:32;not null"`
	ProviderInstanceID string    `json:"provider_instance_id,omitempty" gorm:"size:191;index"`
	State              string    `json:"state" gorm:"size:32;not null;index"`
	LastErrorCode      string    `json:"last_error_code,omitempty" gorm:"size:64"`
	LastErrorMessage   string    `json:"last_error_message,omitempty" gorm:"size:255"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
}

func (OpenClawInstance) TableName() string {
	return "openclaw_instance"
}

// OpenClawOperation persists lifecycle idempotency and its terminal result.
type OpenClawOperation struct {
	ID             uint      `json:"id" gorm:"primaryKey"`
	UserID         int       `json:"user_id" gorm:"not null;uniqueIndex:uidx_openclaw_operation_key,priority:1"`
	InstanceID     *uint     `json:"instance_id,omitempty" gorm:"index"`
	Operation      string    `json:"operation" gorm:"size:32;not null"`
	TargetState    string    `json:"target_state,omitempty" gorm:"size:32"`
	RequestHash    string    `json:"-" gorm:"size:64"`
	IdempotencyKey string    `json:"-" gorm:"size:191;not null;uniqueIndex:uidx_openclaw_operation_key,priority:2"`
	Status         string    `json:"status" gorm:"size:32;not null;index"`
	ErrorCode      string    `json:"error_code,omitempty" gorm:"size:64"`
	ErrorMessage   string    `json:"error_message,omitempty" gorm:"size:255"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

func (OpenClawOperation) TableName() string {
	return "openclaw_operation"
}
