package openclaw

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"treehole_next/models"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type InstanceState string

const (
	StateNotStarted   InstanceState = "not_started"
	StateProvisioning InstanceState = "provisioning"
	StateStarting     InstanceState = "starting"
	StateReady        InstanceState = "ready"
	StateStopping     InstanceState = "stopping"
	StateStopped      InstanceState = "stopped"
	StateResetting    InstanceState = "resetting"
	StateFailed       InstanceState = "failed"
)

type OperationStatus string

const (
	OperationRunning   OperationStatus = "running"
	OperationCompleted OperationStatus = "completed"
	OperationFailed    OperationStatus = "failed"
)

const operationOnboard = "onboard"

var (
	ErrInvalidStateTransition = errors.New("invalid OpenClaw instance state transition")
	ErrInstanceConflict       = errors.New("OpenClaw instance operation conflict")
	ErrOperationInProgress    = errors.New("OpenClaw operation is in progress")
	ErrOperationFailed        = errors.New("OpenClaw operation failed")
)

type Readiness struct {
	ContainerRunning     bool
	GatewayHealthy       bool
	ChannelAuthenticated bool
}

func ValidState(state InstanceState) bool {
	switch state {
	case StateNotStarted, StateProvisioning, StateStarting, StateReady,
		StateStopping, StateStopped, StateResetting, StateFailed:
		return true
	default:
		return false
	}
}

func CanTransition(from, to InstanceState) bool {
	switch from {
	case StateNotStarted:
		return to == StateProvisioning
	case StateProvisioning:
		return to == StateStarting || to == StateFailed
	case StateStarting:
		return to == StateReady || to == StateFailed
	case StateReady:
		return to == StateStopping || to == StateResetting || to == StateFailed
	case StateStopping:
		return to == StateStopped || to == StateFailed
	case StateStopped:
		return to == StateStarting || to == StateResetting || to == StateFailed
	case StateResetting:
		return to == StateNotStarted || to == StateFailed
	case StateFailed:
		return to == StateProvisioning || to == StateStarting ||
			to == StateStopping || to == StateResetting
	default:
		return false
	}
}

func Transition(instance *models.OpenClawInstance, to InstanceState) error {
	if instance == nil || !ValidState(InstanceState(instance.State)) || !ValidState(to) ||
		!CanTransition(InstanceState(instance.State), to) {
		return ErrInvalidStateTransition
	}
	instance.State = string(to)
	instance.UpdatedAt = time.Now()
	return nil
}

func MarkReady(instance *models.OpenClawInstance, readiness Readiness) error {
	if !readiness.ContainerRunning || !readiness.GatewayHealthy || !readiness.ChannelAuthenticated {
		return ErrInvalidStateTransition
	}
	return Transition(instance, StateReady)
}

type InstanceService struct {
	db       *gorm.DB
	provider OpenClawInstanceProvider

	locksMu sync.Mutex
	locks   map[int]*sync.Mutex
}

func NewInstanceService(db *gorm.DB, provider OpenClawInstanceProvider) *InstanceService {
	if db == nil {
		db = models.DB
	}
	return &InstanceService{db: db, provider: provider, locks: make(map[int]*sync.Mutex)}
}

func (s *InstanceService) userLock(userID int) *sync.Mutex {
	s.locksMu.Lock()
	defer s.locksMu.Unlock()
	lock, ok := s.locks[userID]
	if !ok {
		lock = &sync.Mutex{}
		s.locks[userID] = lock
	}
	return lock
}

type OnboardRequest struct {
	Provider string
	Name     string
	Image    string
	Metadata map[string]string
}

type OnboardResult struct {
	Instance  *models.OpenClawInstance
	Operation *models.OpenClawOperation
	Reused    bool
}

func (s *InstanceService) Onboard(ctx context.Context, userID int, idempotencyKey string, req OnboardRequest) (*OnboardResult, error) {
	if s.db == nil {
		return nil, errors.New("OpenClaw instance database is not configured")
	}
	if s.provider == nil {
		return nil, errors.New("OpenClaw instance provider is not configured")
	}
	if userID <= 0 || idempotencyKey == "" || req.Provider == "" {
		return nil, ErrInstanceConflict
	}

	lock := s.userLock(userID)
	lock.Lock()
	defer lock.Unlock()

	instance, operation, execute, reused, err := s.reserveOnboard(userID, idempotencyKey, req)
	if err != nil {
		return nil, err
	}
	if !execute {
		if operation.ID == 0 {
			operation = &models.OpenClawOperation{
				UserID: userID, InstanceID: &instance.ID, Operation: operationOnboard,
				TargetState: instance.State, Status: string(OperationCompleted),
				RequestHash: hashOnboardRequest(req),
			}
		}
		return &OnboardResult{Instance: instance, Operation: operation, Reused: reused}, nil
	}

	providerInstance, err := s.provider.Create(ctx, CreateRequest{
		UserID: userID, Name: req.Name, Image: req.Image, Metadata: req.Metadata,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		if finishErr := s.finishOnboard(operation.ID, instance.ID, "", providerErrorCode(err), safeErrorMessage(err), false); finishErr != nil {
			s.failOperationBestEffort(operation.ID, instance.ID, providerErrorCode(finishErr), "failed to persist provider failure")
		}
		return nil, err
	}
	if providerInstance.ID == "" {
		err = &ProviderError{Operation: "create", Code: ProviderErrorUnknown, Kind: ErrProviderUnknown}
		if finishErr := s.finishOnboard(operation.ID, instance.ID, "", providerErrorCode(err), safeErrorMessage(err), false); finishErr != nil {
			s.failOperationBestEffort(operation.ID, instance.ID, providerErrorCode(finishErr), "failed to persist provider failure")
		}
		return nil, err
	}

	if err := s.finishOnboard(operation.ID, instance.ID, providerInstance.ID, "", "", true); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = s.provider.Destroy(cleanupCtx, providerInstance.ID)
		return nil, err
	}

	instance.ProviderInstanceID = providerInstance.ID
	instance.State = string(StateStarting)
	operation.Status = string(OperationCompleted)
	return &OnboardResult{Instance: instance, Operation: operation}, nil
}

func (s *InstanceService) reserveOnboard(userID int, key string, req OnboardRequest) (*models.OpenClawInstance, *models.OpenClawOperation, bool, bool, error) {
	var instance models.OpenClawInstance
	var operation models.OpenClawOperation
	execute := false
	reused := false
	requestHash := hashOnboardRequest(req)

	err := s.db.Transaction(func(tx *gorm.DB) error {
		err := tx.Where("user_id = ? AND idempotency_key = ?", userID, key).First(&operation).Error
		if err == nil {
			if operation.Operation != operationOnboard {
				return ErrInstanceConflict
			}
			if operation.RequestHash != requestHash {
				return ErrInstanceConflict
			}
			switch OperationStatus(operation.Status) {
			case OperationCompleted:
				if operation.InstanceID == nil {
					return ErrOperationFailed
				}
				if err := tx.First(&instance, *operation.InstanceID).Error; err != nil {
					return err
				}
				reused = true
				return nil
			case OperationRunning:
				if err := tx.First(&instance, *operation.InstanceID).Error; err != nil {
					return err
				}
				reused = true
				return nil
			case OperationFailed:
				return fmt.Errorf("%w: %s", ErrOperationFailed, operation.ErrorCode)
			default:
				return ErrOperationFailed
			}
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("user_id = ?", userID).First(&instance).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			instance = models.OpenClawInstance{
				UserID: userID, Provider: req.Provider, State: string(StateNotStarted),
			}
			if err := tx.Create(&instance).Error; err != nil {
				return err
			}
		} else if err != nil {
			return err
		} else {
			if instance.Provider != req.Provider {
				return ErrInstanceConflict
			}
			if instance.State != string(StateNotStarted) && instance.State != string(StateFailed) {
				if err := tx.Where("user_id = ? AND instance_id = ?", userID, instance.ID).
					Order("id DESC").First(&operation).Error; err != nil {
					return err
				}
				reused = true
				return nil
			}
		}

		operation = models.OpenClawOperation{
			UserID: userID, InstanceID: &instance.ID, Operation: operationOnboard,
			TargetState: string(StateStarting), RequestHash: requestHash,
			IdempotencyKey: key, Status: string(OperationRunning),
		}
		if err := tx.Create(&operation).Error; err != nil {
			return err
		}
		if err := Transition(&instance, StateProvisioning); err != nil {
			return err
		}
		if err := tx.Save(&instance).Error; err != nil {
			return err
		}
		execute = true
		return nil
	})
	if err != nil {
		return nil, nil, false, false, err
	}
	return &instance, &operation, execute, reused, nil
}

func hashOnboardRequest(req OnboardRequest) string {
	data, _ := json.Marshal(struct {
		Provider string            `json:"provider"`
		Name     string            `json:"name"`
		Image    string            `json:"image"`
		Metadata map[string]string `json:"metadata"`
	}{req.Provider, req.Name, req.Image, req.Metadata})
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

func (s *InstanceService) failOperationBestEffort(operationID, instanceID uint, code, message string) {
	_ = s.db.Transaction(func(tx *gorm.DB) error {
		_ = tx.Model(&models.OpenClawInstance{}).Where("id = ?", instanceID).
			Updates(map[string]any{"state": string(StateFailed), "last_error_code": code, "last_error_message": message}).Error
		return tx.Model(&models.OpenClawOperation{}).Where("id = ? AND status = ?", operationID, string(OperationRunning)).
			Updates(map[string]any{"status": string(OperationFailed), "error_code": code, "error_message": message}).Error
	})
}

func (s *InstanceService) finishOnboard(operationID, instanceID uint, providerInstanceID, code, message string, success bool) error {
	return s.db.Transaction(func(tx *gorm.DB) error {
		var instance models.OpenClawInstance
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&instance, instanceID).Error; err != nil {
			return err
		}
		var operation models.OpenClawOperation
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&operation, operationID).Error; err != nil {
			return err
		}

		if success {
			if err := Transition(&instance, StateStarting); err != nil {
				return err
			}
			instance.ProviderInstanceID = providerInstanceID
			instance.LastErrorCode = ""
			instance.LastErrorMessage = ""
			operation.Status = string(OperationCompleted)
		} else {
			if err := Transition(&instance, StateFailed); err != nil {
				return err
			}
			instance.LastErrorCode = code
			instance.LastErrorMessage = message
			operation.Status = string(OperationFailed)
			operation.ErrorCode = code
			operation.ErrorMessage = message
		}
		if err := tx.Save(&instance).Error; err != nil {
			return err
		}
		return tx.Save(&operation).Error
	})
}

func (s *InstanceService) Transition(ctx context.Context, userID int, idempotencyKey string, to InstanceState) (*models.OpenClawInstance, error) {
	if s.db == nil || userID <= 0 || idempotencyKey == "" || !ValidState(to) {
		return nil, ErrInstanceConflict
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	lock := s.userLock(userID)
	lock.Lock()
	defer lock.Unlock()

	var instance models.OpenClawInstance
	err := s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("user_id = ?", userID).First(&instance).Error; err != nil {
			return err
		}

		var existing models.OpenClawOperation
		err := tx.Where("user_id = ? AND idempotency_key = ?", userID, idempotencyKey).First(&existing).Error
		if err == nil {
			if existing.Operation != "transition" || existing.TargetState != string(to) {
				return ErrInstanceConflict
			}
			switch OperationStatus(existing.Status) {
			case OperationCompleted:
				return nil
			case OperationRunning:
				return ErrOperationInProgress
			default:
				return fmt.Errorf("%w: %s", ErrOperationFailed, existing.ErrorCode)
			}
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		if err := Transition(&instance, to); err != nil {
			return err
		}
		operation := models.OpenClawOperation{
			UserID: userID, InstanceID: &instance.ID, Operation: "transition",
			TargetState: string(to), IdempotencyKey: idempotencyKey, Status: string(OperationCompleted),
		}
		if err := tx.Create(&operation).Error; err != nil {
			return err
		}
		return tx.Save(&instance).Error
	})
	return &instance, err
}

func providerErrorCode(err error) string {
	var providerErr *ProviderError
	if errors.As(err, &providerErr) {
		return string(providerErr.Code)
	}
	return "provider_error"
}

func safeErrorMessage(err error) string {
	var providerErr *ProviderError
	if errors.As(err, &providerErr) {
		return providerErr.Error()
	}
	return "provider operation failed"
}
