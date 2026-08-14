package openclaw

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"treehole_next/models"

	"github.com/rs/zerolog/log"
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

const (
	defaultStaleOperationAfter = 15 * time.Minute
	staleOperationCode         = "stale_operation"
	staleOperationMessage      = "operation expired before completion"
)

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
	if to == StateReady {
		return ErrInvalidStateTransition
	}
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
	if instance == nil || InstanceState(instance.State) != StateStarting {
		return ErrInvalidStateTransition
	}
	instance.State = string(StateReady)
	instance.UpdatedAt = time.Now()
	return nil
}

type InstanceService struct {
	db                  *gorm.DB
	provider            OpenClawInstanceProvider
	staleOperationAfter time.Duration

	locksMu sync.Mutex
	locks   map[int]*sync.Mutex
}

func NewInstanceService(db *gorm.DB, provider OpenClawInstanceProvider) *InstanceService {
	if db == nil {
		db = models.DB
	}
	return &InstanceService{
		db:                  db,
		provider:            provider,
		staleOperationAfter: defaultStaleOperationAfter,
		locks:               make(map[int]*sync.Mutex),
	}
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

	instance, operation, execute, reused, cleanupProviderID, err := s.reserveOnboard(userID, idempotencyKey, req)
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

	if cleanupProviderID != "" {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		cleanupErr := s.provider.Destroy(cleanupCtx, cleanupProviderID)
		cancel()
		if cleanupErr != nil {
			cleanupFailure := &ProviderError{
				Operation:  "cleanup",
				Code:       ProviderErrorUnknown,
				Kind:       ErrProviderUnknown,
				CleanupErr: cleanupErr,
			}
			if finishErr := s.finishOnboard(operation.ID, instance.ID, "", providerErrorCode(cleanupFailure), safeErrorMessage(cleanupFailure), false, cleanupFailure); finishErr != nil {
				s.failOperationBestEffort(operation.ID, instance.ID, providerErrorCode(finishErr), "failed to persist provider cleanup failure", nil)
			}
			return nil, cleanupFailure
		}
	}

	providerInstance, err := s.provider.Create(ctx, CreateRequest{
		UserID: userID, Name: req.Name, Image: req.Image, Metadata: req.Metadata,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		if finishErr := s.finishOnboard(operation.ID, instance.ID, "", providerErrorCode(err), safeErrorMessage(err), false, err); finishErr != nil {
			s.failOperationBestEffort(operation.ID, instance.ID, providerErrorCode(finishErr), "failed to persist provider failure", nil)
		}
		return nil, err
	}
	if providerInstance.ID == "" {
		err = &ProviderError{Operation: "create", Code: ProviderErrorUnknown, Kind: ErrProviderUnknown}
		if finishErr := s.finishOnboard(operation.ID, instance.ID, "", providerErrorCode(err), safeErrorMessage(err), false, err); finishErr != nil {
			s.failOperationBestEffort(operation.ID, instance.ID, providerErrorCode(finishErr), "failed to persist provider failure", nil)
		}
		return nil, err
	}

	if err := s.finishOnboard(operation.ID, instance.ID, providerInstance.ID, "", "", true, nil); err != nil {
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

func (s *InstanceService) reserveOnboard(
	userID int,
	key string,
	req OnboardRequest,
) (*models.OpenClawInstance, *models.OpenClawOperation, bool, bool, string, error) {
	var instance models.OpenClawInstance
	var operation models.OpenClawOperation
	execute := false
	reused := false
	cleanupProviderID := ""
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
				if operation.InstanceID == nil {
					return ErrOperationFailed
				}
				if err := tx.First(&instance, *operation.InstanceID).Error; err != nil {
					return err
				}
				if s.operationIsStale(operation) {
					if err := s.recoverStaleOperation(tx, &instance, &operation); err != nil {
						return err
					}
					return fmt.Errorf("%w: %s", ErrOperationFailed, staleOperationCode)
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
			if instance.State == string(StateFailed) {
				cleanupProviderID = strings.TrimSpace(instance.ProviderInstanceID)
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
		return nil, nil, false, false, "", err
	}
	return &instance, &operation, execute, reused, cleanupProviderID, nil
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

func (s *InstanceService) failOperationBestEffort(
	operationID, instanceID uint,
	code, message string,
	cause error,
) {
	_ = s.db.Transaction(func(tx *gorm.DB) error {
		cleanupCode, cleanupMessage := providerCleanupState(cause)
		_ = tx.Model(&models.OpenClawInstance{}).Where("id = ?", instanceID).
			Updates(map[string]any{
				"state": string(StateFailed), "last_error_code": code, "last_error_message": message,
				"cleanup_error_code": cleanupCode, "cleanup_error_message": cleanupMessage,
			}).Error
		return tx.Model(&models.OpenClawOperation{}).Where("id = ? AND status = ?", operationID, string(OperationRunning)).
			Updates(map[string]any{
				"status": string(OperationFailed), "error_code": code, "error_message": message,
				"cleanup_error_code": cleanupCode, "cleanup_error_message": cleanupMessage,
			}).Error
	})
}

func (s *InstanceService) finishOnboard(
	operationID, instanceID uint,
	providerInstanceID, code, message string,
	success bool,
	cause error,
) error {
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
			instance.CleanupErrorCode = ""
			instance.CleanupErrorMessage = ""
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
			operation.CleanupErrorCode, operation.CleanupErrorMessage = providerCleanupState(cause)
			instance.CleanupErrorCode = operation.CleanupErrorCode
			instance.CleanupErrorMessage = operation.CleanupErrorMessage
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

func (s *InstanceService) operationIsStale(operation models.OpenClawOperation) bool {
	if s.staleOperationAfter <= 0 {
		return false
	}
	timestamp := operation.UpdatedAt
	if timestamp.IsZero() {
		timestamp = operation.CreatedAt
	}
	return !timestamp.IsZero() && time.Since(timestamp) > s.staleOperationAfter
}

func (s *InstanceService) recoverStaleOperation(
	tx *gorm.DB,
	instance *models.OpenClawInstance,
	operation *models.OpenClawOperation,
) error {
	if operation == nil {
		return nil
	}
	operation.Status = string(OperationFailed)
	operation.ErrorCode = staleOperationCode
	operation.ErrorMessage = staleOperationMessage
	if instance != nil && InstanceState(instance.State) != StateFailed {
		if CanTransition(InstanceState(instance.State), StateFailed) {
			if err := Transition(instance, StateFailed); err != nil {
				return err
			}
		} else {
			instance.State = string(StateFailed)
			instance.UpdatedAt = time.Now()
		}
	}
	if instance != nil {
		instance.LastErrorCode = staleOperationCode
		instance.LastErrorMessage = staleOperationMessage
		if err := tx.Save(instance).Error; err != nil {
			return err
		}
	}
	if err := tx.Save(operation).Error; err != nil {
		return err
	}
	log.Warn().
		Int("user_id", operation.UserID).
		Uint("operation_id", operation.ID).
		Str("operation", operation.Operation).
		Str("reason", staleOperationCode).
		Msg("OpenClaw stale operation recovered")
	return nil
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

func providerCleanupState(err error) (string, string) {
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || providerErr.CleanupErr == nil {
		return "", ""
	}
	return ProviderCleanupErrorCode, "compensation cleanup failed"
}
