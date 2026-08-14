package openclaw

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"treehole_next/models"

	"github.com/rs/zerolog/log"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrLifecycleNotConfigured = errors.New("OpenClaw lifecycle service is not configured")
	ErrInstanceNotReady       = errors.New("OpenClaw instance is not ready")
	ErrReadinessUnavailable   = errors.New("OpenClaw readiness checks are not configured")
)

const (
	operationStart   = "start"
	operationStop    = "stop"
	operationRestart = "restart"
	operationReset   = "reset"
)

// ReadinessRequest identifies the trusted server-side instance being checked.
// Implementations must not derive either ID from task or message payloads.
type ReadinessRequest struct {
	UserID             int
	InstanceID         uint
	ProviderInstanceID string
}

// ReadinessChecker is the lifecycle boundary for the three independent ready
// signals. It is intentionally injectable so readiness can be tested without
// a live gateway or channel connection.
type ReadinessChecker interface {
	Check(context.Context, ReadinessRequest) (Readiness, error)
}

// ReadinessChecks adapts concrete health and authentication checks to the
// provider-neutral ReadinessChecker contract.
type ReadinessChecks struct {
	ContainerRunning     func(context.Context, string) (bool, error)
	GatewayHealthy       func(context.Context, string) (bool, error)
	ChannelAuthenticated func(context.Context, int, string) (bool, error)
}

// ReadinessAggregator evaluates all three readiness signals and only reports
// a ready aggregate when every signal is true.
type ReadinessAggregator struct {
	checks ReadinessChecks
}

func NewReadinessAggregator(checks ReadinessChecks) *ReadinessAggregator {
	return &ReadinessAggregator{checks: checks}
}

func (a *ReadinessAggregator) Check(ctx context.Context, req ReadinessRequest) (Readiness, error) {
	if a == nil {
		return Readiness{}, ErrReadinessUnavailable
	}

	var readiness Readiness
	var firstErr error

	if a.checks.ContainerRunning == nil {
		firstErr = ErrReadinessUnavailable
	} else {
		readiness.ContainerRunning, firstErr = a.checks.ContainerRunning(ctx, req.ProviderInstanceID)
	}

	var err error
	if a.checks.GatewayHealthy == nil {
		if firstErr == nil {
			firstErr = ErrReadinessUnavailable
		}
	} else {
		readiness.GatewayHealthy, err = a.checks.GatewayHealthy(ctx, req.ProviderInstanceID)
		if firstErr == nil {
			firstErr = err
		}
	}

	if a.checks.ChannelAuthenticated == nil {
		if firstErr == nil {
			firstErr = ErrReadinessUnavailable
		}
	} else {
		readiness.ChannelAuthenticated, err = a.checks.ChannelAuthenticated(
			ctx, req.UserID, req.ProviderInstanceID,
		)
		if firstErr == nil {
			firstErr = err
		}
	}

	return readiness, firstErr
}

type LifecycleResult struct {
	Instance  *models.OpenClawInstance
	Operation *models.OpenClawOperation
	Readiness Readiness
	Reused    bool
}

// LifecycleService coordinates provider calls and the persisted instance
// state machine. It serializes lifecycle work per user and uses the operation
// idempotency key as the request identity.
type LifecycleService struct {
	instances *InstanceService
	db        *gorm.DB
	provider  OpenClawInstanceProvider
	readiness ReadinessChecker
	identity  WorkloadIdentity
}

func NewLifecycleService(db *gorm.DB, provider OpenClawInstanceProvider, readiness ReadinessChecker) *LifecycleService {
	instances := NewInstanceService(db, provider)
	return &LifecycleService{
		instances: instances,
		db:        instances.db,
		provider:  provider,
		readiness: readiness,
	}
}

// SetWorkloadIdentity attaches the workload identity used to provision and
// revoke the user-level OpenClaw token. Passing nil disables workload identity
// handling; the call is safe on a nil receiver.
func (s *LifecycleService) SetWorkloadIdentity(w WorkloadIdentity) {
	if s == nil {
		return
	}
	s.identity = w
}

// Create reuses the M1 onboarding transaction and provider contract. The
// created instance intentionally remains in starting until Start (or a later
// readiness-aware orchestration) has observed all ready signals.
func (s *LifecycleService) Create(
	ctx context.Context, userID int, idempotencyKey string, req OnboardRequest,
) (*LifecycleResult, error) {
	if s == nil || s.instances == nil {
		return nil, ErrLifecycleNotConfigured
	}
	result, err := s.instances.Onboard(ctx, userID, idempotencyKey, req)
	if err != nil {
		logLifecycleFailure(userID, 0, operationOnboard, err)
		return nil, err
	}
	logLifecycleResult(userID, result.Instance, result.Operation)
	return &LifecycleResult{
		Instance:  result.Instance,
		Operation: result.Operation,
		Reused:    result.Reused,
	}, nil
}

// Onboard performs the APP-facing onboarding flow: provision the instance,
// start it, and require all readiness signals before returning success.
// Create remains the lower-level provisioning operation for callers that need
// to manage startup separately.
func (s *LifecycleService) Onboard(
	ctx context.Context, userID int, idempotencyKey string, req OnboardRequest,
) (*LifecycleResult, error) {
	if s.identity != nil {
		env, err := s.identity.Env(ctx, userID)
		if err != nil {
			logLifecycleFailure(userID, 0, operationOnboard, err)
			return nil, err
		}
		// The token is injected as CellEnv, which is excluded from the
		// idempotency hash, so req.Metadata (the hashed part) stays stable.
		req.CellEnv = mergeCellEnv(req.CellEnv, env)
	}

	created, err := s.Create(ctx, userID, idempotencyKey, req)
	if err != nil {
		logLifecycleFailure(userID, 0, operationStart, err)
		return nil, err
	}

	started, err := s.Start(ctx, userID, onboardStartIdempotencyKey(userID, idempotencyKey))
	if err != nil {
		logLifecycleFailure(userID, 0, operationStart, err)
		return nil, err
	}
	logLifecycleResult(userID, started.Instance, started.Operation)
	started.Reused = created.Reused || started.Reused
	return started, nil
}

const onboardStartKeyPrefix = "openclaw:onboard:start:v1:"

func onboardStartIdempotencyKey(userID int, idempotencyKey string) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%d\x00%s", userID, idempotencyKey)))
	return onboardStartKeyPrefix + hex.EncodeToString(digest[:])
}

func (s *LifecycleService) Start(ctx context.Context, userID int, idempotencyKey string) (*LifecycleResult, error) {
	return s.run(ctx, userID, idempotencyKey, operationStart, func(instance *models.OpenClawInstance) (bool, error) {
		if err := s.provider.Start(ctx, instance.ProviderInstanceID); err != nil {
			return false, err
		}
		return true, nil
	}, func(instance *models.OpenClawInstance) (Readiness, error) {
		return s.checkReady(ctx, instance)
	})
}

func (s *LifecycleService) Stop(ctx context.Context, userID int, idempotencyKey string) (*LifecycleResult, error) {
	return s.run(ctx, userID, idempotencyKey, operationStop, func(instance *models.OpenClawInstance) (bool, error) {
		if err := s.provider.Stop(ctx, instance.ProviderInstanceID); err != nil {
			return false, err
		}
		return true, nil
	}, nil)
}

func (s *LifecycleService) Restart(ctx context.Context, userID int, idempotencyKey string) (*LifecycleResult, error) {
	return s.run(ctx, userID, idempotencyKey, operationRestart, func(instance *models.OpenClawInstance) (bool, error) {
		if err := s.provider.Restart(ctx, instance.ProviderInstanceID); err != nil {
			return false, err
		}
		return true, nil
	}, func(instance *models.OpenClawInstance) (Readiness, error) {
		return s.checkReady(ctx, instance)
	})
}

func (s *LifecycleService) Reset(ctx context.Context, userID int, idempotencyKey string) (*LifecycleResult, error) {
	if s == nil || s.instances == nil || s.provider == nil {
		return nil, ErrLifecycleNotConfigured
	}
	if err := validateLifecycleRequest(ctx, userID, idempotencyKey); err != nil {
		logLifecycleFailure(userID, 0, operationReset, err)
		return nil, err
	}

	lock := s.instances.userLock(userID)
	lock.Lock()
	defer lock.Unlock()

	instance, operation, execute, reused, needsStop, err := s.reserve(
		userID, idempotencyKey, operationReset, StateNotStarted,
	)
	if err != nil {
		logLifecycleFailure(userID, 0, operationReset, err)
		return nil, err
	}
	if !execute {
		return &LifecycleResult{Instance: instance, Operation: operation, Reused: reused}, nil
	}

	if needsStop && strings.TrimSpace(instance.ProviderInstanceID) != "" {
		if err := s.provider.Stop(ctx, instance.ProviderInstanceID); err != nil {
			return nil, s.fail(operation.ID, instance.ID, err)
		}
	}
	if strings.TrimSpace(instance.ProviderInstanceID) != "" {
		if err := s.provider.Destroy(ctx, instance.ProviderInstanceID); err != nil {
			return nil, s.fail(operation.ID, instance.ID, err)
		}
	}
	if s.identity != nil {
		if err := s.identity.Revoke(ctx, userID); err != nil {
			log.Warn().
				Int("user_id", userID).
				Uint("instance_id", instance.ID).
				Str("operation", operationReset).
				Err(err).
				Msg("OpenClaw workload identity revocation failed; continuing reset")
		}
	}
	if err := s.finishReset(operation.ID, instance.ID); err != nil {
		return nil, err
	}
	return s.result(operation.ID, instance.ID, false, Readiness{}), nil
}

type lifecycleAction func(*models.OpenClawInstance) (bool, error)
type readinessAction func(*models.OpenClawInstance) (Readiness, error)

func (s *LifecycleService) run(
	ctx context.Context,
	userID int,
	idempotencyKey string,
	operation string,
	action lifecycleAction,
	check readinessAction,
) (*LifecycleResult, error) {
	if s == nil || s.instances == nil || s.provider == nil {
		return nil, ErrLifecycleNotConfigured
	}
	if err := validateLifecycleRequest(ctx, userID, idempotencyKey); err != nil {
		logLifecycleFailure(userID, 0, operation, err)
		return nil, err
	}

	lock := s.instances.userLock(userID)
	lock.Lock()
	defer lock.Unlock()

	target := StateReady
	if operation == operationStop {
		target = StateStopped
	}
	instance, persisted, execute, reused, _, err := s.reserve(
		userID, idempotencyKey, operation, target,
	)
	if err != nil {
		logLifecycleFailure(userID, 0, operation, err)
		return nil, err
	}
	if !execute {
		return &LifecycleResult{Instance: instance, Operation: persisted, Reused: reused}, nil
	}

	if _, err := action(instance); err != nil {
		logLifecycleFailure(userID, instance.ID, operation, err)
		return nil, s.fail(persisted.ID, instance.ID, err)
	}

	var readiness Readiness
	if check != nil {
		readiness, err = check(instance)
		if err != nil {
			logLifecycleFailure(userID, instance.ID, operation, err)
			return nil, s.fail(persisted.ID, instance.ID, err)
		}
		if !readiness.Ready() {
			logLifecycleFailure(userID, instance.ID, operation, ErrInstanceNotReady)
			return nil, s.fail(persisted.ID, instance.ID, ErrInstanceNotReady)
		}
	}

	if operation == operationStop {
		if err := s.finishStop(persisted.ID, instance.ID); err != nil {
			return nil, err
		}
	} else if err := s.finishReady(persisted.ID, instance.ID); err != nil {
		logLifecycleFailure(userID, instance.ID, operation, err)
		return nil, err
	}
	result := s.result(persisted.ID, instance.ID, false, readiness)
	logLifecycleResult(userID, result.Instance, result.Operation)
	return result, nil
}

func (s *LifecycleService) checkReady(ctx context.Context, instance *models.OpenClawInstance) (Readiness, error) {
	if s.readiness == nil {
		return Readiness{}, ErrReadinessUnavailable
	}
	return s.readiness.Check(ctx, ReadinessRequest{
		UserID: instance.UserID, InstanceID: instance.ID,
		ProviderInstanceID: instance.ProviderInstanceID,
	})
}

func (s *LifecycleService) reserve(
	userID int,
	key string,
	operation string,
	target InstanceState,
) (*models.OpenClawInstance, *models.OpenClawOperation, bool, bool, bool, error) {
	var instance models.OpenClawInstance
	var persisted models.OpenClawOperation
	var execute bool
	var reused bool
	var needsStop bool

	err := s.db.Transaction(func(tx *gorm.DB) error {
		err := tx.Where("user_id = ? AND idempotency_key = ?", userID, key).First(&persisted).Error
		if err == nil {
			if persisted.Operation != operation || persisted.TargetState != string(target) {
				return ErrInstanceConflict
			}
			if persisted.InstanceID == nil {
				return ErrOperationFailed
			}
			if err := tx.First(&instance, *persisted.InstanceID).Error; err != nil {
				return err
			}
			switch OperationStatus(persisted.Status) {
			case OperationCompleted:
				return nil
			case OperationRunning:
				if s.instances.operationIsStale(persisted) {
					if err := s.instances.recoverStaleOperation(tx, &instance, &persisted); err != nil {
						return err
					}
					return fmt.Errorf("%w: %s", ErrOperationFailed, staleOperationCode)
				}
				return ErrOperationInProgress
			case OperationFailed:
				return fmt.Errorf("%w: %s", ErrOperationFailed, persisted.ErrorCode)
			default:
				return ErrOperationFailed
			}
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("user_id = ?", userID).First(&instance).Error; err != nil {
			return err
		}
		var running models.OpenClawOperation
		if err := tx.Where("instance_id = ? AND status = ?", instance.ID, string(OperationRunning)).
			First(&running).Error; err == nil {
			if s.instances.operationIsStale(running) {
				if err := s.instances.recoverStaleOperation(tx, &instance, &running); err != nil {
					return err
				}
			} else {
				return ErrOperationInProgress
			}
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		noOp, transitionTo, err := lifecycleTransition(operation, InstanceState(instance.State))
		if err != nil {
			return err
		}
		if noOp {
			persisted = models.OpenClawOperation{
				UserID: userID, InstanceID: &instance.ID, Operation: operation,
				TargetState: string(target), IdempotencyKey: key,
				Status: string(OperationCompleted),
			}
		} else {
			previousState := InstanceState(instance.State)
			if err := Transition(&instance, transitionTo); err != nil {
				if previousState != transitionTo {
					return err
				}
			}
			needsStop = operation == operationReset &&
				previousState != StateStopped &&
				previousState != StateNotStarted
			persisted = models.OpenClawOperation{
				UserID: userID, InstanceID: &instance.ID, Operation: operation,
				TargetState: string(target), IdempotencyKey: key,
				Status: string(OperationRunning),
			}
			execute = true
		}
		if err := tx.Create(&persisted).Error; err != nil {
			return err
		}
		if execute {
			return tx.Save(&instance).Error
		}
		return nil
	})
	if err != nil {
		return nil, nil, false, false, false, err
	}
	if persisted.Status == string(OperationCompleted) {
		reused = true
	}
	return &instance, &persisted, execute, reused, needsStop, nil
}

func lifecycleTransition(operation string, state InstanceState) (bool, InstanceState, error) {
	switch operation {
	case operationStart:
		if state == StateReady {
			return true, state, nil
		}
		if state != StateStarting && state != StateStopped && state != StateFailed {
			return false, state, ErrInstanceConflict
		}
		return false, StateStarting, nil
	case operationStop:
		if state == StateNotStarted || state == StateStopped {
			return true, state, nil
		}
		if state != StateReady && state != StateFailed {
			return false, state, ErrInstanceConflict
		}
		return false, StateStopping, nil
	case operationRestart:
		if state != StateReady && state != StateStopped && state != StateFailed {
			return false, state, ErrInstanceConflict
		}
		if state == StateReady {
			return false, StateStopping, nil
		}
		return false, StateStarting, nil
	case operationReset:
		if state == StateNotStarted {
			return true, state, nil
		}
		if state != StateReady && state != StateStopped && state != StateFailed {
			return false, state, ErrInstanceConflict
		}
		return false, StateResetting, nil
	default:
		return false, state, ErrInstanceConflict
	}
}

func (s *LifecycleService) finishStop(operationID, instanceID uint) error {
	return s.finish(operationID, instanceID, func(instance *models.OpenClawInstance, operation *models.OpenClawOperation) error {
		if err := Transition(instance, StateStopped); err != nil {
			return err
		}
		operation.Status = string(OperationCompleted)
		instance.LastErrorCode = ""
		instance.LastErrorMessage = ""
		instance.CleanupErrorCode = ""
		instance.CleanupErrorMessage = ""
		return nil
	})
}

func (s *LifecycleService) finishReady(operationID, instanceID uint) error {
	return s.finish(operationID, instanceID, func(instance *models.OpenClawInstance, operation *models.OpenClawOperation) error {
		if InstanceState(instance.State) == StateStopping {
			if err := Transition(instance, StateStopped); err != nil {
				return err
			}
			if err := Transition(instance, StateStarting); err != nil {
				return err
			}
		}
		if err := MarkReady(instance, Readiness{
			ContainerRunning: true, GatewayHealthy: true, ChannelAuthenticated: true,
		}); err != nil {
			return err
		}
		operation.Status = string(OperationCompleted)
		instance.LastErrorCode = ""
		instance.LastErrorMessage = ""
		instance.CleanupErrorCode = ""
		instance.CleanupErrorMessage = ""
		return nil
	})
}

func (s *LifecycleService) finishReset(operationID, instanceID uint) error {
	return s.finish(operationID, instanceID, func(instance *models.OpenClawInstance, operation *models.OpenClawOperation) error {
		if err := Transition(instance, StateNotStarted); err != nil {
			return err
		}
		instance.ProviderInstanceID = ""
		instance.LastErrorCode = ""
		instance.LastErrorMessage = ""
		instance.CleanupErrorCode = ""
		instance.CleanupErrorMessage = ""
		operation.Status = string(OperationCompleted)
		return nil
	})
}

func (s *LifecycleService) fail(operationID, instanceID uint, cause error) error {
	if err := s.finish(operationID, instanceID, func(instance *models.OpenClawInstance, operation *models.OpenClawOperation) error {
		if err := Transition(instance, StateFailed); err != nil {
			return err
		}
		operation.Status = string(OperationFailed)
		operation.ErrorCode = providerErrorCode(cause)
		operation.ErrorMessage = safeErrorMessage(cause)
		operation.CleanupErrorCode, operation.CleanupErrorMessage = providerCleanupState(cause)
		instance.LastErrorCode = operation.ErrorCode
		instance.LastErrorMessage = operation.ErrorMessage
		instance.CleanupErrorCode = operation.CleanupErrorCode
		instance.CleanupErrorMessage = operation.CleanupErrorMessage
		return nil
	}); err != nil {
		return err
	}
	return cause
}

func (s *LifecycleService) finish(
	operationID, instanceID uint,
	update func(*models.OpenClawInstance, *models.OpenClawOperation) error,
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
		if OperationStatus(operation.Status) != OperationRunning {
			return ErrInstanceConflict
		}
		if err := update(&instance, &operation); err != nil {
			return err
		}
		if operation.Operation == operationReset {
			if err := tx.Unscoped().Where("instance_id = ?", instanceID).Delete(&models.ClawMessage{}).Error; err != nil {
				return err
			}
			if err := tx.Unscoped().Where("instance_id = ?", instanceID).Delete(&models.ClawSession{}).Error; err != nil {
				return err
			}
		}
		if err := tx.Save(&instance).Error; err != nil {
			return err
		}
		return tx.Save(&operation).Error
	})
}

func (s *LifecycleService) result(operationID, instanceID uint, reused bool, readiness Readiness) *LifecycleResult {
	var instance models.OpenClawInstance
	var operation models.OpenClawOperation
	if err := s.db.First(&instance, instanceID).Error; err != nil {
		return &LifecycleResult{Readiness: readiness, Reused: reused}
	}
	if err := s.db.First(&operation, operationID).Error; err != nil {
		return &LifecycleResult{Instance: &instance, Readiness: readiness, Reused: reused}
	}
	return &LifecycleResult{
		Instance: &instance, Operation: &operation,
		Readiness: readiness, Reused: reused,
	}
}

func validateLifecycleRequest(ctx context.Context, userID int, idempotencyKey string) error {
	if userID <= 0 || strings.TrimSpace(idempotencyKey) == "" {
		return ErrInstanceConflict
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (r Readiness) Ready() bool {
	return r.ContainerRunning && r.GatewayHealthy && r.ChannelAuthenticated
}

func logLifecycleResult(userID int, instance *models.OpenClawInstance, operation *models.OpenClawOperation) {
	if instance == nil || operation == nil {
		return
	}
	log.Info().
		Int("user_id", userID).
		Uint("instance_id", instance.ID).
		Uint("operation_id", operation.ID).
		Str("operation", operation.Operation).
		Str("state", instance.State).
		Str("status", operation.Status).
		Msg("OpenClaw lifecycle operation completed")
}

func logLifecycleFailure(userID int, instanceID uint, operation string, err error) {
	event := log.Warn().
		Int("user_id", userID).
		Uint("instance_id", instanceID).
		Str("operation", operation).
		Str("error_code", providerErrorCode(err))
	if errors.Is(err, ErrOperationInProgress) {
		event = event.Str("reason", "operation_in_progress")
	}
	event.Msg("OpenClaw lifecycle operation failed")
}
