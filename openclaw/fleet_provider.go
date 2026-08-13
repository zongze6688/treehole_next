package openclaw

import (
	"context"
	"errors"
	"strings"
	"time"
)

const (
	defaultProviderTimeout  = 10 * time.Second
	defaultProviderAttempts = 3
	defaultProviderBackoff  = 100 * time.Millisecond
)

type FleetProviderOptions struct {
	Timeout     time.Duration
	MaxAttempts int
	Backoff     time.Duration
	Sleep       func(context.Context, time.Duration) error
}

type FleetInstanceProvider struct {
	transport   FleetTransport
	timeout     time.Duration
	maxAttempts int
	backoff     time.Duration
	sleep       func(context.Context, time.Duration) error
}

func NewFleetInstanceProvider(transport FleetTransport, options FleetProviderOptions) *FleetInstanceProvider {
	timeout := options.Timeout
	if timeout <= 0 {
		timeout = defaultProviderTimeout
	}
	attempts := options.MaxAttempts
	if attempts <= 0 {
		attempts = defaultProviderAttempts
	}
	backoff := options.Backoff
	if backoff <= 0 {
		backoff = defaultProviderBackoff
	}
	sleep := options.Sleep
	if sleep == nil {
		sleep = sleepContext
	}
	return &FleetInstanceProvider{
		transport: transport, timeout: timeout, maxAttempts: attempts, backoff: backoff, sleep: sleep,
	}
}

func (p *FleetInstanceProvider) Create(ctx context.Context, req CreateRequest) (ProviderInstance, error) {
	if p == nil || p.transport == nil {
		return ProviderInstance{}, &ProviderError{
			Operation: "create",
			Code:      ProviderErrorUnavailable,
			Kind:      ErrProviderUnavailable,
		}
	}

	var lastErr error
	attempts := p.maxAttempts
	if strings.TrimSpace(req.IdempotencyKey) == "" {
		// A create without a provider idempotency key must not be retried:
		// a lost response could otherwise create duplicate instances.
		attempts = 1
	}
	for attempt := 1; attempt <= attempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, p.timeout)
		instance, err := p.transport.Create(attemptCtx, FleetCreateRequest{
			UserID: req.UserID, Name: req.Name, Image: req.Image, Metadata: req.Metadata,
			IdempotencyKey: req.IdempotencyKey,
		})
		cancel()
		if err == nil {
			if strings.TrimSpace(instance.ID) == "" {
				return ProviderInstance{}, &ProviderError{
					Operation: "create",
					Code:      ProviderErrorInvalid,
					Kind:      ErrProviderInvalid,
				}
			}
			return ProviderInstance{ID: instance.ID, Status: instance.Status}, nil
		}

		var cleanupErr error
		if instance.ID != "" {
			cleanupErr = p.compensateDestroy(ctx, instance.ID)
		}
		lastErr = err
		if cleanupErr != nil {
			lastErr = &ProviderError{
				Operation:  "create",
				Code:       normalizeFleetCode(err),
				Kind:       normalizeFleetKind(err),
				CleanupErr: cleanupErr,
			}
			break
		}
		if !p.shouldRetryWithLimit(ctx, err, attempt, attempts) {
			break
		}
		if err := p.sleep(ctx, time.Duration(attempt)*p.backoff); err != nil {
			lastErr = err
			break
		}
	}
	return ProviderInstance{}, normalizeFleetError("create", lastErr)
}

func (p *FleetInstanceProvider) Start(ctx context.Context, instanceID string) error {
	if err := validateProviderInstanceID("start", instanceID); err != nil {
		return err
	}
	return p.run(ctx, "start", func(callCtx context.Context) error {
		return p.transport.Start(callCtx, instanceID)
	})
}

func (p *FleetInstanceProvider) Stop(ctx context.Context, instanceID string) error {
	if err := validateProviderInstanceID("stop", instanceID); err != nil {
		return err
	}
	return p.run(ctx, "stop", func(callCtx context.Context) error {
		return p.transport.Stop(callCtx, instanceID)
	})
}

func (p *FleetInstanceProvider) Restart(ctx context.Context, instanceID string) error {
	if err := validateProviderInstanceID("restart", instanceID); err != nil {
		return err
	}
	return p.run(ctx, "restart", func(callCtx context.Context) error {
		return p.transport.Restart(callCtx, instanceID)
	})
}

func (p *FleetInstanceProvider) Destroy(ctx context.Context, instanceID string) error {
	if err := validateProviderInstanceID("destroy", instanceID); err != nil {
		return err
	}
	return p.run(ctx, "destroy", func(callCtx context.Context) error {
		return p.transport.Destroy(callCtx, instanceID)
	})
}

func (p *FleetInstanceProvider) Inspect(ctx context.Context, instanceID string) (ProviderInspection, error) {
	if err := validateProviderInstanceID("inspect", instanceID); err != nil {
		return ProviderInspection{}, err
	}
	instance, err := runFleetValue(p, ctx, "inspect", func(callCtx context.Context) (FleetInstance, error) {
		return p.transport.Inspect(callCtx, instanceID)
	})
	if err != nil {
		return ProviderInspection{}, err
	}
	return ProviderInspection{ID: instance.ID, Status: instance.Status}, nil
}

func (p *FleetInstanceProvider) Logs(ctx context.Context, instanceID string) (ProviderLogs, error) {
	if err := validateProviderInstanceID("logs", instanceID); err != nil {
		return ProviderLogs{}, err
	}
	logs, err := runFleetValue(p, ctx, "logs", func(callCtx context.Context) (FleetLogs, error) {
		return p.transport.Logs(callCtx, instanceID)
	})
	if err != nil {
		return ProviderLogs{}, err
	}
	return ProviderLogs{Content: logs.Content}, nil
}

func (p *FleetInstanceProvider) run(ctx context.Context, operation string, call func(context.Context) error) error {
	_, err := runFleetValue(p, ctx, operation, func(callCtx context.Context) (struct{}, error) {
		return struct{}{}, call(callCtx)
	})
	return err
}

func runFleetValue[T any](p *FleetInstanceProvider, ctx context.Context, operation string, call func(context.Context) (T, error)) (T, error) {
	var zero T
	if p == nil || p.transport == nil {
		return zero, &ProviderError{Operation: operation, Code: ProviderErrorUnavailable, Kind: ErrProviderUnavailable}
	}

	var lastErr error
	for attempt := 1; attempt <= p.maxAttempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, p.timeout)
		value, err := call(attemptCtx)
		cancel()
		if err == nil {
			return value, nil
		}
		lastErr = err
		if !p.shouldRetry(ctx, err, attempt) {
			break
		}
		if err := p.sleep(ctx, time.Duration(attempt)*p.backoff); err != nil {
			lastErr = err
			break
		}
	}
	return zero, normalizeFleetError(operation, lastErr)
}

func (p *FleetInstanceProvider) shouldRetry(parent context.Context, err error, attempt int) bool {
	return p.shouldRetryWithLimit(parent, err, attempt, p.maxAttempts)
}

func (p *FleetInstanceProvider) shouldRetryWithLimit(parent context.Context, err error, attempt, maxAttempts int) bool {
	return attempt < maxAttempts && parent.Err() == nil && IsRetryableFleetError(err)
}

func (p *FleetInstanceProvider) compensateDestroy(ctx context.Context, instanceID string) error {
	cleanupCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	return p.Destroy(cleanupCtx, instanceID)
}

func validateProviderInstanceID(operation, instanceID string) error {
	if strings.TrimSpace(instanceID) == "" {
		return &ProviderError{Operation: operation, Code: ProviderErrorInvalid, Kind: ErrProviderInvalid}
	}
	return nil
}

func normalizeFleetCode(err error) ProviderErrorCode {
	var fleetErr *FleetError
	if errors.As(err, &fleetErr) {
		switch fleetErr.Code {
		case FleetErrorTimeout:
			return ProviderErrorTimeout
		case FleetErrorUnavailable:
			return ProviderErrorUnavailable
		case FleetErrorConflict:
			return ProviderErrorConflict
		case FleetErrorNotFound:
			return ProviderErrorNotFound
		case FleetErrorUnauthorized:
			return ProviderErrorUnauthorized
		case FleetErrorInvalidRequest:
			return ProviderErrorInvalid
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ProviderErrorTimeout
	}
	if errors.Is(err, context.Canceled) {
		return ProviderErrorCanceled
	}
	return ProviderErrorUnknown
}

func normalizeFleetKind(err error) error {
	switch normalizeFleetCode(err) {
	case ProviderErrorTimeout:
		return ErrProviderTimeout
	case ProviderErrorCanceled:
		return ErrProviderCanceled
	case ProviderErrorUnavailable:
		return ErrProviderUnavailable
	case ProviderErrorConflict:
		return ErrProviderConflict
	case ProviderErrorNotFound:
		return ErrProviderNotFound
	case ProviderErrorUnauthorized:
		return ErrProviderUnauthorized
	case ProviderErrorInvalid:
		return ErrProviderInvalid
	default:
		return ErrProviderUnknown
	}
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

var _ OpenClawInstanceProvider = (*FleetInstanceProvider)(nil)
