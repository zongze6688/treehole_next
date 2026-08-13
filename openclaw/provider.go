package openclaw

import (
	"context"
	"errors"
	"fmt"
)

type ProviderStatus string

const (
	ProviderStatusUnknown      ProviderStatus = "unknown"
	ProviderStatusProvisioning ProviderStatus = "provisioning"
	ProviderStatusRunning      ProviderStatus = "running"
	ProviderStatusReady        ProviderStatus = "ready"
	ProviderStatusStopped      ProviderStatus = "stopped"
)

type CreateRequest struct {
	UserID         int
	Name           string
	Image          string
	Metadata       map[string]string
	IdempotencyKey string
}

type ProviderInstance struct {
	ID     string
	Status ProviderStatus
}

type ProviderInspection struct {
	ID     string
	Status ProviderStatus
}

type ProviderLogs struct {
	Content string
}

// OpenClawInstanceProvider is the provider-neutral runtime boundary.
type OpenClawInstanceProvider interface {
	Create(context.Context, CreateRequest) (ProviderInstance, error)
	Start(context.Context, string) error
	Stop(context.Context, string) error
	Restart(context.Context, string) error
	Destroy(context.Context, string) error
	Inspect(context.Context, string) (ProviderInspection, error)
	Logs(context.Context, string) (ProviderLogs, error)
}

type FleetCreateRequest struct {
	UserID         int
	Name           string
	Image          string
	Metadata       map[string]string
	IdempotencyKey string
}

type FleetInstance struct {
	ID     string
	Status ProviderStatus
}

type FleetLogs struct {
	Content string
}

// FleetTransport isolates the adapter from an as-yet unspecified Fleet wire contract.
type FleetTransport interface {
	Create(context.Context, FleetCreateRequest) (FleetInstance, error)
	Start(context.Context, string) error
	Stop(context.Context, string) error
	Restart(context.Context, string) error
	Destroy(context.Context, string) error
	Inspect(context.Context, string) (FleetInstance, error)
	Logs(context.Context, string) (FleetLogs, error)
}

type FleetErrorCode string

const (
	FleetErrorTimeout        FleetErrorCode = "timeout"
	FleetErrorUnavailable    FleetErrorCode = "unavailable"
	FleetErrorConflict       FleetErrorCode = "conflict"
	FleetErrorNotFound       FleetErrorCode = "not_found"
	FleetErrorUnauthorized   FleetErrorCode = "unauthorized"
	FleetErrorInvalidRequest FleetErrorCode = "invalid_request"
	FleetErrorUnknown        FleetErrorCode = "unknown"
)

// FleetError carries only normalized transport metadata, never response bodies or credentials.
type FleetError struct {
	Code       FleetErrorCode
	StatusCode int
	Retryable  bool
	Err        error
}

func (e *FleetError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("fleet request failed: %s", e.Code)
}

func (e *FleetError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func IsRetryableFleetError(err error) bool {
	var fleetErr *FleetError
	if !errors.As(err, &fleetErr) {
		return errors.Is(err, context.DeadlineExceeded)
	}
	return fleetErr.Retryable ||
		fleetErr.Code == FleetErrorTimeout ||
		fleetErr.Code == FleetErrorUnavailable ||
		fleetErr.StatusCode == 429 ||
		fleetErr.StatusCode >= 500
}

type ProviderErrorCode string

const (
	ProviderErrorTimeout      ProviderErrorCode = "timeout"
	ProviderErrorCanceled     ProviderErrorCode = "canceled"
	ProviderErrorUnavailable  ProviderErrorCode = "unavailable"
	ProviderErrorConflict     ProviderErrorCode = "conflict"
	ProviderErrorNotFound     ProviderErrorCode = "not_found"
	ProviderErrorUnauthorized ProviderErrorCode = "unauthorized"
	ProviderErrorInvalid      ProviderErrorCode = "invalid_request"
	ProviderErrorUnknown      ProviderErrorCode = "unknown"
)

var (
	ErrProviderTimeout      = errors.New("provider timeout")
	ErrProviderCanceled     = errors.New("provider operation canceled")
	ErrProviderUnavailable  = errors.New("provider unavailable")
	ErrProviderConflict     = errors.New("provider conflict")
	ErrProviderNotFound     = errors.New("provider instance not found")
	ErrProviderUnauthorized = errors.New("provider unauthorized")
	ErrProviderInvalid      = errors.New("provider invalid request")
	ErrProviderUnknown      = errors.New("provider error")
)

type ProviderError struct {
	Operation  string
	Code       ProviderErrorCode
	Kind       error
	CleanupErr error
}

func (e *ProviderError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("provider %s failed: %s", e.Operation, e.Code)
}

func (e *ProviderError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Kind
}
