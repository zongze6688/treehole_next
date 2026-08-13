package openclaw

import (
	"context"
	"errors"
)

func normalizeFleetError(operation string, err error) error {
	if err == nil {
		return nil
	}

	code := ProviderErrorUnknown
	kind := ErrProviderUnknown
	var fleetErr *FleetError

	switch {
	case errors.Is(err, context.Canceled):
		code, kind = ProviderErrorCanceled, ErrProviderCanceled
	case errors.Is(err, context.DeadlineExceeded):
		code, kind = ProviderErrorTimeout, ErrProviderTimeout
	case errors.As(err, &fleetErr):
		switch fleetErr.Code {
		case FleetErrorTimeout:
			code, kind = ProviderErrorTimeout, ErrProviderTimeout
		case FleetErrorUnavailable:
			code, kind = ProviderErrorUnavailable, ErrProviderUnavailable
		case FleetErrorConflict:
			code, kind = ProviderErrorConflict, ErrProviderConflict
		case FleetErrorNotFound:
			code, kind = ProviderErrorNotFound, ErrProviderNotFound
		case FleetErrorUnauthorized:
			code, kind = ProviderErrorUnauthorized, ErrProviderUnauthorized
		case FleetErrorInvalidRequest:
			code, kind = ProviderErrorInvalid, ErrProviderInvalid
		}
	}

	return &ProviderError{Operation: operation, Code: code, Kind: kind}
}
