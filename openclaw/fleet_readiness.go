package openclaw

import (
	"context"
	"time"
)

const (
	// defaultFleetReadinessWait is the total channel-wait budget.
	defaultFleetReadinessWait = 60 * time.Second
	// defaultFleetReadinessPoll is the interval between channel polls.
	defaultFleetReadinessPoll = time.Second
)

// FleetReadiness wires the three real readiness signals to their sources: the
// Fleet cell status snapshot (container running, gateway healthy) and the
// connection registry (channel authenticated). It is the production companion
// to the injectable ReadinessChecks used in tests.
type FleetReadiness struct {
	// Status returns the Fleet cell status snapshot for a tenant.
	Status func(ctx context.Context, tenant string) (FleetCellStatus, error)
	// IsAuthed reports whether the user has an authenticated channel.
	IsAuthed func(userID int) bool
	// Wait is the total channel-wait budget (default 60s).
	Wait time.Duration
	// Poll is the interval between channel polls (default 1s).
	Poll time.Duration
	// Sleep waits between polls; defaults to sleepContext.
	Sleep func(ctx context.Context, d time.Duration) error
}

// NewFleetReadiness builds the production readiness source wiring.
func NewFleetReadiness(
	status func(context.Context, string) (FleetCellStatus, error),
	isAuthed func(int) bool,
) *FleetReadiness {
	return &FleetReadiness{Status: status, IsAuthed: isAuthed}
}

// Checks adapts the three real signals to the ReadinessChecks contract.
func (f *FleetReadiness) Checks() ReadinessChecks {
	return ReadinessChecks{
		ContainerRunning:     f.containerRunning,
		GatewayHealthy:       f.gatewayHealthy,
		ChannelAuthenticated: f.channelAuthenticated,
	}
}

// ReadinessChecker returns an aggregator over the real readiness checks.
func (f *FleetReadiness) ReadinessChecker() ReadinessChecker {
	return NewReadinessAggregator(f.Checks())
}

// containerRunning reports whether the Fleet cell container is running.
func (f *FleetReadiness) containerRunning(ctx context.Context, tenant string) (bool, error) {
	if f == nil || f.Status == nil {
		return false, ErrReadinessUnavailable
	}
	st, err := f.Status(ctx, tenant)
	return st.Running, err
}

// gatewayHealthy reports whether the Fleet cell gateway is healthy.
func (f *FleetReadiness) gatewayHealthy(ctx context.Context, tenant string) (bool, error) {
	if f == nil || f.Status == nil {
		return false, ErrReadinessUnavailable
	}
	st, err := f.Status(ctx, tenant)
	return st.HealthOK, err
}

// channelAuthenticated polls the registry-backed IsAuthed signal until the
// user's channel is authenticated, the wait budget is exhausted, or ctx is
// canceled. Budget exhaustion is not an error: the channel is simply not ready
// yet. Errors surface only from ctx cancellation/deadline or Sleep.
func (f *FleetReadiness) channelAuthenticated(ctx context.Context, userID int, _ string) (bool, error) {
	if f == nil || f.IsAuthed == nil {
		return false, ErrReadinessUnavailable
	}
	wait := f.Wait
	if wait <= 0 {
		wait = defaultFleetReadinessWait
	}
	poll := f.Poll
	if poll <= 0 {
		poll = defaultFleetReadinessPoll
	}
	sleep := f.Sleep
	if sleep == nil {
		sleep = sleepContext
	}

	if f.IsAuthed(userID) {
		return true, nil
	}

	deadline := time.Now().Add(wait)
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false, nil
		}
		d := poll
		if d > remaining {
			d = remaining
		}
		if err := sleep(ctx, d); err != nil {
			return false, err
		}
		if f.IsAuthed(userID) {
			return true, nil
		}
	}
}
