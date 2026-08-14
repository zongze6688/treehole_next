package openclaw

import (
	"context"
	"time"
)

const (
	// defaultFleetReadinessWait is the total readiness-wait budget.
	defaultFleetReadinessWait = 60 * time.Second
	// defaultFleetReadinessPoll is the interval between readiness polls.
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
	// Wait is the total readiness-wait budget (default 60s).
	Wait time.Duration
	// Poll is the interval between readiness polls (default 1s).
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

// Checks adapts the three real signals to the ReadinessChecks contract. Each
// signal is a single-shot snapshot; the retrying checker in ReadinessChecker
// provides the polling.
func (f *FleetReadiness) Checks() ReadinessChecks {
	return ReadinessChecks{
		ContainerRunning:     f.containerRunning,
		GatewayHealthy:       f.gatewayHealthy,
		ChannelAuthenticated: f.channelAuthenticated,
	}
}

// ReadinessChecker returns a checker that polls the three real signals until
// the aggregate is ready or the wait budget is exhausted. Polling the whole
// aggregate (instead of only the channel) makes Onboard robust to transient
// `fleet status` failures and slow cold-cell boots.
func (f *FleetReadiness) ReadinessChecker() ReadinessChecker {
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
	return &fleetReadinessPoller{
		inner: NewReadinessAggregator(f.Checks()),
		wait:  wait,
		poll:  poll,
		sleep: sleep,
	}
}

// fleetReadinessPoller retries the aggregate readiness check until all three
// signals are ready, the wait budget is exhausted (not ready, no error), or ctx
// is canceled.
type fleetReadinessPoller struct {
	inner ReadinessChecker
	wait  time.Duration
	poll  time.Duration
	sleep func(context.Context, time.Duration) error
}

func (p *fleetReadinessPoller) Check(ctx context.Context, req ReadinessRequest) (Readiness, error) {
	if p == nil || p.inner == nil {
		return Readiness{}, ErrReadinessUnavailable
	}
	deadline := time.Now().Add(p.wait)
	var last Readiness
	var lastErr error
	for {
		if err := ctx.Err(); err != nil {
			return last, err
		}
		last, lastErr = p.inner.Check(ctx, req)
		if lastErr == nil && last.Ready() {
			return last, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			// Budget exhausted: report the last transient error if any,
			// otherwise simply "not ready yet".
			if lastErr != nil {
				return last, lastErr
			}
			return last, nil
		}
		d := p.poll
		if d > remaining {
			d = remaining
		}
		if err := p.sleep(ctx, d); err != nil {
			return last, err
		}
	}
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

// channelAuthenticated reports whether the user's channel is authenticated.
func (f *FleetReadiness) channelAuthenticated(ctx context.Context, userID int, _ string) (bool, error) {
	if f == nil || f.IsAuthed == nil {
		return false, ErrReadinessUnavailable
	}
	return f.IsAuthed(userID), nil
}
