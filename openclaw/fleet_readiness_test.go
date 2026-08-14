package openclaw

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFleetReadinessStatusChecksReflectSnapshot(t *testing.T) {
	f := NewFleetReadiness(func(ctx context.Context, tenant string) (FleetCellStatus, error) {
		return FleetCellStatus{Running: true, HealthOK: false}, nil
	}, func(int) bool { return false })

	running, err := f.containerRunning(context.Background(), "u1")
	require.NoError(t, err)
	require.True(t, running)

	healthy, err := f.gatewayHealthy(context.Background(), "u1")
	require.NoError(t, err)
	require.False(t, healthy)
}

func TestFleetReadinessStatusChecksReflectUnhealthySnapshot(t *testing.T) {
	f := NewFleetReadiness(func(ctx context.Context, tenant string) (FleetCellStatus, error) {
		return FleetCellStatus{Running: false, HealthOK: true}, nil
	}, func(int) bool { return false })

	running, err := f.containerRunning(context.Background(), "u1")
	require.NoError(t, err)
	require.False(t, running)

	healthy, err := f.gatewayHealthy(context.Background(), "u1")
	require.NoError(t, err)
	require.True(t, healthy)
}

func TestFleetReadinessStatusChecksPropagateStatusError(t *testing.T) {
	boom := errors.New("fleet status failed")
	f := NewFleetReadiness(func(ctx context.Context, tenant string) (FleetCellStatus, error) {
		return FleetCellStatus{}, boom
	}, func(int) bool { return false })

	_, err := f.containerRunning(context.Background(), "u1")
	require.ErrorIs(t, err, boom)

	_, err = f.gatewayHealthy(context.Background(), "u1")
	require.ErrorIs(t, err, boom)
}

func TestFleetReadinessChannelAuthenticatedImmediate(t *testing.T) {
	f := NewFleetReadiness(nil, func(userID int) bool { return userID == 7 })

	authed, err := f.channelAuthenticated(context.Background(), 7, "u7")
	require.NoError(t, err)
	require.True(t, authed)
}

func TestFleetReadinessChannelAuthenticatedPollsUntilTrue(t *testing.T) {
	var attempts atomic.Int64
	f := NewFleetReadiness(nil, func(int) bool { return attempts.Add(1) > 2 })
	f.Wait = time.Minute
	f.Poll = time.Millisecond
	f.Sleep = func(ctx context.Context, d time.Duration) error { return nil }

	authed, err := f.channelAuthenticated(context.Background(), 1, "u1")
	require.NoError(t, err)
	require.True(t, authed)
	require.Equal(t, int64(3), attempts.Load())
}

func TestFleetReadinessChannelAuthenticatedExhaustsBudget(t *testing.T) {
	var sleeps atomic.Int64
	f := NewFleetReadiness(nil, func(int) bool { return false })
	f.Wait = 10 * time.Millisecond
	f.Poll = time.Millisecond
	f.Sleep = func(ctx context.Context, d time.Duration) error {
		sleeps.Add(1)
		return nil
	}

	authed, err := f.channelAuthenticated(context.Background(), 1, "u1")
	require.NoError(t, err)
	require.False(t, authed)
	require.Greater(t, sleeps.Load(), int64(0))
}

func TestFleetReadinessChannelAuthenticatedCanceled(t *testing.T) {
	f := NewFleetReadiness(nil, func(int) bool { return false })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	authed, err := f.channelAuthenticated(ctx, 1, "u1")
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, authed)
}

func TestFleetReadinessChannelAuthenticatedSleepError(t *testing.T) {
	boom := errors.New("sleep failed")
	f := NewFleetReadiness(nil, func(int) bool { return false })
	f.Wait = time.Minute
	f.Poll = time.Millisecond
	f.Sleep = func(ctx context.Context, d time.Duration) error { return boom }

	authed, err := f.channelAuthenticated(context.Background(), 1, "u1")
	require.ErrorIs(t, err, boom)
	require.False(t, authed)
}

func TestFleetReadinessDefaultsToBudgetAndPoll(t *testing.T) {
	var polled time.Duration
	var calls int
	f := NewFleetReadiness(nil, nil)
	f.IsAuthed = func(int) bool {
		calls++
		return calls >= 2
	}
	f.Sleep = func(ctx context.Context, d time.Duration) error {
		polled = d
		return nil
	}

	authed, err := f.channelAuthenticated(context.Background(), 1, "u1")
	require.NoError(t, err)
	require.True(t, authed)
	require.Equal(t, time.Second, polled)
}

func TestFleetReadinessUnavailableWhenSignalsMissing(t *testing.T) {
	f := &FleetReadiness{}

	_, err := f.containerRunning(context.Background(), "u1")
	require.ErrorIs(t, err, ErrReadinessUnavailable)

	_, err = f.gatewayHealthy(context.Background(), "u1")
	require.ErrorIs(t, err, ErrReadinessUnavailable)

	_, err = f.channelAuthenticated(context.Background(), 1, "u1")
	require.ErrorIs(t, err, ErrReadinessUnavailable)
}

func TestFleetReadinessCheckerAggregatesRealSignals(t *testing.T) {
	f := NewFleetReadiness(func(ctx context.Context, tenant string) (FleetCellStatus, error) {
		return FleetCellStatus{Running: true, HealthOK: true}, nil
	}, func(int) bool { return true })

	readiness, err := f.ReadinessChecker().Check(context.Background(), ReadinessRequest{
		UserID: 1, InstanceID: 2, ProviderInstanceID: "u1",
	})
	require.NoError(t, err)
	require.True(t, readiness.Ready())
	require.True(t, readiness.ContainerRunning)
	require.True(t, readiness.GatewayHealthy)
	require.True(t, readiness.ChannelAuthenticated)
}
