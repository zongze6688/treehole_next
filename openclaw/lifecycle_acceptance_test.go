package openclaw

import (
	"context"
	"errors"
	"sync"
	"testing"

	"treehole_next/models"

	"github.com/stretchr/testify/require"
)

func TestLifecycleAcceptanceStopPreservesDataAndResetCleansIt(t *testing.T) {
	db := testDB(t)
	provider := &lifecycleProvider{}
	service := NewLifecycleService(db, provider, readyForTest())

	created, err := service.Create(context.Background(), 1, "create-acceptance", OnboardRequest{Provider: "fleet"})
	require.NoError(t, err)
	_, err = service.Start(context.Background(), 1, "start-acceptance")
	require.NoError(t, err)

	_, err = models.CreateSessionForInstance(db, 1, created.Instance.ID, "preserved", "oc-session-1")
	require.NoError(t, err)
	require.NoError(t, models.CreateMessage(db, &models.ClawMessage{
		UserID:     1,
		InstanceID: created.Instance.ID,
		Type:       "message",
		From:       "user",
		Content:    "keep this across stop",
		TaskID:     "task-1",
		SessionID:  "oc-session-1",
	}))

	_, err = service.Stop(context.Background(), 1, "stop-acceptance")
	require.NoError(t, err)

	var sessions int64
	require.NoError(t, db.Model(&models.ClawSession{}).
		Where("user_id = ? AND instance_id = ?", 1, created.Instance.ID).
		Count(&sessions).Error)
	require.Equal(t, int64(1), sessions)

	var messages int64
	require.NoError(t, db.Model(&models.ClawMessage{}).
		Where("user_id = ? AND instance_id = ?", 1, created.Instance.ID).
		Count(&messages).Error)
	require.Equal(t, int64(1), messages)

	_, err = service.Reset(context.Background(), 1, "reset-acceptance")
	require.NoError(t, err)

	require.NoError(t, db.Unscoped().Model(&models.ClawSession{}).
		Where("user_id = ? AND instance_id = ?", 1, created.Instance.ID).
		Count(&sessions).Error)
	require.Equal(t, int64(0), sessions)
	require.NoError(t, db.Unscoped().Model(&models.ClawMessage{}).
		Where("user_id = ? AND instance_id = ?", 1, created.Instance.ID).
		Count(&messages).Error)
	require.Equal(t, int64(0), messages)
}

func TestLifecycleAcceptanceFailedStartCanBeRetried(t *testing.T) {
	startErr := errors.New("gateway start failed")
	db := testDB(t)
	provider := &acceptanceProvider{
		instance:  ProviderInstance{ID: "provider-retry"},
		startErrs: []error{startErr},
	}
	service := NewLifecycleService(db, provider, readyForTest())

	created, err := service.Create(context.Background(), 1, "create-retry", OnboardRequest{Provider: "fleet"})
	require.NoError(t, err)

	_, err = service.Start(context.Background(), 1, "start-retry-1")
	require.ErrorIs(t, err, startErr)

	var failed models.OpenClawInstance
	require.NoError(t, db.First(&failed, created.Instance.ID).Error)
	require.Equal(t, StateFailed, InstanceState(failed.State))

	var failedOperation models.OpenClawOperation
	require.NoError(t, db.
		Where("user_id = ? AND idempotency_key = ?", 1, "start-retry-1").
		First(&failedOperation).Error)
	require.Equal(t, OperationFailed, OperationStatus(failedOperation.Status))

	retried, err := service.Start(context.Background(), 1, "start-retry-2")
	require.NoError(t, err)
	require.Equal(t, StateReady, InstanceState(retried.Instance.State))
	require.Equal(t, 2, provider.startCalls())
}

func TestLifecycleAcceptanceReadyRequiresAllThreeSignals(t *testing.T) {
	tests := []struct {
		name      string
		readiness Readiness
		wantErr   error
		wantState InstanceState
	}{
		{
			name:      "container stopped",
			readiness: Readiness{GatewayHealthy: true, ChannelAuthenticated: true},
			wantErr:   ErrInstanceNotReady,
			wantState: StateFailed,
		},
		{
			name:      "gateway unhealthy",
			readiness: Readiness{ContainerRunning: true, ChannelAuthenticated: true},
			wantErr:   ErrInstanceNotReady,
			wantState: StateFailed,
		},
		{
			name:      "channel unauthenticated",
			readiness: Readiness{ContainerRunning: true, GatewayHealthy: true},
			wantErr:   ErrInstanceNotReady,
			wantState: StateFailed,
		},
		{
			name:      "all checks pass",
			readiness: Readiness{ContainerRunning: true, GatewayHealthy: true, ChannelAuthenticated: true},
			wantState: StateReady,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := testDB(t)
			provider := &lifecycleProvider{}
			service := NewLifecycleService(db, provider, readinessForSignals(tt.readiness))
			_, err := service.Create(context.Background(), 1, "create-ready-"+tt.name, OnboardRequest{Provider: "fleet"})
			require.NoError(t, err)

			result, err := service.Start(context.Background(), 1, "start-ready-"+tt.name)
			if tt.wantErr != nil {
				require.Nil(t, result)
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantState, InstanceState(result.Instance.State))
			}

			var instance models.OpenClawInstance
			require.NoError(t, db.Where("user_id = ?", 1).First(&instance).Error)
			require.Equal(t, tt.wantState, InstanceState(instance.State))
		})
	}
}

func TestLifecycleAcceptanceDuplicateAndNoOpOperationsAreIdempotent(t *testing.T) {
	db := testDB(t)
	provider := &lifecycleProvider{}
	service := NewLifecycleService(db, provider, readyForTest())

	_, err := service.Create(context.Background(), 1, "create-idempotent", OnboardRequest{Provider: "fleet"})
	require.NoError(t, err)

	firstStart, err := service.Start(context.Background(), 1, "start-idempotent")
	require.NoError(t, err)
	duplicateStart, err := service.Start(context.Background(), 1, "start-idempotent")
	require.NoError(t, err)
	require.True(t, duplicateStart.Reused)
	require.Equal(t, firstStart.Operation.ID, duplicateStart.Operation.ID)
	require.Equal(t, 1, provider.startCalls())

	noOpStart, err := service.Start(context.Background(), 1, "start-no-op")
	require.NoError(t, err)
	require.True(t, noOpStart.Reused)
	require.Equal(t, 1, provider.startCalls())

	firstStop, err := service.Stop(context.Background(), 1, "stop-idempotent")
	require.NoError(t, err)
	duplicateStop, err := service.Stop(context.Background(), 1, "stop-idempotent")
	require.NoError(t, err)
	require.True(t, duplicateStop.Reused)
	require.Equal(t, firstStop.Operation.ID, duplicateStop.Operation.ID)
	require.Equal(t, 1, provider.stopCalls())

	noOpStop, err := service.Stop(context.Background(), 1, "stop-no-op")
	require.NoError(t, err)
	require.True(t, noOpStop.Reused)
	require.Equal(t, 1, provider.stopCalls())
}

func TestLifecycleAcceptanceUnknownOperationFailsClosedWithoutMutation(t *testing.T) {
	noOp, target, err := lifecycleTransition("unknown-operation", StateReady)
	require.ErrorIs(t, err, ErrInstanceConflict)
	require.False(t, noOp)
	require.Equal(t, StateReady, target)
}

func TestLifecycleAcceptanceCancellationBeforeStartDoesNotMutateState(t *testing.T) {
	db := testDB(t)
	provider := &acceptanceProvider{instance: ProviderInstance{ID: "provider-cancel-before"}}
	service := NewLifecycleService(db, provider, readyForTest())
	_, err := service.Create(context.Background(), 1, "create-cancel-before", OnboardRequest{Provider: "fleet"})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = service.Start(ctx, 1, "start-cancel-before")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 0, provider.startCalls())

	var instance models.OpenClawInstance
	require.NoError(t, db.Where("user_id = ?", 1).First(&instance).Error)
	require.Equal(t, StateStarting, InstanceState(instance.State))

	var operationCount int64
	require.NoError(t, db.Model(&models.OpenClawOperation{}).
		Where("user_id = ? AND idempotency_key = ?", 1, "start-cancel-before").
		Count(&operationCount).Error)
	require.Equal(t, int64(0), operationCount)
}

func TestLifecycleAcceptanceCancellationDuringCompletedStartDoesNotRewriteResult(t *testing.T) {
	db := testDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	provider := &acceptanceProvider{
		instance: ProviderInstance{ID: "provider-cancel-after-action"},
		startFn: func(context.Context, string) error {
			cancel()
			return nil
		},
	}
	service := NewLifecycleService(db, provider, readyForTest())
	_, err := service.Create(context.Background(), 1, "create-cancel-during", OnboardRequest{Provider: "fleet"})
	require.NoError(t, err)

	result, err := service.Start(ctx, 1, "start-cancel-during")
	require.NoError(t, err)
	require.Equal(t, StateReady, InstanceState(result.Instance.State))
	require.Equal(t, OperationCompleted, OperationStatus(result.Operation.Status))

	reused, err := service.Start(context.Background(), 1, "start-cancel-during")
	require.NoError(t, err)
	require.True(t, reused.Reused)
	require.Equal(t, OperationCompleted, OperationStatus(reused.Operation.Status))
	require.Equal(t, 1, provider.startCalls())
}

func readinessForSignals(want Readiness) ReadinessChecker {
	return NewReadinessAggregator(ReadinessChecks{
		ContainerRunning: func(context.Context, string) (bool, error) {
			return want.ContainerRunning, nil
		},
		GatewayHealthy: func(context.Context, string) (bool, error) {
			return want.GatewayHealthy, nil
		},
		ChannelAuthenticated: func(context.Context, int, string) (bool, error) {
			return want.ChannelAuthenticated, nil
		},
	})
}

type acceptanceProvider struct {
	mu           sync.Mutex
	instance     ProviderInstance
	startErrs    []error
	startFn      func(context.Context, string) error
	startCount   int
	stopCount    int
	restartCount int
	destroyCount int
}

func (p *acceptanceProvider) Create(context.Context, CreateRequest) (ProviderInstance, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.instance, nil
}

func (p *acceptanceProvider) Start(ctx context.Context, instanceID string) error {
	p.mu.Lock()
	p.startCount++
	var err error
	if len(p.startErrs) > 0 {
		err = p.startErrs[0]
		p.startErrs = p.startErrs[1:]
	}
	startFn := p.startFn
	p.mu.Unlock()
	if startFn != nil {
		return startFn(ctx, instanceID)
	}
	return err
}

func (p *acceptanceProvider) Stop(context.Context, string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopCount++
	return nil
}

func (p *acceptanceProvider) Restart(context.Context, string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.restartCount++
	return nil
}

func (p *acceptanceProvider) Destroy(context.Context, string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.destroyCount++
	return nil
}

func (p *acceptanceProvider) Inspect(context.Context, string) (ProviderInspection, error) {
	return ProviderInspection{}, nil
}

func (p *acceptanceProvider) Logs(context.Context, string) (ProviderLogs, error) {
	return ProviderLogs{}, nil
}

func (p *acceptanceProvider) startCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.startCount
}

func (p *acceptanceProvider) stopCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stopCount
}

var _ OpenClawInstanceProvider = (*acceptanceProvider)(nil)
