package openclaw

import (
	"context"
	"errors"
	"sync"
	"testing"

	"treehole_next/models"

	"github.com/stretchr/testify/require"
)

func TestReadinessAggregatorRequiresAllSignals(t *testing.T) {
	aggregator := NewReadinessAggregator(ReadinessChecks{
		ContainerRunning: func(context.Context, string) (bool, error) { return true, nil },
		GatewayHealthy:   func(context.Context, string) (bool, error) { return true, nil },
		ChannelAuthenticated: func(context.Context, int, string) (bool, error) {
			return false, nil
		},
	})

	readiness, err := aggregator.Check(context.Background(), ReadinessRequest{
		UserID: 1, InstanceID: 2, ProviderInstanceID: "provider-1",
	})
	require.NoError(t, err)
	require.False(t, readiness.Ready())
	require.True(t, readiness.ContainerRunning)
	require.True(t, readiness.GatewayHealthy)
	require.False(t, readiness.ChannelAuthenticated)
}

func TestLifecycleServiceRunsFullLifecycleAndPreservesProviderIDUntilReset(t *testing.T) {
	db := testDB(t)
	provider := &lifecycleProvider{}
	readiness := NewReadinessAggregator(ReadinessChecks{
		ContainerRunning: func(context.Context, string) (bool, error) { return true, nil },
		GatewayHealthy:   func(context.Context, string) (bool, error) { return true, nil },
		ChannelAuthenticated: func(context.Context, int, string) (bool, error) {
			return true, nil
		},
	})
	service := NewLifecycleService(db, provider, readiness)

	created, err := service.Create(context.Background(), 1, "create-1", OnboardRequest{Provider: "fleet"})
	require.NoError(t, err)
	require.Equal(t, StateStarting, InstanceState(created.Instance.State))
	require.Equal(t, "provider-1", created.Instance.ProviderInstanceID)

	started, err := service.Start(context.Background(), 1, "start-1")
	require.NoError(t, err)
	require.Equal(t, StateReady, InstanceState(started.Instance.State))

	stopped, err := service.Stop(context.Background(), 1, "stop-1")
	require.NoError(t, err)
	require.Equal(t, StateStopped, InstanceState(stopped.Instance.State))
	require.Equal(t, "provider-1", stopped.Instance.ProviderInstanceID)

	restarted, err := service.Restart(context.Background(), 1, "restart-1")
	require.NoError(t, err)
	require.Equal(t, StateReady, InstanceState(restarted.Instance.State))
	require.Equal(t, "provider-1", restarted.Instance.ProviderInstanceID)

	reset, err := service.Reset(context.Background(), 1, "reset-1")
	require.NoError(t, err)
	require.Equal(t, StateNotStarted, InstanceState(reset.Instance.State))
	require.Empty(t, reset.Instance.ProviderInstanceID)
	require.Equal(t, 1, provider.destroyCalls())
	require.Equal(t, 2, provider.stopCalls())
}

func TestLifecycleServiceOnboardCreatesStartsAndRequiresReadiness(t *testing.T) {
	db := testDB(t)
	provider := &lifecycleProvider{}
	service := NewLifecycleService(db, provider, readyForTest())

	result, err := service.Onboard(context.Background(), 1, "onboard-1", OnboardRequest{Provider: "fleet"})
	require.NoError(t, err)
	require.Equal(t, StateReady, InstanceState(result.Instance.State))
	require.Equal(t, 1, provider.creates)
	require.Equal(t, 1, provider.startCalls())
	require.False(t, result.Reused)

	var operations []models.OpenClawOperation
	require.NoError(t, db.Where("user_id = ?", 1).Order("id ASC").Find(&operations).Error)
	require.Len(t, operations, 2)
	require.Equal(t, operationOnboard, operations[0].Operation)
	require.Equal(t, "onboard-1", operations[0].IdempotencyKey)
	require.Equal(t, operationStart, operations[1].Operation)
	require.Equal(t, onboardStartIdempotencyKey(1, "onboard-1"), operations[1].IdempotencyKey)
	require.NotEqual(t, operations[0].IdempotencyKey, operations[1].IdempotencyKey)
	require.Equal(t, OperationCompleted, OperationStatus(operations[0].Status))
	require.Equal(t, OperationCompleted, OperationStatus(operations[1].Status))
}

func TestLifecycleServiceOnboardDuplicateReusesStableStartOperation(t *testing.T) {
	db := testDB(t)
	provider := &lifecycleProvider{}
	service := NewLifecycleService(db, provider, readyForTest())
	request := OnboardRequest{Provider: "fleet", Name: "agent"}

	first, err := service.Onboard(context.Background(), 1, "onboard-same", request)
	require.NoError(t, err)
	second, err := service.Onboard(context.Background(), 1, "onboard-same", request)
	require.NoError(t, err)

	require.False(t, first.Reused)
	require.True(t, second.Reused)
	require.Equal(t, first.Instance.ID, second.Instance.ID)
	require.Equal(t, first.Operation.ID, second.Operation.ID)
	require.Equal(t, 1, provider.creates)
	require.Equal(t, 1, provider.startCalls())

	var operations []models.OpenClawOperation
	require.NoError(t, db.Where("user_id = ?", 1).Find(&operations).Error)
	require.Len(t, operations, 2)
}

func TestLifecycleServiceOnboardDoesNotReportReadyWhenReadinessFails(t *testing.T) {
	db := testDB(t)
	provider := &lifecycleProvider{}
	service := NewLifecycleService(db, provider, readinessForSignals(Readiness{
		ContainerRunning: true,
		GatewayHealthy:   true,
	}))

	result, err := service.Onboard(context.Background(), 1, "onboard-not-ready", OnboardRequest{Provider: "fleet"})
	require.Nil(t, result)
	require.ErrorIs(t, err, ErrInstanceNotReady)

	var instance models.OpenClawInstance
	require.NoError(t, db.Where("user_id = ?", 1).First(&instance).Error)
	require.Equal(t, StateFailed, InstanceState(instance.State))
	require.Equal(t, 1, provider.startCalls())
}

func TestLifecycleServiceConcurrentStartIsIdempotent(t *testing.T) {
	db := testDB(t)
	provider := &lifecycleProvider{}
	service := NewLifecycleService(db, provider, readyForTest())
	_, err := service.Create(context.Background(), 1, "create-1", OnboardRequest{Provider: "fleet"})
	require.NoError(t, err)

	results := make([]*LifecycleResult, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = service.Start(context.Background(), 1, "same-start")
		}(i)
	}
	wg.Wait()

	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	require.Equal(t, StateReady, InstanceState(results[0].Instance.State))
	require.Equal(t, results[0].Instance.ID, results[1].Instance.ID)
	require.Equal(t, 1, provider.startCalls())
	require.True(t, results[0].Reused || results[1].Reused)
}

func TestLifecycleServiceOnboardInjectsWorkloadIdentityEnv(t *testing.T) {
	db := testDB(t)
	provider := &metadataProvider{instance: ProviderInstance{ID: "provider-1"}}
	identity := &fakeIdentity{env: map[string]string{
		"DANTA_ACCESS_TOKEN": "ocw-test-token",
		"DANTA_WS_URL":       "wss://ws.example.test",
	}}
	service := NewLifecycleService(db, provider, readyForTest())
	service.SetWorkloadIdentity(identity)

	result, err := service.Onboard(context.Background(), 1, "onboard-wi", OnboardRequest{
		Provider: "fleet",
		Metadata: map[string]string{
			"APP_META":           "app-value",
			"DANTA_ACCESS_TOKEN": "stale-token",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, StateReady, InstanceState(result.Instance.State))
	require.Equal(t, 1, identity.envCallCount())

	// The identity env is merged into the provider metadata with the
	// server-provisioned value taking precedence; app metadata is preserved.
	metadata := provider.capturedMetadata()
	require.Equal(t, "app-value", metadata["APP_META"])
	require.Equal(t, "ocw-test-token", metadata["DANTA_ACCESS_TOKEN"])
	require.Equal(t, "wss://ws.example.test", metadata["DANTA_WS_URL"])
}

func TestLifecycleServiceOnboardFailsWhenIdentityEnvFails(t *testing.T) {
	db := testDB(t)
	provider := &metadataProvider{instance: ProviderInstance{ID: "provider-1"}}
	identity := &fakeIdentity{envErr: errors.New("identity provisioning failed")}
	service := NewLifecycleService(db, provider, readyForTest())
	service.SetWorkloadIdentity(identity)

	result, err := service.Onboard(context.Background(), 1, "onboard-wi-fail", OnboardRequest{Provider: "fleet"})
	require.Nil(t, result)
	require.Error(t, err)
	require.Equal(t, 1, identity.envCallCount())
	require.Equal(t, 0, provider.createCount())
}

func TestLifecycleServiceResetRevokesIdentityBestEffort(t *testing.T) {
	db := testDB(t)
	provider := &lifecycleProvider{}
	identity := &fakeIdentity{revokeErr: errors.New("revoke unavailable")}
	service := NewLifecycleService(db, provider, readyForTest())
	service.SetWorkloadIdentity(identity)

	created, err := service.Create(context.Background(), 1, "create-wi", OnboardRequest{Provider: "fleet"})
	require.NoError(t, err)
	started, err := service.Start(context.Background(), 1, "start-wi")
	require.NoError(t, err)
	require.Equal(t, StateReady, InstanceState(started.Instance.State))
	require.Equal(t, created.Instance.ID, started.Instance.ID)

	// A revocation failure must not fail the reset.
	result, err := service.Reset(context.Background(), 1, "reset-wi")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, StateNotStarted, InstanceState(result.Instance.State))
	require.Equal(t, 1, identity.revokeCallCount())
	require.Equal(t, []int{1}, identity.revokedUsers)
}

func TestLifecycleServiceOnboardIgnoresCellEnvForIdempotency(t *testing.T) {
	db := testDB(t)
	provider := &metadataProvider{instance: ProviderInstance{ID: "provider-1"}}
	service := NewLifecycleService(db, provider, readyForTest())

	first, err := service.Onboard(context.Background(), 1, "onboard-wi-same", OnboardRequest{
		Provider: "fleet", Name: "agent",
		CellEnv: map[string]string{"DANTA_ACCESS_TOKEN": "first-token"},
	})
	require.NoError(t, err)
	require.False(t, first.Reused)

	// A different server-provisioned token must not change the idempotency
	// hash: the second call reuses the first onboard instead of re-creating.
	second, err := service.Onboard(context.Background(), 1, "onboard-wi-same", OnboardRequest{
		Provider: "fleet", Name: "agent",
		CellEnv: map[string]string{"DANTA_ACCESS_TOKEN": "rotated-token"},
	})
	require.NoError(t, err)
	require.True(t, second.Reused)
	require.Equal(t, first.Instance.ID, second.Instance.ID)
	require.Equal(t, 1, provider.createCount())
}

func readyForTest() ReadinessChecker {
	return NewReadinessAggregator(ReadinessChecks{
		ContainerRunning: func(context.Context, string) (bool, error) { return true, nil },
		GatewayHealthy:   func(context.Context, string) (bool, error) { return true, nil },
		ChannelAuthenticated: func(context.Context, int, string) (bool, error) {
			return true, nil
		},
	})
}

type lifecycleProvider struct {
	mu       sync.Mutex
	creates  int
	starts   int
	stops    int
	restarts int
	destroys int
}

func (p *lifecycleProvider) Create(context.Context, CreateRequest) (ProviderInstance, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.creates++
	return ProviderInstance{ID: "provider-1"}, nil
}

func (p *lifecycleProvider) Start(context.Context, string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.starts++
	return nil
}

func (p *lifecycleProvider) Stop(context.Context, string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stops++
	return nil
}

func (p *lifecycleProvider) Restart(context.Context, string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.restarts++
	return nil
}

func (p *lifecycleProvider) Destroy(context.Context, string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.destroys++
	return nil
}

func (p *lifecycleProvider) Inspect(context.Context, string) (ProviderInspection, error) {
	return ProviderInspection{}, nil
}

func (p *lifecycleProvider) Logs(context.Context, string) (ProviderLogs, error) {
	return ProviderLogs{}, nil
}

func (p *lifecycleProvider) startCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.starts
}

func (p *lifecycleProvider) stopCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stops
}

func (p *lifecycleProvider) destroyCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.destroys
}

var _ OpenClawInstanceProvider = (*lifecycleProvider)(nil)

type fakeIdentity struct {
	mu           sync.Mutex
	env          map[string]string
	envErr       error
	revokeErr    error
	envCalls     int
	revokeCalls  int
	revokedUsers []int
}

func (f *fakeIdentity) Env(context.Context, int) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.envCalls++
	return f.env, f.envErr
}

func (f *fakeIdentity) Revoke(_ context.Context, userID int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.revokeCalls++
	f.revokedUsers = append(f.revokedUsers, userID)
	return f.revokeErr
}

func (f *fakeIdentity) envCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.envCalls
}

func (f *fakeIdentity) revokeCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.revokeCalls
}

var _ WorkloadIdentity = (*fakeIdentity)(nil)

// metadataProvider records the Metadata seen by provider.Create so tests can
// assert the merged cell environment.
type metadataProvider struct {
	mu       sync.Mutex
	instance ProviderInstance
	creates  int
	metadata map[string]string
}

func (p *metadataProvider) Create(_ context.Context, req CreateRequest) (ProviderInstance, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.creates++
	p.metadata = req.Metadata
	return p.instance, nil
}

func (p *metadataProvider) Start(context.Context, string) error   { return nil }
func (p *metadataProvider) Stop(context.Context, string) error    { return nil }
func (p *metadataProvider) Restart(context.Context, string) error { return nil }
func (p *metadataProvider) Destroy(context.Context, string) error { return nil }

func (p *metadataProvider) Inspect(context.Context, string) (ProviderInspection, error) {
	return ProviderInspection{}, nil
}

func (p *metadataProvider) Logs(context.Context, string) (ProviderLogs, error) {
	return ProviderLogs{}, nil
}

func (p *metadataProvider) createCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.creates
}

func (p *metadataProvider) capturedMetadata() map[string]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.metadata
}

var _ OpenClawInstanceProvider = (*metadataProvider)(nil)
