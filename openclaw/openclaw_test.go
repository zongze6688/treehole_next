package openclaw

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"treehole_next/models"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestStateTransitions(t *testing.T) {
	for _, test := range []struct {
		from InstanceState
		to   InstanceState
		ok   bool
	}{
		{StateNotStarted, StateProvisioning, true},
		{StateProvisioning, StateStarting, true},
		{StateStarting, StateReady, true},
		{StateReady, StateStopping, true},
		{StateStopping, StateStopped, true},
		{StateStopped, StateStarting, true},
		{StateReady, StateResetting, true},
		{StateResetting, StateNotStarted, true},
		{StateReady, StateNotStarted, false},
		{StateNotStarted, StateReady, false},
	} {
		require.Equal(t, test.ok, CanTransition(test.from, test.to), "%s -> %s", test.from, test.to)
	}
}

func TestMarkReadyRequiresAllSignals(t *testing.T) {
	instance := &models.OpenClawInstance{State: string(StateStarting)}
	require.ErrorIs(t, MarkReady(instance, Readiness{ContainerRunning: true, GatewayHealthy: true}), ErrInvalidStateTransition)
	require.NoError(t, MarkReady(instance, Readiness{
		ContainerRunning: true, GatewayHealthy: true, ChannelAuthenticated: true,
	}))
	require.Equal(t, string(StateReady), instance.State)
}

func TestInstanceServiceOnboardIsIdempotent(t *testing.T) {
	db := testDB(t)
	provider := &fakeProvider{instance: ProviderInstance{ID: "fleet-1", Status: ProviderStatusProvisioning}}
	service := NewInstanceService(db, provider)
	request := OnboardRequest{Provider: "fleet", Name: "test", Image: "image"}

	first, err := service.Onboard(context.Background(), 1, "request-1", request)
	require.NoError(t, err)
	require.False(t, first.Reused)
	require.Equal(t, "fleet-1", first.Instance.ProviderInstanceID)
	require.Equal(t, string(StateStarting), first.Instance.State)

	second, err := service.Onboard(context.Background(), 1, "request-1", request)
	require.NoError(t, err)
	require.True(t, second.Reused)
	require.Equal(t, first.Instance.ID, second.Instance.ID)
	require.Equal(t, 1, provider.createCalls())

	var count int64
	require.NoError(t, db.Model(&models.OpenClawInstance{}).Where("user_id = ?", 1).Count(&count).Error)
	require.Equal(t, int64(1), count)
}

func TestInstanceServiceConcurrentOnboardCreatesOneInstance(t *testing.T) {
	db := testDB(t)
	provider := &fakeProvider{instance: ProviderInstance{ID: "fleet-1"}}
	service := NewInstanceService(db, provider)
	request := OnboardRequest{Provider: "fleet"}

	var wg sync.WaitGroup
	results := make([]*OnboardResult, 2)
	errs := make([]error, 2)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = service.Onboard(context.Background(), 1, "request-"+string(rune('a'+i)), request)
		}(i)
	}
	wg.Wait()

	var success int
	for _, err := range errs {
		if err == nil {
			success++
		}
	}
	// Concurrent onboard requests may use different idempotency keys. They
	// both succeed, but only the first request provisions the instance and
	// the second reuses the same record.
	require.Equal(t, 2, success)
	require.Equal(t, 1, provider.createCalls())

	var count int64
	require.NoError(t, db.Model(&models.OpenClawInstance{}).Where("user_id = ?", 1).Count(&count).Error)
	require.Equal(t, int64(1), count)
}

func TestFleetProviderRetriesAndNormalizesErrors(t *testing.T) {
	transport := &fakeTransport{
		createFn: func() (FleetInstance, error) {
			return FleetInstance{}, &FleetError{Code: FleetErrorUnavailable, Retryable: true}
		},
	}
	provider := NewFleetInstanceProvider(transport, FleetProviderOptions{
		Timeout: 50 * time.Millisecond, MaxAttempts: 3, Backoff: time.Nanosecond,
		Sleep: func(context.Context, time.Duration) error { return nil },
	})

	_, err := provider.Create(context.Background(), CreateRequest{UserID: 1})
	require.ErrorIs(t, err, ErrProviderUnavailable)
	require.Equal(t, 3, transport.createCalls)
}

func TestFleetProviderTimeout(t *testing.T) {
	transport := &fakeTransport{
		createCtxFn: func(ctx context.Context) (FleetInstance, error) {
			<-ctx.Done()
			return FleetInstance{}, ctx.Err()
		},
	}
	provider := NewFleetInstanceProvider(transport, FleetProviderOptions{
		Timeout: 5 * time.Millisecond, MaxAttempts: 1,
		Sleep: func(context.Context, time.Duration) error { return nil },
	})

	_, err := provider.Create(context.Background(), CreateRequest{UserID: 1})
	require.ErrorIs(t, err, ErrProviderTimeout)
}

func TestFleetProviderCompensatesAfterPartialCreate(t *testing.T) {
	transport := &fakeTransport{
		createFn: func() (FleetInstance, error) {
			return FleetInstance{ID: "partial"}, errors.New("create response failed")
		},
	}
	provider := NewFleetInstanceProvider(transport, FleetProviderOptions{
		Timeout: 50 * time.Millisecond, MaxAttempts: 1,
		Sleep: func(context.Context, time.Duration) error { return nil },
	})

	_, err := provider.Create(context.Background(), CreateRequest{UserID: 1})
	require.Error(t, err)
	require.Equal(t, []string{"partial"}, transport.destroyed)
}

func testDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.OpenClawInstance{}, &models.OpenClawOperation{}))
	return db
}

type fakeProvider struct {
	mu       sync.Mutex
	instance ProviderInstance
	creates  int
}

func (p *fakeProvider) Create(context.Context, CreateRequest) (ProviderInstance, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.creates++
	return p.instance, nil
}
func (p *fakeProvider) Start(context.Context, string) error   { return nil }
func (p *fakeProvider) Stop(context.Context, string) error    { return nil }
func (p *fakeProvider) Restart(context.Context, string) error { return nil }
func (p *fakeProvider) Destroy(context.Context, string) error { return nil }
func (p *fakeProvider) Inspect(context.Context, string) (ProviderInspection, error) {
	return ProviderInspection{}, nil
}
func (p *fakeProvider) Logs(context.Context, string) (ProviderLogs, error) {
	return ProviderLogs{}, nil
}
func (p *fakeProvider) createCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.creates
}

type fakeTransport struct {
	createFn    func() (FleetInstance, error)
	createCtxFn func(context.Context) (FleetInstance, error)
	createCalls int
	destroyed   []string
}

func (t *fakeTransport) Create(ctx context.Context, _ FleetCreateRequest) (FleetInstance, error) {
	t.createCalls++
	if t.createCtxFn != nil {
		return t.createCtxFn(ctx)
	}
	if t.createFn != nil {
		return t.createFn()
	}
	return FleetInstance{ID: "fleet"}, nil
}
func (t *fakeTransport) Start(context.Context, string) error   { return nil }
func (t *fakeTransport) Stop(context.Context, string) error    { return nil }
func (t *fakeTransport) Restart(context.Context, string) error { return nil }
func (t *fakeTransport) Inspect(context.Context, string) (FleetInstance, error) {
	return FleetInstance{}, nil
}
func (t *fakeTransport) Logs(context.Context, string) (FleetLogs, error) {
	return FleetLogs{}, nil
}
func (t *fakeTransport) Destroy(_ context.Context, id string) error {
	t.destroyed = append(t.destroyed, id)
	return nil
}
