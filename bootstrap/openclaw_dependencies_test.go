package bootstrap

import (
	"context"
	"testing"

	"treehole_next/apis/claw"
	"treehole_next/openclaw"

	"github.com/stretchr/testify/require"
)

func TestConfigureOpenClawLifecycleRequiresCompleteDependencies(t *testing.T) {
	previous := claw.GetLifecycleService()
	t.Cleanup(func() { claw.SetLifecycleService(previous) })

	require.Nil(t, configureOpenClawLifecycle(Dependencies{}))
	require.Nil(t, claw.GetLifecycleService())

	require.Nil(t, configureOpenClawLifecycle(Dependencies{
		OpenClawProvider: &bootstrapProvider{},
	}))
	require.Nil(t, claw.GetLifecycleService())

	provider := &bootstrapProvider{}
	readiness := &bootstrapReadiness{}
	service := configureOpenClawLifecycle(Dependencies{
		OpenClawProvider:  provider,
		OpenClawReadiness: readiness,
	})
	require.NotNil(t, service)
	require.Same(t, service, claw.GetLifecycleService())
}

func TestConfigureOpenClawLifecycleUsesPrebuiltService(t *testing.T) {
	previous := claw.GetLifecycleService()
	t.Cleanup(func() { claw.SetLifecycleService(previous) })

	service := openclaw.NewLifecycleService(nil, &bootstrapProvider{}, &bootstrapReadiness{})
	require.Same(t, service, configureOpenClawLifecycle(Dependencies{
		OpenClawLifecycle: service,
	}))
	require.Same(t, service, claw.GetLifecycleService())
}

type bootstrapProvider struct{}

func (p *bootstrapProvider) Create(context.Context, openclaw.CreateRequest) (openclaw.ProviderInstance, error) {
	return openclaw.ProviderInstance{ID: "test-provider"}, nil
}

func (p *bootstrapProvider) Start(context.Context, string) error   { return nil }
func (p *bootstrapProvider) Stop(context.Context, string) error    { return nil }
func (p *bootstrapProvider) Restart(context.Context, string) error { return nil }
func (p *bootstrapProvider) Destroy(context.Context, string) error { return nil }

func (p *bootstrapProvider) Inspect(context.Context, string) (openclaw.ProviderInspection, error) {
	return openclaw.ProviderInspection{}, nil
}

func (p *bootstrapProvider) Logs(context.Context, string) (openclaw.ProviderLogs, error) {
	return openclaw.ProviderLogs{}, nil
}

type bootstrapReadiness struct{}

func (r *bootstrapReadiness) Check(context.Context, openclaw.ReadinessRequest) (openclaw.Readiness, error) {
	return openclaw.Readiness{
		ContainerRunning:     true,
		GatewayHealthy:       true,
		ChannelAuthenticated: true,
	}, nil
}

var _ openclaw.OpenClawInstanceProvider = (*bootstrapProvider)(nil)
var _ openclaw.ReadinessChecker = (*bootstrapReadiness)(nil)
