package openclaw

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// newMockFleetCLI copies the mock openclaw fleet script from testdata into a
// temp dir and returns a REAL FleetCLITransport that execs it through bash, so
// the full exec.CommandContext path (not an injected Runner) is exercised.
// Every invocation is appended to the returned log file (MOCK_FLEET_LOG), so
// tests can assert the exact CLI argument lines the control plane produced.
// Skips when bash is not available on PATH, keeping the test portable.
func newMockFleetCLI(t *testing.T) (*FleetCLITransport, string) {
	t.Helper()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skipf("bash not available on PATH: %v", err)
	}
	script, err := os.ReadFile(filepath.Join("testdata", "mock_openclaw.sh"))
	require.NoError(t, err)
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "mock_openclaw.sh")
	require.NoError(t, os.WriteFile(scriptPath, script, 0o755))
	logPath := filepath.Join(dir, "fleet.log")
	t.Setenv("MOCK_FLEET_LOG", logPath)
	transport := NewFleetCLITransport(FleetCLIOptions{
		Binary: "bash",
		Prefix: []string{scriptPath},
		Image:  "ghcr.io/openclaw/openclaw:latest",
	})
	return transport, logPath
}

func readFleetLog(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(content)
}

// TestFleetCLIEndToEndLifecycleLoop drives the full Onboard -> Stop ->
// Restart -> Reset loop through the real exec runner against the mock script,
// asserting both the persisted instance states and the exact CLI argument
// lines recorded by the mock. Workload identity and channel auth are faked;
// everything else flows through the REAL FleetCLITransport.
func TestFleetCLIEndToEndLifecycleLoop(t *testing.T) {
	transport, logPath := newMockFleetCLI(t)

	provider := NewFleetInstanceProvider(transport, FleetProviderOptions{})
	// The channel is already authenticated, so readiness never polls.
	readiness := NewFleetReadiness(transport.CellStatus, func(int) bool { return true })
	identity := &fakeIdentity{env: map[string]string{
		"DANTA_ACCESS_TOKEN": "ocw_test_token",
		"DANTA_WS_URL":       "ws://example/api/claw/oc",
	}}
	service := NewLifecycleService(testDB(t), provider, readiness.ReadinessChecker())
	service.SetWorkloadIdentity(identity)

	ctx := context.Background()

	// 1. Onboard creates the cell and requires all readiness signals.
	onboarded, err := service.Onboard(ctx, 123, "key-1", OnboardRequest{
		Provider: "fleet",
		Name:     "u123",
		Metadata: map[string]string{"APP_ENV": "x"},
	})
	require.NoError(t, err)
	require.NotNil(t, onboarded.Instance)
	require.Equal(t, StateReady, InstanceState(onboarded.Instance.State))
	require.Equal(t, "u123", onboarded.Instance.ProviderInstanceID)

	// 2. The create invocation records the SORTED env keys: APP_ENV, then the
	// identity-injected DANTA_ACCESS_TOKEN and DANTA_WS_URL.
	logs := readFleetLog(t, logPath)
	require.Contains(t, logs,
		"fleet create u123 --json --no-start --image ghcr.io/openclaw/openclaw:latest --runtime docker "+
			"--env APP_ENV=x --env DANTA_ACCESS_TOKEN=ocw_test_token "+
			"--env DANTA_WS_URL=ws://example/api/claw/oc")

	// 3. Stop stops the cell.
	stopped, err := service.Stop(ctx, 123, "key-stop")
	require.NoError(t, err)
	require.Equal(t, StateStopped, InstanceState(stopped.Instance.State))
	require.Contains(t, readFleetLog(t, logPath), "stop u123")

	// 4. Restart brings the cell back to ready.
	restarted, err := service.Restart(ctx, 123, "key-restart")
	require.NoError(t, err)
	require.Equal(t, StateReady, InstanceState(restarted.Instance.State))
	require.Contains(t, readFleetLog(t, logPath), "restart u123")

	// 5. Reset destroys the cell (purging data) and revokes the identity.
	reset, err := service.Reset(ctx, 123, "key-reset")
	require.NoError(t, err)
	require.Equal(t, StateNotStarted, InstanceState(reset.Instance.State))
	require.Contains(t, readFleetLog(t, logPath), "rm u123 --purge-data --force")
	require.Equal(t, 1, identity.revokeCallCount())
}

// TestFleetCLIExecRunnerRunsMockScript proves the DEFAULT exec runner (no
// injected Runner) works end to end: transport.CellStatus spawns
// `bash mock_openclaw.sh status u9 --json` and parses the mock JSON into a
// running, healthy cell snapshot.
func TestFleetCLIExecRunnerRunsMockScript(t *testing.T) {
	transport, _ := newMockFleetCLI(t)

	status, err := transport.CellStatus(context.Background(), "u9")
	require.NoError(t, err)
	require.True(t, status.Running)
	require.True(t, status.HealthOK)
	require.Equal(t, 19100, status.Port)
	require.Equal(t, "running", status.State)
}
