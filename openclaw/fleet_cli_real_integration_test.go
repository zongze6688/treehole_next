package openclaw

import (
	"context"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fleetCLIBinary resolves a fleet-capable openclaw CLI: an explicit
// OPENCLAW_FLEET_BIN, or a locally-known wrapper around a fleet-capable build,
// or "openclaw" on PATH.
func fleetCLIBinary() string {
	if bin := os.Getenv("OPENCLAW_FLEET_BIN"); bin != "" {
		return bin
	}
	for _, candidate := range []string{"/tmp/ocfleet2.sh", "/tmp/ocfleet.sh"} {
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	return "openclaw"
}

// realFleetAvailable reports whether the resolved openclaw binary has the
// experimental "fleet" subcommand (requires OpenClaw >= ~2026.6.x / beta).
func realFleetAvailable(t *testing.T) bool {
	t.Helper()
	output, err := exec.Command(fleetCLIBinary(), "fleet", "--help").CombinedOutput()
	if err != nil {
		return false
	}
	return strings.Contains(strings.ToLower(string(output)), "create") &&
		strings.Contains(strings.ToLower(string(output)), "status")
}

// TestFleetCLIRealDockerLifecycle runs the REAL FleetCLITransport exec path
// against the installed "openclaw fleet" CLI and a real local Docker daemon.
// It is skipped when the environment lacks the fleet subcommand or Docker.
// The cell is destroyed (with purge) in cleanup.
func TestFleetCLIRealDockerLifecycle(t *testing.T) {
	if !realFleetAvailable(t) {
		t.Skip("openclaw fleet subcommand not available on this host")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available on this host")
	}

	transport := NewFleetCLITransport(FleetCLIOptions{Binary: fleetCLIBinary()})
	userID := int(70000000 + time.Now().UnixNano()%100000000)
	tenant, err := tenantForUser(userID)
	require.NoError(t, err)

	// Best-effort cleanup: remove any leftover cell for this tenant first.
	cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelCleanup()
	_ = transport.Destroy(cleanupCtx, tenant)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = transport.Destroy(ctx, tenant)
	})

	createCtx, cancelCreate := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancelCreate()

	instance, err := transport.Create(createCtx, FleetCreateRequest{
		UserID: userID,
		Image:  "ghcr.io/openclaw/openclaw:latest",
	})
	require.NoError(t, err)
	require.Equal(t, tenant, instance.ID)

	t.Logf("created real fleet cell %s (id=%s)", tenant, instance.ID)

	actionCtx, cancelAction := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancelAction()

	// Create is --no-start; Start the cell, then the container/gateway come up.
	require.NoError(t, transport.Start(actionCtx, tenant))
	require.Eventually(t, func() bool {
		status, err := transport.CellStatus(context.Background(), tenant)
		return err == nil && status.Running && status.HealthOK
	}, 3*time.Minute, 5*time.Second)

	// CellStatus: the cell should be running and its gateway healthy.
	statusCtx, cancelStatus := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelStatus()
	status, err := transport.CellStatus(statusCtx, tenant)
	require.NoError(t, err)
	require.True(t, status.Running, "expected cell running, got state=%q", status.State)
	require.True(t, status.HealthOK, "expected healthy gateway")
	require.Greater(t, status.Port, 0)

	// Logs: raw tail should be non-empty.
	logsCtx, cancelLogs := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelLogs()
	logs, err := transport.Logs(logsCtx, tenant)
	require.NoError(t, err)
	require.NotEmpty(t, strings.TrimSpace(logs.Content))

	// Stop then Start: state flips.
	require.NoError(t, transport.Stop(actionCtx, tenant))
	stopped, err := transport.CellStatus(context.Background(), tenant)
	require.NoError(t, err)
	require.False(t, stopped.Running, "expected stopped after fleet stop")

	require.NoError(t, transport.Start(actionCtx, tenant))
	running, err := transport.CellStatus(context.Background(), tenant)
	require.NoError(t, err)
	require.True(t, running.Running, "expected running after fleet start")

	// Inspect maps the container to a provider status.
	inspection, err := transport.Inspect(context.Background(), tenant)
	require.NoError(t, err)
	require.Equal(t, tenant, inspection.ID)
	require.Equal(t, ProviderStatusRunning, inspection.Status)

	// Destroy purges the cell (also exercised by t.Cleanup). After removal,
	// fleet status reports the cell as not found, which the transport normalizes
	// to FleetErrorNotFound.
	require.NoError(t, transport.Destroy(actionCtx, tenant))
	_, err = transport.Inspect(context.Background(), tenant)
	require.Error(t, err)
	var goneErr *FleetError
	require.ErrorAs(t, err, &goneErr)
	require.Equal(t, FleetErrorNotFound, goneErr.Code)
}

// TestFleetCLIRealOnboardReachesReady drives a REAL provider-backed Onboard:
// real fleet create + start, with container/gateway readiness sourced from the
// real `fleet status` snapshot. The channel-authenticated signal is mocked (a
// live Danta plugin connection is a deployment concern). The cell is purged in
// cleanup.
func TestFleetCLIRealOnboardReachesReady(t *testing.T) {
	if !realFleetAvailable(t) {
		t.Skip("openclaw fleet subcommand not available on this host")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available on this host")
	}

	transport := NewFleetCLITransport(FleetCLIOptions{Binary: fleetCLIBinary()})
	provider := NewFleetInstanceProvider(transport, FleetProviderOptions{})
	readiness := NewFleetReadiness(transport.CellStatus, func(int) bool { return true })
	service := NewLifecycleService(testDB(t), provider, readiness.ReadinessChecker())

	userID := int(71000000 + time.Now().UnixNano()%100000000)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	t.Cleanup(func() {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelCleanup()
		tenant, err := tenantForUser(userID)
		if err == nil {
			_ = transport.Destroy(cleanupCtx, tenant)
		}
	})

	result, err := service.Onboard(ctx, userID, "real-onboard-key", OnboardRequest{Provider: "fleet"})
	require.NoError(t, err)
	require.NotNil(t, result.Instance)
	require.Equal(t, StateReady, InstanceState(result.Instance.State))
	require.Equal(t, "u"+strconv.Itoa(userID), result.Instance.ProviderInstanceID)
}
