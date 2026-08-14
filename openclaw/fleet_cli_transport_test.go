package openclaw

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
)

// fakeCLIRunner records every invocation and returns canned output, so the
// transport can be tested without a real openclaw binary.
type fakeCLIRunner struct {
	mu     sync.Mutex
	args   [][]string
	stdout []byte
	stderr []byte
	err    error
}

func (f *fakeCLIRunner) run(_ context.Context, args ...string) ([]byte, []byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.args = append(f.args, append([]string(nil), args...))
	return f.stdout, f.stderr, f.err
}

func newCLITransport(runner *fakeCLIRunner) *FleetCLITransport {
	return NewFleetCLITransport(FleetCLIOptions{Runner: runner.run})
}

func assertStringSlices(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("args mismatch: got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("args mismatch at %d: got %q, want %q (full: got %v, want %v)", i, got[i], want[i], got, want)
		}
	}
}

func assertFleetErrorCode(t *testing.T, err error, want FleetErrorCode) {
	t.Helper()
	var fleetErr *FleetError
	if !errors.As(err, &fleetErr) {
		t.Fatalf("error is %T (%v), want *FleetError", err, err)
	}
	if fleetErr.Code != want {
		t.Fatalf("FleetError.Code = %q, want %q", fleetErr.Code, want)
	}
}

// exitStatusError mimics a CLI invocation that exited non-zero.
func exitStatusError() error {
	return &exec.ExitError{ProcessState: &os.ProcessState{}}
}

func TestFleetCLITransportCreateBuildsArgsAndParses(t *testing.T) {
	runner := &fakeCLIRunner{stdout: []byte(`{"tenant":"u123","containerName":"oc-u123","port":8080,"image":"cell/image:latest","runtime":"docker","started":true}`)}
	transport := NewFleetCLITransport(FleetCLIOptions{Runner: runner.run, Image: "cell/image:latest"})
	instance, err := transport.Create(context.Background(), FleetCreateRequest{
		UserID:   123,
		Metadata: map[string]string{"B": "2", "A": "1"},
	})
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if len(runner.args) != 1 {
		t.Fatalf("Create invoked runner %d times, want 1", len(runner.args))
	}
	assertStringSlices(t, runner.args[0], []string{
		"fleet", "create", "u123", "--json", "--no-start", "--image", "cell/image:latest", "--runtime", "docker",
		"--env", "A=1", "--env", "B=2",
	})
	if instance.ID != "u123" {
		t.Fatalf("instance ID = %q, want u123", instance.ID)
	}
	if instance.Status != ProviderStatusRunning {
		t.Fatalf("instance status = %q, want %q", instance.Status, ProviderStatusRunning)
	}
}

func TestFleetCLITransportCreateUsesDefaultImageAndProvisioningStatus(t *testing.T) {
	runner := &fakeCLIRunner{stdout: []byte(`{"tenant":"u5","started":false}`)}
	transport := newCLITransport(runner)
	instance, err := transport.Create(context.Background(), FleetCreateRequest{UserID: 5})
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	assertStringSlices(t, runner.args[0], []string{
		"fleet", "create", "u5", "--json", "--no-start", "--image", defaultFleetImage, "--runtime", "docker",
	})
	if instance.Status != ProviderStatusProvisioning {
		t.Fatalf("instance status = %q, want %q", instance.Status, ProviderStatusProvisioning)
	}
}

func TestFleetCLITransportCreateRejectsInvalidUserIDs(t *testing.T) {
	runner := &fakeCLIRunner{}
	transport := newCLITransport(runner)
	for _, userID := range []int{0, -1, -999} {
		_, err := transport.Create(context.Background(), FleetCreateRequest{UserID: userID})
		assertFleetErrorCode(t, err, FleetErrorInvalidRequest)
	}
	if len(runner.args) != 0 {
		t.Fatalf("runner invoked %d times for invalid user IDs, want 0", len(runner.args))
	}
}

func TestTenantForUserMatchesFleetPattern(t *testing.T) {
	for _, tc := range []struct {
		userID int
		ok     bool
	}{
		{1, true},
		{42, true},
		{1234567890123456789, true},
		{0, false},
		{-1, false},
	} {
		tenant, err := tenantForUser(tc.userID)
		if tc.ok != (err == nil) {
			t.Fatalf("tenantForUser(%d): ok = %v, want %v (tenant %q, err %v)", tc.userID, err == nil, tc.ok, tenant, err)
		}
		if err == nil && !tenantIDPattern.MatchString(tenant) {
			t.Fatalf("tenantForUser(%d) = %q does not match tenant pattern", tc.userID, tenant)
		}
	}
}

func TestTenantIDPattern(t *testing.T) {
	for _, tc := range []struct {
		tenant string
		ok     bool
	}{
		{"u1", true},
		{"u123", true},
		{"u-1", true},
		{"", false},
		{"U1", false},
		{"u_1", false},
		{"u.1", false},
		{"u/1", false},
		{"u-" + strings.Repeat("a", 38), true},
		{"u-" + strings.Repeat("a", 39), false},
	} {
		if got := tenantIDPattern.MatchString(tc.tenant); got != tc.ok {
			t.Fatalf("tenant pattern match(%q) = %v, want %v", tc.tenant, got, tc.ok)
		}
	}
}

func TestFleetCLITransportActionsBuildArgs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		invoke func(*FleetCLITransport, context.Context, string) error
		want   []string
	}{
		{"start", func(tr *FleetCLITransport, ctx context.Context, id string) error { return tr.Start(ctx, id) }, []string{"fleet", "start", "u7"}},
		{"stop", func(tr *FleetCLITransport, ctx context.Context, id string) error { return tr.Stop(ctx, id) }, []string{"fleet", "stop", "u7"}},
		{"restart", func(tr *FleetCLITransport, ctx context.Context, id string) error { return tr.Restart(ctx, id) }, []string{"fleet", "restart", "u7"}},
		{"destroy", func(tr *FleetCLITransport, ctx context.Context, id string) error { return tr.Destroy(ctx, id) }, []string{"fleet", "rm", "u7", "--purge-data", "--force"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runner := &fakeCLIRunner{stdout: []byte(`{"tenant":"u7","action":"` + tc.name + `"}`)}
			transport := newCLITransport(runner)
			if err := tc.invoke(transport, context.Background(), "u7"); err != nil {
				t.Fatalf("%s returned error: %v", tc.name, err)
			}
			if len(runner.args) != 1 {
				t.Fatalf("%s invoked runner %d times, want 1", tc.name, len(runner.args))
			}
			assertStringSlices(t, runner.args[0], tc.want)
		})
	}
}

func TestFleetCLITransportRejectsEmptyInstanceID(t *testing.T) {
	transport := newCLITransport(&fakeCLIRunner{})
	invocations := []func() error{
		func() error { return transport.Start(context.Background(), "") },
		func() error { return transport.Stop(context.Background(), "") },
		func() error { return transport.Restart(context.Background(), "") },
		func() error { return transport.Destroy(context.Background(), "") },
		func() error { _, err := transport.Inspect(context.Background(), ""); return err },
		func() error { _, err := transport.Logs(context.Background(), ""); return err },
		func() error { _, err := transport.CellStatus(context.Background(), ""); return err },
		func() error { _, err := transport.CellStatus(context.Background(), "   "); return err },
	}
	for _, invoke := range invocations {
		assertFleetErrorCode(t, invoke(), FleetErrorInvalidRequest)
	}
}

func TestFleetCLITransportInspectMapsStatus(t *testing.T) {
	for _, tc := range []struct {
		name string
		json string
		want ProviderStatus
	}{
		{
			"running",
			`{"tenant":"u1","port":8080,"container":{"state":"running","running":true,"managed":true},"health":{"status":"ok","url":"http://127.0.0.1:8080/healthz","httpStatus":200}}`,
			ProviderStatusRunning,
		},
		{
			"unhealthy but running",
			`{"tenant":"u1","container":{"state":"running","running":true,"managed":true},"health":{"status":"failed","url":"http://127.0.0.1:8080/healthz","error":"gateway not responding"}}`,
			ProviderStatusRunning,
		},
		{
			"stopped",
			`{"tenant":"u1","container":{"state":"stopped","running":false,"managed":true},"health":{"status":"skipped","url":"","reason":"container not running"}}`,
			ProviderStatusStopped,
		},
		{
			"missing",
			`{"tenant":"u1","container":{"state":"missing","running":false,"managed":false},"health":{"status":"skipped","url":"","reason":"container not found"}}`,
			ProviderStatusUnknown,
		},
		{
			"unknown",
			`{"tenant":"u1","container":{"state":"unknown","running":false,"managed":false,"error":"docker daemon unreachable"},"health":{"status":"failed","url":"","error":"docker daemon unreachable"}}`,
			ProviderStatusUnknown,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runner := &fakeCLIRunner{stdout: []byte(tc.json)}
			transport := newCLITransport(runner)
			instance, err := transport.Inspect(context.Background(), "u1")
			if err != nil {
				t.Fatalf("Inspect returned error: %v", err)
			}
			if instance.ID != "u1" || instance.Status != tc.want {
				t.Fatalf("Inspect = {ID:%q Status:%q}, want {ID:u1 Status:%q}", instance.ID, instance.Status, tc.want)
			}
			assertStringSlices(t, runner.args[0], []string{"fleet", "status", "u1", "--json"})
		})
	}
}

func TestFleetCLITransportCellStatus(t *testing.T) {
	runner := &fakeCLIRunner{stdout: []byte(`{"tenant":"u1","port":18080,"container":{"state":"running","running":true,"managed":true},"health":{"status":"ok","url":"http://127.0.0.1:18080/healthz","httpStatus":200}}`)}
	transport := newCLITransport(runner)
	status, err := transport.CellStatus(context.Background(), "u1")
	if err != nil {
		t.Fatalf("CellStatus returned error: %v", err)
	}
	if status.Port != 18080 || !status.Running || !status.Managed || !status.HealthOK || status.State != "running" {
		t.Fatalf("CellStatus = %+v, want port 18080 running managed healthOK state running", status)
	}

	runner = &fakeCLIRunner{stdout: []byte(`{"tenant":"u1","port":18080,"container":{"state":"stopped","running":false,"managed":true},"health":{"status":"skipped","url":"","reason":"container not running"}}`)}
	transport = newCLITransport(runner)
	status, err = transport.CellStatus(context.Background(), "u1")
	if err != nil {
		t.Fatalf("CellStatus returned error: %v", err)
	}
	if status.Running || status.HealthOK || status.State != "stopped" || !status.Managed {
		t.Fatalf("CellStatus = %+v, want stopped not running unhealthy managed", status)
	}
}

func TestFleetCLITransportLogsReturnsStdout(t *testing.T) {
	runner := &fakeCLIRunner{stdout: []byte("line one\nline two\n")}
	transport := newCLITransport(runner)
	logs, err := transport.Logs(context.Background(), "u1")
	if err != nil {
		t.Fatalf("Logs returned error: %v", err)
	}
	assertStringSlices(t, runner.args[0], []string{"fleet", "logs", "u1", "--tail", "200"})
	if logs.Content != "line one\nline two\n" {
		t.Fatalf("Logs content = %q, want %q", logs.Content, "line one\nline two\n")
	}
}

func TestFleetCLITransportErrorNormalization(t *testing.T) {
	for _, tc := range []struct {
		name          string
		output        string
		err           error
		wantCode      FleetErrorCode
		wantRetryable bool
	}{
		{"already exists", "Error: tenant u1 already exists", exitStatusError(), FleetErrorConflict, false},
		{"duplicate", "tenant u1 is a duplicate", exitStatusError(), FleetErrorConflict, false},
		{"use force", "container is managed; use --force to remove", exitStatusError(), FleetErrorConflict, false},
		{"not found", "unknown tenant u999", exitStatusError(), FleetErrorNotFound, false},
		{"no such container", "no such container: oc-u999", exitStatusError(), FleetErrorNotFound, false},
		{"unauthorized", "unauthorized: invalid gateway token", exitStatusError(), FleetErrorUnauthorized, false},
		{"permission denied", "permission denied on /var/run/docker.sock", exitStatusError(), FleetErrorUnauthorized, false},
		{"invalid tenant", "invalid tenant name", exitStatusError(), FleetErrorInvalidRequest, false},
		{"must match", "tenant must match ^[a-z0-9]", exitStatusError(), FleetErrorInvalidRequest, false},
		{"deadline exceeded", "", context.DeadlineExceeded, FleetErrorTimeout, true},
		{"connection refused", "connection refused: dial tcp 127.0.0.1:2375", exitStatusError(), FleetErrorUnavailable, true},
		{"unavailable", "service unavailable", exitStatusError(), FleetErrorUnavailable, true},
		{"empty output non-zero exit", "", exitStatusError(), FleetErrorUnavailable, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runner := &fakeCLIRunner{stdout: []byte(tc.output), err: tc.err}
			transport := newCLITransport(runner)
			_, err := transport.Create(context.Background(), FleetCreateRequest{UserID: 1})
			assertFleetErrorCode(t, err, tc.wantCode)
			var fleetErr *FleetError
			if !errors.As(err, &fleetErr) {
				t.Fatalf("error is %T, want *FleetError", err)
			}
			if fleetErr.Retryable != tc.wantRetryable {
				t.Fatalf("Retryable = %v, want %v", fleetErr.Retryable, tc.wantRetryable)
			}
			if fleetErr.Err == nil {
				t.Fatalf("FleetError.Err is nil, want wrapped error")
			}
		})
	}
}

func TestFleetCLITransportErrorsAreFleetErrors(t *testing.T) {
	// stderr text is inspected too.
	runner := &fakeCLIRunner{stderr: []byte("already exists"), err: errors.New("exit status 1")}
	transport := newCLITransport(runner)
	err := transport.Start(context.Background(), "u1")
	var fleetErr *FleetError
	if !errors.As(err, &fleetErr) {
		t.Fatalf("Start error is %T, want *FleetError", err)
	}
	if fleetErr.Code != FleetErrorConflict {
		t.Fatalf("Code = %q, want %q", fleetErr.Code, FleetErrorConflict)
	}
	if fleetErr.Retryable {
		t.Fatalf("conflict must not be retryable")
	}
	if IsRetryableFleetError(err) {
		t.Fatalf("IsRetryableFleetError(conflict) = true, want false")
	}

	timeoutRunner := &fakeCLIRunner{err: context.DeadlineExceeded}
	timeoutTransport := newCLITransport(timeoutRunner)
	timeoutErr := timeoutTransport.Start(context.Background(), "u1")
	if !errors.As(timeoutErr, &fleetErr) {
		t.Fatalf("timeout error is %T, want *FleetError", timeoutErr)
	}
	if fleetErr.Code != FleetErrorTimeout {
		t.Fatalf("Code = %q, want %q", fleetErr.Code, FleetErrorTimeout)
	}
	if !IsRetryableFleetError(timeoutErr) {
		t.Fatalf("IsRetryableFleetError(timeout) = false, want true")
	}
}
