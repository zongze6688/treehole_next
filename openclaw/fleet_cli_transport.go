package openclaw

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	defaultFleetBinary  = "openclaw"
	defaultFleetImage   = "ghcr.io/openclaw/openclaw:latest"
	defaultFleetRuntime = "docker"
	fleetLogTail        = "200"
)

// tenantIDPattern matches the OpenClaw Fleet tenant ID contract:
// ^[a-z0-9](?:[a-z0-9-]{0,38}[a-z0-9])?$ (1-40 lowercase alnum/hyphen, no
// uppercase, underscore, slash, or dot).
var tenantIDPattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,38}[a-z0-9])?$`)

// CommandRunner executes the Fleet CLI. Tests inject a fake runner that
// returns canned JSON without requiring a real openclaw binary on PATH.
type CommandRunner func(ctx context.Context, args ...string) (stdout []byte, stderr []byte, err error)

// FleetCLIOptions configures a FleetCLITransport.
type FleetCLIOptions struct {
	// Binary is the fleet CLI executable (default "openclaw").
	Binary string
	// Prefix holds extra leading arguments before the subcommand, e.g.
	// {"ssh", "fleet-host"} to run the CLI on a remote host.
	Prefix []string
	// Image is the default cell image when FleetCreateRequest.Image is empty.
	Image string
	// Runtime is the container runtime ("docker" default, "podman" allowed).
	Runtime string
	// Runner executes the command; when nil the transport runs Binary+Prefix+args.
	Runner CommandRunner
}

// FleetCLITransport implements FleetTransport by exec'ing the host-side
// `openclaw fleet` CLI and parsing its --json output.
type FleetCLITransport struct {
	binary  string
	prefix  []string
	image   string
	runtime string
	runner  CommandRunner
}

// NewFleetCLITransport builds a FleetCLITransport with defaults applied.
func NewFleetCLITransport(opts FleetCLIOptions) *FleetCLITransport {
	binary := opts.Binary
	if binary == "" {
		binary = defaultFleetBinary
	}
	image := opts.Image
	if image == "" {
		image = defaultFleetImage
	}
	runtime := opts.Runtime
	if runtime == "" {
		runtime = defaultFleetRuntime
	}
	return &FleetCLITransport{binary: binary, prefix: opts.Prefix, image: image, runtime: runtime, runner: opts.Runner}
}

var _ FleetTransport = (*FleetCLITransport)(nil)

// run executes the CLI, either through the injected runner or by running
// Binary+Prefix+args via os/exec.
func (t *FleetCLITransport) run(ctx context.Context, args ...string) ([]byte, []byte, error) {
	if t.runner != nil {
		return t.runner(ctx, args...)
	}
	fullArgs := append(append([]string{}, t.prefix...), args...)
	cmd := exec.CommandContext(ctx, t.binary, fullArgs...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.Bytes(), stderr.Bytes(), err
}

// FleetCreateResult is the JSON shape of `openclaw fleet create --json`.
// Token is parsed from the wire response but never logged or persisted here.
type FleetCreateResult struct {
	Tenant        string `json:"tenant"`
	ContainerName string `json:"containerName"`
	Port          int    `json:"port"`
	Image         string `json:"image"`
	Runtime       string `json:"runtime"`
	Started       bool   `json:"started"`
	Token         string `json:"token"`
	TokenNote     string `json:"tokenNote"`
	URL           string `json:"url"`
	NextStep      string `json:"nextStep"`
}

// FleetActionResult is the JSON shape of
// `openclaw fleet start|stop|restart|rm --json`.
type FleetActionResult struct {
	Tenant     string `json:"tenant"`
	Action     string `json:"action"`
	DataPurged bool   `json:"dataPurged"`
}

// FleetContainerStatus is the "container" object of `fleet status --json`.
type FleetContainerStatus struct {
	State   string `json:"state"`
	Running bool   `json:"running"`
	Managed bool   `json:"managed"`
	Error   string `json:"error"`
}

// FleetHealthStatus is the "health" object of `fleet status --json`.
type FleetHealthStatus struct {
	Status     string `json:"status"`
	URL        string `json:"url"`
	HTTPStatus int    `json:"httpStatus"`
	Error      string `json:"error"`
	Reason     string `json:"reason"`
}

// FleetStatusResult is the JSON shape of `openclaw fleet status --json`.
type FleetStatusResult struct {
	Tenant        string               `json:"tenant"`
	ContainerName string               `json:"containerName"`
	Runtime       string               `json:"runtime"`
	Port          int                  `json:"port"`
	Image         string               `json:"image"`
	Created       string               `json:"created"`
	DataDir       string               `json:"dataDir"`
	Container     FleetContainerStatus `json:"container"`
	Health        FleetHealthStatus    `json:"health"`
}

// FleetCellStatus is the readiness snapshot parsed from `fleet status --json`.
// It is a concrete (non-interface) helper for the next lifecycle step.
type FleetCellStatus struct {
	Port     int
	Running  bool
	Managed  bool
	HealthOK bool
	State    string
}

// Create provisions a cell for the user and returns the instance.
func (t *FleetCLITransport) Create(ctx context.Context, req FleetCreateRequest) (FleetInstance, error) {
	tenant, err := tenantForUser(req.UserID)
	if err != nil {
		return FleetInstance{}, err
	}
	image := req.Image
	if image == "" {
		image = t.image
	}
	args := []string{"create", tenant, "--json"}
	if image != "" {
		args = append(args, "--image", image)
	}
	if t.runtime != "" {
		args = append(args, "--runtime", t.runtime)
	}
	keys := make([]string, 0, len(req.Metadata))
	for key := range req.Metadata {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		args = append(args, "--env", key+"="+req.Metadata[key])
	}
	stdout, stderr, runErr := t.run(ctx, args...)
	if runErr != nil {
		// Return the derived tenant so the caller can compensate a partial
		// create that may have left a cell behind.
		return FleetInstance{ID: tenant}, t.normalizeError(ctx, runErr, stdout, stderr)
	}
	var result FleetCreateResult
	if err := json.Unmarshal(stdout, &result); err != nil {
		return FleetInstance{ID: tenant}, &FleetError{Code: FleetErrorUnknown, Retryable: false, Err: err}
	}
	if strings.TrimSpace(result.Tenant) == "" {
		// The CLI accepted the create but returned no tenant: treat as a
		// duplicate/conflict rather than an unknown failure.
		return FleetInstance{}, &FleetError{Code: FleetErrorConflict, Retryable: false, Err: errors.New("fleet create returned an empty tenant")}
	}
	status := ProviderStatusProvisioning
	if result.Started {
		status = ProviderStatusRunning
	}
	return FleetInstance{ID: tenant, Status: status}, nil
}

// Start starts a stopped cell.
func (t *FleetCLITransport) Start(ctx context.Context, id string) error {
	return t.action(ctx, "start", id)
}

// Stop stops a running cell. Cell data is preserved.
func (t *FleetCLITransport) Stop(ctx context.Context, id string) error {
	return t.action(ctx, "stop", id)
}

// Restart restarts a stopped cell.
func (t *FleetCLITransport) Restart(ctx context.Context, id string) error {
	return t.action(ctx, "restart", id)
}

func (t *FleetCLITransport) action(ctx context.Context, command, id string) error {
	if err := validateTenant(id); err != nil {
		return err
	}
	stdout, stderr, runErr := t.run(ctx, command, id, "--json")
	if runErr != nil {
		return t.normalizeError(ctx, runErr, stdout, stderr)
	}
	var result FleetActionResult
	if err := json.Unmarshal(stdout, &result); err != nil {
		return &FleetError{Code: FleetErrorUnknown, Retryable: false, Err: err}
	}
	return nil
}

// Destroy removes the cell and always purges its data. Every current Destroy
// caller is either a Reset (which must delete workspace/memory/state per
// instance_manager.md) or a failed-create compensation (no user data exists
// yet), so there is no caller that expects data to survive.
func (t *FleetCLITransport) Destroy(ctx context.Context, id string) error {
	if err := validateTenant(id); err != nil {
		return err
	}
	stdout, stderr, runErr := t.run(ctx, "rm", id, "--purge-data", "--force", "--json")
	if runErr != nil {
		return t.normalizeError(ctx, runErr, stdout, stderr)
	}
	var result FleetActionResult
	if err := json.Unmarshal(stdout, &result); err != nil {
		return &FleetError{Code: FleetErrorUnknown, Retryable: false, Err: err}
	}
	return nil
}

// Inspect reports the cell's lifecycle status without judging health: a cell
// whose gateway is unhealthy still reports running/stopped/unknown.
func (t *FleetCLITransport) Inspect(ctx context.Context, id string) (FleetInstance, error) {
	if err := validateTenant(id); err != nil {
		return FleetInstance{}, err
	}
	result, err := t.status(ctx, id)
	if err != nil {
		return FleetInstance{}, err
	}
	return FleetInstance{ID: id, Status: fleetStatusToProvider(result.Container)}, nil
}

// Logs returns the raw container log tail; stderr only matters on error.
func (t *FleetCLITransport) Logs(ctx context.Context, id string) (FleetLogs, error) {
	if err := validateTenant(id); err != nil {
		return FleetLogs{}, err
	}
	stdout, stderr, runErr := t.run(ctx, "logs", id, "--tail", fleetLogTail)
	if runErr != nil {
		return FleetLogs{}, t.normalizeError(ctx, runErr, stdout, stderr)
	}
	return FleetLogs{Content: string(stdout)}, nil
}

// CellStatus is a concrete readiness helper for the next lifecycle step: it
// reports the port plus container/health flags parsed from `fleet status --json`.
func (t *FleetCLITransport) CellStatus(ctx context.Context, tenant string) (FleetCellStatus, error) {
	if err := validateTenant(tenant); err != nil {
		return FleetCellStatus{}, err
	}
	result, err := t.status(ctx, tenant)
	if err != nil {
		return FleetCellStatus{}, err
	}
	return FleetCellStatus{
		Port:     result.Port,
		Running:  result.Container.Running && result.Container.State == "running",
		Managed:  result.Container.Managed,
		HealthOK: result.Health.Status == "ok",
		State:    result.Container.State,
	}, nil
}

// status runs `fleet status --json` and parses the result.
func (t *FleetCLITransport) status(ctx context.Context, tenant string) (FleetStatusResult, error) {
	stdout, stderr, runErr := t.run(ctx, "status", tenant, "--json")
	if runErr != nil {
		return FleetStatusResult{}, t.normalizeError(ctx, runErr, stdout, stderr)
	}
	var result FleetStatusResult
	if err := json.Unmarshal(stdout, &result); err != nil {
		return FleetStatusResult{}, &FleetError{Code: FleetErrorUnknown, Retryable: false, Err: err}
	}
	return result, nil
}

func fleetStatusToProvider(container FleetContainerStatus) ProviderStatus {
	switch {
	case container.State == "missing" || container.State == "unknown":
		return ProviderStatusUnknown
	case container.State == "running" && container.Running && container.Managed:
		return ProviderStatusRunning
	case container.State == "stopped" || !container.Running:
		return ProviderStatusStopped
	default:
		return ProviderStatusUnknown
	}
}

// tenantForUser derives the deterministic Fleet tenant from a user ID.
func tenantForUser(userID int) (string, error) {
	if userID <= 0 {
		return "", &FleetError{Code: FleetErrorInvalidRequest, Retryable: false}
	}
	tenant := "u" + strconv.Itoa(userID)
	if !tenantIDPattern.MatchString(tenant) {
		return "", &FleetError{
			Code: FleetErrorInvalidRequest, Retryable: false,
			Err: fmt.Errorf("derived tenant %q does not match the fleet tenant pattern", tenant),
		}
	}
	return tenant, nil
}

// validateTenant rejects empty provider instance IDs, mirroring
// validateProviderInstanceID in fleet_provider.go.
func validateTenant(tenant string) error {
	if strings.TrimSpace(tenant) == "" {
		return &FleetError{Code: FleetErrorInvalidRequest, Retryable: false}
	}
	return nil
}

// normalizeError maps a failed CLI invocation to a *FleetError by inspecting
// the combined stdout/stderr text (lowercased), so fleet_provider.go's
// errors.As-based normalization works. It always returns a *FleetError, never
// a raw error.
func (t *FleetCLITransport) normalizeError(ctx context.Context, runErr error, stdout, stderr []byte) error {
	if runErr == nil {
		return nil
	}
	text := strings.ToLower(string(stdout) + "\n" + string(stderr))
	exitCode := 0
	var exitErr *exec.ExitError
	if errors.As(runErr, &exitErr) {
		exitCode = exitErr.ExitCode()
	}
	fleetErr := func(code FleetErrorCode, retryable bool) error {
		return &FleetError{Code: code, StatusCode: exitCode, Retryable: retryable, Err: runErr}
	}
	switch {
	case containsAny(text, "already exists", "duplicate", "reserved", "use --force", "is running"):
		return fleetErr(FleetErrorConflict, false)
	case containsAny(text, "not found", "unknown tenant", "no such", "missing"):
		return fleetErr(FleetErrorNotFound, false)
	case containsAny(text, "unauthorized", "permission", "auth"):
		return fleetErr(FleetErrorUnauthorized, false)
	case containsAny(text, "invalid", "must match", "rejected"):
		return fleetErr(FleetErrorInvalidRequest, false)
	case errors.Is(runErr, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) ||
		errors.Is(runErr, context.Canceled) || errors.Is(ctx.Err(), context.Canceled):
		return fleetErr(FleetErrorTimeout, true)
	case containsAny(text, "connection refused", "unavailable", "cannot connect"):
		return fleetErr(FleetErrorUnavailable, true)
	default:
		// A non-zero exit (or runner failure) with no matching pattern: the
		// CLI could not be reached or completed, so treat it as unavailable.
		if exitErr != nil {
			return fleetErr(FleetErrorUnavailable, true)
		}
		return &FleetError{Code: FleetErrorUnknown, Retryable: false, Err: runErr}
	}
}

func containsAny(text string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(text, needle) {
			return true
		}
	}
	return false
}
