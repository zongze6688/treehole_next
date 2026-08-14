package openclaw

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// WorkloadIdentity provisions and revokes the user-level OpenClaw token and
// returns the cell environment to inject. It is provider-neutral (the cell env
// keys are defined by the concrete implementation).
type WorkloadIdentity interface {
	Env(ctx context.Context, userID int) (map[string]string, error)
	Revoke(ctx context.Context, userID int) error
}

// HTTPWorkloadIdentityOptions configures the auth_next control-plane client.
// BaseURL must be the auth_next base (config.Config.AuthUrl), for example
// https://auth.fduhole.com/api.
type HTTPWorkloadIdentityOptions struct {
	BaseURL      string       // auth_next base, e.g. https://auth.fduhole.com/api
	ProvisionKey string       // X-Provision-Key value
	WSUrl        string       // DANTA_WS_URL to inject
	Client       *http.Client // injectable; default http.DefaultClient
}

// HTTPWorkloadIdentity provisions and revokes OpenClaw tokens against the
// auth_next openclaw token API. Tokens and the provision key are never logged
// and never appear in error messages.
type HTTPWorkloadIdentity struct {
	baseURL      string
	provisionKey string
	wsURL        string
	client       *http.Client
}

func NewHTTPWorkloadIdentity(opts HTTPWorkloadIdentityOptions) *HTTPWorkloadIdentity {
	client := opts.Client
	if client == nil {
		client = http.DefaultClient
	}
	return &HTTPWorkloadIdentity{
		baseURL:      strings.TrimRight(opts.BaseURL, "/"),
		provisionKey: opts.ProvisionKey,
		wsURL:        opts.WSUrl,
		client:       client,
	}
}

// Env returns the cell environment (OPENCLAW_DANTA_TOKEN, DANTA_WS_URL,
// DANTA_USER_ID, DANTA_INSTANCE_ID) for the user. When the user already holds
// an active token the provisioning endpoint returns no plaintext, so the token
// is rotated to obtain a fresh one.
func (w *HTTPWorkloadIdentity) Env(ctx context.Context, userID int) (map[string]string, error) {
	if w == nil {
		return nil, errors.New("OpenClaw workload identity is not configured")
	}
	tenant, err := tenantForUser(userID)
	if err != nil {
		return nil, err
	}
	token, err := w.callToken(ctx, userID, "token")
	if err != nil {
		return nil, err
	}
	if token == "" {
		token, err = w.callToken(ctx, userID, "token/rotate")
		if err != nil {
			return nil, err
		}
	}
	if token == "" {
		return nil, fmt.Errorf("openclaw workload identity returned no token for user %d", userID)
	}
	return map[string]string{
		"OPENCLAW_DANTA_TOKEN": token,
		"DANTA_WS_URL":         w.wsURL,
		"DANTA_USER_ID":        strconv.Itoa(userID),
		"DANTA_INSTANCE_ID":    tenant,
	}, nil
}

// Revoke invalidates the user's OpenClaw token. Callers decide how to treat
// failures; the lifecycle boundary treats revocation as best-effort.
func (w *HTTPWorkloadIdentity) Revoke(ctx context.Context, userID int) error {
	if w == nil {
		return errors.New("OpenClaw workload identity is not configured")
	}
	_, err := w.callToken(ctx, userID, "token/revoke")
	return err
}

type workloadTokenResponse struct {
	Token string `json:"token"`
}

// callToken performs one POST against the auth_next openclaw token API and
// returns the plaintext token when the endpoint provides one ("" for revoke,
// where the response carries no token). Errors carry only the HTTP status and
// a non-secret message.
func (w *HTTPWorkloadIdentity) callToken(ctx context.Context, userID int, endpoint string) (string, error) {
	body, err := json.Marshal(map[string]int{"user_id": userID})
	if err != nil {
		return "", fmt.Errorf("openclaw workload identity %s: encode request: %w", endpoint, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.baseURL+"/openclaw/"+endpoint, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("openclaw workload identity %s: build request: %w", endpoint, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Provision-Key", w.provisionKey)

	resp, err := w.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("openclaw workload identity %s: request failed: %w", endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return "", &WorkloadIdentityError{
			Operation:  endpoint,
			StatusCode: resp.StatusCode,
			Message:    http.StatusText(resp.StatusCode),
		}
	}

	if endpoint == "token/revoke" {
		_, _ = io.Copy(io.Discard, resp.Body)
		return "", nil
	}
	var parsed workloadTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return "", fmt.Errorf("openclaw workload identity %s: decode response: %w", endpoint, err)
	}
	return parsed.Token, nil
}

// WorkloadIdentityError reports a failed OpenClaw token API call. It carries
// only the HTTP status and a non-secret message, never tokens or keys.
type WorkloadIdentityError struct {
	Operation  string
	StatusCode int
	Message    string
}

func (e *WorkloadIdentityError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("openclaw workload identity %s failed: %s (status %d)", e.Operation, e.Message, e.StatusCode)
}
