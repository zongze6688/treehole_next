package openclaw

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// All provision keys and tokens in this file are test-only fixtures derived
// inline; no real secrets are hardcoded in production source. The token is
// never printed via t.Log or written to any log output: assertions compare
// against the fixture constants without dumping values.

type tokenRequestBody struct {
	UserID int `json:"user_id"`
}

func TestHTTPWorkloadIdentityEnvProvisionsToken(t *testing.T) {
	const wantToken = "ocw_test_provisioned_token"
	const wantKey = "test-provision-key"
	const wantWSUrl = "wss://ws.example.test"

	var mu sync.Mutex
	var method, endpoint, provisionKey string
	var userID int
	var decodeErr error

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		method = r.Method
		endpoint = r.URL.Path
		provisionKey = r.Header.Get("X-Provision-Key")
		var body tokenRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			decodeErr = err
			return
		}
		userID = body.UserID
		_ = json.NewEncoder(w).Encode(map[string]any{
			"token_id": "token-1", "user_id": 7, "status": "active",
			"scopes": []string{"openclaw:connect"}, "token": wantToken,
			"created": true, "created_at": "2026-01-01T00:00:00Z",
		})
	}))
	defer server.Close()

	identity := NewHTTPWorkloadIdentity(HTTPWorkloadIdentityOptions{
		BaseURL: server.URL, ProvisionKey: wantKey, WSUrl: wantWSUrl,
	})
	env, err := identity.Env(context.Background(), 7)
	require.NoError(t, err)

	require.Equal(t, wantToken, env["DANTA_ACCESS_TOKEN"])
	require.Equal(t, wantWSUrl, env["DANTA_WS_URL"])

	mu.Lock()
	defer mu.Unlock()
	require.NoError(t, decodeErr)
	require.Equal(t, "/openclaw/token", endpoint)
	require.Equal(t, http.MethodPost, method)
	require.Equal(t, wantKey, provisionKey)
	require.Equal(t, 7, userID)
}

func TestHTTPWorkloadIdentityEnvRotatesWhenProvisionReturnsNoToken(t *testing.T) {
	const wantToken = "ocw_test_rotated_token"

	var mu sync.Mutex
	var provisionCalls, rotateCalls int
	var rotateProvisionKey string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/openclaw/token":
			// The user already holds an active token: no plaintext is returned.
			provisionCalls++
			_, _ = w.Write([]byte(`{"token_id":"token-1","user_id":7,"status":"active","scopes":["openclaw:connect"]}`))
		case "/openclaw/token/rotate":
			rotateCalls++
			rotateProvisionKey = r.Header.Get("X-Provision-Key")
			_, _ = w.Write([]byte(`{"token_id":"token-2","user_id":7,"status":"active","scopes":["openclaw:connect"],"token":"` + wantToken + `"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	identity := NewHTTPWorkloadIdentity(HTTPWorkloadIdentityOptions{
		BaseURL: server.URL, ProvisionKey: "test-provision-key", WSUrl: "wss://ws.example.test",
	})
	env, err := identity.Env(context.Background(), 7)
	require.NoError(t, err)
	require.Equal(t, wantToken, env["DANTA_ACCESS_TOKEN"])
	require.Equal(t, "wss://ws.example.test", env["DANTA_WS_URL"])

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, provisionCalls)
	require.Equal(t, 1, rotateCalls)
	require.Equal(t, "test-provision-key", rotateProvisionKey)
}

func TestHTTPWorkloadIdentityRevoke(t *testing.T) {
	var mu sync.Mutex
	var method, endpoint, provisionKey string
	var userID int
	var decodeErr error

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		method = r.Method
		endpoint = r.URL.Path
		provisionKey = r.Header.Get("X-Provision-Key")
		var body tokenRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			decodeErr = err
			return
		}
		userID = body.UserID
		_, _ = w.Write([]byte(`{"token_id":"token-1","user_id":7,"status":"revoked"}`))
	}))
	defer server.Close()

	identity := NewHTTPWorkloadIdentity(HTTPWorkloadIdentityOptions{
		BaseURL: server.URL, ProvisionKey: "test-provision-key", WSUrl: "wss://ws.example.test",
	})
	require.NoError(t, identity.Revoke(context.Background(), 7))

	mu.Lock()
	defer mu.Unlock()
	require.NoError(t, decodeErr)
	require.Equal(t, "/openclaw/token/revoke", endpoint)
	require.Equal(t, http.MethodPost, method)
	require.Equal(t, "test-provision-key", provisionKey)
	require.Equal(t, 7, userID)
}

func TestHTTPWorkloadIdentityNon200ReturnsNormalizedError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer server.Close()

	identity := NewHTTPWorkloadIdentity(HTTPWorkloadIdentityOptions{
		BaseURL: server.URL, ProvisionKey: "test-provision-key",
	})

	_, err := identity.Env(context.Background(), 7)
	require.Error(t, err)
	var identityErr *WorkloadIdentityError
	require.ErrorAs(t, err, &identityErr)
	require.Equal(t, http.StatusUnauthorized, identityErr.StatusCode)
	require.Contains(t, identityErr.Message, "Unauthorized")

	require.Error(t, identity.Revoke(context.Background(), 7))
}

func TestHTTPWorkloadIdentityRotateFailureReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/openclaw/token":
			_, _ = w.Write([]byte(`{}`))
		case "/openclaw/token/rotate":
			http.Error(w, "internal error", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	identity := NewHTTPWorkloadIdentity(HTTPWorkloadIdentityOptions{
		BaseURL: server.URL, ProvisionKey: "test-provision-key",
	})
	_, err := identity.Env(context.Background(), 7)
	var identityErr *WorkloadIdentityError
	require.ErrorAs(t, err, &identityErr)
	require.Equal(t, http.StatusInternalServerError, identityErr.StatusCode)
	require.Equal(t, "token/rotate", identityErr.Operation)
}

func TestHTTPWorkloadIdentityErrorsDoNotLeakProvisionKey(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer server.Close()

	identity := NewHTTPWorkloadIdentity(HTTPWorkloadIdentityOptions{
		BaseURL: server.URL, ProvisionKey: "test-provision-key",
	})
	_, err := identity.Env(context.Background(), 7)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "test-provision-key")
}
