package claw

import (
	"bytes"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/opentreehole/go-common"
	"github.com/stretchr/testify/require"

	. "treehole_next/models"
	"treehole_next/openclaw"
)

func TestOnboardReturnsServiceUnavailableWhenLifecycleServiceIsNotInjected(t *testing.T) {
	previousService := lifecycleService
	SetLifecycleService(nil)
	t.Cleanup(func() {
		SetLifecycleService(previousService)
	})

	app := lifecycleTestApp("/claw/onboard", Onboard)
	req := httptest.NewRequest("POST", "/claw/onboard", nil)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, fiber.StatusServiceUnavailable, resp.StatusCode)
}

func TestOnboardRejectsMissingIdempotencyKey(t *testing.T) {
	previousService := lifecycleService
	SetLifecycleService(openclaw.NewLifecycleService(nil, nil, nil))
	t.Cleanup(func() {
		SetLifecycleService(previousService)
	})

	app := lifecycleTestApp("/claw/onboard", Onboard)
	req := httptest.NewRequest(
		"POST",
		"/claw/onboard",
		bytes.NewBufferString(`{"provider":"fleet"}`),
	)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
}

func TestGetInstanceReturnsSafeStatusWithoutProviderIdentifier(t *testing.T) {
	db := newClawWebSocketTestDB(t)
	previousDB := DB
	DB = db
	t.Cleanup(func() { DB = previousDB })

	require.NoError(t, db.Create(&OpenClawInstance{
		UserID:             42,
		Provider:           "fleet",
		ProviderInstanceID: "provider-secret-id",
		State:              "ready",
	}).Error)

	app := fiber.New(fiber.Config{ErrorHandler: common.ErrorHandler})
	app.Use(func(c *fiber.Ctx) error {
		c.Locals("user", &User{ID: 42})
		return c.Next()
	})
	app.Get("/claw/instance", GetInstance)
	req := httptest.NewRequest("GET", "/claw/instance", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, fiber.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NotContains(t, string(body), "provider-secret-id")
	require.NotContains(t, string(body), "provider_instance_id")
	require.Contains(t, string(body), `"instance_id":1`)
	require.Contains(t, string(body), `"state":"ready"`)
	require.Contains(t, string(body), `"status":"ready"`)
}

func lifecycleTestApp(path string, handler fiber.Handler) *fiber.App {
	app := fiber.New(fiber.Config{ErrorHandler: common.ErrorHandler})
	app.Use(func(c *fiber.Ctx) error {
		c.Locals("user", &User{ID: 42})
		return c.Next()
	})
	app.Post(path, handler)
	return app
}
