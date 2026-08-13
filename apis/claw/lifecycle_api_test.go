package claw

import (
	"bytes"
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

func lifecycleTestApp(path string, handler fiber.Handler) *fiber.App {
	app := fiber.New(fiber.Config{ErrorHandler: common.ErrorHandler})
	app.Use(func(c *fiber.Ctx) error {
		c.Locals("user", &User{ID: 42})
		return c.Next()
	})
	app.Post(path, handler)
	return app
}
