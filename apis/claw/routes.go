package claw

import (
	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog/log"
)

// RegisterRoutes 注册OpenClaw相关路由,尤其包含WebSocket端点
func RegisterRoutes(app fiber.Router) {
	log.Info().Msg("registering claw routes")
	// WebSocket 端点: /api/claw/ws
	app.Use("/claw/ws", func(c *fiber.Ctx) error {
		// 检查是否是WebSocket升级请求
		if websocket.IsWebSocketUpgrade(c) {
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	})

	app.Get("/claw/ws", websocket.New(HandleWebSocket))
	// WebSocket 端点: /api/claw/oc (与 /claw/ws 独立，不复用连接池或处理逻辑)
	app.Use("/claw/oc", func(c *fiber.Ctx) error {
		if websocket.IsWebSocketUpgrade(c) {
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	})
	app.Get("/claw/oc", websocket.New(HandleOpenClawWebSocket))
	app.Post("/claw/onboard", Onboard)
	app.Get("/claw/instance", GetInstance)
	app.Post("/claw/start", Start)
	app.Post("/claw/stop", Stop)
	app.Post("/claw/restart", Restart)
	app.Post("/claw/reset", Reset)
	app.Post("/claw/test", clawtest)
	app.Get("/claw/channels", ListChannels)
	app.Get("/claw/messages", ListMessages)
}
