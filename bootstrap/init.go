package bootstrap

import (
	"context"

	"github.com/gofiber/fiber/v2"
	"github.com/opentreehole/go-common"
	"github.com/rs/zerolog/log"

	"treehole_next/apis"
	"treehole_next/apis/claw"
	"treehole_next/apis/hole"
	"treehole_next/apis/message"
	"treehole_next/config"
	"treehole_next/models"
	"treehole_next/openclaw"
	"treehole_next/utils"
	"treehole_next/utils/sensitive"

	"github.com/goccy/go-json"
	"github.com/gofiber/fiber/v2/middleware/pprof"
	"github.com/gofiber/fiber/v2/middleware/recover"
)

// Dependencies is the explicit Control Plane composition boundary. The
// bootstrap package does not construct a Fleet transport; callers must provide
// either a fully constructed lifecycle service or both provider-neutral
// dependencies.
type Dependencies struct {
	OpenClawLifecycle *openclaw.LifecycleService
	OpenClawProvider  openclaw.OpenClawInstanceProvider
	OpenClawReadiness openclaw.ReadinessChecker
}

func Init() (*fiber.App, context.CancelFunc) {
	return InitWithDependencies(Dependencies{})
}

func InitWithDependencies(deps Dependencies) (*fiber.App, context.CancelFunc) {
	config.InitConfig()
	utils.InitCache()
	sensitive.InitSensitiveLabelMap()
	models.Init()
	models.InitDB()
	models.InitAdminList()
	configureOpenClawLifecycle(deps)

	app := fiber.New(fiber.Config{
		ErrorHandler:          common.ErrorHandler,
		JSONEncoder:           json.Marshal,
		JSONDecoder:           json.Unmarshal,
		DisableStartupMessage: true,
	})
	registerMiddlewares(app)
	apis.RegisterRoutes(app)

	return app, startTasks()
}

func configureOpenClawLifecycle(deps Dependencies) *openclaw.LifecycleService {
	if deps.OpenClawLifecycle != nil {
		claw.SetLifecycleService(deps.OpenClawLifecycle)
		return deps.OpenClawLifecycle
	}
	if deps.OpenClawProvider == nil && deps.OpenClawReadiness == nil {
		claw.SetLifecycleService(nil)
		return nil
	}
	if deps.OpenClawProvider == nil || deps.OpenClawReadiness == nil {
		log.Error().Msg("OpenClaw lifecycle dependencies are incomplete; lifecycle routes remain unavailable")
		claw.SetLifecycleService(nil)
		return nil
	}
	service := openclaw.NewLifecycleService(models.DB, deps.OpenClawProvider, deps.OpenClawReadiness)
	claw.SetLifecycleService(service)
	return service
}

func registerMiddlewares(app *fiber.App) {
	app.Use(recover.New(recover.Config{EnableStackTrace: true}))
	app.Use(common.MiddlewareGetUserID)
	if config.Config.Mode != "bench" {
		app.Use(common.MiddlewareCustomLogger)
	}
	app.Use(pprof.New())
}

func startTasks() context.CancelFunc {
	ctx, cancel := context.WithCancel(context.Background())
	go hole.UpdateHoleViews(ctx)
	go hole.PurgeHole(ctx)
	go message.PurgeMessage()
	// go models.UpdateAdminList(ctx)
	go sensitive.UpdateSensitiveLabelMap(ctx)
	return cancel
}
