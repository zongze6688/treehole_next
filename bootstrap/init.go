package bootstrap

import (
	"context"
	"time"

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

// Dependencies is the explicit Control Plane composition boundary. Tests and
// embedders may inject a fully constructed lifecycle service or both
// provider-neutral dependencies; the production path composes a real Fleet
// transport via buildDependenciesFromConfig (see Init).
type Dependencies struct {
	OpenClawLifecycle        *openclaw.LifecycleService
	OpenClawProvider         openclaw.OpenClawInstanceProvider
	OpenClawReadiness        openclaw.ReadinessChecker
	OpenClawWorkloadIdentity openclaw.WorkloadIdentity
}

func Init() (*fiber.App, context.CancelFunc) {
	// Load config before building deps. The second InitConfig call inside
	// InitWithDependencies is intentionally tolerated: env.Parse is idempotent.
	config.InitConfig()
	return InitWithDependencies(buildDependenciesFromConfig())
}

// buildDependenciesFromConfig composes the real Fleet-backed dependencies when
// the control plane is configured via OPENCLAW_FLEET_ENABLED. It fails closed
// (empty Dependencies) otherwise.
func buildDependenciesFromConfig() Dependencies {
	if !config.Config.OpenClawFleetEnabled {
		return Dependencies{}
	}
	transport := openclaw.NewFleetCLITransport(openclaw.FleetCLIOptions{
		Binary:  config.Config.OpenClawFleetBinary,
		Image:   config.Config.OpenClawFleetImage,
		Runtime: config.Config.OpenClawFleetRuntime,
	})
	provider := openclaw.NewFleetInstanceProvider(transport, openclaw.FleetProviderOptions{})
	readiness := openclaw.NewFleetReadiness(transport.CellStatus, claw.IsChannelAuthenticated)
	readiness.Wait = time.Duration(config.Config.OpenClawChannelWaitSeconds) * time.Second
	deps := Dependencies{
		OpenClawProvider:  provider,
		OpenClawReadiness: readiness.ReadinessChecker(),
	}
	// The workload identity is only wired when a provision key is configured;
	// without one every Env call would fail at runtime for each onboard.
	if config.OpenClawSecrets.ProvisionKey != "" {
		deps.OpenClawWorkloadIdentity = openclaw.NewHTTPWorkloadIdentity(openclaw.HTTPWorkloadIdentityOptions{
			BaseURL:      config.Config.AuthUrl,
			ProvisionKey: config.OpenClawSecrets.ProvisionKey,
			WSUrl:        config.Config.OpenClawDantaWsURL,
		})
	}
	return deps
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
	service.SetWorkloadIdentity(deps.OpenClawWorkloadIdentity)
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
