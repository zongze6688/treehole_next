package claw

import (
	"context"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/opentreehole/go-common"
	"gorm.io/gorm"

	. "treehole_next/models"
	"treehole_next/openclaw"
)

var lifecycleService *openclaw.LifecycleService

// SetLifecycleService injects the provider-backed lifecycle service at process
// startup. Fleet transport construction remains outside this package.
func SetLifecycleService(service *openclaw.LifecycleService) {
	lifecycleService = service
}

type lifecycleRequest struct {
	Provider string            `json:"provider"`
	Name     string            `json:"name"`
	Image    string            `json:"image"`
	Metadata map[string]string `json:"metadata"`
}

type instanceStatusResponse struct {
	InstanceID          uint   `json:"instance_id,omitempty"`
	State               string `json:"state"`
	Status              string `json:"status"`
	LastErrorCode       string `json:"last_error_code,omitempty"`
	LastErrorMessage    string `json:"last_error_message,omitempty"`
	CleanupErrorCode    string `json:"cleanup_error_code,omitempty"`
	CleanupErrorMessage string `json:"cleanup_error_message,omitempty"`
}

func currentUserID(c *fiber.Ctx) (int, error) {
	user, err := GetCurrLoginUser(c)
	if err != nil {
		return 0, err
	}
	return user.ID, nil
}

func requireIdempotencyKey(c *fiber.Ctx) (string, error) {
	key := strings.TrimSpace(c.Get("Idempotency-Key"))
	if key == "" {
		return "", common.BadRequest("Idempotency-Key is required")
	}
	return key, nil
}

func GetInstance(c *fiber.Ctx) error {
	userID, err := currentUserID(c)
	if err != nil {
		return err
	}
	var instance OpenClawInstance
	err = DB.Where("user_id = ?", userID).First(&instance).Error
	if err == gorm.ErrRecordNotFound {
		return c.JSON(instanceStatusResponse{
			State:  string(openclaw.StateNotStarted),
			Status: string(openclaw.StateNotStarted),
		})
	}
	if err != nil {
		return common.BadRequest("获取实例状态失败")
	}
	return c.JSON(instanceStatusResponse{
		InstanceID:          instance.ID,
		State:               instance.State,
		Status:              instance.State,
		LastErrorCode:       instance.LastErrorCode,
		LastErrorMessage:    instance.LastErrorMessage,
		CleanupErrorCode:    instance.CleanupErrorCode,
		CleanupErrorMessage: instance.CleanupErrorMessage,
	})
}

func Onboard(c *fiber.Ctx) error {
	if lifecycleService == nil {
		return fiber.ErrServiceUnavailable
	}
	userID, err := currentUserID(c)
	if err != nil {
		return err
	}
	key, err := requireIdempotencyKey(c)
	if err != nil {
		return err
	}
	var body lifecycleRequest
	if err := c.BodyParser(&body); err != nil {
		return common.BadRequest("请求格式错误")
	}
	result, err := lifecycleService.Onboard(context.Background(), userID, key, openclaw.OnboardRequest{
		Provider: body.Provider,
		Name:     body.Name,
		Image:    body.Image,
		Metadata: body.Metadata,
	})
	if err != nil {
		return common.BadRequest("OpenClaw onboard failed")
	}
	return c.JSON(result)
}

func lifecycleAction(
	c *fiber.Ctx,
	action func(context.Context, int, string) (*openclaw.LifecycleResult, error),
) error {
	if lifecycleService == nil {
		return fiber.ErrServiceUnavailable
	}
	userID, err := currentUserID(c)
	if err != nil {
		return err
	}
	key, err := requireIdempotencyKey(c)
	if err != nil {
		return err
	}
	result, err := action(context.Background(), userID, key)
	if err != nil {
		return common.BadRequest("OpenClaw lifecycle operation failed")
	}
	return c.JSON(result)
}

func Start(c *fiber.Ctx) error {
	return lifecycleAction(c, func(ctx context.Context, userID int, key string) (*openclaw.LifecycleResult, error) {
		return lifecycleService.Start(ctx, userID, key)
	})
}

func Stop(c *fiber.Ctx) error {
	return lifecycleAction(c, func(ctx context.Context, userID int, key string) (*openclaw.LifecycleResult, error) {
		return lifecycleService.Stop(ctx, userID, key)
	})
}

func Restart(c *fiber.Ctx) error {
	return lifecycleAction(c, func(ctx context.Context, userID int, key string) (*openclaw.LifecycleResult, error) {
		return lifecycleService.Restart(ctx, userID, key)
	})
}

func Reset(c *fiber.Ctx) error {
	return lifecycleAction(c, func(ctx context.Context, userID int, key string) (*openclaw.LifecycleResult, error) {
		return lifecycleService.Reset(ctx, userID, key)
	})
}
