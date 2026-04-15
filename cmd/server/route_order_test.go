package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/calypr/syfon/config"
	"github.com/gofiber/fiber/v3"
)

func TestGinStaticRouteOverridesParamRoute(t *testing.T) {
	app := fiber.New()
	app.Post(config.RouteInternalIndex+"/register", func(c fiber.Ctx) error {
		return c.SendStatus(http.StatusCreated)
	})
	app.Post(config.RouteInternalIndex+"/:object_id", func(c fiber.Ctx) error {
		return c.SendStatus(http.StatusTeapot)
	})

	req := httptest.NewRequest(http.MethodPost, "/index/register", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected static route to win, got %d", resp.StatusCode)
	}
}
