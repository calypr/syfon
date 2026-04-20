package validate

import (
	"net/http"
	"os"
	"time"

	"github.com/gofiber/fiber/v3"
)

func registerServiceInfoRoute(r fiber.Router) {
	r.Get("/service-info", func(c fiber.Ctx) error {
		return c.Status(http.StatusOK).JSON(fiber.Map{
			"name":      "my-service",
			"version":   os.Getenv("SERVICE_VERSION"),
			"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		})
	})
}
