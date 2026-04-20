package validate

import "github.com/gofiber/fiber/v3"

func registerHealthzRoute(r fiber.Router) {
	r.Get("/healthz", func(c fiber.Ctx) error {
		return c.JSON(fiber.Map{"status": "ok"})
	})
}
