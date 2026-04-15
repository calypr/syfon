package validate

import (
	"flag"
	"log/slog"
	"os"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/recover"
	"github.com/spf13/cobra"
)

const defaultSpecPath = "apigen/api/openapi.yaml"

var Cmd = &cobra.Command{
	Use:   "validate",
	Short: "Starts the validator HTTP server",
	Run: func(cmd *cobra.Command, args []string) {
		addr := flag.String("addr", ":8080", "listen address")
		debug := flag.Bool("debug", false, "enable debug logging")
		flag.Parse()

		specPath := os.Getenv("OPENAPI_SPEC")
		if specPath == "" {
			specPath = defaultSpecPath
		}

		level := slog.LevelInfo
		if *debug {
			level = slog.LevelDebug
		}
		logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
		slog.SetDefault(logger)

		validator, err := newSpecValidator(specPath, true)
		if err != nil {
			logger.Error("openapi validator setup failed", "err", err)
			os.Exit(1)
		}

		app := fiber.New()
		app.Use(recover.New())
		app.Use(validator)
		registerHealthzRoute(app)
		registerServiceInfoRoute(app)

		logger.Info("listening", "addr", *addr)
		if err := app.Listen(*addr); err != nil {
			logger.Error("validator server exited", "err", err)
		}
	},
}
