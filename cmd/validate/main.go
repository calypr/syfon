package validate

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/recover"
	"github.com/spf13/cobra"
)

const defaultSpecPath = "apigen/openapi/openapi.yaml"

var Cmd = newCommand()

func newCommand() *cobra.Command {
	var addr string
	var debug bool

	cmd := &cobra.Command{
		Use:   "validate",
		Short: "Starts the validator HTTP server",
		RunE: func(cmd *cobra.Command, args []string) error {
			specPath := os.Getenv("OPENAPI_SPEC")
			if specPath == "" {
				specPath = defaultSpecPath
			}

			level := slog.LevelInfo
			if debug {
				level = slog.LevelDebug
			}
			logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
			slog.SetDefault(logger)

			validator, err := newSpecValidator(specPath, true)
			if err != nil {
				return fmt.Errorf("openapi validator setup failed: %w", err)
			}

			app := fiber.New()
			app.Use(recover.New())
			app.Use(validator)
			registerHealthzRoute(app)
			registerServiceInfoRoute(app)

			logger.Info("listening", "addr", addr)
			if err := app.Listen(addr); err != nil {
				return fmt.Errorf("validator server exited: %w", err)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&addr, "addr", ":8080", "listen address")
	cmd.Flags().BoolVar(&debug, "debug", false, "enable debug logging")
	return cmd
}
