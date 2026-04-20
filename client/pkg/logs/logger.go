package logs

import (
	"log/slog"
)

type Option func(*config)

type config struct {
	console          bool
	messageFile      bool
	failedLog        bool
	succeededLog     bool
	enableScoreboard bool
	baseLogger       *slog.Logger
}

func WithConsole() Option                     { return func(c *config) { c.console = true } }
func WithNoConsole() Option                   { return func(c *config) { c.console = false } }
func WithMessageFile() Option                 { return func(c *config) { c.messageFile = true } }
func WithNoMessageFile() Option               { return func(c *config) { c.messageFile = false } }
func WithFailedLog() Option                   { return func(c *config) { c.failedLog = true } }
func WithSucceededLog() Option                { return func(c *config) { c.succeededLog = true } }
func WithScoreboard() Option                  { return func(c *config) { c.enableScoreboard = true } }
func WithBaseLogger(base *slog.Logger) Option { return func(c *config) { c.baseLogger = base } }

func defaults() *config {
	return &config{
		console:      true,
		messageFile:  true,
		failedLog:    true,
		succeededLog: true,
		baseLogger:   nil,
	}
}
