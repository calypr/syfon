package logs

import (
	"fmt"
	"log/slog"
	"os"
	"os/user"
	"path/filepath"
	"time"
)

func New(profile string, opts ...Option) (*Gen3Logger, func()) {
	cfg := defaults()
	for _, o := range opts {
		o(cfg)
	}

	logDir := filepath.Join(userHomeDir(), ".gen3", "logs")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "create log directory %s: %v\n", logDir, err)
		logDir = os.TempDir()
	}

	var handlers []slog.Handler
	var messageFile *os.File

	if cfg.baseLogger != nil {
		handlers = append(handlers, cfg.baseLogger.Handler())
	}

	if cfg.console {
		handlers = append(handlers, slog.NewTextHandler(os.Stderr, nil))
	}

	if cfg.messageFile {
		filename := fmt.Sprintf("%s_message_%s_%d.log",
			profile,
			time.Now().Format("20060102150405MST"),
			os.Getpid(),
		)
		f, err := os.OpenFile(filepath.Join(logDir, filename), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err == nil {
			messageFile = f
			handlers = append(handlers, slog.NewTextHandler(f, nil))
			if _, err := fmt.Fprintf(f, "[%s] Message log started\n", time.Now().Format(time.RFC3339)); err != nil {
				fmt.Fprintf(os.Stderr, "write message log start banner: %v\n", err)
			}
		} else {
			fmt.Fprintf(os.Stderr, "open message log file: %v\n", err)
		}
	}

	var rootHandler slog.Handler
	if len(handlers) == 0 {
		rootHandler = slog.NewTextHandler(os.Stderr, nil)
	} else if len(handlers) == 1 {
		rootHandler = handlers[0]
	} else {
		rootHandler = NewTeeHandler(handlers...)
	}

	sl := slog.New(NewProgressHandler(rootHandler))

	t := NewGen3Logger(sl, logDir, profile)

	if cfg.enableScoreboard {
		t.scoreboard = NewSB(5, t.Logger)
	}

	if cfg.failedLog {
		t.failedPath = filepath.Join(logDir, profile+"_failed.json")
		loadJSON(t.failedPath, &t.FailedMap)
	}

	if cfg.succeededLog {
		t.succeededPath = filepath.Join(logDir, profile+"_succeeded.json")
		loadJSON(t.succeededPath, &t.succeededMap)
	}

	cleanup := func() {
		if messageFile != nil {
			if _, err := fmt.Fprintf(messageFile, "[%s] Message log stopped\n", time.Now().Format(time.RFC3339)); err != nil {
				fmt.Fprintf(os.Stderr, "write message log stop banner: %v\n", err)
			}
			if err := messageFile.Close(); err != nil {
				fmt.Fprintf(os.Stderr, "close message log file: %v\n", err)
			}
		}
	}

	return t, cleanup
}

func userHomeDir() string {
	usr, err := user.Current()
	if err == nil && usr.HomeDir != "" {
		return usr.HomeDir
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return home
	}
	return os.TempDir()
}
