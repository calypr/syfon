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

	usr, _ := user.Current()
	logDir := filepath.Join(usr.HomeDir, ".gen3", "logs")
	os.MkdirAll(logDir, 0755)

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
			fmt.Fprintf(f, "[%s] Message log started\n", time.Now().Format(time.RFC3339))
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
			fmt.Fprintf(messageFile, "[%s] Message log stopped\n", time.Now().Format(time.RFC3339))
			messageFile.Close()
		}
	}

	return t, cleanup
}
