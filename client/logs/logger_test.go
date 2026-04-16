package logs

import (
	"io"
	"log/slog"
	"os"
	"testing"
)

func TestNewSlogNoOpLogger(t *testing.T) {
	logger := NewSlogNoOpLogger()

	if logger == nil {
		t.Fatal("Expected non-nil logger")
	}

	// Verify it's a valid slog.Logger
	logger.Info("test message") // Should not panic
	logger.Error("test error")  // Should not panic
}

func TestNew_WithDefaults(t *testing.T) {
	profile := "test-profile"
	logger, cleanup := New(profile)
	defer cleanup()

	if logger == nil {
		t.Fatal("Expected non-nil logger")
	}

	if logger.Logger == nil {
		t.Error("Expected non-nil embedded slog logger")
	}
}

func TestNew_WithConsoleOption(t *testing.T) {
	profile := "test-profile"
	logger, cleanup := New(profile, WithConsole())
	defer cleanup()

	if logger == nil {
		t.Fatal("Expected non-nil logger")
	}

	// Test that we can log without errors
	logger.Info("test console message")
}

func TestNew_WithMessageFileOption(t *testing.T) {
	profile := "test-profile"
	logger, cleanup := New(profile, WithMessageFile())
	defer cleanup()

	if logger == nil {
		t.Fatal("Expected non-nil logger")
	}

	// Test that we can log without errors
	logger.Info("test file message")
}

func TestNew_WithScoreboardOption(t *testing.T) {
	profile := "test-profile"
	logger, cleanup := New(profile, WithScoreboard())
	defer cleanup()

	if logger == nil {
		t.Fatal("Expected non-nil logger")
	}

	if logger.scoreboard == nil {
		t.Error("Expected non-nil scoreboard when WithScoreboard option is used")
	}
}

func TestNew_WithFailedLogOption(t *testing.T) {
	profile := "test-profile"
	logger, cleanup := New(profile, WithFailedLog())
	defer cleanup()

	if logger == nil {
		t.Fatal("Expected non-nil logger")
	}

	if logger.failedPath == "" {
		t.Error("Expected non-empty failed path when WithFailedLog option is used")
	}
}

func TestNew_WithSucceededLogOption(t *testing.T) {
	profile := "test-profile"
	logger, cleanup := New(profile, WithSucceededLog())
	defer cleanup()

	if logger == nil {
		t.Fatal("Expected non-nil logger")
	}

	if logger.succeededPath == "" {
		t.Error("Expected non-empty succeeded path when WithSucceededLog option is used")
	}
}

func TestNew_WithBaseLogger(t *testing.T) {
	profile := "test-profile"
	baseLogger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	logger, cleanup := New(profile, WithBaseLogger(baseLogger))
	defer cleanup()

	if logger == nil {
		t.Fatal("Expected non-nil logger")
	}

	// Test that we can log without errors
	logger.Info("test with base logger")
}

func TestNew_WithMultipleOptions(t *testing.T) {
	profile := "test-profile"
	baseLogger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	logger, cleanup := New(profile,
		WithBaseLogger(baseLogger),
		WithConsole(),
		WithMessageFile(),
		WithScoreboard(),
	)
	defer cleanup()

	if logger == nil {
		t.Fatal("Expected non-nil logger")
	}

	if logger.Logger == nil {
		t.Error("Expected non-nil embedded slog logger")
	}

	if logger.scoreboard == nil {
		t.Error("Expected non-nil scoreboard")
	}

	// Test that we can log without errors
	logger.Info("test with multiple options")
}

func TestGen3Logger_Info(t *testing.T) {
	profile := "test-profile"
	logger, cleanup := New(profile)
	defer cleanup()

	// Should not panic
	logger.Info("test info message")
}

func TestGen3Logger_Error(t *testing.T) {
	profile := "test-profile"
	logger, cleanup := New(profile)
	defer cleanup()

	// Should not panic
	logger.Error("test error message")
}

func TestGen3Logger_Warn(t *testing.T) {
	profile := "test-profile"
	logger, cleanup := New(profile)
	defer cleanup()

	// Should not panic
	logger.Warn("test warning message")
}

func TestGen3Logger_Debug(t *testing.T) {
	profile := "test-profile"
	logger, cleanup := New(profile)
	defer cleanup()

	// Should not panic
	logger.Debug("test debug message")
}

func TestGen3Logger_Printf(t *testing.T) {
	profile := "test-profile"
	logger, cleanup := New(profile)
	defer cleanup()

	// Should not panic
	logger.Printf("test printf message: %s", "value")
}

func TestGen3Logger_Println(t *testing.T) {
	profile := "test-profile"
	logger, cleanup := New(profile)
	defer cleanup()

	// Should not panic
	logger.Println("test println message")
}

// testLogger implements the Logger interface for testing
type testLogger struct {
	writer io.Writer
}

func (l *testLogger) Printf(format string, v ...any) {}
func (l *testLogger) Println(v ...any)               {}
func (l *testLogger) Fatalf(format string, v ...any) {}
func (l *testLogger) Fatal(v ...any)                 {}
func (l *testLogger) Writer() io.Writer              { return l.writer }
