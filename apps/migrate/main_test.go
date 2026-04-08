package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/calypr/syfon/migrate"
)

func TestRunWithArgs_RequiresIndexdURL(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runWithArgs([]string{"--dry-run"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected error when --indexd-url is missing")
	}
	if !strings.Contains(err.Error(), "--indexd-url is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunWithArgs_Version(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runWithArgs([]string{"--version"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if got := strings.TrimSpace(stdout.String()); got != version {
		t.Fatalf("unexpected version output: %q", got)
	}
}

func TestRunWithArgs_DryRunSmoke(t *testing.T) {
	origRunner := migrationRunner
	t.Cleanup(func() { migrationRunner = origRunner })

	called := false
	migrationRunner = func(_ context.Context, cfg migrate.Config) (migrate.Stats, error) {
		called = true
		if cfg.IndexdURL != "https://indexd.example.org" {
			t.Fatalf("unexpected IndexdURL: %q", cfg.IndexdURL)
		}
		if cfg.SyfonURL != "http://127.0.0.1:8080" {
			t.Fatalf("unexpected SyfonURL: %q", cfg.SyfonURL)
		}
		if !cfg.DryRun {
			t.Fatal("expected DryRun=true")
		}
		if cfg.BatchSize != 25 || cfg.Limit != 10 {
			t.Fatalf("unexpected batch/limit: %d/%d", cfg.BatchSize, cfg.Limit)
		}
		if len(cfg.DefaultAuthz) != 2 || cfg.DefaultAuthz[0] != "/a" || cfg.DefaultAuthz[1] != "/b" {
			t.Fatalf("unexpected default authz: %#v", cfg.DefaultAuthz)
		}
		return migrate.Stats{Fetched: 10, Transformed: 10, Loaded: 10}, nil
	}

	var stdout, stderr bytes.Buffer
	err := runWithArgs([]string{
		"--indexd-url", "https://indexd.example.org",
		"--server", "http://127.0.0.1:8080",
		"--batch-size", "25",
		"--limit", "10",
		"--dry-run",
		"--default-authz", "/a,/b",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !called {
		t.Fatal("expected migrationRunner to be called")
	}
	out := stdout.String()
	if !strings.Contains(out, "dry-run: no records will be written to Syfon") {
		t.Fatalf("expected dry-run output, got: %s", out)
	}
	if !strings.Contains(out, "migration complete:") {
		t.Fatalf("expected completion output, got: %s", out)
	}
}

func TestRunWithArgs_PropagatesRunnerError(t *testing.T) {
	origRunner := migrationRunner
	t.Cleanup(func() { migrationRunner = origRunner })

	migrationRunner = func(_ context.Context, _ migrate.Config) (migrate.Stats, error) {
		return migrate.Stats{}, errors.New("boom")
	}

	var stdout, stderr bytes.Buffer
	err := runWithArgs([]string{"--indexd-url", "https://indexd.example.org"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "migration failed") {
		t.Fatalf("unexpected error: %v", err)
	}
}


