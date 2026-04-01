package endpoints

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

const baseURL = "http://localhost:9005"

var (
	testStdout bytes.Buffer
	testStderr bytes.Buffer
)

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	testStdout.Reset()
	testStderr.Reset()

	// 1. Build the binary once
	startDir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	rootDir := findGoModRoot(startDir)
	if rootDir == "" {
		panic("could not find go.mod root")
	}

	tmpFile, err := os.CreateTemp("", "syfon-test-bin-*")
	if err != nil {
		panic(err)
	}
	binaryPath := tmpFile.Name()
	if err := tmpFile.Close(); err != nil {
		panic(err)
	}
	if err := os.Chmod(binaryPath, 0o755); err != nil {
		panic(err)
	}
	defer os.Remove(binaryPath)

	buildCmd := exec.Command("go", "build", "-o", binaryPath, ".")
	buildCmd.Dir = rootDir
	if out, err := buildCmd.CombinedOutput(); err != nil {
		panic(fmt.Sprintf("failed to build binary: %v\n%s", err, string(out)))
	}

	cmd := exec.Command(binaryPath, "serve")
	cmd.Dir = rootDir
	cmd.Env = append(
		os.Environ(),
		"DRS_PORT=9005",
		"DRS_DB_SQLITE_FILE=drs_test.db",
		"DRS_AUTH_MODE=local",
	)

	// Put the child in its own process group so we can kill the whole tree.
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		panic(err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		panic(err)
	}

	go io.Copy(&testStdout, stdoutPipe)
	go io.Copy(&testStderr, stderrPipe)

	if err := cmd.Start(); err != nil {
		panic(err)
	}
	waitErrCh := make(chan error, 1)
	go func() {
		waitErrCh <- cmd.Wait()
	}()

	// Ensure server is torn down after all tests.
	defer func() {
		if cmd.Process != nil && cmd.ProcessState == nil {
			fmt.Printf("killing server process group %d\n", cmd.Process.Pid)
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGINT)
			_ = cmd.Process.Kill()
		}
		select {
		case <-waitErrCh:
		default:
		}
	}()

	if err := waitForServerReady(waitErrCh, 15*time.Second); err != nil {
		dumpServerOutput("server startup failed", err)
		return 1
	}

	code := m.Run()
	if code != 0 {
		dumpServerOutput("endpoint tests failed", nil)
	}
	fmt.Printf("Tests Finished code: %d\n", code)
	return code
}

func dumpServerOutput(prefix string, err error) {
	if err != nil {
		fmt.Printf("%s: %v\n", prefix, err)
	} else {
		fmt.Printf("%s\n", prefix)
	}
	fmt.Printf("stdout bytes: %d\nstdout:\n%s\n", testStdout.Len(), testStdout.String())
	fmt.Printf("stderr bytes: %d\nstderr:\n%s\n", testStderr.Len(), testStderr.String())
}

func waitForServerReady(waitErrCh <-chan error, timeout time.Duration) error {
	client := &http.Client{Timeout: 1 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	const requiredConsecutiveSuccesses = 2
	successes := 0
	interval := 100 * time.Millisecond

	for {
		select {
		case err := <-waitErrCh:
			return fmt.Errorf("server exited before ready: %w", err)
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for /healthz after %s", timeout)
		default:
		}

		resp, err := client.Get(baseURL + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				successes++
				if successes >= requiredConsecutiveSuccesses {
					return nil
				}
			} else {
				successes = 0
			}
		} else {
			successes = 0
		}

		timer := time.NewTimer(interval)
		select {
		case err := <-waitErrCh:
			timer.Stop()
			return fmt.Errorf("server exited before ready: %w", err)
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("timed out waiting for /healthz after %s", timeout)
		case <-timer.C:
		}
		if interval < 1*time.Second {
			interval *= 2
			if interval > 1*time.Second {
				interval = 1 * time.Second
			}
		}
	}
}

func findGoModRoot(startDir string) string {
	dir := startDir
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}
