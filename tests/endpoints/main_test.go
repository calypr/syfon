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

const baseURL = "http://localhost:8080"

var (
	testStdout bytes.Buffer
	testStderr bytes.Buffer
)

func TestMain(m *testing.M) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startDir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	rootDir := findGoModRoot(startDir)
	if rootDir == "" {
		panic("could not find go.mod from " + startDir)
	}

	cmd := exec.CommandContext(ctx, "go", "run", "./cmd/server")
	cmd.Dir = rootDir

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

	// Ensure server is torn down after all tests.
	defer func() {
		// Terminate the process group.
		// log a statement if needed:
		if cmd.Process != nil {
			fmt.Printf("killing server process group %d\n", cmd.Process.Pid)
			// Send SIGINT to the process group (-PID).
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGINT)
			// Fallback: kill the main process if still alive.
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	}()

	// Wait for server to become ready once.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if time.Now().After(deadline) {
			panic("server did not become ready in time\nstdout:\n" +
				testStdout.String() + "\nstderr:\n" + testStderr.String())
		}
		resp, err := http.Get(baseURL + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				panic("healthz not OK at startup")
			}
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	code := m.Run()
	fmt.Printf("Tests Finished code: %d\n", code)
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
