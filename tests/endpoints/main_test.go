package endpoints

import (
	"bytes"
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
	// 1. Build the binary once
	startDir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	rootDir := findGoModRoot(startDir)
	if rootDir == "" {
		panic("could not find go.mod root")
	}
	binaryPath := filepath.Join(rootDir, "drs-server-test-bin")

	buildCmd := exec.Command("go", "build", "-o", binaryPath, ".")
	buildCmd.Dir = rootDir
	if out, err := buildCmd.CombinedOutput(); err != nil {
		panic(fmt.Sprintf("failed to build binary: %v\n%s", err, string(out)))
	}
	defer os.Remove(binaryPath)

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

	// Ensure server is torn down after all tests.
	defer func() {
		if cmd.Process != nil {
			fmt.Printf("killing server process group %d\n", cmd.Process.Pid)
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGINT)
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	}()

	// Wait for server to become ready
	deadline := time.Now().Add(15 * time.Second)
	ready := false
	for time.Now().Before(deadline) {
		resp, err := http.Get(baseURL + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				ready = true
				break
			}
		}
		time.Sleep(500 * time.Millisecond)
	}

	if !ready {
		fmt.Printf("server did not become ready in time\nstdout:\n%s\nstderr:\n%s\n",
			testStdout.String(), testStderr.String())
		return 1
	}

	time.Sleep(1 * time.Second)
	code := m.Run()
	fmt.Printf("Tests Finished code: %d\n", code)
	return code
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
