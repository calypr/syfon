package cmd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"

	syclient "github.com/calypr/syfon/client"
	"github.com/calypr/syfon/internal/crypto"
)

type bucketCommandConfig struct {
	Bucket       string
	Provider     string
	Region       string
	AccessKey    string
	SecretKey    string
	Endpoint     string
	Organization string
	ProjectID    string
}

func startSyfonServerProcessWithConfigPath(t *testing.T, configPath string, extraEnv map[string]string) *syfonServerProcess {
	t.Helper()

	rootDir := findRepoRoot(t)
	binaryPath := buildSyfonBinary(t, rootDir)
	serverPort := extractPortFromConfig(t, configPath)
	serverURL := fmt.Sprintf("http://127.0.0.1:%d", serverPort)

	cmd := exec.Command(binaryPath, "serve", "--config", configPath)
	cmd.Dir = rootDir
	cmd.Env = append(os.Environ(), crypto.CredentialMasterKeyEnv+"="+dockerE2ECredentialKey)
	for key, val := range extraEnv {
		cmd.Env = append(cmd.Env, key+"="+val)
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}

	stdoutBuf := &bytes.Buffer{}
	stderrBuf := &bytes.Buffer{}

	go streamToTestLog(t, "[SERVER STDOUT]", stdoutPipe, stdoutBuf)
	go streamToTestLog(t, "[SERVER STDERR]", stderrPipe, stderrBuf)

	if err := cmd.Start(); err != nil {
		t.Fatalf("start syfon server: %v", err)
	}

	waitErrCh := make(chan error, 1)
	go func() {
		waitErrCh <- cmd.Wait()
	}()

	if err := waitForServerReady(serverURL, waitErrCh, dockerE2EServerReadyWait); err != nil {
		logServerProcessOutput(t, serverURL, stdoutBuf, stderrBuf)
		stopSyfonServerProcess(t, &syfonServerProcess{cmd: cmd, waitErrCh: waitErrCh, stdout: stdoutBuf, stderr: stderrBuf})
		t.Fatalf("wait for server ready: %v", err)
	}

	return &syfonServerProcess{
		url:       serverURL,
		cmd:       cmd,
		waitErrCh: waitErrCh,
		stdout:    stdoutBuf,
		stderr:    stderrBuf,
	}
}

func extractPortFromConfig(t *testing.T, configPath string) int {
	t.Helper()

	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read config file %s: %v", configPath, err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "port:") {
			value := strings.TrimSpace(strings.TrimPrefix(line, "port:"))
			port, convErr := strconv.Atoi(value)
			if convErr != nil {
				t.Fatalf("parse port from %s: %v", configPath, convErr)
			}
			return port
		}
	}
	t.Fatalf("port is missing in %s", configPath)
	return 0
}

func writeProviderConfig(t *testing.T, content string) string {
	t.Helper()
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(configPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write provider config: %v", err)
	}
	return configPath
}

func exerciseAllClientCommands(t *testing.T, serverURL string, bucketCfg bucketCommandConfig) {
	t.Helper()

	versionOut, err := executeRootCommand(t, "--server", serverURL, "version")
	if err != nil {
		t.Fatalf("version failed: %v output=%s", err, versionOut)
	}
	if !strings.Contains(versionOut, "Syfon ") {
		t.Fatalf("unexpected version output: %s", versionOut)
	}

	pingOut, err := executeRootCommand(t, "--server", serverURL, "ping")
	if err != nil {
		t.Fatalf("ping failed: %v output=%s", err, pingOut)
	}
	if !strings.Contains(pingOut, "Syfon is reachable") {
		t.Fatalf("unexpected ping output: %s", pingOut)
	}

	srcPath := filepath.Join(t.TempDir(), "provider-source.txt")
	srcData := []byte("provider docker e2e payload")
	if err := os.WriteFile(srcPath, srcData, 0o644); err != nil {
		t.Fatalf("write source file: %v", err)
	}

	uploadOut, err := executeRootCommand(t, "--server", serverURL, "upload", "--file", srcPath, "--org", "syfon", "--project", "e2e")
	if err != nil {
		t.Fatalf("upload failed: %v output=%s", err, uploadOut)
	}
	uploadedID, err := parseUploadedObjectID(uploadOut)
	if err != nil {
		t.Fatalf("parse uploaded object id: %v output=%s", err, uploadOut)
	}
	fileName := filepath.Base(srcPath)

	lsOut, err := executeRootCommand(t, "--server", serverURL, "ls")
	if err != nil {
		t.Fatalf("ls failed: %v output=%s", err, lsOut)
	}
	if !strings.Contains(lsOut, fileName) {
		t.Fatalf("ls output missing uploaded file name %s (did %s): %s", fileName, uploadedID, lsOut)
	}

	downloadPath := filepath.Join(t.TempDir(), "provider-downloaded.txt")
	downloadOut, err := executeRootCommand(t, "--server", serverURL, "download", "--did", uploadedID, "--out", downloadPath)
	if err != nil {
		t.Fatalf("download failed: %v output=%s", err, downloadOut)
	}
	got, err := os.ReadFile(downloadPath)
	if err != nil {
		t.Fatalf("read downloaded file: %v", err)
	}
	if !bytes.Equal(got, srcData) {
		t.Fatalf("downloaded bytes mismatch")
	}

	sumOut, err := executeRootCommand(t, "--server", serverURL, "sha256sum", "--did", uploadedID)
	if err != nil {
		t.Fatalf("sha256sum failed: %v output=%s", err, sumOut)
	}
	expectedHash := sha256.Sum256(srcData)
	expectedSum := hex.EncodeToString(expectedHash[:])
	if !strings.Contains(sumOut, expectedSum) {
		t.Fatalf("sha256sum output missing expected hash %s: %s", expectedSum, sumOut)
	}

	client, err := syclient.New(serverURL)
	if err != nil {
		t.Fatalf("init client: %v", err)
	}
	rec, err := client.Index().Get(context.Background(), uploadedID)
	if err != nil {
		t.Fatalf("fetch uploaded record: %v", err)
	}
	if rec.Urls == nil || len(*rec.Urls) == 0 || strings.TrimSpace((*rec.Urls)[0]) == "" {
		t.Fatalf("uploaded record missing access url")
	}
	addURLDID := "33333333-3333-3333-3333-333333333333"
	addURLOut, err := executeRootCommand(
		t,
		"--server", serverURL,
		"add-url",
		"--did", addURLDID,
		"--url", (*rec.Urls)[0],
		"--org", "syfon",
		"--project", "e2e",
		"--name", fileName,
		"--size", strconv.Itoa(len(srcData)),
	)
	if err != nil {
		t.Fatalf("add-url failed: %v output=%s", err, addURLOut)
	}

	downloadPath2 := filepath.Join(t.TempDir(), "provider-add-url-downloaded.txt")
	downloadOut2, err := executeRootCommand(t, "--server", serverURL, "download", "--did", addURLDID, "--out", downloadPath2)
	if err != nil {
		t.Fatalf("download add-url object failed: %v output=%s", err, downloadOut2)
	}
	got2, err := os.ReadFile(downloadPath2)
	if err != nil {
		t.Fatalf("read add-url downloaded file: %v", err)
	}
	if !bytes.Equal(got2, srcData) {
		t.Fatalf("downloaded add-url bytes mismatch")
	}

	rmOut, err := executeRootCommand(t, "--server", serverURL, "rm", "--did", uploadedID)
	if err != nil {
		t.Fatalf("rm(uploaded) failed: %v output=%s", err, rmOut)
	}
	if !strings.Contains(rmOut, "removed "+uploadedID) {
		t.Fatalf("unexpected rm output for uploaded did: %s", rmOut)
	}

	rmOut2, err := executeRootCommand(t, "--server", serverURL, "rm", "--did", addURLDID)
	if err != nil {
		t.Fatalf("rm(add-url) failed: %v output=%s", err, rmOut2)
	}
	if !strings.Contains(rmOut2, "removed "+addURLDID) {
		t.Fatalf("unexpected rm output for add-url did: %s", rmOut2)
	}

	lsAfterRm, err := executeRootCommand(t, "--server", serverURL, "ls")
	if err != nil {
		t.Fatalf("ls after rm failed: %v output=%s", err, lsAfterRm)
	}
	if strings.Contains(lsAfterRm, fileName) || strings.Contains(lsAfterRm, addURLDID) {
		t.Fatalf("ls output still includes removed records: %s", lsAfterRm)
	}

	bucketName := strings.TrimSpace(bucketCfg.Bucket)
	if bucketName == "" {
		bucketName = "syfon-provider-cli-bucket"
	}
	providerName := strings.TrimSpace(bucketCfg.Provider)
	if providerName == "" {
		providerName = "s3"
	}
	org := strings.TrimSpace(bucketCfg.Organization)
	if org == "" {
		org = "syfon"
	}
	project := strings.TrimSpace(bucketCfg.ProjectID)
	if project == "" {
		project = "e2e"
	}

	bucketAddArgs := []string{
		"--server", serverURL,
		"bucket", "add", bucketName,
		"--provider", providerName,
		"--organization", org,
		"--project-id", project,
	}
	if v := strings.TrimSpace(bucketCfg.Region); v != "" {
		bucketAddArgs = append(bucketAddArgs, "--region", v)
	}
	if v := strings.TrimSpace(bucketCfg.AccessKey); v != "" {
		bucketAddArgs = append(bucketAddArgs, "--access-key", v)
	}
	if v := strings.TrimSpace(bucketCfg.SecretKey); v != "" {
		bucketAddArgs = append(bucketAddArgs, "--secret-key", v)
	}
	if v := strings.TrimSpace(bucketCfg.Endpoint); v != "" {
		bucketAddArgs = append(bucketAddArgs, "--endpoint", v)
	}

	bucketAddOut, err := executeRootCommand(t, bucketAddArgs...)
	if err != nil {
		t.Fatalf("bucket add failed: %v output=%s", err, bucketAddOut)
	}
	if !strings.Contains(bucketAddOut, "bucket configured: "+bucketName) {
		t.Fatalf("unexpected bucket add output: %s", bucketAddOut)
	}

	bucketListOut, err := executeRootCommand(t, "--server", serverURL, "bucket", "list")
	if err != nil {
		t.Fatalf("bucket list failed: %v output=%s", err, bucketListOut)
	}
	if !strings.Contains(bucketListOut, bucketName) {
		t.Fatalf("bucket list missing %s: %s", bucketName, bucketListOut)
	}

	bucketRemoveOut, err := executeRootCommand(t, "--server", serverURL, "bucket", "remove", bucketName)
	if err != nil {
		t.Fatalf("bucket remove failed: %v output=%s", err, bucketRemoveOut)
	}
	if !strings.Contains(bucketRemoveOut, "bucket removed: "+bucketName) {
		t.Fatalf("unexpected bucket remove output: %s", bucketRemoveOut)
	}

	bucketListOut2, err := executeRootCommand(t, "--server", serverURL, "bucket", "list")
	if err != nil {
		t.Fatalf("bucket list after remove failed: %v output=%s", err, bucketListOut2)
	}
	if strings.Contains(bucketListOut2, bucketName) {
		t.Fatalf("expected removed bucket %s to be absent from list: %s", bucketName, bucketListOut2)
	}

	headlineOut, err := executeRootCommand(t, "--server", serverURL, "ping")
	if err != nil {
		t.Fatalf("post-cleanup ping failed: %v output=%s", err, headlineOut)
	}
	if !strings.Contains(headlineOut, "Syfon is reachable") {
		t.Fatalf("unexpected post-cleanup ping output: %s", headlineOut)
	}
}
