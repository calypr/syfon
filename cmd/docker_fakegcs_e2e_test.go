package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	dockerE2EFakeGCSImage  = "fsouza/fake-gcs-server:1.53.0"
	dockerE2EFakeGCSPort   = "4443/tcp"
	dockerE2EFakeGCSBucket = "syfon-e2e-fakegcs-bucket"
)

type fakeGCSContainer struct {
	container testcontainers.Container
	endpoint  string
	bucket    string
}

func TestSyfonDockerFakeGCSE2E(t *testing.T) {
	if strings.TrimSpace(os.Getenv(dockerE2EEnvVar)) != "1" {
		t.Skipf("set %s=1 to run the Docker-backed fake-gcs-server integration test", dockerE2EEnvVar)
	}
	if testing.Short() {
		t.Skip("skipping Docker-backed fake-gcs-server integration test in short mode")
	}

	ctx := context.Background()
	fakeGCS, err := startFakeGCSContainer(ctx)
	if err != nil {
		if isDockerUnavailable(err) {
			t.Skipf("Docker unavailable for fake-gcs-server test: %v", err)
		}
		t.Fatalf("start fake-gcs-server container: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = fakeGCS.container.Terminate(cleanupCtx)
	})

	if err := createFakeGCSBucket(ctx, fakeGCS.endpoint, fakeGCS.bucket); err != nil {
		t.Fatalf("create fake-gcs bucket: %v", err)
	}

	port := reserveTCPPort(t)
	dbPath := filepath.Join(t.TempDir(), "docker-fakegcs-e2e.db")
	configPath := writeProviderConfig(t, fmt.Sprintf(`port: %d
auth:
  mode: local
routes:
  ga4gh: true
  internal: true
database:
  sqlite:
    file: %q
s3_credentials:
  - bucket: %q
    provider: %q
    endpoint: %q
    billing_log_bucket: %q
    billing_log_prefix: %q
`, port, dbPath, fakeGCS.bucket, "gcs", fakeGCS.endpoint, fakeGCS.bucket, ".syfon/provider-transfer-events"))

	server := startSyfonServerProcessWithConfigPath(t, configPath, map[string]string{
		"STORAGE_EMULATOR_HOST": strings.TrimPrefix(strings.TrimPrefix(fakeGCS.endpoint, "http://"), "https://"),
	})
	t.Cleanup(func() { stopSyfonServerProcess(t, server) })

	exerciseAllClientCommands(t, server.url, bucketCommandConfig{
		Bucket:       fakeGCS.bucket,
		Provider:     "gcs",
		Region:       "us-central1",
		Endpoint:     fakeGCS.endpoint,
		Organization: "syfon",
		ProjectID:    "e2e",
	})
}

func startFakeGCSContainer(ctx context.Context) (*fakeGCSContainer, error) {
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        dockerE2EFakeGCSImage,
			ExposedPorts: []string{dockerE2EFakeGCSPort},
			Cmd:          []string{"-scheme", "http", "-port", "4443"},
			WaitingFor:   wait.ForListeningPort(dockerE2EFakeGCSPort).WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		return nil, err
	}

	host, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, err
	}
	port, err := container.MappedPort(ctx, dockerE2EFakeGCSPort)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, err
	}

	return &fakeGCSContainer{
		container: container,
		endpoint:  fmt.Sprintf("http://%s:%s", host, port.Port()),
		bucket:    dockerE2EFakeGCSBucket,
	}, nil
}

func createFakeGCSBucket(ctx context.Context, endpoint string, bucket string) error {
	body, err := json.Marshal(map[string]string{"name": bucket})
	if err != nil {
		return err
	}
	url := strings.TrimRight(endpoint, "/") + "/storage/v1/b?project=syfon-e2e"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("create bucket failed status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return nil
}
