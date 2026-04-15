package urlmanager

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	azcontainer "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/option"
)

const (
	mockServersEnvVar  = "SYFON_E2E_MOCK_SERVERS"
	fakeGCSImage       = "fsouza/fake-gcs-server:1.53.0"
	azuriteImage       = "mcr.microsoft.com/azure-storage/azurite:3.35.0"
	azuriteAccountName = "devstoreaccount1"
	azuriteAccountKey  = "Eby8vdM02xNOcqFeqCnf2g=="
)

func TestMVPMockServers_FakeGCSAndAzurite(t *testing.T) {
	if strings.TrimSpace(os.Getenv(mockServersEnvVar)) != "1" {
		t.Skipf("set %s=1 to run fake-gcs-server + Azurite MVP smoke tests", mockServersEnvVar)
	}
	if testing.Short() {
		t.Skip("skipping Docker-backed MVP smoke tests in short mode")
	}

	t.Run("fake-gcs-server", func(t *testing.T) {
		ctx := context.Background()
		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        fakeGCSImage,
				ExposedPorts: []string{"4443/tcp"},
				Cmd:          []string{"-scheme", "http", "-port", "4443"},
				WaitingFor:   wait.ForListeningPort("4443/tcp").WithStartupTimeout(2 * time.Minute),
			},
			Started: true,
		})
		if err != nil {
			if isDockerUnavailableForMockTests(err) {
				t.Skipf("Docker is unavailable for fake-gcs-server MVP smoke test: %v", err)
			}
			t.Fatalf("start fake-gcs-server container: %v", err)
		}
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_ = container.Terminate(cleanupCtx)
		})

		host, err := container.Host(ctx)
		if err != nil {
			t.Fatalf("resolve fake-gcs-server host: %v", err)
		}
		port, err := container.MappedPort(ctx, "4443/tcp")
		if err != nil {
			t.Fatalf("resolve fake-gcs-server mapped port: %v", err)
		}
		endpoint := fmt.Sprintf("http://%s:%s/storage/v1/", host, port.Port())

		client, err := storage.NewClient(ctx,
			option.WithEndpoint(endpoint),
			option.WithoutAuthentication(),
		)
		if err != nil {
			t.Fatalf("create gcs client: %v", err)
		}
		t.Cleanup(func() {
			if err := client.Close(); err != nil {
				t.Logf("warning: failed to close gcs client: %v", err)
			}
		})

		const bucketName = "mvp-fake-gcs-bucket"
		const objectName = "smoke/object.txt"
		payload := []byte("fake-gcs-server-mvp")

		if err := client.Bucket(bucketName).Create(ctx, "syfon-mvp", nil); err != nil {
			t.Fatalf("create bucket: %v", err)
		}

		writer := client.Bucket(bucketName).Object(objectName).NewWriter(ctx)
		if _, err := writer.Write(payload); err != nil {
			t.Fatalf("write object: %v", err)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("close writer: %v", err)
		}

		reader, err := client.Bucket(bucketName).Object(objectName).NewReader(ctx)
		if err != nil {
			t.Fatalf("open reader: %v", err)
		}
		t.Cleanup(func() {
			if err := reader.Close(); err != nil {
				t.Logf("warning: failed to close gcs reader: %v", err)
			}
		})

		buf, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("read object: %v", err)
		}
		if !bytes.Equal(buf, payload) {
			t.Fatalf("unexpected fake-gcs payload: got %q want %q", string(buf), string(payload))
		}
	})

	t.Run("azurite", func(t *testing.T) {

		ctx := context.Background()
		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        azuriteImage,
				ExposedPorts: []string{"10000/tcp"},
				Cmd:          []string{"azurite-blob", "--blobHost", "0.0.0.0", "--blobPort", "10000"},
				WaitingFor:   wait.ForListeningPort("10000/tcp").WithStartupTimeout(2 * time.Minute),
			},
			Started: true,
		})
		if err != nil {
			if isDockerUnavailableForMockTests(err) {
				t.Skipf("Docker is unavailable for Azurite MVP smoke test: %v", err)
			}
			t.Fatalf("start Azurite container: %v", err)
		}
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_ = container.Terminate(cleanupCtx)
		})

		host, err := container.Host(ctx)
		if err != nil {
			t.Fatalf("resolve Azurite host: %v", err)
		}
		port, err := container.MappedPort(ctx, "10000/tcp")
		if err != nil {
			t.Fatalf("resolve Azurite mapped port: %v", err)
		}

		const containerName = "mvpazuritecontainer"
		const blobName = "smoke-object.txt"
		payload := []byte("azurite-mvp")

		containerURL := fmt.Sprintf("http://%s:%s/%s/%s", host, port.Port(), azuriteAccountName, containerName)
		cred, err := azcontainer.NewSharedKeyCredential(azuriteAccountName, azuriteAccountKey)
		if err != nil {
			t.Fatalf("create Azurite shared key credential: %v", err)
		}
		client, err := azcontainer.NewClientWithSharedKeyCredential(containerURL, cred, nil)
		if err != nil {
			t.Fatalf("create Azurite container client: %v", err)
		}

		if _, err := client.Create(ctx, nil); err != nil {
			t.Fatalf("create Azurite blob container: %v", err)
		}

		blobClient := client.NewBlockBlobClient(blobName)
		if _, err := blobClient.UploadBuffer(ctx, payload, nil); err != nil {
			t.Fatalf("upload Azurite blob: %v", err)
		}

		downloaded := make([]byte, len(payload))
		if _, err := blobClient.DownloadBuffer(ctx, downloaded, nil); err != nil {
			t.Fatalf("download Azurite blob: %v", err)
		}
		if !bytes.Equal(downloaded, payload) {
			t.Fatalf("unexpected Azurite payload: got %q want %q", string(downloaded), string(payload))
		}
	})
}

func isDockerUnavailableForMockTests(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "docker daemon") ||
		strings.Contains(lower, "no such host") ||
		strings.Contains(lower, "cannot connect") ||
		strings.Contains(lower, "connection refused") ||
		strings.Contains(lower, "failed to create docker provider") ||
		strings.Contains(lower, "context deadline exceeded") ||
		strings.Contains(lower, "failed to create container")
}
