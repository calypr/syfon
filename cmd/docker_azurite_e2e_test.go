package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	azcontainer "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	dockerE2EAzuriteImage       = "mcr.microsoft.com/azure-storage/azurite:3.35.0"
	dockerE2EAzuritePort        = "10000/tcp"
	dockerE2EAzureAccountName   = "devstoreaccount1"
	dockerE2EAzureAccountKey    = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	dockerE2EAzureContainerName = "syfone2eazurite"
)

type azuriteContainer struct {
	container  testcontainers.Container
	serviceURL string
	domain     string
	bucket     string
}

func TestSyfonDockerAzuriteE2E(t *testing.T) {
	if strings.TrimSpace(os.Getenv(dockerE2EEnvVar)) != "1" {
		t.Skipf("set %s=1 to run the Docker-backed Azurite integration test", dockerE2EEnvVar)
	}
	if testing.Short() {
		t.Skip("skipping Docker-backed Azurite integration test in short mode")
	}

	ctx := context.Background()
	azurite, err := startAzuriteContainer(ctx)
	if err != nil {
		if isDockerUnavailable(err) {
			t.Skipf("Docker unavailable for Azurite test: %v", err)
		}
		t.Fatalf("start Azurite container: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = azurite.container.Terminate(cleanupCtx)
	})

	if err := createAzuriteContainer(ctx, azurite.serviceURL, azurite.bucket); err != nil {
		t.Fatalf("create Azurite container %s: %v", azurite.bucket, err)
	}

	port := reserveTCPPort(t)
	dbPath := filepath.Join(t.TempDir(), "docker-azurite-e2e.db")
	configPath := writeScopedProviderConfig(t, providerServerConfig{
		Port:             port,
		DBPath:           dbPath,
		Bucket:           azurite.bucket,
		Provider:         "azure",
		AccessKey:        dockerE2EAzureAccountName,
		SecretKey:        dockerE2EAzureAccountKey,
		Endpoint:         azurite.serviceURL,
		BillingLogBucket: azurite.bucket,
		BillingLogPrefix: ".syfon/provider-transfer-events",
		Organization:     "syfon",
		ProjectID:        "e2e",
	})

	server := startSyfonServerProcessWithConfigPath(t, configPath, map[string]string{
		"AZURE_STORAGE_ACCOUNT":           dockerE2EAzureAccountName,
		"AZURE_STORAGE_KEY":               dockerE2EAzureAccountKey,
		"AZURE_STORAGE_DOMAIN":            azurite.domain,
		"AZURE_STORAGE_PROTOCOL":          "http",
		"AZURE_STORAGE_IS_LOCAL_EMULATOR": "true",
	})
	t.Cleanup(func() { stopSyfonServerProcess(t, server) })

	exerciseAllClientCommands(t, server.url, bucketCommandConfig{
		Bucket:       azurite.bucket,
		Provider:     "azure",
		Region:       "local",
		AccessKey:    dockerE2EAzureAccountName,
		SecretKey:    dockerE2EAzureAccountKey,
		Endpoint:     azurite.serviceURL,
		Organization: "syfon",
		ProjectID:    "e2e",
	})
}

func startAzuriteContainer(ctx context.Context) (*azuriteContainer, error) {
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        dockerE2EAzuriteImage,
			ExposedPorts: []string{dockerE2EAzuritePort},
			Cmd:          []string{"azurite-blob", "--blobHost", "0.0.0.0", "--blobPort", "10000", "--skipApiVersionCheck"},
			WaitingFor:   wait.ForListeningPort(dockerE2EAzuritePort).WithStartupTimeout(2 * time.Minute),
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
	port, err := container.MappedPort(ctx, dockerE2EAzuritePort)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, err
	}

	domain := fmt.Sprintf("%s:%s", host, port.Port())
	serviceURL := fmt.Sprintf("http://%s/%s", domain, dockerE2EAzureAccountName)
	return &azuriteContainer{
		container:  container,
		serviceURL: serviceURL,
		domain:     domain,
		bucket:     dockerE2EAzureContainerName,
	}, nil
}

func createAzuriteContainer(ctx context.Context, serviceURL string, containerName string) error {
	cred, err := azcontainer.NewSharedKeyCredential(dockerE2EAzureAccountName, dockerE2EAzureAccountKey)
	if err != nil {
		return err
	}
	containerURL := strings.TrimRight(serviceURL, "/") + "/" + strings.TrimSpace(containerName)
	client, err := azcontainer.NewClientWithSharedKeyCredential(containerURL, cred, nil)
	if err != nil {
		return err
	}
	if _, err := client.Create(ctx, nil); err != nil && !strings.Contains(strings.ToLower(err.Error()), "containeralreadyexists") {
		return err
	}
	return nil
}
