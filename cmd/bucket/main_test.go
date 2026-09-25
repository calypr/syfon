package bucket

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/calypr/syfon/apigen/bucketapi"
	"github.com/calypr/syfon/cmd/cliauth"
	"github.com/spf13/cobra"
)

func TestAddPreservesStoredRegionWhenFlagIsOmitted(t *testing.T) {
	previousProvider, previousRegion := bucketProvider, bucketRegion
	previousAccessKey, previousSecretKey := bucketAccessKey, bucketSecretKey
	previousEndpoint, previousPath := bucketEndpoint, bucketPath
	t.Cleanup(func() {
		bucketProvider, bucketRegion = previousProvider, previousRegion
		bucketAccessKey, bucketSecretKey = previousAccessKey, previousSecretKey
		bucketEndpoint, bucketPath = previousEndpoint, previousPath
	})
	for _, name := range []string{"SYFON_PROFILE", "SYFON_TOKEN", "SYFON_USERNAME", "SYFON_PASSWORD"} {
		t.Setenv(name, "")
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")

	var stateMu sync.Mutex
	var gotPuts []bucketapi.PutBucketRequest
	var sawList, sawHead bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/data/buckets":
			stateMu.Lock()
			sawList = true
			stateMu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"S3_BUCKETS":{"test-bucket":{"bucket":"test-bucket","provider":"s3","region":"eu-west-2"}}}`))
		case r.Method == http.MethodHead:
			stateMu.Lock()
			sawHead = true
			stateMu.Unlock()
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPut && r.URL.Path == "/data/buckets":
			var request bucketapi.PutBucketRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode bucket update: %v", err)
			}
			stateMu.Lock()
			gotPuts = append(gotPuts, request)
			stateMu.Unlock()
			w.WriteHeader(http.StatusCreated)
		default:
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	}))
	defer server.Close()

	root := &cobra.Command{Use: "syfon"}
	root.PersistentFlags().String("server", server.URL, "")
	cliauth.RegisterRootFlags(root.PersistentFlags())
	for _, name := range []string{"profile", "token", "username", "password"} {
		if err := root.PersistentFlags().Set(name, ""); err != nil {
			t.Fatalf("clear --%s: %v", name, err)
		}
	}
	cmd := &cobra.Command{Use: "add <bucket>", RunE: addCmd.RunE}
	cmd.Flags().StringVar(&bucketProvider, "provider", "s3", "")
	cmd.Flags().StringVar(&bucketRegion, "region", "us-east-1", "")
	cmd.Flags().StringVar(&bucketAccessKey, "access-key", "", "")
	cmd.Flags().StringVar(&bucketSecretKey, "secret-key", "", "")
	cmd.Flags().StringVar(&bucketEndpoint, "endpoint", "", "")
	cmd.Flags().StringVar(&bucketPath, "path", "", "")
	root.AddCommand(cmd)
	cmd.SetContext(context.Background())
	if err := cmd.Flags().Set("endpoint", server.URL); err != nil {
		t.Fatalf("set --endpoint: %v", err)
	}
	if err := cmd.RunE(cmd, []string{"test-bucket"}); err != nil {
		t.Fatalf("bucket add: %v", err)
	}

	stateMu.Lock()
	if putCount := len(gotPuts); putCount != 1 {
		stateMu.Unlock()
		t.Fatalf("bucket puts = %d, want 1", putCount)
	}
	firstPut := gotPuts[0]
	listSeen, headSeen := sawList, sawHead
	stateMu.Unlock()
	if firstPut.Region != nil {
		t.Fatalf("update region = %q, want omitted so the stored eu-west-2 region is preserved", *firstPut.Region)
	}
	if !listSeen {
		t.Fatal("bucket add did not read stored bucket metadata before updating")
	}
	if firstPut.Provider == nil || *firstPut.Provider != "s3" {
		t.Fatalf("update provider = %v, want stored provider s3", firstPut.Provider)
	}
	if headSeen {
		t.Fatal("bucket add validated an existing credential without the stored secret")
	}

	if err := cmd.Flags().Set("region", "ca-central-1"); err != nil {
		t.Fatalf("set explicit update region: %v", err)
	}
	if err := cmd.RunE(cmd, []string{"test-bucket"}); err != nil {
		t.Fatalf("bucket add with explicit region: %v", err)
	}
	stateMu.Lock()
	if putCount := len(gotPuts); putCount != 2 {
		stateMu.Unlock()
		t.Fatalf("bucket puts after explicit update = %d, want 2", putCount)
	}
	secondPut := gotPuts[1]
	headSeen = sawHead
	stateMu.Unlock()
	if got := secondPut.Region; got == nil || *got != "ca-central-1" {
		t.Fatalf("explicit update region = %v, want ca-central-1", got)
	}
	if headSeen {
		t.Fatal("explicit metadata update unexpectedly validated with unavailable stored credentials")
	}

	regionFlag := cmd.Flags().Lookup("region")
	if err := regionFlag.Value.Set("us-east-1"); err != nil {
		t.Fatalf("reset create region: %v", err)
	}
	regionFlag.Changed = false
	if err := cmd.Flags().Set("access-key", "new-access"); err != nil {
		t.Fatalf("set create access key: %v", err)
	}
	if err := cmd.Flags().Set("secret-key", "new-secret"); err != nil {
		t.Fatalf("set create secret key: %v", err)
	}
	if err := cmd.RunE(cmd, []string{"new-bucket"}); err != nil {
		t.Fatalf("bucket add new bucket: %v", err)
	}
	stateMu.Lock()
	if putCount := len(gotPuts); putCount != 3 {
		stateMu.Unlock()
		t.Fatalf("bucket puts after create = %d, want 3", putCount)
	}
	thirdPut := gotPuts[2]
	headSeen = sawHead
	stateMu.Unlock()
	if got := thirdPut.Region; got == nil || *got != "us-east-1" {
		t.Fatalf("new bucket default region = %v, want us-east-1", got)
	}
	if !headSeen {
		t.Fatal("new s3 bucket was not validated before being saved")
	}
}
