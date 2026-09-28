package server

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/calypr/syfon/internal/config"
	"github.com/calypr/syfon/internal/transfers"
)

func TestStartMultipartReconcilerAcceptsMaximumDurationInterval(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("int cannot represent the maximum duration seconds")
	}

	maxSeconds := int64((1<<63 - 1) / int64(time.Second))
	runtime := &serverRuntime{}
	service := transfers.NewService(transfers.Dependencies{})
	startMultipartReconciler(context.Background(), runtime, service, config.MultipartConfig{
		CleanupIntervalSeconds:    int(maxSeconds),
		InactiveTimeoutSeconds:    int(maxSeconds),
		CompletedRetentionSeconds: int(maxSeconds),
		BatchSize:                 1,
	})
	if runtime.multipartCancel == nil {
		t.Fatal("multipart reconciler did not start")
	}

	runtime.multipartCancel()
	runtime.multipartWG.Wait()
}
