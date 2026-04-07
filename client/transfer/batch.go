package transfer

import (
	"context"
	"sync"

	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/pkg/logs"
)

// BatchUpload orchestrates concurrent uploads using the new capability-based flow.
func BatchUpload(
	ctx context.Context,
	resolver Resolver,
	writer ObjectWriter,
	logger *logs.Gen3Logger,
	furObjects []common.FileUploadRequestObject,
	workers int,
	errCh chan error,
) {
	if len(furObjects) == 0 {
		return
	}

	workCh := make(chan common.FileUploadRequestObject, len(furObjects))
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fur := range workCh {
				if err := Upload(ctx, resolver, writer, fur, true); err != nil {
					logger.Failed(fur.SourcePath, fur.ObjectKey, fur.FileMetadata, fur.GUID, 0, false)
					errCh <- err
					continue
				}
				logger.Succeeded(fur.SourcePath, fur.GUID)
			}
		}()
	}

	for _, obj := range furObjects {
		workCh <- obj
	}
	close(workCh)
	wg.Wait()
}
