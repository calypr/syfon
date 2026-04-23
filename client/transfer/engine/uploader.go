package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"strings"

	"github.com/calypr/syfon/client/common"
	"github.com/calypr/syfon/client/transfer"
)

type uploaderResumeState struct {
	SourcePath      string         `json:"source_path"`
	ObjectKey       string         `json:"object_key"`
	GUID            string         `json:"guid"`
	Bucket          string         `json:"bucket"`
	FileSize        int64          `json:"file_size"`
	FileModUnixNano int64          `json:"file_mod_unix_nano"`
	ChunkSize       int64          `json:"chunk_size"`
	UploadID        string         `json:"upload_id"`
	Completed       map[int]string `json:"completed"`
}

type GenericUploader struct {
	Backend transfer.Backend
}

type uploadURLResolver interface {
	ResolveUploadURL(ctx context.Context, guid string, filename string, metadata common.FileMetadata, bucket string) (string, error)
}

func effectiveObjectKey(req transfer.TransferRequest) string {
	if key := strings.TrimSpace(req.ObjectKey); key != "" {
		return key
	}
	return strings.TrimSpace(req.GUID)
}

func (u *GenericUploader) Upload(ctx context.Context, req transfer.TransferRequest, showProgress bool) error {
	file, err := os.Open(req.SourcePath)
	if err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat source: %w", err)
	}

	if req.ForceMultipart || stat.Size() >= common.FileSizeLimit {
		return u.uploadMultipart(ctx, req, file, stat.Size(), showProgress)
	}
	return u.uploadSingle(ctx, req, file, stat.Size(), showProgress)
}

func (u *GenericUploader) uploadSingle(ctx context.Context, req transfer.TransferRequest, file *os.File, size int64, showProgress bool) error {
	objectKey := effectiveObjectKey(req)
	uploadTarget := objectKey
	if uploader, ok := u.Backend.(uploadURLResolver); ok {
		presignedURL, err := uploader.ResolveUploadURL(ctx, req.GUID, objectKey, req.Metadata, req.Bucket)
		if err != nil {
			return err
		}
		uploadTarget = presignedURL
	}
	strategy := transfer.DefaultBackoff()
	if err := transfer.RetryAction(ctx, u.Backend.Logger(), strategy, common.MaxRetryCount, func() error {
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return err
		}
		return u.Backend.Upload(ctx, uploadTarget, file, size)
	}); err != nil {
		return err
	}

	emitProgress(ctx, size, size)
	return nil
}

func (u *GenericUploader) uploadMultipart(ctx context.Context, req transfer.TransferRequest, file *os.File, fileSize int64, showProgress bool) error {
	logger := u.Backend.Logger()
	chunkSize := OptimalChunkSize(fileSize)
	checkpointPath, err := CheckpointPath(req.SourcePath, req.GUID)
	if err != nil {
		return err
	}

	state, loaded := u.loadState(checkpointPath)
	stat, _ := file.Stat()
	objectKey := effectiveObjectKey(req)

	if !loaded || !u.matches(state, req, stat, chunkSize) {
		uploadID, err := u.Backend.MultipartInit(ctx, objectKey)
		if err != nil {
			return err
		}
		state = &uploaderResumeState{
			SourcePath:      req.SourcePath,
			ObjectKey:       objectKey,
			GUID:            req.GUID,
			Bucket:          req.Bucket,
			FileSize:        fileSize,
			FileModUnixNano: stat.ModTime().UnixNano(),
			ChunkSize:       chunkSize,
			UploadID:        uploadID,
			Completed:       map[int]string{},
		}
		u.saveState(checkpointPath, state)
	}

	numChunks := int((fileSize + chunkSize - 1) / chunkSize)
	chunks := make(chan int, numChunks)
	for i := 1; i <= numChunks; i++ {
		if _, ok := state.Completed[i]; !ok {
			chunks <- i
		}
	}
	close(chunks)

	var (
		wg         sync.WaitGroup
		mu         sync.Mutex
		uploadErr  error
		totalBytes int64
		progressed bool
	)

	for i := 0; i < common.MaxConcurrentUploads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for partNum := range chunks {
				offset := int64(partNum-1) * chunkSize
				partSize := chunkSize
				if offset+partSize > fileSize {
					partSize = fileSize - offset
				}

				strategy := transfer.DefaultBackoff()
				err := transfer.RetryAction(ctx, logger, strategy, common.MaxRetryCount, func() error {
					section := io.NewSectionReader(file, offset, partSize)
					etag, retryErr := u.Backend.MultipartPart(ctx, objectKey, state.UploadID, partNum, section)
					if retryErr != nil {
						return retryErr
					}

					mu.Lock()
					state.Completed[partNum] = etag
					u.saveState(checkpointPath, state)
					totalBytes += partSize
					progressed = true
					mu.Unlock()

					emitProgress(ctx, partSize, totalBytes)
					return nil
				})
				if err != nil {
					mu.Lock()
					uploadErr = err
					mu.Unlock()
					return
				}
			}
		}()
	}
	wg.Wait()

	if uploadErr != nil {
		return fmt.Errorf("multipart upload failed: %w", uploadErr)
	}

	if !progressed {
		emitProgress(ctx, fileSize, fileSize)
	}

	parts := make([]transfer.MultipartPart, 0, len(state.Completed))
	for num, etag := range state.Completed {
		parts = append(parts, transfer.MultipartPart{PartNumber: int32(num), ETag: etag})
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].PartNumber < parts[j].PartNumber })

	if err := u.Backend.MultipartComplete(ctx, objectKey, state.UploadID, parts); err != nil {
		return err
	}

	_ = os.Remove(checkpointPath)
	return nil
}

func (u *GenericUploader) loadState(path string) (*uploaderResumeState, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	var st uploaderResumeState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, false
	}
	if st.Completed == nil {
		st.Completed = map[int]string{}
	}
	return &st, true
}

func (u *GenericUploader) saveState(path string, state *uploaderResumeState) {
	data, _ := json.Marshal(state)
	_ = os.WriteFile(path, data, 0o644)
}

func (u *GenericUploader) matches(s *uploaderResumeState, req transfer.TransferRequest, info os.FileInfo, chunkSize int64) bool {
	if s == nil {
		return false
	}
	return s.SourcePath == req.SourcePath &&
		s.GUID == req.GUID &&
		s.ObjectKey == effectiveObjectKey(req) &&
		s.FileSize == info.Size() &&
		s.ChunkSize == chunkSize
}

func emitProgress(ctx context.Context, delta, total int64) {
	progress := common.GetProgress(ctx)
	if progress == nil {
		return
	}
	_ = progress(common.ProgressEvent{
		Event:          "progress",
		Oid:            common.GetOid(ctx),
		BytesSinceLast: delta,
		BytesSoFar:     total,
	})
}
