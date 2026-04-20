package upload

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	internalapi "github.com/calypr/syfon/apigen/internalapi"
	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/xfer"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

type multipartResumeState struct {
	SourcePath      string         `json:"source_path"`
	ObjectKey       string         `json:"object_key"`
	GUID            string         `json:"guid"`
	Bucket          string         `json:"bucket"`
	FileSize        int64          `json:"file_size"`
	FileModUnixNano int64          `json:"file_mod_unix_nano"`
	ChunkSize       int64          `json:"chunk_size"`
	UploadID        string         `json:"upload_id"`
	FinalGUID       string         `json:"final_guid"`
	Key             string         `json:"key"`
	Completed       map[int]string `json:"completed"`
}

func MultipartUpload(ctx context.Context, bk xfer.Uploader, sourcePath, objectKey, guid, bucket string, metadata common.FileMetadata, file *os.File, showProgress bool) error {
	req := uploadRequest{
		sourcePath: sourcePath,
		objectKey:  objectKey,
		guid:       guid,
		bucket:     bucket,
		metadata:   metadata,
	}
	bk.Logger().DebugContext(ctx, "File Multipart Upload Request", "source_path", req.sourcePath, "object_key", req.objectKey, "guid", req.guid, "bucket", req.bucket)
	failUploadOnce := strings.TrimSpace(os.Getenv("DATA_CLIENT_TEST_FAIL_UPLOAD_PART_ONCE")) == "1"
	var injectedUploadFailure atomic.Bool

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("cannot stat file: %w", err)
	}

	fileSize := stat.Size()
	if fileSize == 0 {
		return fmt.Errorf("file is empty: %s", req.objectKey)
	}

	var p *mpb.Progress
	var bar *mpb.Bar
	if showProgress {
		p = mpb.New(mpb.WithOutput(os.Stdout))
		bar = p.AddBar(fileSize,
			mpb.PrependDecorators(
				decor.Name(req.objectKey+" "),
				decor.CountersKibiByte("%.1f / %.1f"),
			),
			mpb.AppendDecorators(
				decor.Percentage(),
				decor.AverageSpeed(decor.SizeB1024(0), " % .1f"),
			),
		)
	}

	chunkSize := OptimalChunkSize(fileSize)
	checkpointPath, err := multipartCheckpointPath(req.sourcePath, req.objectKey, req.guid, req.bucket)
	if err != nil {
		return err
	}
	state, loaded := loadMultipartState(checkpointPath)
	if !loaded || !state.matches(req, stat, chunkSize) {
		uploadID, finalGUID, initErr := initMultipartUpload(ctx, bk, req.guid, req.objectKey, req.bucket)
		if initErr != nil {
			return fmt.Errorf("failed to initiate multipart upload: %w", initErr)
		}
		state = &multipartResumeState{
			SourcePath:      req.sourcePath,
			ObjectKey:       req.objectKey,
			GUID:            req.guid,
			Bucket:          req.bucket,
			FileSize:        fileSize,
			FileModUnixNano: stat.ModTime().UnixNano(),
			ChunkSize:       chunkSize,
			UploadID:        uploadID,
			FinalGUID:       finalGUID,
			Key:             req.objectKey,
			Completed:       map[int]string{},
		}
		if saveErr := saveMultipartState(checkpointPath, state); saveErr != nil {
			return saveErr
		}
	}
	uploadID := state.UploadID
	key := state.Key
	bk.Logger().DebugContext(ctx, "Initialized Upload", "id", uploadID, "guid", state.FinalGUID, "key", key)

	numChunks := int((fileSize + chunkSize - 1) / chunkSize)

	chunks := make(chan int, numChunks)
	for partNum := 1; partNum <= numChunks; partNum++ {
		if _, ok := state.Completed[partNum]; ok {
			continue
		}
		chunks <- partNum
	}
	close(chunks)

	var (
		wg           sync.WaitGroup
		mu           sync.Mutex
		uploadErrors []error
		totalBytes   int64 // Atomic counter for monotonically increasing BytesSoFar
	)
	for partNum := range state.Completed {
		offset := int64(partNum-1) * chunkSize
		size := chunkSize
		if offset+size > fileSize {
			size = fileSize - offset
		}
		totalBytes += size
	}

	progressCallback := common.GetProgress(ctx)
	oid := common.GetOid(ctx)
	if oid == "" {
		oid = resolveUploadOID(req.objectKey, req.guid)
	}

	// 3. Worker logic
	worker := func() {
		defer wg.Done()

		for partNum := range chunks {
			if failUploadOnce && injectedUploadFailure.CompareAndSwap(false, true) {
				mu.Lock()
				uploadErrors = append(uploadErrors, fmt.Errorf("injected test interruption before multipart part %d", partNum))
				mu.Unlock()
				return
			}

			offset := int64(partNum-1) * chunkSize
			size := chunkSize
			if offset+size > fileSize {
				size = fileSize - offset
			}

			// SectionReader implements io.Reader, io.ReaderAt, and io.Seeker
			// It allows each worker to read its own segment without a shared buffer.
			section := io.NewSectionReader(file, offset, size)

			url, err := generateMultipartPresignedURL(ctx, bk, key, uploadID, partNum, req.bucket)
			if err != nil {
				mu.Lock()
				uploadErrors = append(uploadErrors, fmt.Errorf("URL generation failed part %d: %w", partNum, err))
				mu.Unlock()
				return
			}

			// Perform the upload using the section directly
			etag, err := bk.UploadPart(ctx, url, section, size)
			if err != nil {
				mu.Lock()
				uploadErrors = append(uploadErrors, fmt.Errorf("upload failed part %d: %w", partNum, err))
				mu.Unlock()
				return
			}

			mu.Lock()
			state.Completed[partNum] = etag
			if err := saveMultipartState(checkpointPath, state); err != nil {
				uploadErrors = append(uploadErrors, fmt.Errorf("failed to persist multipart resume checkpoint: %w", err))
				mu.Unlock()
				return
			}
			if bar != nil {
				bar.IncrInt64(size)
			}
			if progressCallback != nil {
				currentTotal := atomic.AddInt64(&totalBytes, size)
				_ = progressCallback(common.ProgressEvent{
					Event:          "progress",
					Oid:            oid,
					BytesSinceLast: size,
					BytesSoFar:     currentTotal,
				})
			}
			mu.Unlock()
		}
	}

	// Launch workers
	for range common.MaxConcurrentUploads {
		wg.Add(1)
		go worker()
	}
	wg.Wait()

	if p != nil {
		p.Wait()
	}

	if len(uploadErrors) > 0 {
		return fmt.Errorf("multipart upload failed with %d errors: %v", len(uploadErrors), uploadErrors)
	}

	// 5. Finalize the upload
	parts := make([]internalapi.InternalMultipartPart, 0, len(state.Completed))
	for partNum, etag := range state.Completed {
		parts = append(parts, internalapi.InternalMultipartPart{
			PartNumber: int32(partNum),
			ETag:       etag,
		})
	}
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	if err := CompleteMultipartUpload(ctx, bk, key, uploadID, parts, req.bucket); err != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	bk.Logger().DebugContext(ctx, "Successfully uploaded", "file", req.objectKey, "key", key)
	_ = os.Remove(checkpointPath)
	return nil
}

func initMultipartUpload(ctx context.Context, bk xfer.Uploader, guid, objectKey, bucketName string) (string, string, error) {
	uploadID, key, err := bk.InitMultipartUpload(ctx, guid, objectKey, bucketName)

	if err != nil {
		if strings.Contains(err.Error(), "404") {
			return "", "", errors.New(err.Error() + "\nPlease check to ensure FENCE version is at 2.8.0 or beyond")
		}
		return "", "", errors.New("Error has occurred during multipart upload initialization, detailed error message: " + err.Error())
	}

	if uploadID == "" || key == "" {
		return "", "", errors.New("unknown error has occurred during multipart upload initialization. Please check logs from Gen3 services")
	}
	return uploadID, key, nil
}

func generateMultipartPresignedURL(ctx context.Context, bk xfer.Uploader, key string, uploadID string, partNumber int, bucketName string) (string, error) {
	url, err := bk.GetMultipartUploadURL(ctx, key, uploadID, int32(partNumber), bucketName)
	if err != nil {
		return "", errors.New("Error has occurred during multipart upload presigned url generation, detailed error message: " + err.Error())
	}

	if url == "" {
		return "", errors.New("unknown error has occurred during multipart upload presigned url generation. Please check logs from Gen3 services")
	}
	return url, nil
}

func CompleteMultipartUpload(ctx context.Context, bk xfer.Uploader, key string, uploadID string, parts []internalapi.InternalMultipartPart, bucketName string) error {
	err := bk.CompleteMultipartUpload(ctx, key, uploadID, parts, bucketName)
	if err != nil {
		return errors.New("Error has occurred during completing multipart upload, detailed error message: " + err.Error())
	}
	return nil
}


func (s *multipartResumeState) matches(req uploadRequest, info os.FileInfo, chunkSize int64) bool {
	if s == nil {
		return false
	}
	return s.SourcePath == req.sourcePath &&
		s.ObjectKey == req.objectKey &&
		s.GUID == req.guid &&
		s.Bucket == req.bucket &&
		s.FileSize == info.Size() &&
		s.FileModUnixNano == info.ModTime().UnixNano() &&
		s.ChunkSize == chunkSize &&
		s.UploadID != "" &&
		s.Key != ""
}

func multipartCheckpointPath(sourcePath, objectKey, guid, bucket string) (string, error) {
	cacheDir := strings.TrimSpace(os.Getenv("DATA_CLIENT_CACHE_DIR"))
	if cacheDir == "" {
		var err error
		cacheDir, err = os.UserCacheDir()
		if err != nil || cacheDir == "" {
			cacheDir = os.TempDir()
		}
	}
	base := filepath.Join(cacheDir, "calypr", "data-client", "multipart-resume")
	if err := os.MkdirAll(base, 0o755); err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(sourcePath + "|" + objectKey + "|" + guid + "|" + bucket))
	name := hex.EncodeToString(sum[:]) + ".json"
	return filepath.Join(base, name), nil
}

func loadMultipartState(path string) (*multipartResumeState, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	var st multipartResumeState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, false
	}
	if st.Completed == nil {
		st.Completed = map[int]string{}
	}
	return &st, true
}

func saveMultipartState(path string, state *multipartResumeState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}
