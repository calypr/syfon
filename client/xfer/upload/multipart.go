package upload

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/transfer"
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

func MultipartUpload(ctx context.Context, bk transfer.Uploader, req common.FileUploadRequestObject, file *os.File, showProgress bool) error {
	bk.Logger().DebugContext(ctx, "File Multipart Upload Request", "request", req)
	failUploadOnce := strings.TrimSpace(os.Getenv("DATA_CLIENT_TEST_FAIL_UPLOAD_PART_ONCE")) == "1"
	var injectedUploadFailure atomic.Bool

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("cannot stat file: %w", err)
	}

	fileSize := stat.Size()
	if fileSize == 0 {
		return fmt.Errorf("file is empty: %s", req.ObjectKey)
	}

	var p *mpb.Progress
	var bar *mpb.Bar
	if showProgress {
		p = mpb.New(mpb.WithOutput(os.Stdout))
		bar = p.AddBar(fileSize,
			mpb.PrependDecorators(
				decor.Name(req.ObjectKey+" "),
				decor.CountersKibiByte("%.1f / %.1f"),
			),
			mpb.AppendDecorators(
				decor.Percentage(),
				decor.AverageSpeed(decor.SizeB1024(0), " % .1f"),
			),
		)
	}

	chunkSize := OptimalChunkSize(fileSize)
	checkpointPath, err := multipartCheckpointPath(req)
	if err != nil {
		return err
	}
	state, loaded := loadMultipartState(checkpointPath)
	if !loaded || !state.matches(req, stat, chunkSize) {
		uploadID, finalGUID, initErr := initMultipartUpload(ctx, bk, req, req.Bucket)
		if initErr != nil {
			return fmt.Errorf("failed to initiate multipart upload: %w", initErr)
		}
		state = &multipartResumeState{
			SourcePath:      req.SourcePath,
			ObjectKey:       req.ObjectKey,
			GUID:            req.GUID,
			Bucket:          req.Bucket,
			FileSize:        fileSize,
			FileModUnixNano: stat.ModTime().UnixNano(),
			ChunkSize:       chunkSize,
			UploadID:        uploadID,
			FinalGUID:       finalGUID,
			Key:             req.ObjectKey,
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
		oid = resolveUploadOID(req)
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

			url, err := generateMultipartPresignedURL(ctx, bk, key, uploadID, partNum, req.Bucket)
			if err != nil {
				mu.Lock()
				uploadErrors = append(uploadErrors, fmt.Errorf("URL generation failed part %d: %w", partNum, err))
				mu.Unlock()
				return
			}

			// Perform the upload using the section directly
			etag, err := uploadPart(ctx, url, section, size)
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
	parts := make([]common.MultipartUploadPart, 0, len(state.Completed))
	for partNum, etag := range state.Completed {
		parts = append(parts, common.MultipartUploadPart{
			PartNumber: int32(partNum),
			ETag:       etag,
		})
	}
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	if err := CompleteMultipartUpload(ctx, bk, key, uploadID, parts, req.Bucket); err != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	bk.Logger().DebugContext(ctx, "Successfully uploaded", "file", req.ObjectKey, "key", key)
	_ = os.Remove(checkpointPath)
	return nil
}

func initMultipartUpload(ctx context.Context, bk transfer.Uploader, furObject common.FileUploadRequestObject, bucketName string) (string, string, error) {
	msg, err := bk.InitMultipartUpload(ctx, furObject.GUID, furObject.ObjectKey, bucketName)

	if err != nil {
		if strings.Contains(err.Error(), "404") {
			return "", "", errors.New(err.Error() + "\nPlease check to ensure FENCE version is at 2.8.0 or beyond")
		}
		return "", "", errors.New("Error has occurred during multipart upload initialization, detailed error message: " + err.Error())
	}

	if msg.UploadID == "" || msg.GUID == "" {
		return "", "", errors.New("unknown error has occurred during multipart upload initialization. Please check logs from Gen3 services")
	}
	return msg.UploadID, msg.GUID, nil
}

func generateMultipartPresignedURL(ctx context.Context, bk transfer.Uploader, key string, uploadID string, partNumber int, bucketName string) (string, error) {
	url, err := bk.GetMultipartUploadURL(ctx, key, uploadID, int32(partNumber), bucketName)
	if err != nil {
		return "", errors.New("Error has occurred during multipart upload presigned url generation, detailed error message: " + err.Error())
	}

	if url == "" {
		return "", errors.New("unknown error has occurred during multipart upload presigned url generation. Please check logs from Gen3 services")
	}
	return url, nil
}

func CompleteMultipartUpload(ctx context.Context, bk transfer.Uploader, key string, uploadID string, parts []common.MultipartUploadPart, bucketName string) error {
	err := bk.CompleteMultipartUpload(ctx, key, uploadID, parts, bucketName)
	if err != nil {
		return errors.New("Error has occurred during completing multipart upload, detailed error message: " + err.Error())
	}
	return nil
}

// uploadPart now returns the ETag and error directly.
// It accepts a Context to allow for cancellation (e.g., if another part fails).
func uploadPart(ctx context.Context, url string, data io.Reader, partSize int64) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, data)
	if err != nil {
		return "", err
	}
	if partSize > 0 {
		req.ContentLength = partSize
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("upload part failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	etag := strings.Trim(resp.Header.Get("ETag"), `"`)
	if etag == "" {
		return "", errors.New("no ETag returned")
	}

	return etag, nil
}

func (s *multipartResumeState) matches(req common.FileUploadRequestObject, info os.FileInfo, chunkSize int64) bool {
	if s == nil {
		return false
	}
	return s.SourcePath == req.SourcePath &&
		s.ObjectKey == req.ObjectKey &&
		s.GUID == req.GUID &&
		s.Bucket == req.Bucket &&
		s.FileSize == info.Size() &&
		s.FileModUnixNano == info.ModTime().UnixNano() &&
		s.ChunkSize == chunkSize &&
		s.UploadID != "" &&
		s.Key != ""
}

func multipartCheckpointPath(req common.FileUploadRequestObject) (string, error) {
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
	sum := sha256.Sum256([]byte(req.SourcePath + "|" + req.ObjectKey + "|" + req.GUID + "|" + req.Bucket))
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
