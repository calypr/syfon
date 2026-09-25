package lfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/errorapi"
	"github.com/calypr/syfon/apigen/lfsapi"
	"github.com/calypr/syfon/internal/access"
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/storage"
	"github.com/calypr/syfon/internal/storage/address"
	"github.com/calypr/syfon/internal/transfers"
)

const multipartPartSize = 64 * 1024 * 1024
const maxMultipartParts = 10_000
const MaxUploadSizeBytes = multipartPartSize * maxMultipartParts

const (
	PendingMetadataTTL    = 20 * time.Minute
	MaxPendingMetadataTTL = 24 * time.Hour
)

const uploadReceiptTTL = 24 * time.Hour
const multipartCleanupTimeout = 10 * time.Second

type PendingMetadata struct {
	OID           string
	Candidate     lfsapi.DrsObjectCandidate
	UploadReceipt *UploadReceipt
	CreatedAt     time.Time
	ExpiresAt     time.Time
}

// UploadReceipt is durable evidence that the bytes for an LFS object were
// fully received, matched its OID, and were committed to storage.
type UploadReceipt struct {
	OID         string    `json:"oid"`
	Size        int64     `json:"size"`
	SHA256      string    `json:"sha256"`
	StorageURL  string    `json:"storage_url"`
	CompletedAt time.Time `json:"completed_at"`
	ExpiresAt   time.Time `json:"expires_at"`
}

type PendingStore interface {
	SavePendingMetadata(context.Context, []PendingMetadata) error
	GetPendingMetadata(context.Context, string) (*PendingMetadata, error)
	ConsumePendingMetadata(context.Context, PendingMetadata) (bool, error)
}

type UploadEvidenceStore interface {
	SaveLFSUploadReceipt(context.Context, UploadReceipt) error
	GetLFSUploadReceipt(context.Context, string) (*UploadReceipt, error)
}

type UploadAccounting interface {
	RecordFileUpload(context.Context, string) error
}

type signedPartUploader func(context.Context, string, []byte) (string, error)

func uploadSignedMultipartPart(ctx context.Context, signedURL string, content []byte) (string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, signedURL, bytes.NewReader(content))
	if err != nil {
		return "", err
	}
	request.ContentLength = int64(len(content))
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 2048))
		if readErr != nil {
			return "", fmt.Errorf("read multipart part error body: %w", readErr)
		}
		return "", fmt.Errorf("multipart part put failed status=%d body=%s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	etag := strings.Trim(strings.TrimSpace(response.Header.Get("ETag")), "\"")
	if etag == "" {
		return "", fmt.Errorf("multipart part upload missing etag")
	}
	return etag, nil
}

type sanitizedSignedPartError struct {
	message string
	cause   error
}

func (e *sanitizedSignedPartError) Error() string { return e.message }

func (e *sanitizedSignedPartError) Unwrap() error { return e.cause }

func sanitizeSignedPartRequestError(err error, signedURL string) error {
	sanitized, _ := sanitizeSignedPartErrorChain(err, signedURL)
	return sanitized
}

func sanitizeSignedPartErrorChain(err error, signedURL string) (error, bool) {
	if err == nil {
		return nil, false
	}
	if requestErr, ok := err.(*url.Error); ok {
		sanitizedCause, causeChanged := sanitizeSignedPartErrorChain(requestErr.Err, signedURL)
		sanitizedURL := sanitizeSignedPartURL(requestErr.URL)
		if !causeChanged && sanitizedURL == requestErr.URL {
			return err, false
		}
		return &url.Error{Op: requestErr.Op, URL: sanitizedURL, Err: sanitizedCause}, true
	}

	message := sanitizeSignedPartErrorText(err.Error(), signedURL)
	if many, ok := err.(interface{ Unwrap() []error }); ok {
		causes := many.Unwrap()
		sanitizedCauses := make([]error, len(causes))
		changed := message != err.Error()
		for i, cause := range causes {
			var causeChanged bool
			sanitizedCauses[i], causeChanged = sanitizeSignedPartErrorChain(cause, signedURL)
			changed = changed || causeChanged
			if causeChanged && cause != nil && sanitizedCauses[i] != nil {
				message = strings.ReplaceAll(message, cause.Error(), sanitizedCauses[i].Error())
			}
		}
		if !changed {
			return err, false
		}
		return &sanitizedSignedPartError{message: message, cause: errors.Join(sanitizedCauses...)}, true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		originalCause := wrapped.Unwrap()
		cause, causeChanged := sanitizeSignedPartErrorChain(originalCause, signedURL)
		if causeChanged && originalCause != nil && cause != nil {
			message = strings.ReplaceAll(message, originalCause.Error(), cause.Error())
		}
		if !causeChanged && message == err.Error() {
			return err, false
		}
		return &sanitizedSignedPartError{message: message, cause: cause}, true
	}
	if message != err.Error() {
		return &sanitizedSignedPartError{message: message, cause: err}, true
	}
	return err, false
}

func sanitizeSignedPartURL(rawURL string) string {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		if queryStart := strings.IndexAny(rawURL, "?#"); queryStart >= 0 {
			return rawURL[:queryStart]
		}
		return rawURL
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.ForceQuery = false
	parsed.Fragment = ""
	parsed.RawFragment = ""
	return parsed.String()
}

func sanitizeSignedPartErrorText(message, rawURL string) string {
	safeURL := sanitizeSignedPartURL(rawURL)
	for _, candidate := range []string{rawURL, strings.TrimSpace(rawURL)} {
		if candidate != "" {
			message = strings.ReplaceAll(message, candidate, safeURL)
		}
	}
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return message
	}
	message = strings.ReplaceAll(message, parsed.String(), safeURL)
	if parsed.User != nil {
		message = strings.ReplaceAll(message, parsed.User.String(), "[redacted]")
	}
	if parsed.RawQuery != "" {
		message = strings.ReplaceAll(message, parsed.RawQuery, "[redacted]")
	}
	if parsed.Fragment != "" {
		message = strings.ReplaceAll(message, parsed.Fragment, "[redacted]")
	}
	return message
}

type ObjectPort interface {
	GetObject(context.Context, string, string) (*drs.DrsObject, error)
	RegisterObjectsIfPending(context.Context, []drs.DrsObject, objects.PendingRegistration) ([]drs.DrsObject, error)
}

type DownloadPreparation struct{ SignedURL string }

type DownloadLookupError struct{ Err error }

func (e *DownloadLookupError) Error() string {
	if e == nil || e.Err == nil {
		return "object lookup failed"
	}
	return e.Err.Error()
}

func (e *DownloadLookupError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type UploadPreparation struct {
	Existing bool
	Size     int64
}

type BatchRequest struct {
	Operation string
	Objects   []BatchObject
}

type BatchObject struct {
	OID  string
	Size int64
}

type BatchObjectResult struct {
	OID         string
	Size        int64
	Existing    bool
	DownloadURL string
	Err         error
}

type BatchResult struct{ Objects []BatchObjectResult }

type Service struct {
	transfer    *transfers.Service
	objects     ObjectPort
	credentials buckets.CredentialReader
	pending     PendingStore
	accounting  UploadAccounting
	uploader    signedPartUploader
	now         func() time.Time
}

func NewService(transfer *transfers.Service, objectPort ObjectPort, credentials buckets.CredentialReader, pending PendingStore, accounting UploadAccounting, uploader signedPartUploader) *Service {
	if uploader == nil {
		uploader = uploadSignedMultipartPart
	}
	return &Service{transfer: transfer, objects: objectPort, credentials: credentials, pending: pending, accounting: accounting, uploader: uploader, now: time.Now}
}

func (s *Service) Batch(ctx context.Context, request BatchRequest) (BatchResult, error) {
	result := BatchResult{Objects: make([]BatchObjectResult, 0, len(request.Objects))}
	for _, object := range request.Objects {
		item := BatchObjectResult{OID: object.OID, Size: object.Size}
		if request.Operation == "download" {
			preparation, err := s.PrepareDownload(ctx, object.OID)
			if err != nil {
				item.Err = err
			} else {
				item.DownloadURL = preparation.SignedURL
			}
		} else {
			preparation, err := s.PrepareUpload(ctx, object.OID, object.Size)
			item.Size = preparation.Size
			item.Existing = preparation.Existing
			item.Err = err
		}
		result.Objects = append(result.Objects, item)
	}
	return result, nil
}

func (s *Service) PrepareDownload(ctx context.Context, oid string) (DownloadPreparation, error) {
	if s == nil || s.transfer == nil {
		return DownloadPreparation{}, fmt.Errorf("LFS transfer service is not configured")
	}
	result, err := s.transfer.Download(ctx, transfers.DownloadRequest{ObjectID: oid, Accounting: transfers.AccountingDownloadBeforeEvent})
	if err != nil {
		return DownloadPreparation{}, &DownloadLookupError{Err: err}
	}
	return DownloadPreparation{SignedURL: result.URL}, nil
}

func (s *Service) PrepareUpload(ctx context.Context, oid string, size int64) (UploadPreparation, error) {
	result := UploadPreparation{Size: size}
	if s == nil || s.objects == nil {
		return result, fmt.Errorf("LFS object service is not configured")
	}
	existing, err := s.objects.GetObject(ctx, oid, "read")
	if err == nil {
		return UploadPreparation{Existing: true, Size: existing.Size}, nil
	}
	if !errorapi.IsNotFoundError(err) {
		return result, err
	}
	if !access.HasObjectMethodAccess(ctx, "create", []string{"/data_file"}) {
		return result, errorapi.ErrAccessDenied
	}
	if _, err := s.firstConfiguredBucket(ctx); err != nil {
		return result, err
	}
	if size < 0 {
		result.Size = 0
	}
	return result, nil
}

func (s *Service) UploadProxy(ctx context.Context, oid string, body io.Reader) (returnErr error) {
	if s == nil || s.transfer == nil || s.objects == nil {
		return fmt.Errorf("LFS upload service is not configured")
	}
	oid = objects.NormalizeOID(oid)
	if oid == "" {
		return fmt.Errorf("invalid LFS upload OID")
	}
	evidence, ok := s.pending.(UploadEvidenceStore)
	if !ok {
		return fmt.Errorf("LFS upload evidence store is not configured")
	}
	target, objectID, authorization, err := s.resolveUploadTarget(ctx, oid)
	if err != nil {
		return err
	}
	init, err := s.transfer.BeginMultipart(ctx, transfers.MultipartInitRequest{GUID: &objectID, Target: &target, Authorization: &authorization})
	if err != nil {
		return fmt.Errorf("failed to initialize multipart upload: %w", err)
	}
	multipartCompleted := false
	defer func() {
		if multipartCompleted {
			return
		}
		abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), multipartCleanupTimeout)
		defer cancel()
		if abortErr := s.transfer.AbortMultipart(abortCtx, init.UploadID); abortErr != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("failed to abort incomplete LFS multipart upload: %w", abortErr))
		}
	}()
	parts := make([]transfers.CompletedPart, 0, 16)
	partNumber := int32(1)
	buffer := make([]byte, multipartPartSize)
	hasher := sha256.New()
	var uploadedBytes int64
	for {
		readCount, readErr := io.ReadFull(body, buffer)
		if readErr == io.EOF || (readErr == io.ErrUnexpectedEOF && readCount == 0) {
			break
		}
		if readErr != nil && readErr != io.ErrUnexpectedEOF {
			return fmt.Errorf("failed reading upload stream: %w", readErr)
		}
		uploadedBytes += int64(readCount)
		if uploadedBytes > MaxUploadSizeBytes {
			return &UploadSizeLimitError{Limit: MaxUploadSizeBytes}
		}
		if _, err := hasher.Write(buffer[:readCount]); err != nil {
			return fmt.Errorf("failed hashing upload stream: %w", err)
		}
		partURL, err := s.transfer.SignMultipartPart(ctx, init.UploadID, partNumber)
		if err != nil {
			return fmt.Errorf("failed to sign multipart part: %w", err)
		}
		etag, err := s.uploader(ctx, partURL, buffer[:readCount])
		if err != nil {
			return fmt.Errorf("failed uploading multipart part %d: %w", partNumber, sanitizeSignedPartRequestError(err, partURL))
		}
		parts = append(parts, transfers.CompletedPart{PartNumber: partNumber, ETag: etag})
		partNumber++
		if readErr == io.ErrUnexpectedEOF {
			break
		}
	}
	if len(parts) == 0 {
		partURL, err := s.transfer.SignMultipartPart(ctx, init.UploadID, 1)
		if err != nil {
			return fmt.Errorf("failed to sign multipart part: %w", err)
		}
		etag, err := s.uploader(ctx, partURL, nil)
		if err != nil {
			return fmt.Errorf("failed uploading multipart part 1: %w", sanitizeSignedPartRequestError(err, partURL))
		}
		parts = append(parts, transfers.CompletedPart{PartNumber: 1, ETag: etag})
	}
	actualOID := hex.EncodeToString(hasher.Sum(nil))
	if actualOID != oid {
		return &UploadIntegrityError{ExpectedOID: oid, ActualOID: actualOID, Size: uploadedBytes}
	}
	if _, err := s.transfer.CompleteMultipart(ctx, init.UploadID, parts); err != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}
	multipartCompleted = true
	completedAt := s.currentTime()
	finalizeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), multipartCleanupTimeout)
	defer cancel()
	if err := evidence.SaveLFSUploadReceipt(finalizeCtx, UploadReceipt{
		OID:         oid,
		Size:        uploadedBytes,
		SHA256:      actualOID,
		StorageURL:  target.CanonicalURL,
		CompletedAt: completedAt,
		ExpiresAt:   completedAt.Add(uploadReceiptTTL),
	}); err != nil {
		return fmt.Errorf("failed to record completed upload evidence: %w", err)
	}
	if s.accounting == nil {
		return fmt.Errorf("failed to record upload usage: file counters are not configured")
	}
	if err := s.accounting.RecordFileUpload(finalizeCtx, objectID); err != nil {
		return fmt.Errorf("failed to record upload usage: %w", err)
	}
	return nil
}

func (s *Service) resolveUploadTarget(ctx context.Context, oid string) (storage.Target, string, transfers.MultipartAuthorization, error) {
	if object, err := s.objects.GetObject(ctx, oid, "update"); err == nil {
		target, targetErr := s.targetForObject(ctx, object)
		return target, object.Id, transfers.MultipartAuthorization{Resources: objects.AccessResources(object), Methods: []string{"update"}}, targetErr
	} else if !errorapi.IsNotFoundError(err) {
		return storage.Target{}, "", transfers.MultipartAuthorization{}, err
	}
	createAuthorization := transfers.MultipartAuthorization{Resources: []string{"/data_file"}, Methods: []string{"create"}}
	if s.pending != nil {
		if pending, err := s.pending.GetPendingMetadata(ctx, oid); err == nil {
			object, conversionErr := materializeCandidate(pending.Candidate, s.currentTime())
			if conversionErr != nil {
				return storage.Target{}, "", transfers.MultipartAuthorization{}, conversionErr
			}
			target, targetErr := s.targetForObject(ctx, &object)
			return target, oid, createAuthorization, targetErr
		} else if !errorapi.IsNotFoundError(err) {
			return storage.Target{}, "", transfers.MultipartAuthorization{}, err
		}
	}
	bucket, err := s.firstConfiguredBucket(ctx)
	if err != nil {
		return storage.Target{}, "", transfers.MultipartAuthorization{}, err
	}
	return storage.Target{Provider: "s3", LookupKey: bucket, PhysicalBucket: bucket, Key: oid, CanonicalURL: address.BucketToURL(bucket, oid), LookupCandidates: []string{bucket}}, oid, createAuthorization, nil
}

func (s *Service) targetForObject(ctx context.Context, object *drs.DrsObject) (storage.Target, error) {
	canonical, err := s.transfer.ResolveCanonicalStorageTarget(ctx, transfers.CanonicalStorageTargetRequest{Object: object, PreferChecksum: true})
	if err != nil {
		return storage.Target{}, err
	}
	parsed, parseErr := address.ParseLocation(canonical.URL)
	if parseErr != nil {
		return storage.Target{}, fmt.Errorf("canonical LFS upload location is not an s3 url: %w", parseErr)
	}
	return storage.Target{Provider: parsed.Provider, LookupKey: parsed.Bucket, PhysicalBucket: canonical.Bucket, Key: canonical.Key, Path: parsed.Path, CanonicalURL: canonical.URL, LookupCandidates: []string{canonical.Bucket}}, nil
}

func (s *Service) firstConfiguredBucket(ctx context.Context) (string, error) {
	if s.credentials == nil {
		return "", errorapi.ErrBucketNotConfigured
	}
	credentials, err := s.credentials.ListS3Credentials(ctx)
	if err != nil {
		return "", err
	}
	if len(credentials) == 0 || strings.TrimSpace(credentials[0].Bucket) == "" {
		return "", errorapi.ErrBucketNotConfigured
	}
	return strings.TrimSpace(credentials[0].Bucket), nil
}

func (s *Service) Stage(ctx context.Context, candidates []lfsapi.DrsObjectCandidate, ttl time.Duration) error {
	if !access.HasObjectMethodAccess(ctx, "create", []string{"/data_file"}) {
		return errorapi.ErrAccessDenied
	}
	if ttl < time.Second || ttl > MaxPendingMetadataTTL {
		return fmt.Errorf("pending metadata TTL must be between 1s and %s", MaxPendingMetadataTTL)
	}
	now := s.currentTime()
	entries := make([]PendingMetadata, 0, len(candidates))
	for index, candidate := range candidates {
		internalObject, err := materializeCandidate(candidate, now)
		if err != nil {
			return &MetadataStageError{Index: index, Err: err}
		}
		oid, ok := objects.CanonicalSHA256(internalObject.Checksums)
		if !ok {
			return &MetadataStageError{Index: index, MissingSHA: true}
		}
		entries = append(entries, PendingMetadata{OID: oid, Candidate: candidate, CreatedAt: now, ExpiresAt: now.Add(ttl)})
	}
	if s.pending == nil {
		return fmt.Errorf("pending LFS metadata store is not configured")
	}
	return s.pending.SavePendingMetadata(ctx, entries)
}

func (s *Service) Verify(ctx context.Context, oid string, expectedSize int64) error {
	if expectedSize < 0 {
		return &MetadataCandidateError{Err: fmt.Errorf("size must be non-negative")}
	}
	if s == nil {
		return fmt.Errorf("LFS verify service is not configured")
	}
	if s.objects == nil {
		return fmt.Errorf("LFS object service is not configured")
	}
	oid = objects.NormalizeOID(oid)
	if oid == "" {
		return &MetadataCandidateError{Err: fmt.Errorf("invalid OID")}
	}
	object, objectErr := s.objects.GetObject(ctx, oid, "read")
	if objectErr != nil && !errorapi.IsNotFoundError(objectErr) {
		return objectErr
	}
	existingObject := objectErr == nil && object != nil
	if existingObject {
		if err := verifyRecordedSize(expectedSize, object.Size); err != nil {
			return err
		}
	}
	if s.pending == nil {
		if existingObject {
			return nil
		}
		return fmt.Errorf("pending LFS metadata store is not configured")
	}
	pending, err := s.pending.GetPendingMetadata(ctx, oid)
	if errorapi.IsNotFoundError(err) {
		if existingObject {
			return nil
		}
		return objectErr
	}
	if err != nil {
		return err
	}
	if !access.HasObjectMethodAccess(ctx, "create", []string{"/data_file"}) {
		return errorapi.ErrAccessDenied
	}
	evidence, ok := s.pending.(UploadEvidenceStore)
	if !ok {
		return fmt.Errorf("LFS upload evidence store is not configured")
	}
	receipt, err := evidence.GetLFSUploadReceipt(ctx, oid)
	if err != nil {
		return err
	}
	if receipt == nil || receipt.OID != oid || objects.NormalizeOID(receipt.SHA256) != oid || receipt.StorageURL == "" || receipt.CompletedAt.IsZero() {
		return &MetadataCandidateError{Err: fmt.Errorf("completed upload evidence does not match OID %s", oid)}
	}
	if err := verifyRecordedSize(expectedSize, receipt.Size); err != nil {
		return &MetadataCandidateError{Err: fmt.Errorf("uploaded size: %w", err)}
	}
	internalObject, err := materializeCandidate(pending.Candidate, s.currentTime())
	if err != nil {
		return &MetadataCandidateError{Err: err}
	}
	if err := verifyRecordedSize(expectedSize, internalObject.Size); err != nil {
		return err
	}
	storageURL, err := s.candidateStorageURL(ctx, &internalObject)
	if err != nil {
		return &MetadataCandidateError{Err: err}
	}
	if receipt.StorageURL == "" || storageURL != receipt.StorageURL {
		return &MetadataCandidateError{Err: fmt.Errorf("uploaded storage target does not match staged metadata")}
	}
	candidateJSON, err := json.Marshal(pending.Candidate)
	if err != nil {
		return fmt.Errorf("encode staged LFS candidate: %w", err)
	}
	receiptJSON, err := json.Marshal(pending.UploadReceipt)
	if err != nil {
		return fmt.Errorf("encode staged LFS receipt: %w", err)
	}
	registered, err := s.objects.RegisterObjectsIfPending(ctx, []drs.DrsObject{internalObject}, objects.PendingRegistration{
		OID: oid, CandidateJSON: candidateJSON, ReceiptJSON: receiptJSON,
		CreatedAt: pending.CreatedAt, ExpiresAt: pending.ExpiresAt,
	})
	if err != nil {
		if errors.Is(err, errorapi.ErrConflict) {
			if _, pendingErr := s.pending.GetPendingMetadata(ctx, oid); errorapi.IsNotFoundError(pendingErr) {
				if current, lookupErr := s.objects.GetObject(ctx, oid, "read"); lookupErr == nil && current != nil {
					return nil
				}
			}
		}
		return err
	}
	if len(registered) != 1 {
		return fmt.Errorf("registration returned %d records, want 1", len(registered))
	}
	owned, err := s.pending.ConsumePendingMetadata(ctx, *pending)
	if err != nil {
		return err
	}
	if !owned {
		return nil
	}
	return nil
}

func (s *Service) candidateStorageURL(ctx context.Context, object *drs.DrsObject) (string, error) {
	if s.transfer != nil {
		target, err := s.targetForObject(ctx, object)
		if err != nil {
			return "", err
		}
		return target.CanonicalURL, nil
	}
	var methods []drs.AccessMethod
	if object.AccessMethods != nil {
		methods = *object.AccessMethods
	}
	for _, method := range methods {
		if !strings.EqualFold(string(method.Type), "s3") || method.AccessUrl == nil {
			continue
		}
		parsed, err := address.ParseLocation(method.AccessUrl.Url)
		if err != nil {
			return "", fmt.Errorf("staged LFS upload location is invalid: %w", err)
		}
		return parsed.URL, nil
	}
	return "", fmt.Errorf("staged LFS metadata has no s3 access method")
}

type UploadIntegrityError struct {
	ExpectedOID string
	ActualOID   string
	Size        int64
}

func (e *UploadIntegrityError) Error() string {
	return fmt.Sprintf("uploaded content SHA-256 %s does not match requested OID %s", e.ActualOID, e.ExpectedOID)
}

type UploadSizeLimitError struct{ Limit int64 }

func (e *UploadSizeLimitError) Error() string {
	return fmt.Sprintf("LFS upload exceeds maximum object size of %d bytes", e.Limit)
}

func verifyRecordedSize(expected, recorded int64) error {
	if expected == recorded {
		return nil
	}
	return &MetadataCandidateError{Err: fmt.Errorf("size mismatch: expected %d, recorded %d", expected, recorded)}
}

func materializeCandidate(value lfsapi.DrsObjectCandidate, now time.Time) (drs.DrsObject, error) {
	var aliases []string
	if value.Aliases != nil {
		aliases = append(aliases, (*value.Aliases)...)
	}
	explicitID := ""
	if value.Id != nil {
		explicitID = strings.TrimSpace(*value.Id)
	}
	if explicitID == "" {
		var sourceChecksums []lfsapi.Checksum
		if value.Checksums != nil {
			sourceChecksums = *value.Checksums
		}
		for _, checksum := range sourceChecksums {
			if strings.EqualFold(strings.TrimSpace(checksum.Type), "sha256") {
				explicitID = objects.NormalizeOID(checksum.Checksum)
				break
			}
		}
	}
	if explicitID != "" {
		aliases = append([]string{"id:" + explicitID}, aliases...)
	}
	var sourceMethods []lfsapi.AccessMethod
	if value.AccessMethods != nil {
		sourceMethods = *value.AccessMethods
	}
	methods := make([]drs.AccessMethod, 0, len(sourceMethods))
	if value.AccessMethods != nil {
		for _, method := range sourceMethods {
			converted := drs.AccessMethod{AccessId: method.AccessId}
			if method.Type != nil {
				converted.Type = drs.AccessMethodType(*method.Type)
			}
			if method.AccessUrl != nil && method.AccessUrl.Url != nil {
				converted.AccessUrl = &drs.AccessURL{Url: *method.AccessUrl.Url}
			}
			methods = append(methods, converted)
		}
	}
	var accessMethods *[]drs.AccessMethod
	if value.AccessMethods != nil {
		accessMethods = &methods
	}
	var size int64
	if value.Size != nil {
		size = *value.Size
	}
	var sourceChecksums []lfsapi.Checksum
	if value.Checksums != nil {
		sourceChecksums = *value.Checksums
	}
	checksums := make([]drs.Checksum, 0, len(sourceChecksums))
	for _, checksum := range sourceChecksums {
		checksums = append(checksums, drs.Checksum{Type: checksum.Type, Checksum: checksum.Checksum})
	}
	return objects.MaterializeCandidate(drs.DrsObjectCandidate{
		AccessMethods:    accessMethods,
		Aliases:          &aliases,
		Checksums:        checksums,
		ControlledAccess: value.ControlledAccess,
		Description:      value.Description,
		Name:             value.Name,
		Size:             size,
	}, now)
}

func (s *Service) currentTime() time.Time {
	if s != nil && s.now != nil {
		return s.now().UTC()
	}
	return time.Now().UTC()
}

type MetadataCandidateError struct{ Err error }

func (e *MetadataCandidateError) Error() string {
	if e == nil || e.Err == nil {
		return "invalid LFS metadata candidate"
	}
	return e.Err.Error()
}
func (e *MetadataCandidateError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type MetadataStageError struct {
	Index      int
	Err        error
	MissingSHA bool
}

func (e *MetadataStageError) Error() string {
	if e.MissingSHA {
		return "candidate missing canonical sha256"
	}
	if e.Err == nil {
		return "candidate is invalid"
	}
	return e.Err.Error()
}
func (e *MetadataStageError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}
