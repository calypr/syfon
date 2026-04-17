package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/apigen/server/lfsapi"
	"github.com/calypr/syfon/internal/authz"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/db"
	"github.com/calypr/syfon/internal/models"
	"github.com/calypr/syfon/internal/urlmanager"
	"github.com/gofiber/fiber/v3"
)

type contextKey string

var BaseURLContextKey contextKey = "baseURL"

type Options struct {
	MaxBatchObjects              int
	MaxBatchBodyBytes            int64
	RequestLimitPerMinute        int
	BandwidthLimitBytesPerMinute int64
}

type windowCounter struct {
	Minute int64
	Count  int
}

type windowBytes struct {
	Minute int64
	Bytes  int64
}

var (
	limitMu            sync.Mutex
	requestWindowMap   = map[string]windowCounter{}
	bandwidthWindowMap = map[string]windowBytes{}
)

func DefaultOptions() Options {
	return Options{
		MaxBatchObjects:              1000,
		MaxBatchBodyBytes:            10 * 1024 * 1024,
		RequestLimitPerMinute:        1200,
		BandwidthLimitBytesPerMinute: 0,
	}
}

type LFSServer struct {
	database db.LFSStore
	uM       urlmanager.UrlManager
	opts     Options
}

func NewLFSServer(database db.LFSStore, uM urlmanager.UrlManager, opts Options) *LFSServer {
	return &LFSServer{database: database, uM: uM, opts: opts}
}

func RegisterLFSRoutes(router fiber.Router, database db.LFSStore, uM urlmanager.UrlManager, opts ...Options) {
	effective := DefaultOptions()
	if len(opts) > 0 {
		effective = opts[0]
	}
	server := NewLFSServer(database, uM, effective)
	strict := lfsapi.NewStrictHandler(server, []lfsapi.StrictMiddlewareFunc{
		LFSRequestMiddleware(effective),
	})
	router.Use(func(c fiber.Ctx) error {
		c.SetContext(context.WithValue(c.Context(), BaseURLContextKey, c.BaseURL()))
		return c.Next()
	})
	lfsapi.RegisterHandlers(router, strict)
}

func LFSRequestMiddleware(opts Options) lfsapi.StrictMiddlewareFunc {
	return func(next lfsapi.StrictHandlerFunc, operationID string) lfsapi.StrictHandlerFunc {
		return func(ctx fiber.Ctx, args interface{}) (interface{}, error) {
			switch operationID {
			case "LfsBatch":
				if !ValidateLFSRequestHeaders(ctx, true, true) {
					return nil, nil
				}
				if !enforceRequestLimit(ctx, opts) {
					return nil, nil
				}
				if opts.MaxBatchBodyBytes > 0 && int64(len(ctx.Request().Body())) > opts.MaxBatchBodyBytes {
					WriteLFSError(ctx, http.StatusRequestEntityTooLarge, "batch request body too large", false)
					return nil, nil
				}
				if req, ok := args.(lfsapi.LfsBatchRequestObject); ok && req.Body != nil {
					totalBytes := int64(0)
					for _, in := range req.Body.Objects {
						if in.Size > 0 {
							totalBytes += in.Size
						}
					}
					if !enforceBandwidthLimit(ctx, opts, totalBytes) {
						return nil, nil
					}
				}
			case "LfsStageMetadata":
				if !ValidateLFSRequestHeaders(ctx, false, true) {
					return nil, nil
				}
				if !enforceRequestLimit(ctx, opts) {
					return nil, nil
				}
			case "LfsVerify":
				if !ValidateLFSRequestHeaders(ctx, true, true) {
					return nil, nil
				}
				if !enforceRequestLimit(ctx, opts) {
					return nil, nil
				}
			case "LfsUploadProxy":
				if !enforceRequestLimit(ctx, opts) {
					return nil, nil
				}
			}
			return next(ctx, args)
		}
	}
}

func ResetLFSLimitersForTest() {
	limitMu.Lock()
	defer limitMu.Unlock()
	requestWindowMap = map[string]windowCounter{}
	bandwidthWindowMap = map[string]windowBytes{}
}

func ValidateLFSRequestHeaders(c fiber.Ctx, requireAccept bool, requireContentType bool) bool {
	const mediaType = "application/vnd.git-lfs+json"
	if requireAccept {
		accept := strings.ToLower(strings.TrimSpace(c.Get("Accept")))
		if accept == "" || (!strings.Contains(accept, mediaType) && !strings.Contains(accept, "*/*")) {
			WriteLFSError(c, http.StatusNotAcceptable, "Accept header must include application/vnd.git-lfs+json", false)
			return false
		}
	}
	if requireContentType {
		contentType := strings.TrimSpace(c.Get("Content-Type"))
		if contentType == "" {
			WriteLFSError(c, http.StatusUnprocessableEntity, "Content-Type must be application/vnd.git-lfs+json", false)
			return false
		}
		parsed, _, err := mime.ParseMediaType(contentType)
		if err != nil || strings.ToLower(parsed) != mediaType {
			WriteLFSError(c, http.StatusUnprocessableEntity, "Content-Type must be application/vnd.git-lfs+json", false)
			return false
		}
	}
	return true
}

func WriteLFSError(c fiber.Ctx, status int, message string, challenge bool) error {
	if challenge {
		c.Set("LFS-Authenticate", `Basic realm="Git LFS"`)
	}
	c.Set("Content-Type", "application/vnd.git-lfs+json")
	payload := lfsapi.LFSErrorResponse{Message: message}
	if reqID := common.GetRequestID(c.Context()); reqID != "" {
		payload.RequestId = &reqID
	}
	docURL := "https://github.com/git-lfs/git-lfs/blob/main/docs/api"
	payload.DocumentationUrl = &docURL
	return c.Status(status).JSON(payload)
}

func enforceRequestLimit(c fiber.Ctx, opts Options) bool {
	if opts.RequestLimitPerMinute <= 0 {
		return true
	}
	nowMinute := time.Now().UTC().Unix() / 60
	key := requestClientKey(c)
	limitMu.Lock()
	defer limitMu.Unlock()
	win := requestWindowMap[key]
	if win.Minute != nowMinute {
		win.Minute = nowMinute
		win.Count = 0
	}
	win.Count++
	requestWindowMap[key] = win
	if win.Count > opts.RequestLimitPerMinute {
		WriteLFSError(c, http.StatusTooManyRequests, "rate limit exceeded", false)
		return false
	}
	return true
}

func enforceBandwidthLimit(c fiber.Ctx, opts Options, bytes int64) bool {
	if opts.BandwidthLimitBytesPerMinute <= 0 || bytes <= 0 {
		return true
	}
	nowMinute := time.Now().UTC().Unix() / 60
	key := requestClientKey(c)
	limitMu.Lock()
	defer limitMu.Unlock()
	win := bandwidthWindowMap[key]
	if win.Minute != nowMinute {
		win.Minute = nowMinute
		win.Bytes = 0
	}
	if win.Bytes+bytes > opts.BandwidthLimitBytesPerMinute {
		WriteLFSError(c, 509, "bandwidth limit exceeded", false)
		return false
	}
	win.Bytes += bytes
	bandwidthWindowMap[key] = win
	return true
}

func requestClientKey(c fiber.Ctx) string {
	if auth := strings.TrimSpace(c.Get("Authorization")); auth != "" {
		if len(auth) > 64 {
			auth = auth[:64]
		}
		return "auth:" + auth
	}
	return "addr:" + c.IP()
}

func NormalizeOID(raw string) string {
	oid := strings.TrimSpace(strings.ToLower(raw))
	if len(oid) != 64 {
		return ""
	}
	for _, ch := range oid {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return ""
		}
	}
	return oid
}

func ResolveObjectForOID(ctx context.Context, database db.ObjectStore, oid string) (*models.InternalObject, error) {
	byChecksum, err := database.GetObjectsByChecksum(ctx, oid)
	if err != nil {
		return nil, err
	}
	if len(byChecksum) == 0 {
		if obj, getErr := database.GetObject(ctx, oid); getErr == nil {
			return obj, nil
		} else if !isNotFound(getErr) {
			return nil, getErr
		}
		return nil, fmt.Errorf("%w: object not found", common.ErrNotFound)
	}
	obj := byChecksum[0]
	return &obj, nil
}

func CanonicalSHA256(checksums []drs.Checksum) (string, bool) {
	for _, c := range checksums {
		if strings.EqualFold(strings.TrimSpace(c.Type), "sha256") {
			oid := NormalizeOID(c.Checksum)
			if oid != "" {
				return oid, true
			}
		}
	}
	return "", false
}

func UniqueAuthz(accessMethods []drs.AccessMethod) []string {
	if len(accessMethods) == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	out := make([]string, 0)
	for _, am := range accessMethods {
		if am.Authorizations != nil && am.Authorizations.BearerAuthIssuers != nil {
			for _, issuer := range *am.Authorizations.BearerAuthIssuers {
				issuer = strings.TrimSpace(issuer)
				if issuer == "" {
					continue
				}
				if _, ok := seen[issuer]; ok {
					continue
				}
				seen[issuer] = struct{}{}
				out = append(out, issuer)
			}
		}
	}
	return out
}

func CandidateToInternalObject(c drs.DrsObjectCandidate, now time.Time) (models.InternalObject, error) {
	oid, ok := CanonicalSHA256(c.Checksums)
	if !ok {
		return models.InternalObject{}, fmt.Errorf("candidate must include sha256 checksum")
	}
	var ams []drs.AccessMethod
	if c.AccessMethods != nil {
		ams = *c.AccessMethods
	}
	authz := UniqueAuthz(ams)
	obj := drs.DrsObject{
		Id:          common.MintObjectIDFromChecksum(oid, authz),
		Size:        c.Size,
		CreatedTime: now,
		UpdatedTime: &now,
		Version:     common.Ptr("1"),
		MimeType:    c.MimeType,
		Description: c.Description,
		Aliases:     common.Ptr(append([]string(nil), derefStringSlice(c.Aliases)...)),
		SelfUri:     "",
		Checksums:   []drs.Checksum{{Type: "sha256", Checksum: oid}},
	}
	if c.Name != nil {
		obj.Name = c.Name
	}
	obj.SelfUri = "drs://" + obj.Id
	if obj.Name == nil || strings.TrimSpace(*obj.Name) == "" {
		obj.Name = &oid
	}
	seenAccess := make(map[string]struct{})
	if c.AccessMethods != nil {
		for _, am := range *c.AccessMethods {
			url := ""
			if am.AccessUrl != nil {
				url = strings.TrimSpace(am.AccessUrl.Url)
			}
			if url == "" {
				continue
			}
			key := string(am.Type) + "|" + url
			if _, ok := seenAccess[key]; ok {
				continue
			}
			seenAccess[key] = struct{}{}
			accessID := am.AccessId
			if accessID == nil || strings.TrimSpace(*accessID) == "" {
				accessID = common.Ptr(string(am.Type))
			}
			if obj.AccessMethods == nil {
				obj.AccessMethods = &[]drs.AccessMethod{}
			}
			*obj.AccessMethods = append(*obj.AccessMethods, drs.AccessMethod{
				Type:     drs.AccessMethodType(am.Type),
				AccessId: accessID,
				AccessUrl: &struct {
					Headers *[]string `json:"headers,omitempty"`
					Url     string    `json:"url"`
				}{Url: url},
				Authorizations: &struct {
					BearerAuthIssuers   *[]string                                       `json:"bearer_auth_issuers,omitempty"`
					DrsObjectId         *string                                         `json:"drs_object_id,omitempty"`
					PassportAuthIssuers *[]string                                       `json:"passport_auth_issuers,omitempty"`
					SupportedTypes      *[]drs.AccessMethodAuthorizationsSupportedTypes `json:"supported_types,omitempty"`
				}{
					BearerAuthIssuers: &authz,
				},
			})
		}
	}
	return models.InternalObject{DrsObject: obj, Authorizations: authz}, nil
}

func LFSCandidateToDRS(in lfsapi.DrsObjectCandidate) drs.DrsObjectCandidate {
	var size int64
	if in.Size != nil {
		size = *in.Size
	}
	out := drs.DrsObjectCandidate{
		Name:        in.Name,
		Size:        size,
		MimeType:    in.MimeType,
		Description: in.Description,
		Aliases:     in.Aliases,
	}
	if in.Checksums != nil {
		out.Checksums = make([]drs.Checksum, 0, len(*in.Checksums))
		for _, c := range *in.Checksums {
			out.Checksums = append(out.Checksums, drs.Checksum{
				Type:     c.Type,
				Checksum: c.Checksum,
			})
		}
	}
	if in.AccessMethods != nil {
		ams := make([]drs.AccessMethod, 0, len(*in.AccessMethods))
		for _, am := range *in.AccessMethods {
			drsMethod := drs.AccessMethod{
				Type:     drs.AccessMethodType(common.DerefString(am.Type)),
				Region:   am.Region,
				AccessId: am.AccessId,
			}
			if am.AccessUrl != nil {
				drsMethod.AccessUrl = &struct {
					Headers *[]string `json:"headers,omitempty"`
					Url     string    `json:"url"`
				}{}
				if am.AccessUrl.Url != nil {
					drsMethod.AccessUrl.Url = *am.AccessUrl.Url
				}
			}
			if am.Authorizations != nil {
				drsMethod.Authorizations = &struct {
					BearerAuthIssuers   *[]string                                       `json:"bearer_auth_issuers,omitempty"`
					DrsObjectId         *string                                         `json:"drs_object_id,omitempty"`
					PassportAuthIssuers *[]string                                       `json:"passport_auth_issuers,omitempty"`
					SupportedTypes      *[]drs.AccessMethodAuthorizationsSupportedTypes `json:"supported_types,omitempty"`
				}{
					BearerAuthIssuers: am.Authorizations.BearerAuthIssuers,
				}
			}
			ams = append(ams, drsMethod)
		}
		out.AccessMethods = &ams
	}
	return out
}

func PrepareDownloadActions(ctx context.Context, database db.LFSStore, uM urlmanager.UrlManager, oid string) (*lfsapi.BatchActions, *lfsapi.ObjectError) {
	obj, err := ResolveObjectForOID(ctx, database, oid)
	if err != nil {
		return nil, dbErrToBatchError(err, ctx)
	}
	if len(obj.Authorizations) > 0 && !hasMethodAccess(ctx, "read", obj.Authorizations) {
		return nil, &lfsapi.ObjectError{Code: int32(unauthorizedStatus(ctx)), Message: "unauthorized"}
	}
	var src string
	if obj.AccessMethods != nil {
		for _, am := range *obj.AccessMethods {
			if am.AccessUrl != nil && strings.TrimSpace(am.AccessUrl.Url) != "" {
				src = am.AccessUrl.Url
				break
			}
		}
	}
	if src == "" {
		return nil, &lfsapi.ObjectError{Code: int32(http.StatusNotFound), Message: "no object location available"}
	}
	signed, err := uM.SignURL(ctx, "", src, urlmanager.SignOptions{})
	if err != nil {
		return nil, &lfsapi.ObjectError{Code: int32(http.StatusInternalServerError), Message: err.Error()}
	}
	usageObjectID := oid
	if strings.TrimSpace(obj.Id) != "" {
		usageObjectID = strings.TrimSpace(obj.Id)
	}
	if recErr := database.RecordFileDownload(ctx, usageObjectID); recErr != nil {
	}
	action := lfsapi.Action{Href: signed}
	return &lfsapi.BatchActions{Download: &action}, nil
}

func PrepareUploadActions(ctx context.Context, database db.LFSStore, uM urlmanager.UrlManager, oid string, reqSize int64, baseURL string) (*lfsapi.BatchActions, int64, *lfsapi.ObjectError) {
	existing, err := ResolveObjectForOID(ctx, database, oid)
	if err == nil {
		if len(existing.Authorizations) > 0 && !hasMethodAccess(ctx, "read", existing.Authorizations) {
			return nil, existing.Size, &lfsapi.ObjectError{Code: int32(unauthorizedStatus(ctx)), Message: "unauthorized"}
		}
		return nil, existing.Size, nil
	}
	if !isNotFound(err) {
		return nil, reqSize, dbErrToBatchError(err, ctx)
	}

	targetResources := []string{"/data_file"}
	if !hasMethodAccess(ctx, "file_upload", targetResources) && !hasMethodAccess(ctx, "create", targetResources) {
		return nil, reqSize, &lfsapi.ObjectError{Code: int32(unauthorizedStatus(ctx)), Message: "unauthorized"}
	}

	creds, credErr := database.ListS3Credentials(ctx)
	if credErr != nil || len(creds) == 0 || strings.TrimSpace(creds[0].Bucket) == "" {
		if credErr == nil {
			credErr = fmt.Errorf("no bucket configured")
		}
		return nil, reqSize, &lfsapi.ObjectError{Code: int32(http.StatusInsufficientStorage), Message: credErr.Error()}
	}
	size := reqSize
	if size < 0 {
		size = 0
	}
	uploadURL := baseURL + "/info/lfs/objects/" + oid
	verifyURL := baseURL + "/info/lfs/verify"
	uploadAction := lfsapi.Action{Href: uploadURL}
	verifyAction := lfsapi.Action{Href: verifyURL}
	return &lfsapi.BatchActions{Upload: &uploadAction, Verify: &verifyAction}, size, nil
}

func ProxySinglePut(ctx context.Context, uM urlmanager.UrlManager, bucket, key string) error {
	s3URL := fmt.Sprintf("s3://%s/%s", bucket, key)
	signedURL, err := uM.SignUploadURL(ctx, "", s3URL, urlmanager.SignOptions{})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, signedURL, bytes.NewReader(nil))
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("s3 put failed status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func UploadPartToSignedURL(ctx context.Context, signedURL string, content []byte) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, signedURL, bytes.NewReader(content))
	if err != nil {
		return "", err
	}
	req.ContentLength = int64(len(content))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return "", fmt.Errorf("multipart part put failed status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	etag := strings.TrimSpace(resp.Header.Get("ETag"))
	etag = strings.Trim(etag, "\"")
	if etag == "" {
		return "", fmt.Errorf("multipart part upload missing etag")
	}
	return etag, nil
}

func S3KeyFromCandidateForBucket(candidate drs.DrsObjectCandidate, bucket string) (string, bool) {
	targetBucket := strings.TrimSpace(bucket)
	if targetBucket == "" {
		return "", false
	}
	if candidate.AccessMethods != nil {
		for _, am := range *candidate.AccessMethods {
			if am.AccessUrl == nil {
				continue
			}
			raw := strings.TrimSpace(am.AccessUrl.Url)
			if raw == "" {
				continue
			}
			u, err := url.Parse(raw)
			if err != nil || !strings.EqualFold(u.Scheme, "s3") {
				continue
			}
			if strings.TrimSpace(u.Host) != targetBucket {
				continue
			}
			key := strings.TrimPrefix(strings.TrimSpace(u.Path), "/")
			if key != "" {
				return key, true
			}
		}
	}
	return "", false
}

func hasCtxMethodAccess(ctx context.Context, method string, resources []string) bool {
	return authz.HasMethodAccess(ctx, method, resources)
}

func IsAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already exists") ||
		strings.Contains(msg, "unique constraint") ||
		strings.Contains(msg, "duplicate key") ||
		strings.Contains(msg, "unique violation")
}

func isNotFound(err error) bool {
	return errors.Is(err, common.ErrNotFound)
}

func unauthorizedStatus(ctx context.Context) int {
	if authz.IsGen3Mode(ctx) && !authz.HasAuthHeader(ctx) {
		return http.StatusUnauthorized
	}
	return http.StatusForbidden
}

func hasMethodAccess(ctx context.Context, method string, resources []string) bool {
	return authz.HasMethodAccess(ctx, method, resources)
}

func hasGlobalAccess(ctx context.Context, method string) bool {
	return authz.HasMethodAccess(ctx, method, []string{"/data_file"})
}

func dbErrToBatchError(err error, ctx context.Context) *lfsapi.ObjectError {
	if isNotFound(err) {
		return &lfsapi.ObjectError{Code: int32(http.StatusNotFound), Message: "object not found"}
	}
	if errors.Is(err, common.ErrUnauthorized) {
		return &lfsapi.ObjectError{Code: int32(unauthorizedStatus(ctx)), Message: "unauthorized"}
	}
	return &lfsapi.ObjectError{Code: int32(http.StatusInternalServerError), Message: err.Error()}
}

func derefStringSlice(in *[]string) []string {
	if in == nil {
		return nil
	}
	return append([]string(nil), (*in)...)
}
