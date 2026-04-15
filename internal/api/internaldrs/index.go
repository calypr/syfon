package internaldrs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/internalapi"
	"github.com/calypr/syfon/internal/db/core"
	"github.com/calypr/syfon/internal/api/routeutil"
	corelogic "github.com/calypr/syfon/internal/coreapi"
	"github.com/calypr/syfon/internal/provider"
	"github.com/calypr/syfon/internal/urlmanager"
	"github.com/gofiber/fiber/v3"
)
type InternalServer struct {
	database core.DatabaseInterface
	uM       urlmanager.UrlManager
}

func NewInternalServer(database core.DatabaseInterface, uM urlmanager.UrlManager) *InternalServer {
	return &InternalServer{
		database: database,
		uM:       uM,
	}
}

// RegisterInternalIndexRoutes registers the Internal-compatible routes on the router.
func RegisterInternalIndexRoutes(router fiber.Router, database core.DatabaseInterface, uM ...urlmanager.UrlManager) {
	var manager urlmanager.UrlManager
	if len(uM) > 0 {
		manager = uM[0]
	}
	server := NewInternalServer(database, manager)
	strict := internalapi.NewStrictHandler(server, nil)
	internalapi.RegisterHandlers(router, strict)
	router.Get("/index/index", routeutil.Handler(drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleInternalList(w, r, database)
	}), "InternalList")))
	router.Get("/index/index/{id}", routeutil.Handler(drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleInternalGet(w, r, database)
	}), "InternalGet"), "id"))
	router.Post("/index/index/bulk/hashes", routeutil.Handler(drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleInternalBulkHashes(database).ServeHTTP(w, r)
	}), "InternalBulkHashes")))
	router.Post("/index/index/bulk", routeutil.Handler(drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleInternalBulkCreate(database).ServeHTTP(w, r)
	}), "InternalBulkCreate")))
}

func (s *InternalServer) InternalList(ctx context.Context, request internalapi.InternalListRequestObject) (internalapi.InternalListResponseObject, error) {
	params := request.Params
	// Query params: hash, hash_type
	var hash string
	if params.Hash != nil {
		hash = *params.Hash
	}

	if hash != "" {
		hashType := ""
		// Original handleInternalList parsed hashType from query, but params.HashType is not in InternalListParams?
		// Wait, let's check internal.gen.go again for InternalListParams.
		// Actually, I saw it before.
		hashType, hash = parseHashQuery(hash, "")

		objs, err := s.database.GetObjectsByChecksum(ctx, hash)
		if err != nil {
			return nil, err
		}

		var records []internalapi.InternalRecord
		for _, o := range objs {
			if hashType != "" && !objectHasChecksumTypeAndValue(o, hashType, hash) {
				continue
			}
			records = append(records, *drsToInternalRecord(&o))
		}

		return internalapi.InternalList200JSONResponse{Records: &records}, nil
	}

	scopePrefix, hasScope, err := parseScopeQueryFromParams(params)
	if err != nil {
		return internalapi.InternalList400Response{}, nil
	}

	limit := 50
	if params.Limit != nil {
		limit = *params.Limit
	}
	page := 0
	if params.Page != nil {
		page = *params.Page
	}
	offset := page * limit

	if hasScope {
		if core.IsGen3Mode(ctx) && !core.HasAuthHeader(ctx) {
			return internalapi.InternalList403Response{}, nil
		}
		ids, err := s.database.ListObjectIDsByResourcePrefix(ctx, scopePrefix)
		if err != nil {
			return nil, err
		}
		records := make([]internalapi.InternalRecord, 0, len(ids))
		for _, id := range ids {
			obj, err := s.database.GetObject(ctx, id)
			if err != nil {
				if errors.Is(err, core.ErrUnauthorized) || errors.Is(err, core.ErrNotFound) {
					continue
				}
				return nil, err
			}
			if len(obj.Authorizations) > 0 && !core.HasMethodAccess(ctx, "read", obj.Authorizations) {
				continue
			}
			records = append(records, *drsToInternalRecord(obj))
		}
		records = paginateRecords(records, offset, limit)
		return internalapi.InternalList200JSONResponse{Records: &records}, nil
	}

	if core.IsGen3Mode(ctx) && !core.HasAuthHeader(ctx) {
		return internalapi.InternalList403Response{}, nil
	}

	// Unscoped list: use root resource prefix to include all scoped records.
	ids, err := s.database.ListObjectIDsByResourcePrefix(ctx, "/")
	if err != nil {
		return nil, err
	}
	records := make([]internalapi.InternalRecord, 0, len(ids))
	for _, id := range ids {
		obj, err := s.database.GetObject(ctx, id)
		if err != nil {
			if errors.Is(err, core.ErrUnauthorized) || errors.Is(err, core.ErrNotFound) {
				continue
			}
			return nil, err
		}
		if len(obj.Authorizations) > 0 && !core.HasMethodAccess(ctx, "read", obj.Authorizations) {
			continue
		}
		records = append(records, *drsToInternalRecord(obj))
	}
	records = paginateRecords(records, offset, limit)
	return internalapi.InternalList200JSONResponse{Records: &records}, nil
}

func parseScopeQueryFromParams(params internalapi.InternalListParams) (string, bool, error) {
	authz := ""
	if params.Authz != nil {
		authz = strings.TrimSpace(*params.Authz)
	}
	if authz != "" {
		return authz, true, nil
	}
	org := ""
	if params.Organization != nil {
		org = strings.TrimSpace(*params.Organization)
	}
	if org == "" && params.Program != nil {
		org = strings.TrimSpace(*params.Program)
	}
	project := ""
	if params.Project != nil {
		project = strings.TrimSpace(*params.Project)
	}
	if project != "" && org == "" {
		return "", false, fmt.Errorf("organization is required when project is set")
	}
	path := core.ResourcePathForScope(org, project)
	if path != "" {
		return path, true, nil
	}
	return "", false, nil
}

func (s *InternalServer) InternalCreate(ctx context.Context, request internalapi.InternalCreateRequestObject) (internalapi.InternalCreateResponseObject, error) {
	req := request.Body
	if req == nil {
		return internalapi.InternalCreate400Response{}, nil
	}
	obj, err := internalToDrs(req)
	if err != nil {
		return internalapi.InternalCreate400Response{}, nil
	}
	aliased, canonicalObj, aliasErr := maybeAliasBySHA256(ctx, s.database, req, obj)
	if aliasErr != nil {
		return nil, aliasErr
	}
	if aliased {
		response := drsToInternal(canonicalObj)
		response.Did = obj.Id
		return internalapi.InternalCreate201JSONResponse(*response), nil
	}
	if err := s.database.CreateObject(ctx, obj); err != nil {
		return nil, err
	}

	response := drsToInternal(obj)
	return internalapi.InternalCreate201JSONResponse(*response), nil
}

func (s *InternalServer) InternalDeleteByQuery(ctx context.Context, request internalapi.InternalDeleteByQueryRequestObject) (internalapi.InternalDeleteByQueryResponseObject, error) {
	scopePrefix, hasScope, err := parseScopeQueryFromDeleteParams(request.Params)
	if err != nil {
		return internalapi.InternalDeleteByQuery400Response{}, nil
	}

	hash := ""
	if request.Params.Hash != nil {
		hash = *request.Params.Hash
	}
	hashType := ""
	if request.Params.HashType != nil {
		hashType = *request.Params.HashType
	}

	if !hasScope && hash == "" {
		return internalapi.InternalDeleteByQuery400Response{}, nil
	}
	if core.IsGen3Mode(ctx) && !core.HasAuthHeader(ctx) {
		return internalapi.InternalDeleteByQuery403Response{}, nil
	}

	var ids []string
	if hasScope {
		scopeIDs, err := s.database.ListObjectIDsByResourcePrefix(ctx, scopePrefix)
		if err != nil {
			return nil, err
		}
		ids = append(ids, scopeIDs...)
	}

	if hash != "" {
		hashType, hash = parseHashQuery(hash, hashType)
		objs, err := s.database.GetObjectsByChecksum(ctx, hash)
		if err != nil {
			return nil, err
		}
		for _, o := range objs {
			if hashType != "" && !objectHasChecksumTypeAndValue(o, hashType, hash) {
				continue
			}
			ids = append(ids, o.Id)
		}
	}

	toDelete := make([]string, 0, len(ids))
	for _, id := range ids {
		obj, err := s.database.GetObject(ctx, id)
		if err != nil {
			if errors.Is(err, core.ErrNotFound) {
				continue
			}
			return nil, err
		}
		targetResources := obj.Authorizations
		if !core.HasMethodAccess(ctx, "delete", targetResources) {
			continue
		}
		toDelete = append(toDelete, id)
	}
	if len(toDelete) > 0 {
		if err := s.database.BulkDeleteObjects(ctx, toDelete); err != nil {
			return nil, err
		}
	}
	count := int(len(toDelete))
	return internalapi.InternalDeleteByQuery200JSONResponse{Deleted: &count}, nil
}

func parseScopeQueryFromDeleteParams(params internalapi.InternalDeleteByQueryParams) (string, bool, error) {
	authz := ""
	if params.Authz != nil {
		authz = strings.TrimSpace(*params.Authz)
	}
	if authz != "" {
		return authz, true, nil
	}
	org := ""
	if params.Organization != nil {
		org = strings.TrimSpace(*params.Organization)
	}
	if org == "" && params.Program != nil {
		org = strings.TrimSpace(*params.Program)
	}
	project := ""
	if params.Project != nil {
		project = strings.TrimSpace(*params.Project)
	}
	if project != "" && org == "" {
		return "", false, fmt.Errorf("organization is required when project is set")
	}
	path := core.ResourcePathForScope(org, project)
	if path != "" {
		return path, true, nil
	}
	return "", false, nil
}

func (s *InternalServer) InternalGet(ctx context.Context, request internalapi.InternalGetRequestObject) (internalapi.InternalGetResponseObject, error) {
	id := request.Id

	obj, err := s.database.GetObject(ctx, id)
	if err != nil {
		if errors.Is(err, core.ErrNotFound) {
			return internalapi.InternalGet404Response{}, nil
		}
		return nil, err
	}

	record := drsToInternal(obj)
	return internalapi.InternalGet200JSONResponse(*record), nil
}

func (s *InternalServer) InternalUpdate(ctx context.Context, request internalapi.InternalUpdateRequestObject) (internalapi.InternalUpdateResponseObject, error) {
	id := request.Id
	req := request.Body
	if req == nil {
		return internalapi.InternalUpdate400Response{}, nil
	}

	// Fetch existing first to check existence
	existing, err := s.database.GetObject(ctx, id)
	if err != nil {
		if errors.Is(err, core.ErrNotFound) {
			return internalapi.InternalUpdate404Response{}, nil
		}
		return nil, err
	}
	if req.Did != "" && req.Did != id {
		return internalapi.InternalUpdate400Response{}, nil // did cannot be changed
	}

	updated := *existing
	now := time.Now()
	updated.UpdatedTime = &now
	updated.Id = id
	updated.SelfUri = "drs://" + id

	// Internal PUT typically sends full record payload. We treat present fields as replacements.
	if req.Size != nil {
		updated.Size = *req.Size
	}
	if req.FileName != nil && *req.FileName != "" {
		updated.Name = req.FileName
	}

	if req.Urls != nil {
		methods := make([]drs.AccessMethod, 0, len(*req.Urls))
		for _, uString := range *req.Urls {
			parsed, pErr := url.Parse(uString)
			pType := "s3" // default
			if pErr == nil && parsed.Scheme != "" {
				pType = provider.FromScheme(parsed.Scheme)
				if pType == "" {
					pType = parsed.Scheme
				}
			}
			am := drs.AccessMethod{
				Type:      drs.AccessMethodType(pType),
				AccessUrl: &struct {
					Headers *[]string `json:"headers,omitempty"`
					Url     string    `json:"url"`
				}{Url: uString},
				Region: core.Ptr("us-east-1"),
			}
			methods = append(methods, am)
		}
		updated.AccessMethods = &methods
	}

	updated.Authorizations = append([]string(nil), req.Authz...)
	if req.Hashes != nil && len(*req.Hashes) > 0 {
		updated.Checksums = nil
		for t, v := range *req.Hashes {
			updated.Checksums = append(updated.Checksums, drs.Checksum{Type: t, Checksum: v})
		}
		if len(updated.Checksums) == 0 {
			updated.Checksums = append(updated.Checksums, drs.Checksum{Type: "sha256", Checksum: id})
		}
	}

	if err := s.database.RegisterObjects(ctx, []core.InternalObject{
		updated,
	}); err != nil {
		return nil, err
	}

	// Re-fetch to return latest state
	updatedObj, err := s.database.GetObject(ctx, id)
	if err != nil {
		return nil, err
	}

	response := drsToInternal(updatedObj)
	return internalapi.InternalUpdate200JSONResponse(*response), nil
}

func (s *InternalServer) InternalDelete(ctx context.Context, request internalapi.InternalDeleteRequestObject) (internalapi.InternalDeleteResponseObject, error) {
	id := request.Id

	if err := s.database.DeleteObject(ctx, id); err != nil {
		if errors.Is(err, core.ErrNotFound) {
			return internalapi.InternalDelete404Response{}, nil
		}
		return nil, err
	}

	return internalapi.InternalDelete200Response{}, nil
}

func (s *InternalServer) InternalBulkCreate(ctx context.Context, request internalapi.InternalBulkCreateRequestObject) (internalapi.InternalBulkCreateResponseObject, error) {
	req := request.Body
	if req == nil {
		return internalapi.InternalBulkCreate400Response{}, nil
	}
	if len(req.Records) == 0 {
		return internalapi.InternalBulkCreate400Response{}, nil
	}
	results := make([]internalapi.InternalRecord, 0, len(req.Records))
	for i := range req.Records {
		rec := &req.Records[i]
		obj, err := internalToDrs(rec)
		if err != nil {
			return internalapi.InternalBulkCreate400Response{}, nil
		}
		targetResources := obj.Authorizations
		if !core.HasMethodAccess(ctx, "create", targetResources) {
			return internalapi.InternalBulkCreate403Response{}, nil
		}

		aliased, canonicalObj, aliasErr := maybeAliasBySHA256(ctx, s.database, rec, obj)
		if aliasErr != nil {
			return nil, aliasErr
		}
		if aliased {
			resp := drsToInternalRecord(canonicalObj)
			resp.Did = obj.Id
			results = append(results, *resp)
			continue
		}
		if err := s.database.CreateObject(ctx, obj); err != nil {
			return nil, err
		}
		results = append(results, *drsToInternalRecord(obj))
	}
	return internalapi.InternalBulkCreate201JSONResponse{Records: &results}, nil
}

func (s *InternalServer) InternalBulkDeleteHashes(ctx context.Context, request internalapi.InternalBulkDeleteHashesRequestObject) (internalapi.InternalBulkDeleteHashesResponseObject, error) {
	req := request.Body
	if req == nil {
		return internalapi.InternalBulkDeleteHashes400Response{}, nil
	}
	if len(req.Hashes) == 0 {
		return internalapi.InternalBulkDeleteHashes400Response{}, nil
	}

	targetHashes := make([]string, len(req.Hashes))
	targetTypes := make([]string, len(req.Hashes))
	for i, h := range req.Hashes {
		targetTypes[i], targetHashes[i] = parseHashQuery(h, "")
	}

	objsMap, err := s.database.GetObjectsByChecksums(ctx, targetHashes)
	if err != nil {
		return nil, err
	}

	toDelete := make([]string, 0)
	seen := make(map[string]struct{})
	for i := range targetHashes {
		hash := targetHashes[i]
		objs := objsMap[hash]
		for _, o := range objs {
			if targetTypes[i] != "" && !objectHasChecksumTypeAndValue(o, targetTypes[i], hash) {
				continue
			}
			if _, exists := seen[o.Id]; exists {
				continue
			}
			targetResources := o.Authorizations
			if !core.HasMethodAccess(ctx, "delete", targetResources) {
				continue
			}
			seen[o.Id] = struct{}{}
			toDelete = append(toDelete, o.Id)
		}
	}

	if len(toDelete) > 0 {
		if err := s.database.BulkDeleteObjects(ctx, toDelete); err != nil {
			return nil, err
		}
	}

	count := int(len(toDelete))
	return internalapi.InternalBulkDeleteHashes200JSONResponse{Deleted: &count}, nil
}

func (s *InternalServer) InternalBulkSHA256Validity(ctx context.Context, request internalapi.InternalBulkSHA256ValidityRequestObject) (internalapi.InternalBulkSHA256ValidityResponseObject, error) {
	req := request.Body
	if req == nil {
		return internalapi.InternalBulkSHA256Validity400Response{}, nil
	}

	input := make([]string, 0)
	if req.Sha256 != nil {
		input = *req.Sha256
	} else if req.Hashes != nil {
		input = *req.Hashes
	}

	if len(input) == 0 {
		return internalapi.InternalBulkSHA256Validity400Response{}, nil
	}

	resp, err := corelogic.ComputeSHA256Validity(ctx, s.database, input)
	if err != nil {
		if errors.Is(err, corelogic.ErrNoValidSHA256) {
			return internalapi.InternalBulkSHA256Validity400Response{}, nil
		}
		return nil, err
	}

	return internalapi.InternalBulkSHA256Validity200JSONResponse(resp), nil
}

func (s *InternalServer) InternalBulkDocuments(ctx context.Context, request internalapi.InternalBulkDocumentsRequestObject) (internalapi.InternalBulkDocumentsResponseObject, error) {
	req := request.Body
	if req == nil {
		return internalapi.InternalBulkDocuments400Response{}, nil
	}

	var dids []string
	if d0, err := req.AsBulkDocumentsRequest0(); err == nil {
		dids = d0
	} else if d1, err := req.AsBulkDocumentsRequest1(); err == nil {
		if d1.Dids != nil {
			dids = *d1.Dids
		} else if d1.Ids != nil {
			dids = *d1.Ids
		}
	}

	if len(dids) == 0 {
		return internalapi.InternalBulkDocuments400Response{}, nil
	}

	objs, err := s.database.GetBulkObjects(ctx, dids)
	if err != nil {
		return nil, err
	}

	out := make([]internalapi.InternalRecordResponse, 0, len(objs))
	for i := range objs {
		if len(objs[i].Authorizations) > 0 && !core.HasMethodAccess(ctx, "read", objs[i].Authorizations) {
			continue
		}
		out = append(out, *drsToInternal(&objs[i]))
	}

	return internalapi.InternalBulkDocuments200JSONResponse(out), nil
}

func (s *InternalServer) InternalBulkHashes(ctx context.Context, request internalapi.InternalBulkHashesRequestObject) (internalapi.InternalBulkHashesResponseObject, error) {
	req := request.Body
	if req == nil {
		return internalapi.InternalBulkHashes400Response{}, nil
	}

	targetHashes := make([]string, len(req.Hashes))
	targetTypes := make([]string, len(req.Hashes))
	for i, h := range req.Hashes {
		targetTypes[i], targetHashes[i] = parseHashQuery(h, "")
	}

	objsMap, err := s.database.GetObjectsByChecksums(ctx, targetHashes)
	if err != nil {
		return nil, err
	}

	results := make([]internalapi.InternalRecord, 0)
	seen := make(map[string]struct{})
	for i := range targetHashes {
		hash := targetHashes[i]
		objs := objsMap[hash]
		for _, o := range objs {
			if targetTypes[i] != "" && !objectHasChecksumTypeAndValue(o, targetTypes[i], hash) {
				continue
			}
			if len(o.Authorizations) > 0 && !core.HasMethodAccess(ctx, "read", o.Authorizations) {
				continue
			}
			if _, exists := seen[o.Id]; exists {
				continue
			}
			seen[o.Id] = struct{}{}
			results = append(results, *drsToInternalRecord(&o))
		}
	}

	return internalapi.InternalBulkHashes200JSONResponse{Records: &results}, nil
}

// handleInternalGet retrieves a record by DID.
func handleInternalGet(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	id := routeutil.PathParam(r, "id")

	obj, err := database.GetObject(r.Context(), id)
	if err != nil {
		writeDBError(w, r, err)
		return
	}

	record := drsToInternal(obj)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(record); err != nil {
		slog.Error("gen3 encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
	}
}

// handleInternalCreate creates a new record.
func handleInternalCreate(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	var req internalapi.InternalRecord
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, "Invalid request body", nil)
		return
	}
	obj, err := internalToDrs(&req)
	if err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, "Invalid request body", err)
		return
	}
	aliased, canonicalObj, aliasErr := maybeAliasBySHA256(r.Context(), database, &req, obj)
	if aliasErr != nil {
		writeDBError(w, r, aliasErr)
		return
	}
	if aliased {
		response := drsToInternal(canonicalObj)
		response.Did = obj.Id
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		if err := json.NewEncoder(w).Encode(response); err != nil {
			slog.Error("gen3 encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
		}
		return
	}
	if err := database.CreateObject(r.Context(), obj); err != nil {
		writeDBError(w, r, err)
		return
	}

	response := drsToInternal(obj)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated) // 201
	if err := json.NewEncoder(w).Encode(response); err != nil {
		slog.Error("gen3 encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
	}
}

func handleInternalBulkHashes(database core.DatabaseInterface) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req internalapi.InternalBulkHashesJSONRequestBody
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeHTTPError(w, r, http.StatusBadRequest, "Invalid request body", nil)
			return
		}

		resp, err := NewInternalServer(database, nil).InternalBulkHashes(r.Context(), internalapi.InternalBulkHashesRequestObject{Body: &req})
		if err != nil {
			writeDBError(w, r, err)
			return
		}

		writeInternalAPIResponse(w, resp)
	})
}

func handleInternalBulkCreate(database core.DatabaseInterface) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req internalapi.InternalBulkCreateJSONRequestBody
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeHTTPError(w, r, http.StatusBadRequest, "Invalid request body", nil)
			return
		}

		resp, err := NewInternalServer(database, nil).InternalBulkCreate(r.Context(), internalapi.InternalBulkCreateRequestObject{Body: &req})
		if err != nil {
			writeDBError(w, r, err)
			return
		}

		writeInternalAPIResponse(w, resp)
	})
}

func writeInternalAPIResponse(w http.ResponseWriter, resp any) {
	w.Header().Set("Content-Type", "application/json")
	switch v := resp.(type) {
	case internalapi.InternalBulkHashes200JSONResponse:
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(v)
	case internalapi.InternalBulkHashes400Response:
		w.WriteHeader(http.StatusBadRequest)
	case internalapi.InternalBulkHashes500Response:
		w.WriteHeader(http.StatusInternalServerError)
	case internalapi.InternalBulkCreate201JSONResponse:
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(v)
	case internalapi.InternalBulkCreate400Response:
		w.WriteHeader(http.StatusBadRequest)
	case internalapi.InternalBulkCreate403Response:
		w.WriteHeader(http.StatusForbidden)
	case internalapi.InternalBulkCreate500Response:
		w.WriteHeader(http.StatusInternalServerError)
	default:
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(v)
	}
}

func maybeAliasBySHA256(ctx context.Context, database core.DatabaseInterface, req *internalapi.InternalRecord, obj *core.InternalObject) (bool, *core.InternalObject, error) {
	if obj == nil {
		return false, nil, nil
	}
	sha := ""
	if req != nil && req.Hashes != nil {
		sha = strings.TrimSpace((*req.Hashes)["sha256"])
	}
	if sha == "" {
		return false, nil, nil
	}

	existing, err := database.GetObjectsByChecksum(ctx, sha)
	if err != nil {
		return false, nil, err
	}
	if len(existing) == 0 {
		return false, nil, nil
	}
	sort.Slice(existing, func(i, j int) bool { return existing[i].Id < existing[j].Id })
	canonical := existing[0]
	if strings.TrimSpace(canonical.Id) == "" || canonical.Id == obj.Id {
		return false, nil, nil
	}
	if err := database.CreateObjectAlias(ctx, obj.Id, canonical.Id); err != nil {
		return false, nil, err
	}
	canonicalCopy := canonical
	return true, &canonicalCopy, nil
}

// handleInternalUpdate updates an existing record.
func handleInternalUpdate(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	id := routeutil.PathParam(r, "id")

	// Decode into InternalRecordResponse because the client serializes that type,
	// which contains extra read-only fields (baseid, rev, created_date, etc.) that
	// are absent from InternalRecord. InternalRecord's UnmarshalJSON uses
	// DisallowUnknownFields and would reject those extra fields with a 400.
	// Both types share the same writable fields so we can read them from the response type.
	var req internalapi.InternalRecordResponse
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, "Invalid request body", nil)
		return
	}

	// Fetch existing first to check existence
	existing, err := database.GetObject(r.Context(), id)
	if err != nil {
		writeDBError(w, r, err)
		return
	}
	if req.Did != "" && req.Did != id {
		writeHTTPError(w, r, http.StatusBadRequest, "did cannot be changed", nil)
		return
	}

	updated := *existing
	now := time.Now()
	updated.UpdatedTime = &now
	updated.Id = id
	updated.SelfUri = "drs://" + id

	// Internal PUT typically sends full record payload. We treat present fields as replacements.
	if req.Size != nil {
		updated.Size = *req.Size
	}
	if req.FileName != nil && strings.TrimSpace(*req.FileName) != "" {
		updated.Name = req.FileName
	}

	if req.Urls != nil {
		methods := make([]drs.AccessMethod, 0, len(*req.Urls))
		for _, uString := range *req.Urls {
			parsed, pErr := url.Parse(uString)
			pType := "s3" // default
			if pErr == nil && parsed.Scheme != "" {
				pType = provider.FromScheme(parsed.Scheme)
				if pType == "" {
					pType = parsed.Scheme
				}
			}
			am := drs.AccessMethod{
				Type:      drs.AccessMethodType(pType),
				AccessUrl: &struct {
					Headers *[]string `json:"headers,omitempty"`
					Url     string    `json:"url"`
				}{Url: uString},
				Region: core.Ptr("us-east-1"),
			}
			methods = append(methods, am)
		}
		updated.AccessMethods = &methods
	}

	updated.Authorizations = append([]string(nil), req.Authz...)
	if req.Hashes != nil && len(*req.Hashes) > 0 {
		updated.Checksums = nil
		for t, v := range *req.Hashes {
			updated.Checksums = append(updated.Checksums, drs.Checksum{Type: t, Checksum: v})
		}
		if len(updated.Checksums) == 0 {
			updated.Checksums = append(updated.Checksums, drs.Checksum{Type: "sha256", Checksum: id})
		}
	}

	if err := database.RegisterObjects(r.Context(), []core.InternalObject{
		updated,
	}); err != nil {
		writeHTTPError(w, r, http.StatusInternalServerError, fmt.Sprintf("Failed to update object: %v", err), err)
		return
	}

	// Re-fetch to return latest state
	updatedObj, err := database.GetObject(r.Context(), id)
	if err != nil {
		writeDBError(w, r, err)
		return
	}

	response := drsToInternal(updatedObj)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		slog.Error("gen3 encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
	}
}

// handleInternalDelete deletes a record.
func handleInternalDelete(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	id := routeutil.PathParam(r, "id")

	if err := database.DeleteObject(r.Context(), id); err != nil {
		writeDBError(w, r, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func parseScopeQuery(r *http.Request) (string, bool, error) {
	authz := strings.TrimSpace(r.URL.Query().Get("authz"))
	if authz != "" {
		return authz, true, nil
	}
	org := strings.TrimSpace(r.URL.Query().Get("organization"))
	if org == "" {
		org = strings.TrimSpace(r.URL.Query().Get("program"))
	}
	project := strings.TrimSpace(r.URL.Query().Get("project"))
	if project != "" && org == "" {
		return "", false, fmt.Errorf("organization is required when project is set")
	}
	path := core.ResourcePathForScope(org, project)
	if path != "" {
		return path, true, nil
	}
	return "", false, nil
}

func handleInternalDeleteByQuery(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	scopePrefix, hasScope, err := parseScopeQuery(r)
	if err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, err.Error(), err)
		return
	}

	hash := r.URL.Query().Get("hash")
	hashType := r.URL.Query().Get("hash_type")

	if !hasScope && hash == "" {
		writeHTTPError(w, r, http.StatusBadRequest, "organization/project, authz, or hash query is required", nil)
		return
	}
	if core.IsGen3Mode(r.Context()) && !core.HasAuthHeader(r.Context()) {
		writeAuthError(w, r)
		return
	}

	var ids []string
	if hasScope {
		scopeIDs, err := database.ListObjectIDsByResourcePrefix(r.Context(), scopePrefix)
		if err != nil {
			writeHTTPError(w, r, http.StatusInternalServerError, fmt.Sprintf("failed to list records by scope: %v", err), err)
			return
		}
		ids = append(ids, scopeIDs...)
	}

	if hash != "" {
		hashType, hash = parseHashQuery(hash, hashType)
		objs, err := database.GetObjectsByChecksum(r.Context(), hash)
		if err != nil {
			writeDBError(w, r, err)
			return
		}
		for _, o := range objs {
			if hashType != "" && !objectHasChecksumTypeAndValue(o, hashType, hash) {
				continue
			}
			ids = append(ids, o.Id)
		}
	}

	toDelete := make([]string, 0, len(ids))
	for _, id := range ids {
		obj, err := database.GetObject(r.Context(), id)
		if err != nil {
			if errors.Is(err, core.ErrNotFound) {
				continue
			}
			writeDBError(w, r, err)
			return
		}
		targetResources := obj.Authorizations
		if !core.HasMethodAccess(r.Context(), "delete", targetResources) {
			writeAuthError(w, r)
			return
		}
		toDelete = append(toDelete, id)
	}
	if len(toDelete) > 0 {
		if err := database.BulkDeleteObjects(r.Context(), toDelete); err != nil {
			writeHTTPError(w, r, http.StatusInternalServerError, fmt.Sprintf("failed to delete records: %v", err), err)
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	count := len(toDelete)
	if err := json.NewEncoder(w).Encode(internalapi.DeleteByQueryResponse{Deleted: &count}); err != nil {
		slog.Error("gen3 encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
	}
}

// handleInternalList handles listing, primarily to support lookup by hash.
func handleInternalList(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	// Query params: hash, hash_type
	hash := r.URL.Query().Get("hash")
	hashType := r.URL.Query().Get("hash_type")

	if hash != "" {
		hashType, hash = parseHashQuery(hash, hashType)

		objs, err := database.GetObjectsByChecksum(r.Context(), hash)
		if err != nil {
			writeDBError(w, r, err)
			return
		}

		var records []internalapi.InternalRecord
		for _, o := range objs {
			if hashType != "" && !objectHasChecksumTypeAndValue(o, hashType, hash) {
				continue
			}
			records = append(records, *drsToInternalRecord(&o))
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{"records": records}); err != nil {
			slog.Error("gen3 encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
		}
		return
	}
	scopePrefix, hasScope, err := parseScopeQuery(r)
	if err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, err.Error(), err)
		return
	}
	limit, page, err := parseListPagination(r)
	if err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, err.Error(), err)
		return
	}
	offset := page * limit
	if hasScope {
		if core.IsGen3Mode(r.Context()) && !core.HasAuthHeader(r.Context()) {
			writeAuthError(w, r)
			return
		}
		ids, err := database.ListObjectIDsByResourcePrefix(r.Context(), scopePrefix)
		if err != nil {
			writeHTTPError(w, r, http.StatusInternalServerError, fmt.Sprintf("Error listing records: %v", err), err)
			return
		}
		records := make([]internalapi.InternalRecord, 0, len(ids))
		for _, id := range ids {
			obj, err := database.GetObject(r.Context(), id)
			if err != nil {
				if errors.Is(err, core.ErrUnauthorized) || errors.Is(err, core.ErrNotFound) {
					continue
				}
				writeHTTPError(w, r, http.StatusInternalServerError, fmt.Sprintf("Error fetching object %s: %v", id, err), err)
				return
			}
			if len(obj.Authorizations) > 0 && !core.HasMethodAccess(r.Context(), "read", obj.Authorizations) {
				continue
			}
			records = append(records, *drsToInternalRecord(obj))
		}
		records = paginateRecords(records, offset, limit)
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{"records": records}); err != nil {
			slog.Error("gen3 encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
		}
		return
	}
	if core.IsGen3Mode(r.Context()) && !core.HasAuthHeader(r.Context()) {
		writeAuthError(w, r)
		return
	}
	// Unscoped list: use root resource prefix to include all scoped records.
	ids, err := database.ListObjectIDsByResourcePrefix(r.Context(), "/")
	if err != nil {
		writeHTTPError(w, r, http.StatusInternalServerError, fmt.Sprintf("Error listing records: %v", err), err)
		return
	}
	records := make([]internalapi.InternalRecord, 0, len(ids))
	for _, id := range ids {
		obj, err := database.GetObject(r.Context(), id)
		if err != nil {
			if errors.Is(err, core.ErrUnauthorized) || errors.Is(err, core.ErrNotFound) {
				continue
			}
			writeHTTPError(w, r, http.StatusInternalServerError, fmt.Sprintf("Error fetching object %s: %v", id, err), err)
			return
		}
		if len(obj.Authorizations) > 0 && !core.HasMethodAccess(r.Context(), "read", obj.Authorizations) {
			continue
		}
		records = append(records, *drsToInternalRecord(obj))
	}
	records = paginateRecords(records, offset, limit)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{"records": records}); err != nil {
		slog.Error("gen3 encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
	}
}

func parseListPagination(r *http.Request) (int, int, error) {
	const (
		defaultLimit = 50
		maxLimit     = 1000
	)
	limit := defaultLimit
	page := 0
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v <= 0 {
			return 0, 0, fmt.Errorf("limit must be a positive integer")
		}
		if v > maxLimit {
			v = maxLimit
		}
		limit = v
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("page")); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 0 {
			return 0, 0, fmt.Errorf("page must be a non-negative integer")
		}
		page = v
	}
	return limit, page, nil
}

func paginateRecords(records []internalapi.InternalRecord, offset, limit int) []internalapi.InternalRecord {
	if offset >= len(records) {
		return []internalapi.InternalRecord{}
	}
	end := offset + limit
	if end > len(records) {
		end = len(records)
	}
	return records[offset:end]
}
