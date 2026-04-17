package drsapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/api/routeutil"
	"github.com/calypr/syfon/internal/authz"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/db"
	"github.com/calypr/syfon/internal/models"
	"github.com/calypr/syfon/internal/urlmanager"
	"github.com/gofiber/fiber/v3"
)

// RegisterDRSRoutes binds standard GA4GH DRS handlers to the router.
func RegisterDRSRoutes(router fiber.Router, database db.DatabaseInterface, uM urlmanager.UrlManager) {
	// Standard GA4GH Routes
	router.Get("/objects/:object_id", routeutil.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleGetObject(w, r, database)
	}), "object_id"))

	router.Get("/objects/:object_id/access/:access_id", routeutil.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleGetAccessURL(w, r, database, uM)
	}), "object_id", "access_id"))

	router.Post("/objects/register", routeutil.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleRegisterObjects(w, r, database)
	})))

	// Helper for bulk resolution
	router.Post("/objects", routeutil.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleGetBulkObjects(w, r, database)
	})))

	router.Get("/service-info", routeutil.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleGetServiceInfo(w, r, database)
	})))

	router.Delete("/objects/:object_id", routeutil.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleDeleteObject(w, r, database)
	}), "object_id"))
}

func handleGetObject(w http.ResponseWriter, r *http.Request, db db.DatabaseInterface) {
	objectID := routeutil.PathParam(r, "object_id")
	obj, err := db.GetObject(r.Context(), objectID)
	if err != nil {
		writeDRSError(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(obj.DrsObject)
}

func handleGetAccessURL(w http.ResponseWriter, r *http.Request, db db.DatabaseInterface, uM urlmanager.UrlManager) {
	objectID := routeutil.PathParam(r, "object_id")
	accessID := routeutil.PathParam(r, "access_id")

	obj, err := db.GetObject(r.Context(), objectID)
	if err != nil {
		writeDRSError(w, r, err)
		return
	}

	if !authz.HasMethodAccess(r.Context(), "read", obj.Authorizations) {
		writeDRSAuthError(w, r)
		return
	}

	var accessURL string
	if obj.AccessMethods != nil {
		for _, am := range *obj.AccessMethods {
			if am.AccessId != nil && *am.AccessId == accessID {
				accessURL = am.AccessUrl.Url
				break
			}
			if string(am.Type) == accessID {
				accessURL = am.AccessUrl.Url
			}
		}
	}

	if accessURL == "" {
		writeDRSHTTPError(w, r, http.StatusNotFound, "access_id not found")
		return
	}

	bucketID := ""
	if parsed, err := url.Parse(accessURL); err == nil {
		bucketID = parsed.Host
	}

	signed, err := uM.SignURL(r.Context(), bucketID, accessURL, urlmanager.SignOptions{})
	if err != nil {
		writeDRSHTTPError(w, r, http.StatusInternalServerError, err.Error())
		return
	}

	_ = db.RecordFileDownload(r.Context(), objectID)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(drs.AccessURL{Url: signed})
}

func handleRegisterObjects(w http.ResponseWriter, r *http.Request, db db.DatabaseInterface) {
	var body drs.RegisterObjectsBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeDRSHTTPError(w, r, http.StatusBadRequest, "invalid request body")
		return
	}

	now := time.Now()
	internalObjs := make([]models.InternalObject, 0, len(body.Candidates))
	externalObjs := make([]drs.DrsObject, 0, len(body.Candidates))

	for _, c := range body.Candidates {
		sha, ok := common.CanonicalSHA256(c.Checksums)
		if !ok {
			writeDRSHTTPError(w, r, http.StatusBadRequest, "missing sha256 checksums")
			return
		}

		authz := []string{"/data_file"}
		obj := drs.DrsObject{
			Name:        c.Name,
			Size:        c.Size,
			CreatedTime: now,
			UpdatedTime: &now,
			Checksums:   []drs.Checksum{{Type: "sha256", Checksum: sha}},
		}

		if c.AccessMethods != nil {
			obj.AccessMethods = c.AccessMethods
		}

		id := common.MintObjectIDFromChecksum(sha, authz)
		obj.Id = id
		obj.SelfUri = "drs://" + id

		internalObjs = append(internalObjs, models.InternalObject{
			DrsObject:      obj,
			Authorizations: authz,
		})
		externalObjs = append(externalObjs, obj)
	}

	if err := db.RegisterObjects(r.Context(), internalObjs); err != nil {
		writeDRSHTTPError(w, r, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusCreated)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(drs.N201ObjectsCreatedJSONResponse{Objects: externalObjs})
}

func handleGetBulkObjects(w http.ResponseWriter, r *http.Request, db db.DatabaseInterface) {
	var body struct {
		BulkObjectIds []string `json:"bulk_object_ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeDRSHTTPError(w, r, http.StatusBadRequest, "invalid request body")
		return
	}

	fetched, err := db.GetBulkObjects(r.Context(), body.BulkObjectIds)
	if err != nil {
		writeDRSError(w, r, err)
		return
	}

	resolved := make([]drs.DrsObject, 0)
	for _, obj := range fetched {
		if authz.HasMethodAccess(r.Context(), "read", obj.Authorizations) {
			resolved = append(resolved, obj.DrsObject)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(drs.N200OkDrsObjectsJSONResponse{
		ResolvedDrsObject: &resolved,
		Summary: &drs.Summary{
			Requested: common.Ptr(len(body.BulkObjectIds)),
			Resolved:  common.Ptr(len(resolved)),
		},
	})
}

func handleGetServiceInfo(w http.ResponseWriter, r *http.Request, db db.DatabaseInterface) {
	info, err := db.GetServiceInfo(r.Context())
	if err != nil {
		writeDRSHTTPError(w, r, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(info)
}

func handleDeleteObject(w http.ResponseWriter, r *http.Request, db db.DatabaseInterface) {
	objectID := routeutil.PathParam(r, "object_id")
	obj, err := db.GetObject(r.Context(), objectID)
	if err != nil {
		writeDRSError(w, r, err)
		return
	}

	if !authz.HasMethodAccess(r.Context(), "delete", obj.Authorizations) {
		writeDRSAuthError(w, r)
		return
	}

	if err := db.DeleteObject(r.Context(), objectID); err != nil {
		writeDRSHTTPError(w, r, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// Errors
func writeDRSHTTPError(w http.ResponseWriter, r *http.Request, status int, msg string) {
	slog.Warn("drs request error", "status", status, "msg", msg, "path", r.URL.Path)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(drs.Error{
		Msg:        common.Ptr(msg),
		StatusCode: common.Ptr(status),
	})
}

func writeDRSAuthError(w http.ResponseWriter, r *http.Request) {
	code := http.StatusForbidden
	if authz.IsGen3Mode(r.Context()) && !authz.HasAuthHeader(r.Context()) {
		code = http.StatusUnauthorized
	}
	writeDRSHTTPError(w, r, code, "Forbidden")
}

func writeDRSError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, common.ErrNotFound) {
		writeDRSHTTPError(w, r, http.StatusNotFound, "Not Found")
		return
	}
	writeDRSHTTPError(w, r, http.StatusInternalServerError, err.Error())
}
