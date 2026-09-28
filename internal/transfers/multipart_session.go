package transfers

import (
	"context"
	"fmt"
	"time"

	"github.com/calypr/syfon/apigen/errorapi"
	"github.com/calypr/syfon/internal/access"
	"github.com/calypr/syfon/internal/storage"
)

type MultipartState string

const (
	MultipartStateActive     MultipartState = "active"
	MultipartStateCompleting MultipartState = "completing"
	MultipartStateCompleted  MultipartState = "completed"
)

type MultipartOperation string

const (
	MultipartOperationNone     MultipartOperation = ""
	MultipartOperationComplete MultipartOperation = "complete"
	MultipartOperationAbort    MultipartOperation = "abort"
)

type MultipartAuthorization struct {
	Resources []string     `json:"resources,omitempty"`
	Methods   []string     `json:"methods,omitempty"`
	Scope     *AccessScope `json:"scope,omitempty"`
}

func (a MultipartAuthorization) Authorize(ctx context.Context) error {
	if !access.IsAuthzEnforced(ctx) {
		return nil
	}
	if a.Scope != nil {
		return access.AuthorizeScopeWrite(ctx, a.Scope.Organization, a.Scope.Project, a.Methods...)
	}
	for _, method := range a.Methods {
		if access.HasObjectMethodAccess(ctx, method, a.Resources) {
			return nil
		}
	}
	return errorapi.ErrAccessDenied
}

type MultipartSession struct {
	UploadID          string                 `json:"upload_id"`
	CompletionID      string                 `json:"completion_id"`
	Target            storage.Target         `json:"target"`
	Authorization     MultipartAuthorization `json:"authorization"`
	State             MultipartState         `json:"state"`
	CompletionToken   string                 `json:"completion_token,omitempty"`
	PartsFingerprint  string                 `json:"parts_fingerprint,omitempty"`
	Operation         MultipartOperation     `json:"operation,omitempty"`
	CompletionParts   []CompletedPart        `json:"completion_parts,omitempty"`
	CompletedLocation string                 `json:"completed_location,omitempty"`
	CreatedAt         time.Time              `json:"created_at"`
	UpdatedAt         time.Time              `json:"updated_at"`
}

func multipartNotFound(uploadID string) error {
	return fmt.Errorf("%w: %s", errorapi.ErrMultipartUploadNotFound, uploadID)
}
