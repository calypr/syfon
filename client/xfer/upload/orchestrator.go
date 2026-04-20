package upload

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	drsapi "github.com/calypr/syfon/apigen/client/drs"
	"github.com/calypr/syfon/client/common"
	"github.com/calypr/syfon/client/transfer"
)

type MetadataClient interface {
	GetObject(ctx context.Context, objectID string) (drsapi.DrsObject, error)
	RegisterObjects(ctx context.Context, req drsapi.RegisterObjectsJSONRequestBody) (drsapi.N201ObjectsCreated, error)
}

// RegisterFile orchestrates the full registration and upload flow:
// 1. Build a DRS object from the local file (if not provided).
// 2. Register metadata with the DRS server via the provided drs.Client.
// 3. Upload the file content via the provided Backend.
func RegisterFile(ctx context.Context, bk UploadBackend, dc MetadataClient, drsObject *drsapi.DrsObject, filePath string, bucketName string) (*drsapi.DrsObject, error) {
	// 1. Ensure we have a valid OID/metadata.
	// (Logic ported and generalized from git-drs/client/local/local_client.go)

	if drsObject == nil {
		return nil, fmt.Errorf("drsObject must be provided (containing at least checksums/size)")
	}
	requestedID := strings.TrimSpace(drsObject.Id)
	storageID := requestedID

	// 2. Register with DRS server
	requestedAlias := "id:" + requestedID
	finalAliases := []string{requestedAlias}
	if drsObject.Aliases != nil {
		finalAliases = append(finalAliases, *drsObject.Aliases...)
	}

	candidates := []drsapi.DrsObjectCandidate{{
		Name:          drsObject.Name,
		Size:          drsObject.Size,
		Checksums:     drsObject.Checksums,
		Aliases:       &finalAliases,
		AccessMethods: nil, // Will be filled after upload
	}}
	res, err := dc.RegisterObjects(ctx, drsapi.RegisterObjectsJSONRequestBody{
		Candidates: candidates,
	})
	if err == nil && len(res.Objects) > 0 && strings.TrimSpace(res.Objects[0].Id) != "" {
		drsObject.Id = res.Objects[0].Id
	}

	// 3. Check if file is already downloadable (optional but good optimization)
	// (Skipping for now to prioritize core functionality, but can be added back)

	// 4. Determine upload filename/key
	// Content-Addressable Storage (CAS): We prioritize the SHA256 hash as the storage key.
	uploadFilename := filepath.Base(filePath)
	for _, c := range drsObject.Checksums {
		if strings.ToLower(c.Type) == "sha256" {
			uploadFilename = c.Checksum
			break
		}
	}

	if drsObject.AccessMethods != nil && len(*drsObject.AccessMethods) > 0 {
		for _, am := range *drsObject.AccessMethods {
			if am.Type == "s3" || am.Type == "gs" {
				if am.AccessUrl != nil && am.AccessUrl.Url == "" {
					continue
				}
				if am.AccessUrl != nil {
					parts := strings.Split(am.AccessUrl.Url, "/")
					if candidate := parts[len(parts)-1]; candidate != "" {
						uploadFilename = candidate
						break
					}
				}
			}
		}
	}

	// 5. Perform Upload
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file for upload: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	threshold := int64(4.5 * float64(common.GB)) // Default threshold with safety buffer
	if stat.Size() < threshold {
		uploadURL, err := bk.ResolveUploadURL(ctx, storageID, uploadFilename, common.FileMetadata{}, bucketName)
		if err != nil {
			return nil, fmt.Errorf("failed to get upload URL: %w", err)
		}
		if err := bk.Upload(ctx, uploadURL, file, stat.Size()); err != nil {
			return nil, fmt.Errorf("upload failed: %w", err)
		}

		// 6. Finalize registration for single-part (Multipart handles its own completion)
		canonical, err := bk.CanonicalObjectURL(uploadURL, bucketName, storageID)
		if err != nil || canonical == "" {
			if err == nil {
				err = fmt.Errorf("empty canonical URL returned")
			}
			return nil, fmt.Errorf("failed to derive canonical object URL: %w", err)
		}

		// Fetch the latest record to preserve existing metadata (like Authz)
		current, getErr := dc.GetObject(ctx, drsObject.Id)
		if getErr != nil {
			current = *drsObject
		}

		u, parseErr := url.Parse(canonical)
		if parseErr != nil || u.Scheme == "" {
			return nil, fmt.Errorf("failed to determine provider type from canonical URL: %s", canonical)
		}
		pType := u.Scheme

		// Capture authorizations from existing access methods
		var authz []string
		if current.AccessMethods != nil {
			for _, m := range *current.AccessMethods {
				if m.Authorizations != nil && m.Authorizations.BearerAuthIssuers != nil && len(*m.Authorizations.BearerAuthIssuers) > 0 {
					authz = *m.Authorizations.BearerAuthIssuers
					break
				}
			}
		}

		am := drsapi.AccessMethod{
			Type: drsapi.AccessMethodType(pType),
			AccessUrl: &struct {
				Headers *[]string `json:"headers,omitempty"`
				Url     string    `json:"url"`
			}{Url: canonical},
			Authorizations: &struct {
				BearerAuthIssuers   *[]string                                          `json:"bearer_auth_issuers,omitempty"`
				DrsObjectId         *string                                            `json:"drs_object_id,omitempty"`
				PassportAuthIssuers *[]string                                          `json:"passport_auth_issuers,omitempty"`
				SupportedTypes      *[]drsapi.AccessMethodAuthorizationsSupportedTypes `json:"supported_types,omitempty"`
			}{
				BearerAuthIssuers: &authz,
			},
		}

		// Deep merge or update access methods
		found := false
		if current.AccessMethods != nil {
			for i, existing := range *current.AccessMethods {
				// Match by URL or by the specific pType we just uploaded to
				if (existing.AccessUrl != nil && existing.AccessUrl.Url == canonical) || (string(existing.Type) == pType && (existing.AccessUrl == nil || existing.AccessUrl.Url == "")) {
					// Update while keeping existing authorizations if our new am is empty
					if len(authz) == 0 && existing.Authorizations != nil && existing.Authorizations.BearerAuthIssuers != nil && len(*existing.Authorizations.BearerAuthIssuers) > 0 {
						am.Authorizations = existing.Authorizations
					}
					(*current.AccessMethods)[i] = am
					found = true
					break
				}
			}
		}
		if !found {
			if current.AccessMethods == nil {
				current.AccessMethods = &[]drsapi.AccessMethod{}
			}
			*current.AccessMethods = append(*current.AccessMethods, am)
		}

		// Finalize registration by updating with the access method
		candidates = []drsapi.DrsObjectCandidate{{
			// We use Aliases to reference the existing ID if we can't use id field
			Name:      drsObject.Name,
			Size:      drsObject.Size,
			Checksums: drsObject.Checksums,
			Aliases:   &[]string{requestedID},
			AccessMethods: &[]drsapi.AccessMethod{{
				Type:           drsapi.AccessMethodType(pType),
				AccessUrl:      am.AccessUrl,
				Authorizations: am.Authorizations,
			}},
		}}
		if _, updateErr := dc.RegisterObjects(ctx, drsapi.RegisterObjectsJSONRequestBody{
			Candidates: candidates,
		}); updateErr != nil {
			// Keep the local object usable even if the server-side metadata refresh is unavailable.
			// The caller can still persist the final access URL through the index API.
			drsObject.AccessMethods = &[]drsapi.AccessMethod{{
				Type:           drsapi.AccessMethodType(pType),
				AccessUrl:      am.AccessUrl,
				Authorizations: am.Authorizations,
			}}
		} else {
			drsObject.AccessMethods = &[]drsapi.AccessMethod{{
				Type:           drsapi.AccessMethodType(pType),
				AccessUrl:      am.AccessUrl,
				Authorizations: am.Authorizations,
			}}
		}
	} else {
		if err := Upload(ctx, bk, filePath, uploadFilename, storageID, bucketName, common.FileMetadata{}, false, true); err != nil {
			return nil, fmt.Errorf("multipart upload failed: %w", err)
		}
	}

	return drsObject, nil
}

type UploadBackend interface {
	transfer.Uploader
	transfer.Backend
}
