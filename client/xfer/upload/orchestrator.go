package upload

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/calypr/syfon/client/drs"
	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/xfer"
)

// RegisterFile orchestrates the full registration and upload flow:
// 1. Build a DRS object from the local file (if not provided).
// 2. Register metadata with the DRS server via the provided drs.Client.
// 3. Upload the file content via the provided Backend.
func RegisterFile(ctx context.Context, bk UploadBackend, dc drs.Client, drsObject *drs.DRSObject, filePath string, bucketName string) (*drs.DRSObject, error) {
	// 1. Ensure we have a valid OID/metadata.
	// (Logic ported and generalized from git-drs/client/local/local_client.go)

	if drsObject == nil {
		return nil, fmt.Errorf("drsObject must be provided (containing at least checksums/size)")
	}

	// 2. Register with DRS server
	res, err := dc.RegisterRecord(ctx, drsObject)
	if err != nil {
		return nil, fmt.Errorf("failed to register record: %w", err)
	}
	drsObject = res

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

	if len(drsObject.AccessMethods) > 0 {
		for _, am := range drsObject.AccessMethods {
			if am.Type == "s3" || am.Type == "gs" {
				if am.AccessUrl.Url == "" {
					continue
				}
				parts := strings.Split(am.AccessUrl.Url, "/")
				if candidate := parts[len(parts)-1]; candidate != "" {
					uploadFilename = candidate
					break
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
		uploadURL, err := bk.ResolveUploadURL(ctx, drsObject.Id, uploadFilename, common.FileMetadata{}, bucketName)
		if err != nil {
			return nil, fmt.Errorf("failed to get upload URL: %w", err)
		}
		if err := bk.Upload(ctx, uploadURL, file, stat.Size()); err != nil {
			return nil, fmt.Errorf("upload failed: %w", err)
		}

		// 6. Finalize registration for single-part (Multipart handles its own completion)
		canonical, err := bk.CanonicalObjectURL(uploadURL, bucketName, drsObject.Id)
		if err != nil || canonical == "" {
			if err == nil {
				err = fmt.Errorf("empty canonical URL returned")
			}
			return nil, fmt.Errorf("failed to derive canonical object URL: %w", err)
		}

		// Fetch the latest record to preserve existing metadata (like Authz)
		current, getErr := dc.GetObject(ctx, drsObject.Id)
		if getErr != nil {
			current = drsObject
		}

		u, parseErr := url.Parse(canonical)
		if parseErr != nil || u.Scheme == "" {
			return nil, fmt.Errorf("failed to determine provider type from canonical URL: %s", canonical)
		}
		pType := u.Scheme

		// Capture authorizations from existing access methods
		var authz []string
		for _, m := range current.AccessMethods {
			if len(m.Authorizations.BearerAuthIssuers) > 0 {
				authz = m.Authorizations.BearerAuthIssuers
				break
			}
		}

		am := drs.AccessMethod{
			Type:      pType,
			AccessUrl: drs.AccessURL{Url: canonical},
			Authorizations: drs.Authorizations{
				BearerAuthIssuers: authz,
			},
		}

		// Deep merge or update access methods
		found := false
		for i, existing := range current.AccessMethods {
			// Match by URL or by the specific pType we just uploaded to
			if existing.AccessUrl.Url == canonical || (existing.Type == pType && existing.AccessUrl.Url == "") {
				// Update while keeping existing authorizations if our new am is empty
				if len(am.Authorizations.BearerAuthIssuers) == 0 && len(existing.Authorizations.BearerAuthIssuers) > 0 {
					am.Authorizations = existing.Authorizations
				}
				current.AccessMethods[i] = am
				found = true
				break
			}
		}
		if !found {
			current.AccessMethods = append(current.AccessMethods, am)
		}

		if _, updateErr := dc.UpdateRecord(ctx, current, drsObject.Id); updateErr != nil {
			return nil, fmt.Errorf("failed to finalize registration with server: %w", updateErr)
		}
	} else {
		if err := MultipartUpload(ctx, bk, filePath, uploadFilename, drsObject.Id, bucketName, common.FileMetadata{}, file, false); err != nil {
			return nil, fmt.Errorf("multipart upload failed: %w", err)
		}
	}

	return drsObject, nil
}

type UploadBackend = xfer.Uploader
