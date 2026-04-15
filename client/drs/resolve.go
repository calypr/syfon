package drs

import (
	"context"
	"fmt"
	"strings"

	"github.com/calypr/syfon/client/pkg/hash"
)

// ResolveObject centralizes object-id vs checksum resolution logic.
func ResolveObject(ctx context.Context, client Client, guid string) (*DRSObject, error) {
	id := ParseObjectIdentifier(guid)
	switch id.Kind() {
	case IdentifierUUID:
		return client.GetObject(ctx, id.UUID.String())
	case IdentifierChecksum:
		if hash.NormalizeChecksumType(id.Hash.Type) == hash.ChecksumTypeSHA256 {
			if cached, ok := PrefetchedBySHA(ctx, id.Hash.Checksum); ok {
				obj := cached
				return &obj, nil
			}
		}
		if recs, err := client.GetObjectByHash(ctx, id.Hash); err == nil && len(recs) > 0 {
			return &recs[0], nil
		}
		return client.GetObject(ctx, strings.TrimSpace(guid))
	default:
		if obj, err := client.GetObject(ctx, strings.TrimSpace(guid)); err == nil {
			return obj, nil
		}
		if parsedHash, ok := parseValidatedHashIdentifier(guid); ok {
			if hash.NormalizeChecksumType(parsedHash.Type) == hash.ChecksumTypeSHA256 {
				if cached, ok := PrefetchedBySHA(ctx, parsedHash.Checksum); ok {
					obj := cached
					return &obj, nil
				}
			}
			if recs, err := client.GetObjectByHash(ctx, parsedHash); err == nil && len(recs) > 0 {
				return &recs[0], nil
			}
		}
		return client.GetObject(ctx, strings.TrimSpace(guid))
	}
}

// ResolveDownloadURL resolves access method and object id when caller does not already provide a concrete access id.
func ResolveDownloadURL(ctx context.Context, client Client, guid string, accessID string) (string, error) {
	obj, err := ResolveObject(ctx, client, guid)
	if err != nil {
		return "", err
	}

	resolvedID := strings.TrimSpace(obj.Id)
	if resolvedID == "" {
		resolvedID = guid
	}

	if accessID == "" {
		if obj.AccessMethods != nil {
			for _, am := range *obj.AccessMethods {
				if am.AccessId != nil && *am.AccessId != "" {
					accessID = *am.AccessId
					break
				}
			}
		}
		if accessID == "" {
			if obj.AccessMethods != nil {
				for _, am := range *obj.AccessMethods {
					if am.AccessUrl.Url != "" {
						return am.AccessUrl.Url, nil
					}
				}
			}
			return "", fmt.Errorf("no suitable access method found for object %s", guid)
		}
	}

	accessURL, err := client.GetDownloadURL(ctx, resolvedID, accessID)
	if err != nil {
		return "", err
	}
	if accessURL == nil || accessURL.Url == "" {
		return "", fmt.Errorf("empty access URL for object %s", guid)
	}
	return accessURL.Url, nil
}
