package converters

import (
	"fmt"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/apigen/server/lfsapi"
	syfoncommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/models"
)

func CandidateToInternalObject(c drs.DrsObjectCandidate, now time.Time) (models.InternalObject, error) {
	oid, ok := common.CanonicalSHA256(c.Checksums)
	if !ok {
		return models.InternalObject{}, fmt.Errorf("candidate must include sha256 checksum")
	}
	var ams []drs.AccessMethod
	if c.AccessMethods != nil {
		ams = *c.AccessMethods
	}
	authz := common.UniqueStrings(UniqueAuthz(ams))
	obj := drs.DrsObject{
		Id:          common.MintObjectIDFromChecksum(oid, authz),
		Size:        c.Size,
		CreatedTime: now,
		UpdatedTime: &now,
		Version:     common.Ptr("1"),
		MimeType:    c.MimeType,
		Description: c.Description,
		Aliases:     common.Ptr(append([]string(nil), common.DerefStringSlice(c.Aliases)...)),
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

			newMethod := drs.AccessMethod{
				Type:     drs.AccessMethodType(am.Type),
				AccessId: accessID,
				AccessUrl: &struct {
					Headers *[]string `json:"headers,omitempty"`
					Url     string    `json:"url"`
				}{Url: url},
			}
			if authzMap := syfoncommon.AuthzListToMap(authz); authzMap != nil {
				newMethod.Authorizations = &authzMap
			}
			*obj.AccessMethods = append(*obj.AccessMethods, newMethod)
		}
	}
	return models.InternalObject{
		DrsObject:      obj,
		Authorizations: authz,
	}, nil
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
			if am.Authorizations != nil && am.Authorizations.BearerAuthIssuers != nil {
				if authzMap := syfoncommon.AuthzListToMap(*am.Authorizations.BearerAuthIssuers); authzMap != nil {
					drsMethod.Authorizations = &authzMap
				}
			}
			ams = append(ams, drsMethod)
		}
		out.AccessMethods = &ams
	}
	return out
}
