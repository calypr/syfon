package drsapi

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
	syfoncommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/api/apiutil"
	"github.com/calypr/syfon/internal/common"
	httpdrs "github.com/calypr/syfon/internal/httpapi/drs"
	"github.com/calypr/syfon/internal/objects"
	"github.com/gofiber/fiber/v3"
)

func handleRegisterObjectsFiber(service *objects.Service) fiber.Handler {
	return func(c fiber.Ctx) error {
		var body registerObjectsRequest
		if err := json.Unmarshal(c.Body(), &body); err != nil || len(body.Candidates) == 0 {
			var single registerObjectCandidate
			if err2 := json.Unmarshal(c.Body(), &single); err2 == nil && len(single.Checksums) > 0 {
				internalObj, err := registerCandidateToRecord(single, time.Now().UTC())
				if err != nil {
					return apiutil.HandleError(c, err)
				}
				if err := service.RegisterObjects(c.Context(), []objects.Record{internalObj}); err != nil {
					return apiutil.HandleError(c, err)
				}
				// Fetch back for full population (SelfUri, and access methods)
				finalObj, err := service.GetObject(c.Context(), string(internalObj.Id), "read")
				if err != nil {
					return apiutil.HandleError(c, err)
				}
				return c.Status(fiber.StatusCreated).JSON(fiber.Map{
					"objects": []any{httpdrs.ObjectPayload(*finalObj)},
				})
			}
			return c.Status(fiber.StatusBadRequest).JSON(drs.Error{Msg: common.Ptr("Invalid request body")})
		}

		// List of internal objects to register
		toRegister := make([]objects.Record, 0, len(body.Candidates))
		for _, cand := range body.Candidates {
			internalObj, err := registerCandidateToRecord(cand, time.Now().UTC())
			if err != nil {
				return apiutil.HandleError(c, err)
			}
			toRegister = append(toRegister, internalObj)
		}

		if err := service.RegisterObjects(c.Context(), toRegister); err != nil {
			return apiutil.HandleError(c, err)
		}

		// Reconstruct registered objects summary for response
		registered := make([]any, len(toRegister))
		for i, internal := range toRegister {
			// Fetch back to ensure full population
			obj, err := service.GetObject(c.Context(), string(internal.Id), "read")
			if err != nil {
				return apiutil.HandleError(c, err)
			}
			registered[i] = httpdrs.ObjectPayload(*obj)
		}

		return c.Status(fiber.StatusCreated).JSON(fiber.Map{"objects": registered})
	}
}

type registerObjectsRequest struct {
	Candidates []registerObjectCandidate `json:"candidates"`
}

type registerObjectCandidate struct {
	drs.DrsObjectCandidate
}

func registerCandidateToRecord(c registerObjectCandidate, now time.Time) (objects.Record, error) {
	return candidateToRecord(httpdrs.FromGeneratedCandidate(c.DrsObjectCandidate), now)
}

// candidateToRecord translates the HTTP-neutral registration candidate into a
// persisted object record before it enters objects.Service. Keeping this
// boundary adapter here avoids coupling the generated DRS contract to the
// object service package.
func candidateToRecord(c objects.Candidate, now time.Time) (objects.Record, error) {
	checksums := append([]objects.Checksum(nil), candidateChecksums(c.Checksums)...)
	oid, ok := objects.CanonicalSHA256(checksums)
	if !ok {
		return objects.Record{}, objects.ErrNoValidSHA256
	}
	if c.AccessMethods == nil || len(*c.AccessMethods) == 0 {
		return objects.Record{}, objects.ErrAccessMethodsRequired
	}
	authzList := syfoncommon.ControlledAccessToAuthzMap(common.DerefStringSlice(c.ControlledAccess))

	id := ""
	if c.Aliases != nil {
		for _, alias := range *c.Aliases {
			if strings.HasPrefix(alias, "id:") {
				id = strings.TrimPrefix(alias, "id:")
				break
			}
		}
	}
	if id == "" {
		mintedID, err := objects.MintRecordIDFromChecksum(oid, syfoncommon.AuthzMapToList(authzList))
		if err != nil {
			return objects.Record{}, err
		}
		id = string(mintedID)
	}

	obj := objects.Record{
		Id:          objects.RecordID(id),
		Size:        common.Int64Val(c.Size),
		CreatedTime: now,
		UpdatedTime: &now,
		Version:     common.Ptr("1"),
		MimeType:    c.MimeType,
		Description: c.Description,
		Aliases:     c.Aliases,
		Contents:    c.Contents,
		Checksums:   []objects.Checksum{{Type: "sha256", Checksum: oid}},
	}
	if c.ControlledAccess != nil {
		controlled := syfoncommon.NormalizeAccessResources(*c.ControlledAccess)
		obj.ControlledAccess = &controlled
	}
	if c.Name != nil {
		name := objects.CleanToBasename(*c.Name)
		if name != "" {
			obj.Name = common.Ptr(name)
		}
	}
	if obj.Name == nil {
		obj.Name = &oid
	}
	obj.SelfUri = "drs://" + string(obj.Id)

	methods := make([]objects.AccessMethod, 0, len(*c.AccessMethods))
	for _, method := range *c.AccessMethods {
		if method.AccessId == nil || *method.AccessId == "" {
			method.AccessId = common.Ptr(method.Type)
		}
		methods = append(methods, method)
	}
	obj.AccessMethods = &methods
	if len(methods) == 0 {
		return objects.Record{}, objects.ErrAccessMethodsRequired
	}
	obj.Authorizations = authzList
	return obj, nil
}

func candidateChecksums(value *[]objects.Checksum) []objects.Checksum {
	if value == nil {
		return nil
	}
	return *value
}
