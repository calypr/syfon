package drsapi

import (
	"encoding/json"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/api/apiutil"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/core"
	httpdrs "github.com/calypr/syfon/internal/httpapi/drs"
	"github.com/calypr/syfon/internal/objects"
	"github.com/gofiber/fiber/v3"
)

func handleRegisterObjectsFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		var body registerObjectsRequest
		if err := json.Unmarshal(c.Body(), &body); err != nil || len(body.Candidates) == 0 {
			var single registerObjectCandidate
			if err2 := json.Unmarshal(c.Body(), &single); err2 == nil && len(single.Checksums) > 0 {
				internalObj, err := registerCandidateToRecord(single, time.Now().UTC())
				if err != nil {
					return apiutil.HandleError(c, err)
				}
				if err := om.RegisterObjects(c.Context(), []objects.Record{internalObj}); err != nil {
					return apiutil.HandleError(c, err)
				}
				// Fetch back for full population (SelfUri, and access methods)
				finalObj, err := om.GetObject(c.Context(), string(internalObj.Id), "read")
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

		if err := om.RegisterObjects(c.Context(), toRegister); err != nil {
			return apiutil.HandleError(c, err)
		}

		// Reconstruct registered objects summary for response
		registered := make([]any, len(toRegister))
		for i, internal := range toRegister {
			// Fetch back to ensure full population
			obj, err := om.GetObject(c.Context(), string(internal.Id), "read")
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
	return core.CandidateToRecord(httpdrs.FromGeneratedCandidate(c.DrsObjectCandidate), now)
}
