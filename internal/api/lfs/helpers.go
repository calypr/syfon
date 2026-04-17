package lfs

import (
	"context"
	"errors"
	"time"

	"github.com/calypr/syfon/apigen/server/lfsapi"
	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/db"
	"github.com/calypr/syfon/internal/models"
	"github.com/calypr/syfon/internal/service"
	"github.com/gofiber/fiber/v3"
)

var baseURLKey = service.BaseURLContextKey

func validateLFSRequestHeaders(c fiber.Ctx, requireAccept bool, requireContentType bool) bool {
	return service.ValidateLFSRequestHeaders(c, requireAccept, requireContentType)
}

func writeLFSError(c fiber.Ctx, status int, message string, challenge bool) error {
	return service.WriteLFSError(c, status, message, challenge)
}

func normalizeOID(raw string) string {
	return service.NormalizeOID(raw)
}

func resolveObjectForOID(ctx context.Context, database db.ObjectStore, oid string) (*models.InternalObject, error) {
	return service.ResolveObjectForOID(ctx, database, oid)
}

func requestBaseURL(c fiber.Ctx) string {
	return c.BaseURL()
}

func canonicalSHA256(checksums []drs.Checksum) (string, bool) {
	return service.CanonicalSHA256(checksums)
}

func uniqueAuthz(accessMethods []drs.AccessMethod) []string {
	return service.UniqueAuthz(accessMethods)
}

func candidateToInternalObject(c drs.DrsObjectCandidate, now time.Time) (models.InternalObject, error) {
	return service.CandidateToInternalObject(c, now)
}

func lfsCandidateToDRS(in lfsapi.DrsObjectCandidate) drs.DrsObjectCandidate {
	return service.LFSCandidateToDRS(in)
}

func isAlreadyExists(err error) bool {
	return service.IsAlreadyExists(err)
}

func isNotFound(err error) bool {
	return errors.Is(err, common.ErrNotFound)
}

func s3KeyFromObjectForBucket(obj *models.InternalObject, bucket string) (string, bool) {
	return service.S3KeyFromInternalObjectForBucket(obj, bucket)
}

func s3KeyFromCandidateForBucket(candidate drs.DrsObjectCandidate, bucket string) (string, bool) {
	return service.S3KeyFromCandidateForBucket(candidate, bucket)
}
