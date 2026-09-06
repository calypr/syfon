package attribution

import (
	"context"
	"net/url"
	"strings"
	"time"

	"github.com/calypr/syfon/internal/access"
	"github.com/calypr/syfon/internal/core"

	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/requestmeta"
	"github.com/calypr/syfon/internal/storage/address"
	"github.com/calypr/syfon/internal/usage"
)

type AccessDetails struct {
	AccessID       string
	Direction      string
	StorageURL     string
	RangeStart     *int64
	RangeEnd       *int64
	BytesRequested int64
	ClientName     string
	ClientVersion  string
}

func RecordAccessIssued(ctx context.Context, om *core.ObjectManager, obj *objects.Record, details AccessDetails) error {
	if om == nil || obj == nil {
		return nil
	}
	ev := EventFromObject(ctx, obj, usage.TransferEventAccessIssued, details)
	if ev.EventID == "" {
		return nil
	}
	return om.RecordTransferAttributionEvents(ctx, []usage.Event{ev})
}

func EventFromObject(ctx context.Context, obj *objects.Record, eventType string, details AccessDetails) usage.Event {
	if obj == nil {
		return usage.Event{}
	}
	storageURL := strings.TrimSpace(details.StorageURL)
	accessID := strings.TrimSpace(details.AccessID)
	if storageURL == "" || accessID == "" {
		for _, am := range accessMethods(obj) {
			if accessID != "" && !strings.EqualFold(accessMethodID(am), accessID) {
				continue
			}
			if accessID == "" {
				accessID = accessMethodID(am)
			}
			if am.AccessUrl != nil {
				storageURL = strings.TrimSpace(am.AccessUrl.Url)
			}
			if storageURL != "" {
				break
			}
		}
	}
	org, project := scopeForAccess(obj, accessID)
	provider, bucket := providerBucket(storageURL)
	direction := strings.ToLower(strings.TrimSpace(details.Direction))
	switch direction {
	case usage.ProviderTransferDirectionUpload:
	default:
		direction = usage.ProviderTransferDirectionDownload
	}
	sha := sha256ForObject(obj)
	bytesRequested := details.BytesRequested
	if bytesRequested <= 0 && details.RangeStart != nil && details.RangeEnd != nil && *details.RangeEnd >= *details.RangeStart {
		bytesRequested = *details.RangeEnd - *details.RangeStart + 1
	}
	if bytesRequested <= 0 {
		bytesRequested = obj.Size
	}
	when := time.Now().UTC()
	ev := usage.Event{
		EventType:      eventType,
		Direction:      direction,
		EventTime:      when,
		RequestID:      requestmeta.GetRequestID(ctx),
		ObjectID:       string(obj.Id),
		SHA256:         sha,
		ObjectSize:     obj.Size,
		Organization:   org,
		Project:        project,
		AccessID:       accessID,
		Provider:       provider,
		Bucket:         bucket,
		StorageURL:     storageURL,
		RangeStart:     details.RangeStart,
		RangeEnd:       details.RangeEnd,
		BytesRequested: bytesRequested,
		ActorEmail:     ActorEmail(ctx),
		ActorSubject:   ActorSubject(ctx),
		AuthMode:       authMode(ctx),
		ClientName:     details.ClientName,
		ClientVersion:  details.ClientVersion,
	}
	ev.EventID = usage.EventID(ev)
	ev.AccessGrantID = usage.GrantID(ev)
	return ev
}

func ActorSubject(ctx context.Context) string {
	return strings.TrimSpace(access.FromContext(ctx).Subject)
}

func ActorEmail(ctx context.Context) string {
	claims := access.FromContext(ctx).Claims
	for _, key := range []string{"email", "preferred_username", "username"} {
		if v, ok := claims[key].(string); ok && strings.Contains(v, "@") {
			return strings.TrimSpace(v)
		}
	}
	sub := ActorSubject(ctx)
	if strings.Contains(sub, "@") {
		return sub
	}
	return ""
}

func authMode(ctx context.Context) string {
	return strings.TrimSpace(access.FromContext(ctx).Mode)
}

func accessMethods(obj *objects.Record) []objects.AccessMethod {
	if obj == nil || obj.AccessMethods == nil {
		return nil
	}
	return *obj.AccessMethods
}

func accessMethodID(am objects.AccessMethod) string {
	if am.AccessId != nil && strings.TrimSpace(*am.AccessId) != "" {
		return strings.TrimSpace(*am.AccessId)
	}
	return strings.TrimSpace(am.Type)
}

func scopeForAccess(obj *objects.Record, accessID string) (string, string) {
	for _, am := range accessMethods(obj) {
		if accessID != "" && !strings.EqualFold(accessMethodID(am), accessID) {
			continue
		}
	}
	if len(obj.Authorizations) > 0 {
		for org, projects := range obj.Authorizations {
			if len(projects) == 0 {
				return org, ""
			}
			return org, projects[0]
		}
	}
	return "", ""
}

func providerBucket(raw string) (string, string) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", ""
	}
	return address.ProviderFromScheme(u.Scheme), strings.TrimSpace(u.Host)
}

func sha256ForObject(obj *objects.Record) string {
	for _, c := range obj.Checksums {
		if strings.EqualFold(c.Type, "sha256") {
			return strings.TrimSpace(c.Checksum)
		}
	}
	return ""
}
