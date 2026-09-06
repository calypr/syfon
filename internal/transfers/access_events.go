package transfers

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/calypr/syfon/internal/access"
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

// AccessRequest is the transfer-domain request for one access-issued event.
// Object is already authorized and hydrated by the caller; transfers does not
// perform another object lookup.
type AccessRequest struct {
	Object         *objects.Record
	AccessID       string
	Direction      string
	StorageURL     string
	RangeStart     *int64
	RangeEnd       *int64
	BytesRequested int64
	ClientName     string
	ClientVersion  string
}

// RecordAccessIssued assembles one usage event and sends it to the narrow
// recorder. A nil object remains a no-op for new-object upload paths.
func (s *Service) RecordAccessIssued(ctx context.Context, request AccessRequest) error {
	if request.Object == nil {
		return nil
	}
	if s == nil || s.events == nil {
		return fmt.Errorf("transfer event recorder is not configured")
	}
	event := EventFromObject(ctx, request.Object, usage.TransferEventAccessIssued, AccessDetails{
		AccessID:       request.AccessID,
		Direction:      request.Direction,
		StorageURL:     request.StorageURL,
		RangeStart:     request.RangeStart,
		RangeEnd:       request.RangeEnd,
		BytesRequested: request.BytesRequested,
		ClientName:     request.ClientName,
		ClientVersion:  request.ClientVersion,
	})
	if event.EventID == "" {
		return nil
	}
	return s.events.RecordTransferAttributionEvents(ctx, []usage.Event{event})
}

func RecordAccessIssued(ctx context.Context, recorder EventRecorder, request AccessRequest) error {
	if request.Object == nil {
		return nil
	}
	if recorder == nil {
		return fmt.Errorf("transfer event recorder is not configured")
	}
	event := EventFromObject(ctx, request.Object, usage.TransferEventAccessIssued, AccessDetails{
		AccessID:       request.AccessID,
		Direction:      request.Direction,
		StorageURL:     request.StorageURL,
		RangeStart:     request.RangeStart,
		RangeEnd:       request.RangeEnd,
		BytesRequested: request.BytesRequested,
		ClientName:     request.ClientName,
		ClientVersion:  request.ClientVersion,
	})
	if event.EventID == "" {
		return nil
	}
	return recorder.RecordTransferAttributionEvents(ctx, []usage.Event{event})
}

func EventFromObject(ctx context.Context, obj *objects.Record, eventType string, details AccessDetails) usage.Event {
	if obj == nil {
		return usage.Event{}
	}
	storageURL := strings.TrimSpace(details.StorageURL)
	accessID := strings.TrimSpace(details.AccessID)
	if storageURL == "" || accessID == "" {
		for _, method := range accessMethods(obj) {
			if accessID != "" && !strings.EqualFold(accessMethodID(method), accessID) {
				continue
			}
			if accessID == "" {
				accessID = accessMethodID(method)
			}
			if method.AccessUrl != nil {
				storageURL = strings.TrimSpace(method.AccessUrl.Url)
			}
			if storageURL != "" {
				break
			}
		}
	}
	organization, project := scopeForAccess(obj, accessID)
	provider, bucket := providerBucket(storageURL)
	direction := strings.ToLower(strings.TrimSpace(details.Direction))
	if direction != usage.ProviderTransferDirectionUpload {
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
	event := usage.Event{
		EventType:      eventType,
		Direction:      direction,
		EventTime:      time.Now().UTC(),
		RequestID:      requestmeta.GetRequestID(ctx),
		ObjectID:       string(obj.Id),
		SHA256:         sha,
		ObjectSize:     obj.Size,
		Organization:   organization,
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
	event.EventID = usage.EventID(event)
	event.AccessGrantID = usage.GrantID(event)
	return event
}

// ActorSubject returns the authenticated subject from the request session.
func ActorSubject(ctx context.Context) string {
	return strings.TrimSpace(access.FromContext(ctx).Subject)
}

// ActorEmail preserves the claim precedence used by the existing attribution
// projection: email, preferred_username, username, then an email subject.
func ActorEmail(ctx context.Context) string {
	claims := access.FromContext(ctx).Claims
	for _, key := range []string{"email", "preferred_username", "username"} {
		if value, ok := claims[key].(string); ok && strings.Contains(value, "@") {
			return strings.TrimSpace(value)
		}
	}
	subject := ActorSubject(ctx)
	if strings.Contains(subject, "@") {
		return subject
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

func accessMethodID(method objects.AccessMethod) string {
	if method.AccessId != nil && strings.TrimSpace(*method.AccessId) != "" {
		return strings.TrimSpace(*method.AccessId)
	}
	return strings.TrimSpace(method.Type)
}

func scopeForAccess(obj *objects.Record, accessID string) (string, string) {
	for _, method := range accessMethods(obj) {
		if accessID != "" && !strings.EqualFold(accessMethodID(method), accessID) {
			continue
		}
	}
	if len(obj.Authorizations) > 0 {
		for organization, projects := range obj.Authorizations {
			if len(projects) == 0 {
				return organization, ""
			}
			return organization, projects[0]
		}
	}
	return "", ""
}

func providerBucket(raw string) (string, string) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", ""
	}
	return address.ProviderFromScheme(parsed.Scheme), strings.TrimSpace(parsed.Host)
}

func sha256ForObject(obj *objects.Record) string {
	for _, checksum := range obj.Checksums {
		if strings.EqualFold(checksum.Type, "sha256") {
			return strings.TrimSpace(checksum.Checksum)
		}
	}
	return ""
}
