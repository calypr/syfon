package metrics

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/server/metricsapi"
	"github.com/calypr/syfon/internal/api/attribution"
	apimiddleware "github.com/calypr/syfon/internal/api/middleware"
	internalauth "github.com/calypr/syfon/internal/auth"
	"github.com/calypr/syfon/internal/authz"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/models"
)

type transferEventsRequest struct {
	Events []transferEventPayload `json:"events"`
}

type transferEventPayload struct {
	EventID           string `json:"event_id"`
	EventType         string `json:"event_type"`
	Direction         string `json:"direction"`
	EventTime         string `json:"event_time"`
	RequestID         string `json:"request_id"`
	ObjectID          string `json:"object_id"`
	SHA256            string `json:"sha256"`
	ObjectSize        int64  `json:"object_size"`
	Organization      string `json:"organization"`
	Project           string `json:"project"`
	AccessID          string `json:"access_id"`
	Provider          string `json:"provider"`
	Bucket            string `json:"bucket"`
	StorageURL        string `json:"storage_url"`
	RangeStart        *int64 `json:"range_start"`
	RangeEnd          *int64 `json:"range_end"`
	BytesRequested    int64  `json:"bytes_requested"`
	BytesCompleted    int64  `json:"bytes_completed"`
	ActorEmail        string `json:"actor_email"`
	ActorSubject      string `json:"actor_subject"`
	AuthMode          string `json:"auth_mode"`
	ClientName        string `json:"client_name"`
	ClientVersion     string `json:"client_version"`
	TransferSessionID string `json:"transfer_session_id"`
}

type providerTransferPayload struct {
	ProviderEventID      string `json:"provider_event_id"`
	AccessGrantID        string `json:"access_grant_id"`
	Direction            string `json:"direction"`
	EventTime            string `json:"event_time"`
	RequestID            string `json:"request_id"`
	ProviderRequestID    string `json:"provider_request_id"`
	ObjectID             string `json:"object_id"`
	SHA256               string `json:"sha256"`
	ObjectSize           int64  `json:"object_size"`
	Organization         string `json:"organization"`
	Project              string `json:"project"`
	AccessID             string `json:"access_id"`
	Provider             string `json:"provider"`
	Bucket               string `json:"bucket"`
	ObjectKey            string `json:"object_key"`
	StorageURL           string `json:"storage_url"`
	RangeStart           *int64 `json:"range_start"`
	RangeEnd             *int64 `json:"range_end"`
	BytesTransferred     int64  `json:"bytes_transferred"`
	HTTPMethod           string `json:"http_method"`
	HTTPStatus           int    `json:"http_status"`
	RequesterPrincipal   string `json:"requester_principal"`
	SourceIP             string `json:"source_ip"`
	UserAgent            string `json:"user_agent"`
	RawEventRef          string `json:"raw_event_ref"`
	ActorEmail           string `json:"actor_email"`
	ActorSubject         string `json:"actor_subject"`
	AuthMode             string `json:"auth_mode"`
	ReconciliationStatus string `json:"reconciliation_status"`
}

func (s *MetricsServer) RecordProviderTransferEvents(ctx context.Context, request metricsapi.RecordProviderTransferEventsRequestObject) (metricsapi.RecordProviderTransferEventsResponseObject, error) {
	statusCode, ok := checkProviderMetricsIngestAuth(ctx)
	if !ok {
		return recordProviderTransferEventsAuthResponse(statusCode), nil
	}
	if request.Body == nil || len(request.Body.Events) == 0 {
		return metricsapi.RecordProviderTransferEvents400Response{}, nil
	}
	events := make([]models.ProviderTransferEvent, 0, len(request.Body.Events))
	for _, item := range request.Body.Events {
		ev, err := providerTransferPayloadToModel(providerTransferGeneratedEventToPayload(item))
		if err != nil {
			return metricsapi.RecordProviderTransferEvents400Response{}, nil
		}
		events = append(events, ev)
	}
	if err := s.database.RecordProviderTransferEvents(ctx, events); err != nil {
		return metricsapi.RecordProviderTransferEvents500Response{}, nil
	}
	recorded := len(events)
	return metricsapi.RecordProviderTransferEvents201JSONResponse{Recorded: &recorded}, nil
}

func checkProviderMetricsIngestAuth(ctx context.Context) (int, bool) {
	if !authz.IsGen3Mode(ctx) {
		return 0, true
	}
	if apimiddleware.MissingGen3AuthHeader(ctx) {
		return http.StatusUnauthorized, false
	}
	if authz.HasMethodAccess(ctx, "create", []string{common.MetricsIngestResource}) ||
		authz.HasMethodAccess(ctx, "*", []string{common.MetricsIngestResource}) {
		return 0, true
	}
	return http.StatusForbidden, false
}

func transferPayloadToModel(ctx context.Context, item transferEventPayload) (models.TransferAttributionEvent, error) {
	eventType := strings.TrimSpace(item.EventType)
	switch eventType {
	case models.TransferEventAccessIssued:
	default:
		return models.TransferAttributionEvent{}, errors.New("invalid event_type")
	}
	when := time.Now().UTC()
	if strings.TrimSpace(item.EventTime) != "" {
		parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(item.EventTime))
		if err != nil {
			return models.TransferAttributionEvent{}, errors.New("invalid event_time")
		}
		when = parsed.UTC()
	}
	if item.BytesCompleted < 0 || item.BytesRequested < 0 {
		return models.TransferAttributionEvent{}, errors.New("bytes cannot be negative")
	}
	if eventType != models.TransferEventAccessIssued && item.BytesCompleted <= 0 {
		return models.TransferAttributionEvent{}, errors.New("bytes_completed is required")
	}
	ev := models.TransferAttributionEvent{
		EventID:           strings.TrimSpace(item.EventID),
		EventType:         eventType,
		Direction:         normalizeTransferDirection(item.Direction),
		EventTime:         when,
		RequestID:         strings.TrimSpace(item.RequestID),
		ObjectID:          strings.TrimSpace(item.ObjectID),
		SHA256:            strings.TrimSpace(item.SHA256),
		ObjectSize:        item.ObjectSize,
		Organization:      strings.TrimSpace(item.Organization),
		Project:           strings.TrimSpace(item.Project),
		AccessID:          strings.TrimSpace(item.AccessID),
		Provider:          strings.TrimSpace(item.Provider),
		Bucket:            strings.TrimSpace(item.Bucket),
		StorageURL:        strings.TrimSpace(item.StorageURL),
		RangeStart:        item.RangeStart,
		RangeEnd:          item.RangeEnd,
		BytesRequested:    item.BytesRequested,
		BytesCompleted:    item.BytesCompleted,
		ActorEmail:        strings.TrimSpace(item.ActorEmail),
		ActorSubject:      strings.TrimSpace(item.ActorSubject),
		AuthMode:          strings.TrimSpace(item.AuthMode),
		ClientName:        strings.TrimSpace(item.ClientName),
		ClientVersion:     strings.TrimSpace(item.ClientVersion),
		TransferSessionID: strings.TrimSpace(item.TransferSessionID),
	}
	if ev.RequestID == "" {
		ev.RequestID = common.GetRequestID(ctx)
	}
	if ev.ActorEmail == "" {
		ev.ActorEmail = attribution.ActorEmail(ctx)
	}
	if ev.ActorSubject == "" {
		ev.ActorSubject = attribution.ActorSubject(ctx)
	}
	if ev.AuthMode == "" {
		ev.AuthMode = strings.TrimSpace(internalauth.FromContext(ctx).Mode)
	}
	if ev.EventID == "" {
		ev.EventID = attribution.EventID(ev)
	}
	if ev.AccessGrantID == "" {
		ev.AccessGrantID = attribution.AccessGrantID(ev)
	}
	return ev, nil
}

func normalizeTransferDirection(direction string) string {
	switch strings.ToLower(strings.TrimSpace(direction)) {
	case models.ProviderTransferDirectionUpload:
		return models.ProviderTransferDirectionUpload
	default:
		return models.ProviderTransferDirectionDownload
	}
}

func providerTransferPayloadToModel(item providerTransferPayload) (models.ProviderTransferEvent, error) {
	direction := strings.ToLower(strings.TrimSpace(item.Direction))
	switch direction {
	case models.ProviderTransferDirectionDownload, models.ProviderTransferDirectionUpload:
	default:
		return models.ProviderTransferEvent{}, errors.New("invalid direction")
	}
	if strings.TrimSpace(item.ProviderEventID) == "" || strings.TrimSpace(item.Provider) == "" || strings.TrimSpace(item.Bucket) == "" {
		return models.ProviderTransferEvent{}, errors.New("provider_event_id, provider, and bucket are required")
	}
	if item.BytesTransferred < 0 {
		return models.ProviderTransferEvent{}, errors.New("bytes_transferred cannot be negative")
	}
	status := strings.TrimSpace(item.ReconciliationStatus)
	switch status {
	case "", models.ProviderTransferMatched, models.ProviderTransferAmbiguous, models.ProviderTransferUnmatched:
	default:
		return models.ProviderTransferEvent{}, errors.New("invalid reconciliation_status")
	}
	when := time.Now().UTC()
	if strings.TrimSpace(item.EventTime) != "" {
		parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(item.EventTime))
		if err != nil {
			return models.ProviderTransferEvent{}, errors.New("invalid event_time")
		}
		when = parsed.UTC()
	}
	return models.ProviderTransferEvent{
		ProviderEventID:      strings.TrimSpace(item.ProviderEventID),
		AccessGrantID:        strings.TrimSpace(item.AccessGrantID),
		Direction:            direction,
		EventTime:            when,
		RequestID:            strings.TrimSpace(item.RequestID),
		ProviderRequestID:    strings.TrimSpace(item.ProviderRequestID),
		ObjectID:             strings.TrimSpace(item.ObjectID),
		SHA256:               strings.TrimSpace(item.SHA256),
		ObjectSize:           item.ObjectSize,
		Organization:         strings.TrimSpace(item.Organization),
		Project:              strings.TrimSpace(item.Project),
		AccessID:             strings.TrimSpace(item.AccessID),
		Provider:             strings.TrimSpace(item.Provider),
		Bucket:               strings.TrimSpace(item.Bucket),
		ObjectKey:            strings.TrimLeft(strings.TrimSpace(item.ObjectKey), "/"),
		StorageURL:           strings.TrimSpace(item.StorageURL),
		RangeStart:           item.RangeStart,
		RangeEnd:             item.RangeEnd,
		BytesTransferred:     item.BytesTransferred,
		HTTPMethod:           strings.ToUpper(strings.TrimSpace(item.HTTPMethod)),
		HTTPStatus:           item.HTTPStatus,
		RequesterPrincipal:   strings.TrimSpace(item.RequesterPrincipal),
		SourceIP:             strings.TrimSpace(item.SourceIP),
		UserAgent:            strings.TrimSpace(item.UserAgent),
		RawEventRef:          strings.TrimSpace(item.RawEventRef),
		ActorEmail:           strings.TrimSpace(item.ActorEmail),
		ActorSubject:         strings.TrimSpace(item.ActorSubject),
		AuthMode:             strings.TrimSpace(item.AuthMode),
		ReconciliationStatus: status,
	}, nil
}

func recordProviderTransferEventsAuthResponse(statusCode int) metricsapi.RecordProviderTransferEventsResponseObject {
	switch statusCode {
	case http.StatusUnauthorized:
		return metricsapi.RecordProviderTransferEvents401Response{}
	case http.StatusForbidden:
		return metricsapi.RecordProviderTransferEvents403Response{}
	default:
		return metricsapi.RecordProviderTransferEvents400Response{}
	}
}

func providerTransferGeneratedEventToPayload(item metricsapi.ProviderTransferEvent) providerTransferPayload {
	out := providerTransferPayload{
		ProviderEventID:  item.ProviderEventId,
		Direction:        string(item.Direction),
		Provider:         item.Provider,
		Bucket:           item.Bucket,
		BytesTransferred: item.BytesTransferred,
	}
	if item.AccessGrantId != nil {
		out.AccessGrantID = *item.AccessGrantId
	}
	if item.EventTime != nil {
		out.EventTime = item.EventTime.Format(time.RFC3339Nano)
	}
	if item.RequestId != nil {
		out.RequestID = *item.RequestId
	}
	if item.ProviderRequestId != nil {
		out.ProviderRequestID = *item.ProviderRequestId
	}
	if item.ObjectId != nil {
		out.ObjectID = *item.ObjectId
	}
	if item.Sha256 != nil {
		out.SHA256 = *item.Sha256
	}
	if item.ObjectSize != nil {
		out.ObjectSize = *item.ObjectSize
	}
	if item.Organization != nil {
		out.Organization = *item.Organization
	}
	if item.Project != nil {
		out.Project = *item.Project
	}
	if item.AccessId != nil {
		out.AccessID = *item.AccessId
	}
	if item.ObjectKey != nil {
		out.ObjectKey = *item.ObjectKey
	}
	if item.StorageUrl != nil {
		out.StorageURL = *item.StorageUrl
	}
	out.RangeStart = item.RangeStart
	out.RangeEnd = item.RangeEnd
	if item.HttpMethod != nil {
		out.HTTPMethod = *item.HttpMethod
	}
	if item.HttpStatus != nil {
		out.HTTPStatus = *item.HttpStatus
	}
	if item.RequesterPrincipal != nil {
		out.RequesterPrincipal = *item.RequesterPrincipal
	}
	if item.SourceIp != nil {
		out.SourceIP = *item.SourceIp
	}
	if item.UserAgent != nil {
		out.UserAgent = *item.UserAgent
	}
	if item.RawEventRef != nil {
		out.RawEventRef = *item.RawEventRef
	}
	if item.ActorEmail != nil {
		out.ActorEmail = *item.ActorEmail
	}
	if item.ActorSubject != nil {
		out.ActorSubject = *item.ActorSubject
	}
	if item.AuthMode != nil {
		out.AuthMode = *item.AuthMode
	}
	if item.ReconciliationStatus != nil {
		out.ReconciliationStatus = string(*item.ReconciliationStatus)
	}
	return out
}
