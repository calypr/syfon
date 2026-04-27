package metrics

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/server/metricsapi"
	sycommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/api/attribution"
	"github.com/calypr/syfon/internal/authz"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/db"
	"github.com/calypr/syfon/internal/models"
	"github.com/gofiber/fiber/v3"
)

type metricsQueryContextKey struct{}

type metricsQueryParams struct {
	organization string
	program      string
	project      string
}

type MetricsServer struct {
	database db.MetricsStore
}

func NewMetricsServer(database db.MetricsStore) *MetricsServer {
	return &MetricsServer{database: database}
}

func RegisterMetricsRoutes(router fiber.Router, database db.MetricsStore) {
	router.Use(func(c fiber.Ctx) error {
		params := metricsQueryParams{
			organization: strings.TrimSpace(c.Query("organization")),
			program:      strings.TrimSpace(c.Query("program")),
			project:      strings.TrimSpace(c.Query("project")),
		}
		c.SetContext(context.WithValue(c.Context(), metricsQueryContextKey{}, params))
		return c.Next()
	})

	server := NewMetricsServer(database)
	strict := metricsapi.NewStrictHandler(server, nil)
	metricsapi.RegisterHandlers(router, strict)
}

func (s *MetricsServer) ListMetricsFiles(ctx context.Context, request metricsapi.ListMetricsFilesRequestObject) (metricsapi.ListMetricsFilesResponseObject, error) {
	limit := 200
	if request.Params.Limit != nil {
		limit = *request.Params.Limit
	}
	offset := 0
	if request.Params.Offset != nil {
		offset = *request.Params.Offset
	}

	if limit < 1 || limit > 1000 || offset < 0 {
		return metricsapi.ListMetricsFiles400Response{}, nil
	}

	inactiveSince, err := parseInactiveSince(request.Params.InactiveDays)
	if err != nil {
		return metricsapi.ListMetricsFiles400Response{}, nil
	}

	access, statusCode, ok := s.checkAuth(ctx)
	if !ok {
		switch statusCode {
		case http.StatusUnauthorized:
			return metricsapi.ListMetricsFiles401Response{}, nil
		case http.StatusForbidden:
			return metricsapi.ListMetricsFiles403Response{}, nil
		default:
			return metricsapi.ListMetricsFiles400Response{}, nil
		}
	}

	var data []models.FileUsage
	if access.isScoped() {
		data, _, err = listScopedFileUsage(ctx, s.database, access.organization, access.project, limit, offset, inactiveSince)
	} else {
		data, err = s.database.ListFileUsage(ctx, limit, offset, inactiveSince)
	}
	if err != nil {
		return metricsapi.ListMetricsFiles500Response{}, nil
	}

	items := make([]metricsapi.FileUsage, 0, len(data))
	for _, v := range data {
		items = append(items, toMetricsFileUsage(v))
	}

	return metricsapi.ListMetricsFiles200JSONResponse{
		Data:   &items,
		Limit:  &limit,
		Offset: &offset,
	}, nil
}

func (s *MetricsServer) GetMetricsFile(ctx context.Context, request metricsapi.GetMetricsFileRequestObject) (metricsapi.GetMetricsFileResponseObject, error) {
	objectID := request.ObjectId
	if objectID == "" {
		return metricsapi.GetMetricsFile400Response{}, nil
	}

	access, statusCode, ok := s.checkAuth(ctx)
	if !ok {
		switch statusCode {
		case http.StatusUnauthorized:
			return metricsapi.GetMetricsFile401Response{}, nil
		case http.StatusForbidden:
			return metricsapi.GetMetricsFile403Response{}, nil
		default:
			return metricsapi.GetMetricsFile400Response{}, nil
		}
	}

	if access.isScoped() {
		inside, err := objectInScope(ctx, s.database, objectID, access.organization, access.project)
		if err != nil {
			return metricsapi.GetMetricsFile500Response{}, nil
		}
		if !inside {
			return metricsapi.GetMetricsFile404Response{}, nil
		}
	}

	usage, err := s.database.GetFileUsage(ctx, objectID)
	if err != nil {
		if errors.Is(err, common.ErrNotFound) {
			return metricsapi.GetMetricsFile404Response{}, nil
		}
		return metricsapi.GetMetricsFile500Response{}, nil
	}

	return metricsapi.GetMetricsFile200JSONResponse(toMetricsFileUsage(*usage)), nil
}

func (s *MetricsServer) GetMetricsSummary(ctx context.Context, request metricsapi.GetMetricsSummaryRequestObject) (metricsapi.GetMetricsSummaryResponseObject, error) {
	inactiveSince, err := parseInactiveSince(request.Params.InactiveDays)
	if err != nil {
		return metricsapi.GetMetricsSummary400Response{}, nil
	}

	access, statusCode, ok := s.checkAuth(ctx)
	if !ok {
		switch statusCode {
		case http.StatusUnauthorized:
			return metricsapi.GetMetricsSummary401Response{}, nil
		case http.StatusForbidden:
			return metricsapi.GetMetricsSummary403Response{}, nil
		default:
			return metricsapi.GetMetricsSummary400Response{}, nil
		}
	}

	var summary models.FileUsageSummary
	if access.isScoped() {
		_, summary, err = listScopedFileUsage(ctx, s.database, access.organization, access.project, 0, 0, inactiveSince)
	} else {
		summary, err = s.database.GetFileUsageSummary(ctx, inactiveSince)
	}
	if err != nil {
		return metricsapi.GetMetricsSummary500Response{}, nil
	}

	return metricsapi.GetMetricsSummary200JSONResponse{
		TotalFiles:        &summary.TotalFiles,
		TotalUploads:      &summary.TotalUploads,
		TotalDownloads:    &summary.TotalDownloads,
		InactiveFileCount: &summary.InactiveFileCount,
	}, nil
}

func (s *MetricsServer) checkAuth(ctx context.Context) (metricsAccess, int, bool) {
	access, err := resolveMetricsAccess(ctx)
	if err != nil {
		return metricsAccess{}, http.StatusBadRequest, false
	}

	if !authz.IsGen3Mode(ctx) {
		return access, 0, true
	}
	if !authz.HasAuthHeader(ctx) {
		return access, http.StatusUnauthorized, false
	}

	// Baseline read access for metrics: global access or scoped access
	if authz.HasMethodAccess(ctx, "read", []string{"/data_file"}) ||
		authz.HasMethodAccess(ctx, "read", []string{"/programs"}) {
		return access, 0, true
	}

	if access.isScoped() {
		scope, err := sycommon.ResourcePath(access.organization, access.project)
		if err != nil {
			return access, http.StatusBadRequest, false
		}
		if authz.HasMethodAccess(ctx, "read", []string{scope}) {
			return access, 0, true
		}
	}

	return access, http.StatusForbidden, false
}

type transferEventsRequest struct {
	Events []transferEventPayload `json:"events"`
}

type transferEventPayload struct {
	EventID           string `json:"event_id"`
	EventType         string `json:"event_type"`
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

func (s *MetricsServer) RecordProviderTransferSync(ctx context.Context, request metricsapi.RecordProviderTransferSyncRequestObject) (metricsapi.RecordProviderTransferSyncResponseObject, error) {
	statusCode, ok := checkProviderMetricsIngestAuth(ctx)
	if !ok {
		return recordProviderTransferSyncAuthResponse(statusCode), nil
	}
	if request.Body == nil || !request.Body.From.Before(request.Body.To) {
		return metricsapi.RecordProviderTransferSync400Response{}, nil
	}

	runs, err := s.providerSyncRunsForRequest(ctx, *request.Body)
	if err != nil {
		if errors.Is(err, common.ErrNotFound) {
			return metricsapi.RecordProviderTransferSync400Response{}, nil
		}
		return metricsapi.RecordProviderTransferSync500Response{}, nil
	}
	if len(runs) == 0 {
		return metricsapi.RecordProviderTransferSync400Response{}, nil
	}
	if shouldCollectProviderTransferSync(*request.Body) {
		runs = s.collectProviderTransferSyncRuns(ctx, *request.Body, runs)
	}
	if err := s.database.RecordProviderTransferSyncRuns(ctx, runs); err != nil {
		return metricsapi.RecordProviderTransferSync500Response{}, nil
	}
	recorded := len(runs)
	generated := make([]metricsapi.ProviderTransferSyncRun, 0, len(runs))
	for _, run := range runs {
		generated = append(generated, toGeneratedProviderTransferSyncRun(run))
	}
	return metricsapi.RecordProviderTransferSync201JSONResponse{
		Recorded: &recorded,
		SyncRuns: &generated,
	}, nil
}

func (s *MetricsServer) ListProviderTransferSync(ctx context.Context, request metricsapi.ListProviderTransferSyncRequestObject) (metricsapi.ListProviderTransferSyncResponseObject, error) {
	if _, statusCode, ok := s.checkAuth(ctx); !ok {
		return listProviderTransferSyncAuthResponse(statusCode), nil
	}
	filter := providerTransferSyncParamsToFilter(request.Params)
	limit := 100
	if request.Params.Limit != nil {
		limit = *request.Params.Limit
	}
	if limit < 1 || limit > 1000 {
		return metricsapi.ListProviderTransferSync400Response{}, nil
	}
	runs, err := s.database.ListProviderTransferSyncRuns(ctx, filter, limit)
	if err != nil {
		return metricsapi.ListProviderTransferSync500Response{}, nil
	}
	generated := make([]metricsapi.ProviderTransferSyncRun, 0, len(runs))
	for _, run := range runs {
		generated = append(generated, toGeneratedProviderTransferSyncRun(run))
	}
	recorded := len(generated)
	return metricsapi.ListProviderTransferSync200JSONResponse{
		Recorded: &recorded,
		SyncRuns: &generated,
	}, nil
}

func checkProviderMetricsIngestAuth(ctx context.Context) (int, bool) {
	if !authz.IsGen3Mode(ctx) {
		return 0, true
	}
	if !authz.HasAuthHeader(ctx) {
		return http.StatusUnauthorized, false
	}
	if authz.HasMethodAccess(ctx, "write", []string{common.MetricsIngestResource}) ||
		authz.HasMethodAccess(ctx, "create", []string{common.MetricsIngestResource}) ||
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
		if s, ok := ctx.Value(common.AuthModeKey).(string); ok {
			ev.AuthMode = strings.TrimSpace(s)
		}
	}
	if ev.EventID == "" {
		ev.EventID = attribution.EventID(ev)
	}
	if ev.AccessGrantID == "" {
		ev.AccessGrantID = attribution.AccessGrantID(ev)
	}
	return ev, nil
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

func (s *MetricsServer) GetTransferSummary(ctx context.Context, request metricsapi.GetTransferSummaryRequestObject) (metricsapi.GetTransferSummaryResponseObject, error) {
	if _, statusCode, ok := s.checkAuth(ctx); !ok {
		return getTransferSummaryAuthResponse(statusCode), nil
	}
	filter := transferSummaryParamsToFilter(request.Params)
	freshness, _, err := s.transferFreshness(ctx, filter)
	if err != nil {
		return metricsapi.GetTransferSummary500Response{}, nil
	}
	summary, err := s.database.GetTransferAttributionSummary(ctx, filter)
	if err != nil {
		return metricsapi.GetTransferSummary500Response{}, nil
	}
	generated := toGeneratedTransferSummary(summary)
	generated.Freshness = &freshness
	return metricsapi.GetTransferSummary200JSONResponse(generated), nil
}

func (s *MetricsServer) GetTransferBreakdown(ctx context.Context, request metricsapi.GetTransferBreakdownRequestObject) (metricsapi.GetTransferBreakdownResponseObject, error) {
	if _, statusCode, ok := s.checkAuth(ctx); !ok {
		return getTransferBreakdownAuthResponse(statusCode), nil
	}
	filter := transferBreakdownParamsToFilter(request.Params)
	freshness, _, err := s.transferFreshness(ctx, filter)
	if err != nil {
		return metricsapi.GetTransferBreakdown500Response{}, nil
	}
	groupBy := "scope"
	if request.Params.GroupBy != nil {
		groupBy = string(*request.Params.GroupBy)
	}
	switch groupBy {
	case "scope", "user", "provider", "object":
	default:
		return metricsapi.GetTransferBreakdown400Response{}, nil
	}
	items, err := s.database.GetTransferAttributionBreakdown(ctx, filter, groupBy)
	if err != nil {
		return metricsapi.GetTransferBreakdown500Response{}, nil
	}
	generatedItems := make([]metricsapi.TransferAttributionBreakdown, 0, len(items))
	for _, item := range items {
		generatedItems = append(generatedItems, toGeneratedTransferBreakdown(item))
	}
	generatedGroupBy := metricsapi.TransferBreakdownResponseGroupBy(groupBy)
	return metricsapi.GetTransferBreakdown200JSONResponse{
		Data:      &generatedItems,
		Freshness: &freshness,
		GroupBy:   &generatedGroupBy,
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

func recordProviderTransferSyncAuthResponse(statusCode int) metricsapi.RecordProviderTransferSyncResponseObject {
	switch statusCode {
	case http.StatusUnauthorized:
		return metricsapi.RecordProviderTransferSync401Response{}
	case http.StatusForbidden:
		return metricsapi.RecordProviderTransferSync403Response{}
	default:
		return metricsapi.RecordProviderTransferSync400Response{}
	}
}

func listProviderTransferSyncAuthResponse(statusCode int) metricsapi.ListProviderTransferSyncResponseObject {
	switch statusCode {
	case http.StatusUnauthorized:
		return metricsapi.ListProviderTransferSync401Response{}
	case http.StatusForbidden:
		return metricsapi.ListProviderTransferSync403Response{}
	default:
		return metricsapi.ListProviderTransferSync400Response{}
	}
}

func getTransferSummaryAuthResponse(statusCode int) metricsapi.GetTransferSummaryResponseObject {
	switch statusCode {
	case http.StatusUnauthorized:
		return metricsapi.GetTransferSummary401Response{}
	case http.StatusForbidden:
		return metricsapi.GetTransferSummary403Response{}
	default:
		return metricsapi.GetTransferSummary400Response{}
	}
}

func getTransferBreakdownAuthResponse(statusCode int) metricsapi.GetTransferBreakdownResponseObject {
	switch statusCode {
	case http.StatusUnauthorized:
		return metricsapi.GetTransferBreakdown401Response{}
	case http.StatusForbidden:
		return metricsapi.GetTransferBreakdown403Response{}
	default:
		return metricsapi.GetTransferBreakdown400Response{}
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

func transferSummaryParamsToFilter(params metricsapi.GetTransferSummaryParams) models.TransferAttributionFilter {
	return models.TransferAttributionFilter{
		Organization:         generatedString(params.Organization),
		Project:              generatedString(params.Project),
		Direction:            generatedString(params.Direction),
		ReconciliationStatus: generatedString(params.ReconciliationStatus),
		From:                 generatedTime(params.From),
		To:                   generatedTime(params.To),
		Provider:             generatedString(params.Provider),
		Bucket:               generatedString(params.Bucket),
		SHA256:               generatedString(params.Sha256),
		User:                 generatedString(params.User),
	}
}

func transferBreakdownParamsToFilter(params metricsapi.GetTransferBreakdownParams) models.TransferAttributionFilter {
	return models.TransferAttributionFilter{
		Organization:         generatedString(params.Organization),
		Project:              generatedString(params.Project),
		Direction:            generatedString(params.Direction),
		ReconciliationStatus: generatedString(params.ReconciliationStatus),
		From:                 generatedTime(params.From),
		To:                   generatedTime(params.To),
		Provider:             generatedString(params.Provider),
		Bucket:               generatedString(params.Bucket),
		SHA256:               generatedString(params.Sha256),
		User:                 generatedString(params.User),
	}
}

func providerTransferSyncParamsToFilter(params metricsapi.ListProviderTransferSyncParams) models.TransferAttributionFilter {
	return models.TransferAttributionFilter{
		Organization: generatedString(params.Organization),
		Project:      generatedString(params.Project),
		From:         generatedTime(params.From),
		To:           generatedTime(params.To),
		Provider:     generatedString(params.Provider),
		Bucket:       generatedString(params.Bucket),
	}
}

func generatedString[T ~string](v *T) string {
	if v == nil {
		return ""
	}
	return string(*v)
}

func generatedTime(v *time.Time) *time.Time {
	if v == nil {
		return nil
	}
	t := v.UTC()
	return &t
}

func toGeneratedTransferSummary(summary models.TransferAttributionSummary) metricsapi.TransferAttributionSummary {
	return metricsapi.TransferAttributionSummary{
		EventCount:         &summary.EventCount,
		AccessIssuedCount:  &summary.AccessIssuedCount,
		DownloadEventCount: &summary.DownloadEventCount,
		UploadEventCount:   &summary.UploadEventCount,
		BytesRequested:     &summary.BytesRequested,
		BytesDownloaded:    &summary.BytesDownloaded,
		BytesUploaded:      &summary.BytesUploaded,
	}
}

func toGeneratedProviderTransferSyncRun(run models.ProviderTransferSyncRun) metricsapi.ProviderTransferSyncRun {
	status := metricsapi.ProviderTransferSyncStatus(run.Status)
	from := run.From.UTC()
	to := run.To.UTC()
	requested := run.RequestedAt.UTC()
	return metricsapi.ProviderTransferSyncRun{
		SyncId:          stringPtr(run.SyncID),
		Provider:        stringPtr(run.Provider),
		Bucket:          stringPtr(run.Bucket),
		Organization:    stringPtr(run.Organization),
		Project:         stringPtr(run.Project),
		From:            &from,
		To:              &to,
		Status:          &status,
		RequestedAt:     &requested,
		StartedAt:       run.StartedAt,
		CompletedAt:     run.CompletedAt,
		ImportedEvents:  int64Ptr(run.ImportedEvents),
		MatchedEvents:   int64Ptr(run.MatchedEvents),
		AmbiguousEvents: int64Ptr(run.AmbiguousEvents),
		UnmatchedEvents: int64Ptr(run.UnmatchedEvents),
		ErrorMessage:    stringPtr(run.ErrorMessage),
	}
}

func stringPtr(v string) *string {
	if v == "" {
		return nil
	}
	return &v
}

func int64Ptr(v int64) *int64 {
	return &v
}

func toGeneratedTransferBreakdown(item models.TransferAttributionBreakdown) metricsapi.TransferAttributionBreakdown {
	return metricsapi.TransferAttributionBreakdown{
		Key:              &item.Key,
		Organization:     &item.Organization,
		Project:          &item.Project,
		Provider:         &item.Provider,
		Bucket:           &item.Bucket,
		Sha256:           &item.SHA256,
		ActorEmail:       &item.ActorEmail,
		ActorSubject:     &item.ActorSubject,
		EventCount:       &item.EventCount,
		BytesRequested:   &item.BytesRequested,
		BytesDownloaded:  &item.BytesDownloaded,
		BytesUploaded:    &item.BytesUploaded,
		LastTransferTime: item.LastTransferTime,
	}
}

func toMetricsFileUsage(v models.FileUsage) metricsapi.FileUsage {
	return metricsapi.FileUsage{
		ObjectId:         &v.ObjectID,
		Name:             &v.Name,
		Size:             &v.Size,
		UploadCount:      &v.UploadCount,
		DownloadCount:    &v.DownloadCount,
		LastUploadTime:   v.LastUploadTime,
		LastDownloadTime: v.LastDownloadTime,
		LastAccessTime:   v.LastAccessTime,
	}
}

func (s *MetricsServer) providerSyncRunsForRequest(ctx context.Context, body metricsapi.ProviderTransferSyncRequest) ([]models.ProviderTransferSyncRun, error) {
	status := models.ProviderTransferSyncPending
	if body.Status != nil {
		status = string(*body.Status)
	}
	switch status {
	case models.ProviderTransferSyncPending, models.ProviderTransferSyncCompleted, models.ProviderTransferSyncFailed:
	default:
		return nil, common.ErrNotFound
	}

	provider := strings.TrimSpace(generatedString(body.Provider))
	bucket := strings.TrimSpace(generatedString(body.Bucket))
	buckets, err := s.providerSyncCredentials(ctx, provider, bucket)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	var startedAt, completedAt *time.Time
	if status == models.ProviderTransferSyncCompleted || status == models.ProviderTransferSyncFailed {
		t := now
		startedAt = &t
		completedAt = &t
	}

	org := strings.TrimSpace(generatedString(body.Organization))
	project := strings.TrimSpace(generatedString(body.Project))
	from := body.From.UTC()
	to := body.To.UTC()
	runs := make([]models.ProviderTransferSyncRun, 0, len(buckets))
	for _, target := range buckets {
		run := models.ProviderTransferSyncRun{
			SyncID:          providerSyncID(target.provider, target.bucket, org, project, from, to),
			Provider:        target.provider,
			Bucket:          target.bucket,
			Organization:    org,
			Project:         project,
			From:            from,
			To:              to,
			Status:          status,
			RequestedAt:     now,
			StartedAt:       startedAt,
			CompletedAt:     completedAt,
			ImportedEvents:  int64PtrVal(body.ImportedEvents),
			MatchedEvents:   int64PtrVal(body.MatchedEvents),
			AmbiguousEvents: int64PtrVal(body.AmbiguousEvents),
			UnmatchedEvents: int64PtrVal(body.UnmatchedEvents),
			ErrorMessage:    strings.TrimSpace(generatedString(body.ErrorMessage)),
		}
		runs = append(runs, run)
	}
	return runs, nil
}

type providerSyncTarget struct {
	provider string
	bucket   string
	cred     models.S3Credential
}

func (s *MetricsServer) providerSyncBuckets(ctx context.Context, provider, bucket string) ([]providerSyncTarget, error) {
	if bucket != "" {
		if provider == "" {
			provider = common.S3Provider
		}
		return []providerSyncTarget{{provider: provider, bucket: bucket}}, nil
	}
	creds, err := s.database.ListS3Credentials(ctx)
	if err != nil {
		return nil, err
	}
	targets := make([]providerSyncTarget, 0, len(creds))
	for _, cred := range creds {
		credProvider := common.NormalizeProvider(cred.Provider, common.S3Provider)
		if provider != "" && provider != credProvider {
			continue
		}
		if strings.TrimSpace(cred.Bucket) == "" {
			continue
		}
		targets = append(targets, providerSyncTarget{provider: credProvider, bucket: strings.TrimSpace(cred.Bucket), cred: cred})
	}
	return targets, nil
}

func (s *MetricsServer) providerSyncCredentials(ctx context.Context, provider, bucket string) ([]providerSyncTarget, error) {
	creds, err := s.database.ListS3Credentials(ctx)
	if err != nil {
		return nil, err
	}
	if bucket != "" {
		if provider == "" {
			provider = "s3"
		}
		for _, cred := range creds {
			credProvider := common.NormalizeProvider(cred.Provider, common.S3Provider)
			if strings.TrimSpace(cred.Bucket) == bucket && credProvider == provider {
				return []providerSyncTarget{{provider: provider, bucket: bucket, cred: cred}}, nil
			}
		}
		return nil, common.ErrNotFound
	}
	targets := make([]providerSyncTarget, 0, len(creds))
	for _, cred := range creds {
		credProvider := common.NormalizeProvider(cred.Provider, common.S3Provider)
		if provider != "" && provider != credProvider {
			continue
		}
		if strings.TrimSpace(cred.Bucket) == "" {
			continue
		}
		targets = append(targets, providerSyncTarget{provider: credProvider, bucket: strings.TrimSpace(cred.Bucket), cred: cred})
	}
	return targets, nil
}

func shouldCollectProviderTransferSync(body metricsapi.ProviderTransferSyncRequest) bool {
	if body.Status == nil {
		return true
	}
	return string(*body.Status) == models.ProviderTransferSyncPending
}

func (s *MetricsServer) collectProviderTransferSyncRuns(ctx context.Context, body metricsapi.ProviderTransferSyncRequest, runs []models.ProviderTransferSyncRun) []models.ProviderTransferSyncRun {
	targets, err := s.providerSyncCredentials(ctx, strings.TrimSpace(generatedString(body.Provider)), strings.TrimSpace(generatedString(body.Bucket)))
	if err != nil {
		now := time.Now().UTC()
		for i := range runs {
			runs[i].Status = models.ProviderTransferSyncFailed
			runs[i].StartedAt = &now
			runs[i].CompletedAt = &now
			runs[i].ErrorMessage = err.Error()
		}
		return runs
	}
	targetByKey := make(map[string]providerSyncTarget, len(targets))
	for _, target := range targets {
		targetByKey[target.provider+"\x00"+target.bucket] = target
	}

	org := strings.TrimSpace(generatedString(body.Organization))
	project := strings.TrimSpace(generatedString(body.Project))
	for i := range runs {
		started := time.Now().UTC()
		runs[i].StartedAt = &started
		target, ok := targetByKey[runs[i].Provider+"\x00"+runs[i].Bucket]
		if !ok {
			completed := time.Now().UTC()
			runs[i].Status = models.ProviderTransferSyncFailed
			runs[i].CompletedAt = &completed
			runs[i].ErrorMessage = "bucket credential not found"
			continue
		}

		events, err := s.collectProviderTransferEvents(ctx, target.cred, runs[i].From, runs[i].To, org, project)
		completed := time.Now().UTC()
		runs[i].CompletedAt = &completed
		if err != nil {
			runs[i].Status = models.ProviderTransferSyncFailed
			runs[i].ErrorMessage = err.Error()
			slog.Warn("provider transfer sync failed",
				"provider", runs[i].Provider,
				"bucket", runs[i].Bucket,
				"prefix", strings.Trim(strings.TrimSpace(target.cred.BillingLogPrefix), "/"),
				"organization", org,
				"project", project,
				"from", runs[i].From.Format(time.RFC3339Nano),
				"to", runs[i].To.Format(time.RFC3339Nano),
				"duration_ms", completed.Sub(started).Milliseconds(),
				"err", err,
			)
			continue
		}
		if len(events) > 0 {
			if err := s.database.RecordProviderTransferEvents(ctx, events); err != nil {
				runs[i].Status = models.ProviderTransferSyncFailed
				runs[i].ErrorMessage = err.Error()
				slog.Warn("provider transfer sync failed to persist events",
					"provider", runs[i].Provider,
					"bucket", runs[i].Bucket,
					"prefix", strings.Trim(strings.TrimSpace(target.cred.BillingLogPrefix), "/"),
					"organization", org,
					"project", project,
					"from", runs[i].From.Format(time.RFC3339Nano),
					"to", runs[i].To.Format(time.RFC3339Nano),
					"imported", len(events),
					"duration_ms", completed.Sub(started).Milliseconds(),
					"err", err,
				)
				continue
			}
		} else {
			runs[i].ErrorMessage = "provider sync completed but no billable transfer events were found in the configured log source"
		}
		runs[i].Status = models.ProviderTransferSyncCompleted
		runs[i].ImportedEvents = int64(len(events))
		runs[i].MatchedEvents, runs[i].AmbiguousEvents, runs[i].UnmatchedEvents = providerTransferEventStatusCounts(events)
		slog.Info("provider transfer sync complete",
			"provider", runs[i].Provider,
			"bucket", runs[i].Bucket,
			"prefix", strings.Trim(strings.TrimSpace(target.cred.BillingLogPrefix), "/"),
			"organization", org,
			"project", project,
			"from", runs[i].From.Format(time.RFC3339Nano),
			"to", runs[i].To.Format(time.RFC3339Nano),
			"imported", runs[i].ImportedEvents,
			"matched", runs[i].MatchedEvents,
			"ambiguous", runs[i].AmbiguousEvents,
			"unmatched", runs[i].UnmatchedEvents,
			"duration_ms", completed.Sub(started).Milliseconds(),
			"warning", runs[i].ErrorMessage,
		)
	}
	return runs
}

func providerTransferEventStatusCounts(events []models.ProviderTransferEvent) (matched, ambiguous, unmatched int64) {
	for _, ev := range events {
		switch ev.ReconciliationStatus {
		case models.ProviderTransferMatched:
			matched++
		case models.ProviderTransferAmbiguous:
			ambiguous++
		default:
			unmatched++
		}
	}
	return matched, ambiguous, unmatched
}

func providerSyncID(provider, bucket, organization, project string, from, to time.Time) string {
	raw := strings.Join([]string{
		strings.TrimSpace(provider),
		strings.TrimSpace(bucket),
		strings.TrimSpace(organization),
		strings.TrimSpace(project),
		from.UTC().Format(time.RFC3339Nano),
		to.UTC().Format(time.RFC3339Nano),
	}, "\x00")
	sum := sha256.Sum256([]byte(raw))
	return "sync-" + hex.EncodeToString(sum[:16])
}

func int64PtrVal(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}

func (s *MetricsServer) transferFreshness(ctx context.Context, filter models.TransferAttributionFilter) (metricsapi.TransferMetricsFreshness, bool, error) {
	stale := false
	missing := make([]string, 0)
	freshness := metricsapi.TransferMetricsFreshness{
		IsStale:        &stale,
		MissingBuckets: &missing,
		RequiredFrom:   filter.From,
		RequiredTo:     filter.To,
	}
	if filter.From == nil || filter.To == nil || !filter.From.Before(*filter.To) {
		stale = true
		missing = append(missing, "time_window")
		freshness.IsStale = &stale
		freshness.MissingBuckets = &missing
		return freshness, stale, nil
	}

	targets, err := s.providerSyncBuckets(ctx, strings.TrimSpace(filter.Provider), strings.TrimSpace(filter.Bucket))
	if err != nil {
		return freshness, false, err
	}
	if len(targets) == 0 {
		stale = true
		missing = append(missing, "provider_bucket")
	}

	var latest *time.Time
	for _, target := range targets {
		syncFilter := models.TransferAttributionFilter{
			Organization: filter.Organization,
			Project:      filter.Project,
			From:         filter.From,
			To:           filter.To,
			Provider:     target.provider,
			Bucket:       target.bucket,
		}
		runs, err := s.database.ListProviderTransferSyncRuns(ctx, syncFilter, 50)
		if err != nil {
			return freshness, false, err
		}
		covered := false
		for _, run := range runs {
			if run.Status != models.ProviderTransferSyncCompleted {
				continue
			}
			if run.From.After(*filter.From) || run.To.Before(*filter.To) {
				continue
			}
			covered = true
			if run.CompletedAt != nil && (latest == nil || run.CompletedAt.After(*latest)) {
				t := run.CompletedAt.UTC()
				latest = &t
			}
			break
		}
		if !covered {
			stale = true
			missing = append(missing, target.provider+":"+target.bucket)
		}
	}
	freshness.IsStale = &stale
	freshness.MissingBuckets = &missing
	freshness.LatestCompletedSync = latest
	return freshness, stale, nil
}

func parseInactiveSince(inactiveDays *int) (*time.Time, error) {
	if inactiveDays == nil {
		return nil, nil
	}
	days := *inactiveDays
	if days < 0 {
		return nil, errors.New("inactive_days must be a non-negative integer")
	}
	t := time.Now().UTC().AddDate(0, 0, -days)
	return &t, nil
}

type metricsAccess struct {
	organization string
	project      string
}

func (a metricsAccess) isScoped() bool {
	return strings.TrimSpace(a.organization) != ""
}

func resolveMetricsAccess(ctx context.Context) (metricsAccess, error) {
	org, project, _, err := parseScopeQuery(ctx)
	if err != nil {
		return metricsAccess{}, err
	}
	return metricsAccess{organization: org, project: project}, nil
}

func hasGlobalMetricsReadAccess(ctx context.Context) bool {
	return authz.HasMethodAccess(ctx, "read", []string{"/data_file"}) ||
		authz.HasMethodAccess(ctx, "read", []string{"/programs"})
}

func parseScopeQuery(ctx context.Context) (string, string, bool, error) {
	params, _ := ctx.Value(metricsQueryContextKey{}).(metricsQueryParams)
	org := strings.TrimSpace(params.organization)
	if org == "" {
		org = strings.TrimSpace(params.program)
	}
	project := strings.TrimSpace(params.project)
	if project != "" && org == "" {
		return "", "", false, errors.New("organization is required when project is set")
	}
	if org != "" {
		return org, project, true, nil
	}
	return "", "", false, nil
}

func collectScopedUsage(ctx context.Context, database db.MetricsStore, organization, project string, inactiveSince *time.Time) ([]models.FileUsage, models.FileUsageSummary, error) {
	ids, err := database.ListObjectIDsByScope(ctx, organization, project)
	if err != nil {
		return nil, models.FileUsageSummary{}, err
	}
	sort.Strings(ids)

	summary := models.FileUsageSummary{TotalFiles: int64(len(ids))}
	usages := make([]models.FileUsage, 0, len(ids))
	for _, id := range ids {
		usage, err := database.GetFileUsage(ctx, id)
		if err != nil {
			if errors.Is(err, common.ErrNotFound) {
				if inactiveSince != nil {
					summary.InactiveFileCount++
				}
				obj, objErr := database.GetObject(ctx, id)
				if objErr != nil {
					if errors.Is(objErr, common.ErrNotFound) {
						continue
					}
					return nil, models.FileUsageSummary{}, objErr
				}
				usages = append(usages, models.FileUsage{
					ObjectID: id,
					Name:     common.StringVal(obj.Name),
					Size:     obj.Size,
				})
				continue
			}
			return nil, models.FileUsageSummary{}, err
		}
		summary.TotalUploads += usage.UploadCount
		summary.TotalDownloads += usage.DownloadCount
		if inactiveSince != nil && (usage.LastDownloadTime == nil || usage.LastDownloadTime.Before(*inactiveSince)) {
			summary.InactiveFileCount++
		}
		if inactiveSince != nil && usage.LastDownloadTime != nil && !usage.LastDownloadTime.Before(*inactiveSince) {
			continue
		}
		usages = append(usages, *usage)
	}
	return usages, summary, nil
}

func listScopedFileUsage(ctx context.Context, database db.MetricsStore, organization, project string, limit, offset int, inactiveSince *time.Time) ([]models.FileUsage, models.FileUsageSummary, error) {
	usages, summary, err := collectScopedUsage(ctx, database, organization, project, inactiveSince)
	if err != nil {
		return nil, models.FileUsageSummary{}, err
	}
	if limit <= 0 {
		return usages, summary, nil
	}
	if offset >= len(usages) {
		return []models.FileUsage{}, summary, nil
	}
	end := offset + limit
	if end > len(usages) {
		end = len(usages)
	}
	return usages[offset:end], summary, nil
}

func objectInScope(ctx context.Context, database db.MetricsStore, objectID, organization, project string) (bool, error) {
	obj, err := database.GetObject(ctx, objectID)
	if err != nil {
		if errors.Is(err, common.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	if strings.TrimSpace(organization) == "" {
		return true, nil
	}
	projects, ok := obj.Authorizations[organization]
	if !ok {
		return false, nil
	}
	if strings.TrimSpace(project) == "" || len(projects) == 0 {
		return true, nil
	}
	for _, p := range projects {
		if p == project {
			return true, nil
		}
	}
	return false, nil
}
