package metrics

import (
	"context"
	"errors"
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
	} else if access.hasScopeAggregate() {
		data, _, err = listMultiScopedFileUsage(ctx, s.database, access.scopes, limit, offset, inactiveSince)
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
	} else if access.hasScopeAggregate() {
		inside, err := objectInAnyScope(ctx, s.database, objectID, access.scopes)
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
	} else if access.hasScopeAggregate() {
		_, summary, err = listMultiScopedFileUsage(ctx, s.database, access.scopes, 0, 0, inactiveSince)
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

	if !authz.IsAuthzEnforced(ctx) {
		return access, 0, true
	}
	if authz.IsGen3Mode(ctx) && !authz.HasAuthHeader(ctx) {
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
		return access, http.StatusForbidden, false
	}

	scopes := readableMetricsScopes(ctx)
	if len(scopes) > 0 {
		access.scopes = scopes
		return access, 0, true
	}

	return access, http.StatusForbidden, false
}

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

func (s *MetricsServer) GetTransferSummary(ctx context.Context, request metricsapi.GetTransferSummaryRequestObject) (metricsapi.GetTransferSummaryResponseObject, error) {
	access, statusCode, ok := s.checkAuth(ctx)
	if !ok {
		return getTransferSummaryAuthResponse(statusCode), nil
	}
	filter := transferSummaryParamsToFilter(request.Params)
	freshness, _, err := s.transferFreshness(ctx, filter)
	if err != nil {
		return metricsapi.GetTransferSummary500Response{}, nil
	}
	var summary models.TransferAttributionSummary
	if access.hasScopeAggregate() && filter.Organization == "" {
		summary, err = s.getScopedTransferAttributionSummary(ctx, filter, access.scopes)
	} else {
		summary, err = s.database.GetTransferAttributionSummary(ctx, filter)
	}
	if err != nil {
		return metricsapi.GetTransferSummary500Response{}, nil
	}
	generated := toGeneratedTransferSummary(summary)
	generated.Freshness = &freshness
	return metricsapi.GetTransferSummary200JSONResponse(generated), nil
}

func (s *MetricsServer) GetTransferBreakdown(ctx context.Context, request metricsapi.GetTransferBreakdownRequestObject) (metricsapi.GetTransferBreakdownResponseObject, error) {
	access, statusCode, ok := s.checkAuth(ctx)
	if !ok {
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
	var items []models.TransferAttributionBreakdown
	if access.hasScopeAggregate() && filter.Organization == "" {
		items, err = s.getScopedTransferAttributionBreakdown(ctx, filter, groupBy, access.scopes)
	} else {
		items, err = s.database.GetTransferAttributionBreakdown(ctx, filter, groupBy)
	}
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

func (s *MetricsServer) transferFreshness(ctx context.Context, filter models.TransferAttributionFilter) (metricsapi.TransferMetricsFreshness, bool, error) {
	stale := false
	missing := make([]string, 0)
	freshness := metricsapi.TransferMetricsFreshness{
		IsStale:        &stale,
		MissingBuckets: &missing,
		RequiredFrom:   filter.From,
		RequiredTo:     filter.To,
	}
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
	scopes       []metricsScope
}

func (a metricsAccess) isScoped() bool {
	return strings.TrimSpace(a.organization) != ""
}

func (a metricsAccess) hasScopeAggregate() bool {
	return !a.isScoped() && len(a.scopes) > 0
}

type metricsScope struct {
	organization string
	project      string
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

func readableMetricsScopes(ctx context.Context) []metricsScope {
	privs := authz.GetUserPrivileges(ctx)
	scopes := make([]metricsScope, 0, len(privs))
	seen := map[string]bool{}
	for resource, methods := range privs {
		if !(methods["read"] || methods["*"]) {
			continue
		}
		scope, ok := metricsScopeFromResource(resource)
		if !ok {
			continue
		}
		key := scope.organization + "\x00" + scope.project
		if seen[key] {
			continue
		}
		seen[key] = true
		scopes = append(scopes, scope)
	}
	orgWide := map[string]bool{}
	for _, scope := range scopes {
		if scope.project == "" {
			orgWide[scope.organization] = true
		}
	}
	if len(orgWide) > 0 {
		filtered := scopes[:0]
		for _, scope := range scopes {
			if scope.project != "" && orgWide[scope.organization] {
				continue
			}
			filtered = append(filtered, scope)
		}
		scopes = filtered
	}
	sort.Slice(scopes, func(i, j int) bool {
		if scopes[i].organization == scopes[j].organization {
			return scopes[i].project < scopes[j].project
		}
		return scopes[i].organization < scopes[j].organization
	})
	return scopes
}

func metricsScopeFromResource(resource string) (metricsScope, bool) {
	parts := strings.Split(strings.Trim(resource, "/"), "/")
	if len(parts) == 2 && parts[0] == "programs" && parts[1] != "" {
		return metricsScope{organization: parts[1]}, true
	}
	if len(parts) == 4 && parts[0] == "programs" && parts[2] == "projects" && parts[1] != "" && parts[3] != "" {
		return metricsScope{organization: parts[1], project: parts[3]}, true
	}
	return metricsScope{}, false
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

func listMultiScopedFileUsage(ctx context.Context, database db.MetricsStore, scopes []metricsScope, limit, offset int, inactiveSince *time.Time) ([]models.FileUsage, models.FileUsageSummary, error) {
	byID := map[string]models.FileUsage{}
	var summary models.FileUsageSummary
	for _, scope := range scopes {
		usages, scopedSummary, err := collectScopedUsage(ctx, database, scope.organization, scope.project, inactiveSince)
		if err != nil {
			return nil, models.FileUsageSummary{}, err
		}
		summary.TotalFiles += scopedSummary.TotalFiles
		summary.TotalUploads += scopedSummary.TotalUploads
		summary.TotalDownloads += scopedSummary.TotalDownloads
		summary.InactiveFileCount += scopedSummary.InactiveFileCount
		for _, usage := range usages {
			byID[usage.ObjectID] = usage
		}
	}
	usages := make([]models.FileUsage, 0, len(byID))
	for _, usage := range byID {
		usages = append(usages, usage)
	}
	sort.Slice(usages, func(i, j int) bool {
		return usages[i].ObjectID < usages[j].ObjectID
	})
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

func objectInAnyScope(ctx context.Context, database db.MetricsStore, objectID string, scopes []metricsScope) (bool, error) {
	for _, scope := range scopes {
		inside, err := objectInScope(ctx, database, objectID, scope.organization, scope.project)
		if err != nil || inside {
			return inside, err
		}
	}
	return false, nil
}

func (s *MetricsServer) getScopedTransferAttributionSummary(ctx context.Context, filter models.TransferAttributionFilter, scopes []metricsScope) (models.TransferAttributionSummary, error) {
	var out models.TransferAttributionSummary
	for _, scope := range scopes {
		scoped := filter
		scoped.Organization = scope.organization
		scoped.Project = scope.project
		summary, err := s.database.GetTransferAttributionSummary(ctx, scoped)
		if err != nil {
			return models.TransferAttributionSummary{}, err
		}
		out.EventCount += summary.EventCount
		out.AccessIssuedCount += summary.AccessIssuedCount
		out.DownloadEventCount += summary.DownloadEventCount
		out.UploadEventCount += summary.UploadEventCount
		out.BytesRequested += summary.BytesRequested
		out.BytesDownloaded += summary.BytesDownloaded
		out.BytesUploaded += summary.BytesUploaded
	}
	return out, nil
}

func (s *MetricsServer) getScopedTransferAttributionBreakdown(ctx context.Context, filter models.TransferAttributionFilter, groupBy string, scopes []metricsScope) ([]models.TransferAttributionBreakdown, error) {
	byKey := map[string]*models.TransferAttributionBreakdown{}
	for _, scope := range scopes {
		scoped := filter
		scoped.Organization = scope.organization
		scoped.Project = scope.project
		items, err := s.database.GetTransferAttributionBreakdown(ctx, scoped, groupBy)
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			key := item.Key
			if key == "" {
				key = item.Organization + "/" + item.Project + "/" + item.Provider + "/" + item.Bucket + "/" + item.SHA256 + "/" + item.ActorEmail + "/" + item.ActorSubject
			}
			merged := byKey[key]
			if merged == nil {
				copy := item
				byKey[key] = &copy
				continue
			}
			merged.EventCount += item.EventCount
			merged.BytesRequested += item.BytesRequested
			merged.BytesDownloaded += item.BytesDownloaded
			merged.BytesUploaded += item.BytesUploaded
			if item.LastTransferTime != nil && (merged.LastTransferTime == nil || item.LastTransferTime.After(*merged.LastTransferTime)) {
				t := *item.LastTransferTime
				merged.LastTransferTime = &t
			}
		}
	}
	out := make([]models.TransferAttributionBreakdown, 0, len(byKey))
	for _, item := range byKey {
		out = append(out, *item)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].LastTransferTime == nil || out[j].LastTransferTime == nil {
			return out[i].Key < out[j].Key
		}
		if out[i].LastTransferTime.Equal(*out[j].LastTransferTime) {
			return out[i].Key < out[j].Key
		}
		return out[i].LastTransferTime.After(*out[j].LastTransferTime)
	})
	return out, nil
}
