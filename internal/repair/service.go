package repair

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"

	drsapi "github.com/calypr/syfon/apigen/client/drs"
	"github.com/calypr/syfon/apigen/client/internalapi"
	"github.com/calypr/syfon/client/request"
	syfoncommon "github.com/calypr/syfon/common"
	intcommon "github.com/calypr/syfon/internal/common"
)

const defaultPageSize = 500

func (s *Service) Audit(ctx context.Context, opts Options) (Report, error) {
	state, err := s.audit(ctx, opts)
	if err != nil {
		return Report{}, err
	}
	return state.report, nil
}

func (s *Service) Apply(ctx context.Context, opts Options) (ApplyResult, error) {
	opts.Organization = strings.TrimSpace(opts.Organization)
	opts.Project = strings.TrimSpace(opts.Project)
	if opts.Organization == "" || opts.Project == "" {
		return ApplyResult{}, fmt.Errorf("apply requires --organization and --project")
	}

	state, err := s.audit(ctx, opts)
	if err != nil {
		return ApplyResult{}, err
	}

	result := ApplyResult{Report: state.report}
	for _, obj := range state.objects {
		if obj.updated == nil {
			continue
		}
		result.AutoFixable++
		if _, err := s.index.Update(ctx, obj.record.Did, *obj.updated); err != nil {
			result.Skipped++
			continue
		}
		result.Mutated++
	}
	return result, nil
}

func (s *Service) audit(ctx context.Context, opts Options) (*auditState, error) {
	scopes, err := s.loadScopeTargets(ctx)
	if err != nil {
		return nil, err
	}
	records, scanned, err := s.listRecords(ctx, opts)
	if err != nil {
		return nil, err
	}

	state := &auditState{
		report: Report{
			Organization: strings.TrimSpace(opts.Organization),
			Project:      strings.TrimSpace(opts.Project),
			Scanned:      scanned,
		},
	}
	for _, rec := range records {
		obj, include := s.auditRecord(ctx, rec, scopes, opts)
		if !include {
			continue
		}
		state.objects = append(state.objects, obj)
	}
	s.addDuplicateFindings(state.objects)
	sort.Slice(state.objects, func(i, j int) bool { return state.objects[i].record.Did < state.objects[j].record.Did })
	for _, obj := range state.objects {
		if len(obj.findings) == 0 {
			continue
		}
		state.report.Objects = append(state.report.Objects, ObjectReport{
			ObjectID:             obj.record.Did,
			SHA256:               obj.sha256,
			Organization:         obj.scope.Organization,
			Project:              obj.scope.Project,
			CurrentAccessURLs:    append([]string(nil), obj.currentURLs...),
			ProposedCanonicalURL: obj.canonicalURL,
			AutoFixable:          obj.updated != nil,
			Findings:             append([]Finding(nil), obj.findings...),
		})
	}
	return state, nil
}

func (s *Service) listRecords(ctx context.Context, opts Options) ([]internalapi.InternalRecord, int, error) {
	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	var out []internalapi.InternalRecord
	start := ""
	scanned := 0
	for {
		limit := pageSize
		if opts.Limit > 0 && opts.Limit-scanned < limit {
			limit = opts.Limit - scanned
		}
		if limit <= 0 && opts.Limit > 0 {
			break
		}
		resp, err := s.index.List(ctx, ListRecordsOptions{Limit: limit, Start: start})
		if err != nil {
			return nil, scanned, err
		}
		if resp.Records == nil || len(*resp.Records) == 0 {
			break
		}
		records := *resp.Records
		out = append(out, records...)
		scanned += len(records)
		start = strings.TrimSpace(records[len(records)-1].Did)
		if len(records) < limit || start == "" {
			break
		}
	}
	return out, scanned, nil
}

func (s *Service) auditRecord(ctx context.Context, rec internalapi.InternalRecord, scopes map[string][]scopeTarget, opts Options) (*auditedObject, bool) {
	sha := ""
	if rec.Hashes != nil {
		sha = syfoncommon.NormalizeChecksum((*rec.Hashes)["sha256"])
	}
	obj := &auditedObject{
		record:      rec,
		sha256:      sha,
		currentURLs: accessMethodURLs(rec.AccessMethods),
	}

	resource, known, ambiguous := inferRecordResource(rec, sha, scopes)
	obj.scopeKnown = known
	obj.scopeAmbiguous = ambiguous
	obj.inferredScope = resource
	if known {
		obj.scope = scopes[resource][0]
		obj.canonicalURL = canonicalAccessURL(obj.scope, rec.Did, sha)
	}

	targetResource := ""
	if strings.TrimSpace(opts.Organization) != "" && strings.TrimSpace(opts.Project) != "" {
		targetResource, _ = syfoncommon.ResourcePath(opts.Organization, opts.Project)
	}
	if targetResource != "" && !recordMatchesResource(rec, targetResource) && obj.inferredScope != targetResource {
		return obj, false
	}

	if targetResource != "" && !recordMatchesResource(rec, targetResource) && obj.inferredScope == targetResource && sha != "" {
		obj.findings = append(obj.findings, newFinding(FindingMissingControlledAccess, SeverityWarn, rec, sha, obj.currentURLs, obj.canonicalURL, true, "missing controlled_access row recoverable from deterministic scope"))
		updated := cloneRecord(rec)
		updated.ControlledAccess = addControlledAccess(updated.ControlledAccess, targetResource)
		obj.updated = &updated
	}
	if obj.scopeKnown && obj.canonicalURL != "" {
		s.classifyAccessMethods(obj)
	}
	if opts.CheckStorage {
		s.addStorageFindings(ctx, obj)
	}
	return obj, true
}

func (s *Service) classifyAccessMethods(obj *auditedObject) {
	if obj.record.AccessMethods == nil {
		return
	}
	methods := cloneAccessMethods(*obj.record.AccessMethods)
	hasCanonical := false
	for _, raw := range obj.currentURLs {
		if raw == obj.canonicalURL {
			hasCanonical = true
			break
		}
	}

	changed := false
	for i := range methods {
		raw := accessMethodURL(methods[i])
		if raw == "" || raw == obj.canonicalURL {
			continue
		}
		if isLegacyPathStyleURL(raw) {
			if hasCanonical {
				obj.findings = append(obj.findings, newFinding(FindingLegacyAccessURLRemovable, SeverityWarn, obj.record, obj.sha256, obj.currentURLs, obj.canonicalURL, true, "legacy META/CONFIG URL has canonical sibling"))
				methods[i].AccessUrl = nil
				changed = true
				continue
			}
			obj.findings = append(obj.findings, newFinding(FindingLegacyAccessURLRewritable, SeverityWarn, obj.record, obj.sha256, obj.currentURLs, obj.canonicalURL, true, "legacy META/CONFIG URL can be rewritten to canonical scoped URL"))
			setAccessMethodURL(&methods[i], obj.canonicalURL)
			changed = true
			continue
		}
		obj.findings = append(obj.findings, newFinding(FindingNonCanonicalAccessURL, SeverityInfo, obj.record, obj.sha256, obj.currentURLs, obj.canonicalURL, false, "access URL does not match canonical scoped target"))
	}

	if !changed {
		return
	}
	filtered := make([]drsapi.AccessMethod, 0, len(methods))
	for _, method := range methods {
		if accessMethodURL(method) == "" {
			continue
		}
		filtered = append(filtered, method)
	}
	updated := cloneRecord(obj.record)
	updated.AccessMethods = &filtered
	if obj.updated != nil && obj.updated.ControlledAccess != nil {
		updated.ControlledAccess = obj.updated.ControlledAccess
	}
	obj.updated = &updated
}

func (s *Service) addStorageFindings(ctx context.Context, obj *auditedObject) {
	for _, raw := range obj.currentURLs {
		req := storageInspectRequest{
			Organization: obj.scope.Organization,
			Project:      obj.scope.Project,
			ObjectURL:    raw,
		}
		var resp storageInspectResponse
		err := s.requestor.Do(ctx, http.MethodPost, "/data/inspect", req, &resp)
		if err == nil {
			continue
		}
		kind := FindingStorageProbeError
		severity := SeverityWarn
		message := err.Error()
		if respErr, ok := err.(*request.ResponseError); ok && respErr.Status == http.StatusNotFound {
			kind = FindingStorageObjectMissing
			severity = SeverityError
			message = "storage object not found"
		}
		obj.findings = append(obj.findings, newFinding(kind, severity, obj.record, obj.sha256, []string{raw}, obj.canonicalURL, false, message))
	}
}

func (s *Service) loadScopeTargets(ctx context.Context) (map[string][]scopeTarget, error) {
	buckets, err := s.buckets.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list buckets: %w", err)
	}
	out := map[string][]scopeTarget{}
	for bucket := range buckets.S3BUCKETS {
		scopes, err := s.buckets.ListScopes(ctx, bucket)
		if err != nil {
			return nil, fmt.Errorf("list bucket scopes for %s: %w", bucket, err)
		}
		for _, scope := range scopes {
			resource, err := syfoncommon.ResourcePath(strings.TrimSpace(scope.Organization), strings.TrimSpace(scope.ProjectId))
			if err != nil || resource == "" {
				continue
			}
			target := scopeTarget{
				Resource:     resource,
				Organization: strings.TrimSpace(scope.Organization),
				Project:      strings.TrimSpace(scope.ProjectId),
				Bucket:       bucket,
			}
			if scope.Path != nil {
				parsed, err := url.Parse(strings.TrimSpace(*scope.Path))
				if err == nil {
					if host := strings.TrimSpace(parsed.Host); host != "" {
						target.Bucket = host
					}
					target.Prefix = strings.Trim(strings.TrimSpace(parsed.Path), "/")
				}
			}
			out[resource] = append(out[resource], target)
		}
	}
	return out, nil
}

func (s *Service) addDuplicateFindings(objects []*auditedObject) {
	byKey := map[string][]*auditedObject{}
	for _, obj := range objects {
		if obj.sha256 == "" {
			continue
		}
		for _, resource := range recordProjectResources(obj.record, obj.inferredScope) {
			byKey[resource+"|"+obj.sha256] = append(byKey[resource+"|"+obj.sha256], obj)
		}
	}
	for key, group := range byKey {
		if len(group) < 2 {
			continue
		}
		resource := strings.SplitN(key, "|", 2)[0]
		scope := intcommon.ParseResourcePath(resource)
		for _, obj := range group {
			obj.findings = append(obj.findings, newFinding(FindingDuplicateSHA256Sibling, SeverityWarn, obj.record, obj.sha256, obj.currentURLs, obj.canonicalURL, false, "same sha256 appears in multiple DIDs for this scope"))
			if obj.scope.Organization == "" {
				obj.scope.Organization = scope.Organization
				obj.scope.Project = scope.Project
			}
		}
	}
}

func inferRecordResource(rec internalapi.InternalRecord, sha string, scopes map[string][]scopeTarget) (string, bool, bool) {
	projectResources := recordProjectResources(rec, "")
	if len(projectResources) == 1 {
		return projectResources[0], true, false
	}
	if len(projectResources) > 1 {
		return "", false, true
	}
	if strings.TrimSpace(sha) == "" {
		return "", false, false
	}
	matches := []string{}
	for resource, targets := range scopes {
		if len(targets) == 0 || strings.TrimSpace(targets[0].Project) == "" {
			continue
		}
		minted, err := intcommon.MintObjectIDFromChecksum(sha, []string{resource})
		if err != nil {
			continue
		}
		if minted == strings.TrimSpace(rec.Did) {
			matches = append(matches, resource)
		}
	}
	if len(matches) == 1 {
		return matches[0], true, false
	}
	if len(matches) > 1 {
		return "", false, true
	}
	return "", false, false
}

func recordProjectResources(rec internalapi.InternalRecord, inferred string) []string {
	seen := map[string]struct{}{}
	var out []string
	if rec.ControlledAccess != nil {
		for _, resource := range syfoncommon.NormalizeAccessResources(*rec.ControlledAccess) {
			org, project, ok := syfoncommon.ResourceScope(resource)
			if !ok || org == "" || project == "" {
				continue
			}
			if _, exists := seen[resource]; exists {
				continue
			}
			seen[resource] = struct{}{}
			out = append(out, resource)
		}
	}
	if inferred != "" {
		if _, exists := seen[inferred]; !exists {
			out = append(out, inferred)
		}
	}
	sort.Strings(out)
	return out
}

func recordMatchesResource(rec internalapi.InternalRecord, resource string) bool {
	if rec.ControlledAccess == nil {
		return false
	}
	for _, candidate := range syfoncommon.NormalizeAccessResources(*rec.ControlledAccess) {
		if candidate == resource {
			return true
		}
	}
	return false
}

func accessMethodURLs(methods *[]drsapi.AccessMethod) []string {
	if methods == nil {
		return nil
	}
	var out []string
	for _, method := range *methods {
		if raw := accessMethodURL(method); raw != "" {
			out = append(out, raw)
		}
	}
	return out
}

func accessMethodURL(method drsapi.AccessMethod) string {
	if method.AccessUrl == nil {
		return ""
	}
	return strings.TrimSpace(method.AccessUrl.Url)
}

func setAccessMethodURL(method *drsapi.AccessMethod, raw string) {
	if method.AccessUrl == nil {
		method.AccessUrl = &struct {
			Headers *[]string `json:"headers,omitempty"`
			Url     string    `json:"url"`
		}{}
	}
	method.AccessUrl.Url = raw
}

func cloneAccessMethods(in []drsapi.AccessMethod) []drsapi.AccessMethod {
	out := make([]drsapi.AccessMethod, len(in))
	copy(out, in)
	return out
}

func cloneRecord(rec internalapi.InternalRecord) internalapi.InternalRecord {
	out := rec
	if rec.AccessMethods != nil {
		methods := cloneAccessMethods(*rec.AccessMethods)
		out.AccessMethods = &methods
	}
	if rec.ControlledAccess != nil {
		controlled := append([]string(nil), (*rec.ControlledAccess)...)
		out.ControlledAccess = &controlled
	}
	if rec.Hashes != nil {
		hashes := make(internalapi.HashInfo, len(*rec.Hashes))
		for k, v := range *rec.Hashes {
			hashes[k] = v
		}
		out.Hashes = &hashes
	}
	return out
}

func addControlledAccess(controlled *[]string, resource string) *[]string {
	resources := append([]string(nil), syfoncommon.NormalizeAccessResources(derefStrings(controlled))...)
	resources = append(resources, resource)
	normalized := syfoncommon.NormalizeAccessResources(resources)
	return &normalized
}

func derefStrings(values *[]string) []string {
	if values == nil {
		return nil
	}
	return *values
}

func canonicalAccessURL(target scopeTarget, did, sha string) string {
	if strings.TrimSpace(target.Bucket) == "" || strings.TrimSpace(did) == "" || strings.TrimSpace(sha) == "" {
		return ""
	}
	parts := []string{}
	if prefix := strings.Trim(target.Prefix, "/"); prefix != "" {
		parts = append(parts, prefix)
	}
	parts = append(parts, strings.TrimSpace(did), strings.TrimSpace(sha))
	return "s3://" + strings.TrimSpace(target.Bucket) + "/" + strings.Join(parts, "/")
}

func isLegacyPathStyleURL(raw string) bool {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || !strings.EqualFold(parsed.Scheme, "s3") {
		return false
	}
	path := strings.Trim(strings.TrimSpace(parsed.Path), "/")
	if path == "" {
		return false
	}
	head := strings.ToUpper(strings.Split(path, "/")[0])
	return head == "META" || head == "CONFIG"
}

func newFinding(kind FindingKind, severity Severity, rec internalapi.InternalRecord, sha string, currentURLs []string, canonical string, autoFixable bool, msg string) Finding {
	finding := Finding{
		Kind:                 kind,
		Severity:             severity,
		ObjectID:             rec.Did,
		SHA256:               sha,
		CurrentAccessURLs:    append([]string(nil), currentURLs...),
		ProposedCanonicalURL: canonical,
		AutoFixable:          autoFixable,
		Message:              msg,
	}
	if rec.ControlledAccess != nil {
		for _, resource := range syfoncommon.NormalizeAccessResources(*rec.ControlledAccess) {
			org, project, ok := syfoncommon.ResourceScope(resource)
			if ok && org != "" {
				finding.Organization = org
				finding.Project = project
				break
			}
		}
	}
	return finding
}
