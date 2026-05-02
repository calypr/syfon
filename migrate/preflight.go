package migrate

import (
	"context"
	"fmt"
	"sort"
	"strings"

	syfoncommon "github.com/calypr/syfon/common"
)

const maxPreflightMissingScopes = 10

type ImportPreflightReport struct {
	Records           int
	RequiredResources []string
	MissingResources  []string
	MissingRecords    int
	FirstDeniedRecord string
}

type ImportPreflightError struct {
	Report ImportPreflightReport
}

func (e *ImportPreflightError) Error() string {
	report := e.Report
	scopes := FormatPreflightScopes(report.MissingResources)
	if len(scopes) > maxPreflightMissingScopes {
		truncated := len(scopes) - maxPreflightMissingScopes
		scopes = scopes[:maxPreflightMissingScopes]
		scopes = append(scopes, fmt.Sprintf("(and %d more)", truncated))
	}

	var b strings.Builder
	fmt.Fprintf(&b, "missing create access for %d/%d records", report.MissingRecords, report.Records)
	if report.FirstDeniedRecord != "" {
		fmt.Fprintf(&b, "; first denied record=%q", report.FirstDeniedRecord)
	}
	if len(scopes) > 0 {
		fmt.Fprintf(&b, "; missing organization/project scopes: %s", strings.Join(scopes, ", "))
	}
	return b.String()
}

func PreflightImport(ctx context.Context, reader DumpReader, privileges PrivilegeLister, batchSize int) (ImportPreflightReport, error) {
	if reader == nil {
		return ImportPreflightReport{}, fmt.Errorf("reader is required")
	}
	if privileges == nil {
		return ImportPreflightReport{}, fmt.Errorf("privilege lister is required")
	}
	userPrivileges, err := privileges.UserPrivileges(ctx)
	if err != nil {
		return ImportPreflightReport{}, fmt.Errorf("load target privileges: %w", err)
	}
	userPrivileges = normalizePrivileges(userPrivileges)

	report := ImportPreflightReport{}
	required := map[string]struct{}{}
	missing := map[string]struct{}{}

	err = reader.ReadBatches(ctx, batchSize, func(records []MigrationRecord) error {
		for _, record := range records {
			if err := Validate(record); err != nil {
				continue
			}
			report.Records++
			resources := syfoncommon.NormalizeAccessResources(migrationRecordControlledAccess(record))
			for _, resource := range resources {
				required[resource] = struct{}{}
			}
			if hasCreateAccess(userPrivileges, resources) {
				continue
			}
			report.MissingRecords++
			if report.FirstDeniedRecord == "" {
				report.FirstDeniedRecord = record.ID
			}
			for _, resource := range resources {
				missing[resource] = struct{}{}
			}
		}
		return nil
	})
	if err != nil {
		return report, err
	}

	report.RequiredResources = sortedMapKeys(required)
	report.MissingResources = sortedMapKeys(missing)
	if report.MissingRecords > 0 {
		return report, &ImportPreflightError{Report: report}
	}
	return report, nil
}

func extractUserPrivileges(resourceAccess map[string]any) map[string]map[string]bool {
	out := make(map[string]map[string]bool, len(resourceAccess))
	for rawPath, rawEntries := range resourceAccess {
		path := syfoncommon.NormalizeAccessResource(rawPath)
		if path == "" {
			continue
		}
		methods := map[string]bool{}
		entries, ok := rawEntries.([]any)
		if !ok {
			out[path] = methods
			continue
		}
		for _, entry := range entries {
			mm, ok := entry.(map[string]any)
			if !ok {
				continue
			}
			service, _ := mm["service"].(string)
			if service != "" && service != "indexd" && service != "drs" && service != "*" {
				continue
			}
			method, _ := mm["method"].(string)
			method = strings.TrimSpace(method)
			if method == "" {
				continue
			}
			methods[method] = true
		}
		out[path] = methods
	}
	return out
}

func hasCreateAccess(privileges map[string]map[string]bool, resources []string) bool {
	resources = syfoncommon.NormalizeAccessResources(resources)
	if len(resources) == 0 {
		return false
	}
	for _, resource := range resources {
		methods, ok := privileges[resource]
		if !ok {
			continue
		}
		if methods["create"] || methods["*"] {
			return true
		}
	}
	return false
}

func normalizePrivileges(privileges map[string]map[string]bool) map[string]map[string]bool {
	if len(privileges) == 0 {
		return nil
	}
	out := make(map[string]map[string]bool, len(privileges))
	for rawResource, methods := range privileges {
		resource := syfoncommon.NormalizeAccessResource(rawResource)
		if resource == "" {
			continue
		}
		if out[resource] == nil {
			out[resource] = map[string]bool{}
		}
		for method, allowed := range methods {
			if allowed {
				out[resource][method] = true
			}
		}
	}
	return out
}

func sortedMapKeys(in map[string]struct{}) []string {
	out := make([]string, 0, len(in))
	for key := range in {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func FormatPreflightScopes(resources []string) []string {
	out := make([]string, 0, len(resources))
	for _, resource := range resources {
		formatted := formatPreflightScope(resource)
		if formatted == "" {
			continue
		}
		out = append(out, formatted)
	}
	return out
}

func formatPreflightScope(resource string) string {
	resource = syfoncommon.NormalizeAccessResource(resource)
	org, project, ok := syfoncommon.ResourceScope(resource)
	if !ok {
		return resource
	}
	if org != "" && project != "" {
		return org + "/" + project
	}
	if strings.TrimSpace(org) != "" {
		return org + "/*"
	}
	return resource
}
