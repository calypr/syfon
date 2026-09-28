package objects

import (
	"context"
	"sort"
	"strings"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/errorapi"
	clientaccess "github.com/calypr/syfon/client/access"
	"github.com/calypr/syfon/internal/access"
)

const maxDeniedAccessResources = 25

// ContentAccess is the access policy read inside a content write transaction.
type ContentAccess struct {
	Resources  []string
	PublicRead bool
}

// AuthorizeRegistration checks additions against the current stored policy.
// A nil current policy means this registration creates a new content row.
func AuthorizeRegistration(ctx context.Context, incoming *drs.DrsObject, current *ContentAccess) error {
	resources := AccessResources(incoming)
	var existing []string
	if current != nil {
		existing = current.Resources
	}
	added := addedResources(resources, existing)
	if current != nil && !current.PublicRead && (len(added) > 0 || len(existing) == 0 || incoming.AccessMethods != nil) && !canReadContent(ctx, existing) {
		return errorapi.ErrAccessDenied
	}
	return authorizeAddedResources(ctx, added)
}

func AuthorizeReplacementResources(ctx context.Context, incoming []string, current ContentAccess) error {
	added := addedResources(incoming, current.Resources)
	if len(added) == 0 {
		return nil
	}
	if !current.PublicRead && !canReadContent(ctx, current.Resources) {
		return errorapi.ErrAccessDenied
	}
	return authorizeAddedResources(ctx, added)
}

func addedResources(incoming, current []string) []string {
	existing := make(map[string]struct{}, len(current))
	for _, resource := range current {
		existing[resource] = struct{}{}
	}
	var added []string
	for _, resource := range incoming {
		if _, ok := existing[resource]; !ok {
			added = append(added, resource)
		}
	}
	return added
}

func authorizeAddedResources(ctx context.Context, resources []string) error {
	for _, resource := range resources {
		if !access.HasMethodAccess(ctx, objectMethodCreate, []string{resource}) {
			return errorapi.ErrAccessDenied
		}
	}
	return nil
}

func canReadContent(ctx context.Context, resources []string) bool {
	if !access.IsAuthzEnforced(ctx) {
		return true
	}
	return len(resources) > 0 && access.HasObjectMethodAccess(ctx, objectMethodRead, resources)
}

func requireScopeMethod(ctx context.Context, organization, project, method string) error {
	resource, err := clientaccess.ResourcePath(organization, project)
	if err != nil {
		return err
	}
	if strings.TrimSpace(resource) == "" {
		return errorapi.ErrAccessDenied
	}
	if access.HasObjectMethodAccess(ctx, method, []string{resource}) {
		return nil
	}
	return errorapi.ErrAccessDenied
}

func requireAllObjectMethod(ctx context.Context, obj *drs.DrsObject, method string) error {
	resources := AccessResources(obj)
	if len(resources) == 0 {
		if access.HasObjectMethodAccess(ctx, method, resources) {
			return nil
		}
		return errorapi.ErrAccessDenied
	}
	if access.HasMethodAccess(ctx, method, resources) {
		return nil
	}
	return errorapi.ErrAccessDenied
}

func hasObjectMethod(ctx context.Context, obj *drs.DrsObject, method string, publicRead map[string]bool) bool {
	method = strings.TrimSpace(method)
	if method == "" {
		return true
	}
	if strings.EqualFold(method, objectMethodRead) && obj != nil {
		if isPublic, known := publicRead[obj.Id]; known {
			if isPublic {
				return true
			}
			if len(AccessResources(obj)) == 0 {
				return false
			}
		}
	}
	if access.HasMethodAccess(ctx, method, []string{"/programs"}) || access.HasMethodAccess(ctx, method, []string{"/data_file"}) {
		return true
	}
	return access.HasObjectMethodAccess(ctx, method, AccessResources(obj))
}

func bulkObjectMethodError(ctx context.Context, objs []drs.DrsObject, method string, publicRead map[string]bool) error {
	resources := make(map[string]struct{})
	var firstDeniedID string
	deniedRecords := 0
	for i := range objs {
		if hasObjectMethod(ctx, &objs[i], method, publicRead) {
			continue
		}
		deniedRecords++
		if firstDeniedID == "" {
			firstDeniedID = objs[i].Id
		}
		for _, resource := range AccessResources(&objs[i]) {
			if strings.TrimSpace(resource) == "" {
				continue
			}
			resources[resource] = struct{}{}
		}
	}
	if deniedRecords == 0 {
		return nil
	}

	resourceList := make([]string, 0, len(resources))
	for resource := range resources {
		resourceList = append(resourceList, resource)
	}
	sort.Strings(resourceList)

	truncated := 0
	if len(resourceList) > maxDeniedAccessResources {
		truncated = len(resourceList) - maxDeniedAccessResources
		resourceList = resourceList[:maxDeniedAccessResources]
	}

	return &access.AuthorizationError{
		Method:             method,
		RecordID:           firstDeniedID,
		Resources:          resourceList,
		DeniedRecords:      deniedRecords,
		TotalRecords:       len(objs),
		TruncatedResources: truncated,
	}
}
