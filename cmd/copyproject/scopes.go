package copyproject

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/calypr/syfon/apigen/bucketapi"
	"github.com/calypr/syfon/client/services"
	"github.com/calypr/syfon/cmd/projectcopy"
	"github.com/calypr/syfon/internal/storage/address"
	"github.com/spf13/cobra"
)

type resolvedCopyScopes struct {
	source projectcopy.Scope
	target projectcopy.Scope

	sourceBucket   string
	targetBucket   string
	targetProvider string

	sourceProject *bucketapi.BucketScopeResponse
	sourceOrg     *bucketapi.BucketScopeResponse
	targetProject *bucketapi.BucketScopeResponse
	targetOrg     *bucketapi.BucketScopeResponse
}

type destinationScopeCandidate struct {
	bucket  string
	org     *bucketapi.BucketScopeResponse
	project *bucketapi.BucketScopeResponse
}

func resolveCopyScopes(ctx context.Context, sourceBuckets, targetBuckets *services.BucketsService, sourceBucketMap, targetBucketMap map[string]bucketapi.BucketMetadata, srcScope, dstScope projectcopy.Scope) (*resolvedCopyScopes, error) {
	resolved := &resolvedCopyScopes{
		source: srcScope,
		target: dstScope,
	}

	for bucketName := range sourceBucketMap {
		scopes, err := sourceBuckets.ListScopes(ctx, bucketName)
		if err != nil {
			return nil, fmt.Errorf("failed to list source scopes for bucket %q: %w", bucketName, err)
		}
		for _, scope := range scopes {
			switch {
			case scope.Organization == srcScope.Organization && scope.ProjectId == srcScope.Project:
				scopeCopy := scope
				resolved.sourceProject = &scopeCopy
				resolved.sourceBucket = bucketName
			case scope.Organization == srcScope.Organization && scope.ProjectId == "":
				scopeCopy := scope
				resolved.sourceOrg = &scopeCopy
			}
		}
	}

	targetBucketNames := make([]string, 0, len(targetBucketMap))
	for bucketName := range targetBucketMap {
		targetBucketNames = append(targetBucketNames, bucketName)
	}
	sort.Strings(targetBucketNames)
	targetCandidates := make([]destinationScopeCandidate, 0, len(targetBucketNames))
	for _, bucketName := range targetBucketNames {
		scopes, err := targetBuckets.ListScopes(ctx, bucketName)
		if err != nil {
			return nil, fmt.Errorf("failed to list destination scopes for bucket %q: %w", bucketName, err)
		}
		candidate := destinationScopeCandidate{bucket: bucketName}
		for _, scope := range scopes {
			switch {
			case scope.Organization == dstScope.Organization && scope.ProjectId == dstScope.Project:
				scopeCopy := scope
				candidate.project = &scopeCopy
			case scope.Organization == dstScope.Organization && scope.ProjectId == "":
				scopeCopy := scope
				candidate.org = &scopeCopy
			}
		}
		targetCandidates = append(targetCandidates, candidate)
	}
	for _, candidate := range targetCandidates {
		if candidate.project == nil {
			continue
		}
		resolved.targetBucket = candidate.bucket
		resolved.targetOrg = candidate.org
		resolved.targetProject = candidate.project
		break
	}
	if resolved.targetBucket == "" {
		for _, candidate := range targetCandidates {
			if candidate.org == nil {
				continue
			}
			resolved.targetBucket = candidate.bucket
			resolved.targetOrg = candidate.org
			break
		}
	}

	if resolved.sourceProject == nil {
		return resolved, nil
	}
	if resolved.sourceBucket == "" {
		return nil, fmt.Errorf("source project scope %s/%s exists but has no bucket mapping", srcScope.Organization, srcScope.Project)
	}
	if resolved.targetBucket == "" {
		if _, ok := targetBucketMap[resolved.sourceBucket]; ok {
			resolved.targetBucket = resolved.sourceBucket
		} else {
			return nil, fmt.Errorf("destination scope %s/%s has no bucket mapping on the destination instance, and source bucket %q is not configured there", dstScope.Organization, dstScope.Project, resolved.sourceBucket)
		}
	}

	if metadata, ok := targetBucketMap[resolved.targetBucket]; ok && metadata.Provider != nil {
		targetProvider, err := address.ParseBucketProvider(*metadata.Provider)
		if err != nil {
			return nil, fmt.Errorf("destination bucket %q has unsupported provider: %w", resolved.targetBucket, err)
		}
		resolved.targetProvider = targetProvider
	} else {
		resolved.targetProvider = address.S3Provider
	}
	return resolved, nil
}

func ensureDestinationScopes(ctx context.Context, cmd *cobra.Command, buckets *services.BucketsService, resolved *resolvedCopyScopes) error {
	if resolved.targetBucket == "" {
		return fmt.Errorf("failed to resolve a destination bucket for %s/%s", resolved.target.Organization, resolved.target.Project)
	}

	orgPath := ""
	if resolved.targetOrg == nil {
		orgPath = defaultOrgScopePath(resolved.targetBucket, resolved.target.Organization)
		if remapped, ok := remapOrgScopePath(resolved.sourceOrg, resolved.source.Organization, resolved.targetBucket, resolved.target.Organization); ok {
			orgPath = remapped
		}
	} else if resolved.targetOrg.Path != nil {
		orgPath = strings.TrimRight(strings.TrimSpace(*resolved.targetOrg.Path), "/")
	}

	projectPath := ""
	if resolved.targetProject == nil {
		projectPath = defaultProjectScopePath(resolved.targetBucket, resolved.target.Organization, resolved.target.Project)
		if orgPath != "" {
			projectPath = orgPath + "/" + resolved.target.Project
		}
		sourcePath := pathOrEmpty(resolved.sourceProject)
		targetOrg := resolved.targetOrg
		if targetOrg == nil {
			targetOrg = &bucketapi.BucketScopeResponse{
				Organization: resolved.target.Organization,
				Path:         &orgPath,
			}
		}
		if remapped, ok := remapProjectScopePath(resolved.sourceProject, resolved.sourceOrg, targetOrg, resolved.source, resolved.target, resolved.targetBucket); ok {
			projectPath = remapped
		} else if sourcePath != "" && !isBucketRootStoragePath(sourcePath) {
			return fmt.Errorf("cannot translate source project scope path %q to destination %s/%s", sourcePath, resolved.target.Organization, resolved.target.Project)
		}
	}

	if resolved.targetOrg == nil {
		fmt.Fprintf(cmd.OutOrStdout(), "Creating organization scope mapping on bucket %s: %s -> %s\n", resolved.targetBucket, resolved.target.Organization, orgPath)
		if err := buckets.AddScope(ctx, resolved.targetBucket, bucketapi.AddBucketScopeRequest{
			Organization: resolved.target.Organization,
			Path:         &orgPath,
		}); err != nil {
			return fmt.Errorf("failed to map organization scope on target bucket: %w", err)
		}

		resolved.targetOrg = &bucketapi.BucketScopeResponse{
			Organization: resolved.target.Organization,
			Path:         &orgPath,
		}
	}

	if resolved.targetProject != nil {
		return nil
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Creating project scope mapping on bucket %s: %s/%s -> %s\n", resolved.targetBucket, resolved.target.Organization, resolved.target.Project, projectPath)
	if err := buckets.AddScope(ctx, resolved.targetBucket, bucketapi.AddBucketScopeRequest{
		Organization: resolved.target.Organization,
		ProjectId:    resolved.target.Project,
		Path:         &projectPath,
	}); err != nil {
		return fmt.Errorf("failed to map project scope on target bucket: %w", err)
	}

	resolved.targetProject = &bucketapi.BucketScopeResponse{
		Organization: resolved.target.Organization,
		ProjectId:    resolved.target.Project,
		Path:         &projectPath,
	}
	return nil
}
