package core

import "github.com/calypr/syfon/internal/objects"

// ObjectAccessResources is retained as a compatibility helper for core
// adapters. Resource normalization belongs to the objects package.
func ObjectAccessResources(obj *objects.Record) []string {
	return objects.AccessResources(obj)
}
