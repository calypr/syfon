package core

import (
	syfoncommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/objects"
)

func ObjectAccessResources(obj *objects.Record) []string {
	if obj == nil {
		return nil
	}
	if obj.ControlledAccess != nil {
		return syfoncommon.NormalizeAccessResources(*obj.ControlledAccess)
	}
	return syfoncommon.AuthzMapToList(obj.Authorizations)
}
