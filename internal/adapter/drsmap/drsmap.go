package drsmap

import (
	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/db/core"
)

func ToExternal(obj core.InternalObject) drs.DrsObject {
	return obj.DrsObject
}

func ToExternalSlice(objects []core.InternalObject) []drs.DrsObject {
	out := make([]drs.DrsObject, 0, len(objects))
	for _, obj := range objects {
		out = append(out, obj.DrsObject)
	}
	return out
}

func ToExternalMap(objects map[string][]core.InternalObject) map[string][]drs.DrsObject {
	out := make(map[string][]drs.DrsObject, len(objects))
	for key, objs := range objects {
		out[key] = ToExternalSlice(objs)
	}
	return out
}

func WrapExternal(obj drs.DrsObject, authz []string) core.InternalObject {
	return core.InternalObject{
		DrsObject:      obj,
		Authorizations: append([]string(nil), authz...),
	}
}
