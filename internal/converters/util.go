package converters

import (
	"sort"

	"github.com/calypr/syfon/apigen/server/drs"
	syfoncommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/models"
)

func UniqueAuthz(values any) []string {
	switch v := values.(type) {
	case nil:
		return nil
	case []string:
		return common.UniqueStrings(v)
	case *[]string:
		if v == nil {
			return nil
		}
		return common.UniqueStrings(*v)
	case []drs.AccessMethod:
		out := make([]string, 0)
		for _, method := range v {
			if method.Authorizations == nil {
				continue
			}
			out = append(out, syfoncommon.AuthzMapToList(*method.Authorizations)...)
		}
		return common.UniqueStrings(out)
	case *[]drs.AccessMethod:
		if v == nil {
			return nil
		}
		return UniqueAuthz(*v)
	default:
		return nil
	}
}

func SortInternalObjects(objects []models.InternalObject) {
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Id < objects[j].Id
	})
}
