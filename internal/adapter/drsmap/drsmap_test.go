package drsmap

import (
	"testing"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/internal/db/core"
)

func TestToExternalSliceAndMapAndWrap(t *testing.T) {
	obj1 := core.InternalObject{DrsObject: drs.DrsObject{Id: "id-1"}, Authorizations: []string{"/a"}}
	obj2 := core.InternalObject{DrsObject: drs.DrsObject{Id: "id-2"}, Authorizations: []string{"/b"}}

	single := ToExternal(obj1)
	if single.Id != "id-1" {
		t.Fatalf("unexpected external object: %#v", single)
	}

	slice := ToExternalSlice([]core.InternalObject{obj1, obj2})
	if len(slice) != 2 || slice[0].Id != "id-1" || slice[1].Id != "id-2" {
		t.Fatalf("unexpected external slice: %#v", slice)
	}

	m := ToExternalMap(map[string][]core.InternalObject{"k": {obj1, obj2}})
	if len(m["k"]) != 2 || m["k"][0].Id != "id-1" {
		t.Fatalf("unexpected external map: %#v", m)
	}

	wrapped := WrapExternal(drs.DrsObject{Id: "id-3"}, []string{"/x", "/y"})
	if wrapped.Id != "id-3" {
		t.Fatalf("unexpected wrapped id: %#v", wrapped)
	}
	if len(wrapped.Authorizations) != 2 {
		t.Fatalf("unexpected wrapped authz: %#v", wrapped.Authorizations)
	}

	orig := []string{"/q"}
	wrapped2 := WrapExternal(drs.DrsObject{Id: "id-4"}, orig)
	orig[0] = "/mutated"
	if wrapped2.Authorizations[0] != "/q" {
		t.Fatalf("expected authz copy isolation, got %#v", wrapped2.Authorizations)
	}
}
