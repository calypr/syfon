package drs

import (
	"reflect"
	"testing"

	generated "github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/objects"
)

func TestGeneratedRoundTripPreservesObjectValues(t *testing.T) {
	accessID := "read"
	available := true
	supported := []string{"bearer"}
	objectID := "record-1"
	generatedValue := generated.DrsObject{
		Id:        objectID,
		Aliases:   &[]string{"legacy"},
		Checksums: []generated.Checksum{{Type: "sha256", Checksum: "abc"}},
		Contents: &[]generated.ContentsObject{{
			Id:       &objectID,
			Name:     "bundle",
			Contents: &[]generated.ContentsObject{{Name: "child"}},
		}},
		AccessMethods: &[]generated.AccessMethod{{
			AccessId:  &accessID,
			Available: &available,
			Type:      generated.AccessMethodTypeS3,
			AccessUrl: &struct {
				Headers *[]string `json:"headers,omitempty"`
				Url     string    `json:"url"`
			}{Url: "s3://bucket/key"},
			Authorizations: &struct {
				BearerAuthIssuers   *[]string                                             `json:"bearer_auth_issuers,omitempty"`
				DrsObjectId         *string                                               `json:"drs_object_id,omitempty"`
				PassportAuthIssuers *[]string                                             `json:"passport_auth_issuers,omitempty"`
				SupportedTypes      *[]generated.AccessMethodAuthorizationsSupportedTypes `json:"supported_types,omitempty"`
			}{SupportedTypes: &[]generated.AccessMethodAuthorizationsSupportedTypes{generated.AccessMethodAuthorizationsSupportedTypes(supported[0])}},
		}},
	}
	domain := FromGenerated(generatedValue)
	if domain.Id != objects.RecordID(objectID) || domain.AccessMethods == nil || domain.Contents == nil {
		t.Fatalf("domain conversion lost fields: %+v", domain)
	}
	if (*domain.Contents)[0].Contents == nil || len(*(*domain.Contents)[0].Contents) != 1 {
		t.Fatalf("nested contents lost: %+v", domain.Contents)
	}
	if (*domain.AccessMethods)[0].Authorizations == nil || (*(*domain.AccessMethods)[0].Authorizations.SupportedTypes)[0] != "bearer" {
		t.Fatalf("access authorizations lost: %+v", domain.AccessMethods)
	}
	if !reflect.DeepEqual(FromGenerated(ToGenerated(domain)), domain) {
		t.Fatalf("round trip changed domain value: %#v", FromGenerated(ToGenerated(domain)))
	}
}

func TestGeneratedChecksumNilAndEmptySlicesRemainDistinct(t *testing.T) {
	nilValue := ToGenerated(objects.Record{})
	if nilValue.Checksums != nil {
		t.Fatalf("nil domain checksums became empty generated slice: %#v", nilValue.Checksums)
	}
	emptyValue := ToGenerated(objects.Record{Checksums: []objects.Checksum{}})
	if emptyValue.Checksums == nil || len(emptyValue.Checksums) != 0 {
		t.Fatalf("empty domain checksums changed: %#v", emptyValue.Checksums)
	}
	if FromGenerated(generated.DrsObject{}).Checksums != nil {
		t.Fatal("nil generated checksums became empty domain slice")
	}
	if got := FromGenerated(generated.DrsObject{Checksums: []generated.Checksum{}}).Checksums; got == nil || len(got) != 0 {
		t.Fatalf("empty generated checksums changed: %#v", got)
	}
}
