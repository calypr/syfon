package coreapi

import (
	"context"
	"testing"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/db/core"
	"github.com/calypr/syfon/testutils"
)

func TestNormalizeSHA256(t *testing.T) {
	in := []string{"  ABC  ", "abc", "", "   ", "def"}
	out := NormalizeSHA256(in)
	if len(out) != 2 {
		t.Fatalf("expected 2 normalized values, got %d: %#v", len(out), out)
	}
	if out[0] != "abc" || out[1] != "def" {
		t.Fatalf("unexpected normalized output: %#v", out)
	}
}

func TestComputeSHA256Validity(t *testing.T) {
	db := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"sha-ok": {
				Id: "sha-ok",
				AccessMethods: &[]drs.AccessMethod{
					{Type: drs.AccessMethodTypeS3, AccessUrl: &struct {
						Headers *[]string `json:"headers,omitempty"`
						Url     string    `json:"url"`
					}{Url: "s3://bucket-a/key"}},
				},
			},
			"sha-bad": {
				Id: "sha-bad",
				AccessMethods: &[]drs.AccessMethod{
					{Type: drs.AccessMethodTypeS3, AccessUrl: &struct {
						Headers *[]string `json:"headers,omitempty"`
						Url     string    `json:"url"`
					}{Url: "s3://missing-bucket/key"}},
				},
			},
		},
		Credentials: map[string]core.S3Credential{
			"bucket-a": {Bucket: "bucket-a", Region: "us-east-1"},
		},
	}
	resp, err := ComputeSHA256Validity(context.Background(), db, []string{"sha-ok", "sha-bad", "sha-missing"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp["sha-ok"] {
		t.Fatalf("expected sha-ok valid")
	}
	if resp["sha-bad"] {
		t.Fatalf("expected sha-bad invalid")
	}
	if resp["sha-missing"] {
		t.Fatalf("expected sha-missing invalid")
	}
}
