package drs

import "testing"

func TestDrsObjectToSyfonInternalRecordCopiesNameToFileName(t *testing.T) {
	name := "sample.bin"
	obj := &DRSObject{
		Id:   "did-123",
		Name: &name,
	}

	rec, err := drsObjectToSyfonInternalRecord(obj)
	if err != nil {
		t.Fatalf("conversion failed: %v", err)
	}
	if rec == nil {
		t.Fatalf("expected record, got nil")
	}
	if rec.FileName == nil || *rec.FileName != name {
		t.Fatalf("expected file_name %q, got %+v", name, rec.FileName)
	}
}

func TestDrsObjectToSyfonInternalRecordSkipsNilAccessURL(t *testing.T) {
	obj := &DRSObject{
		Id: "did-456",
		AccessMethods: &[]AccessMethod{
			{Type: "s3"},
		},
	}

	rec, err := drsObjectToSyfonInternalRecord(obj)
	if err != nil {
		t.Fatalf("conversion failed: %v", err)
	}
	if rec == nil {
		t.Fatalf("expected record, got nil")
	}
	if rec.Urls != nil && len(*rec.Urls) != 0 {
		t.Fatalf("expected nil or empty urls, got %+v", rec.Urls)
	}
}
