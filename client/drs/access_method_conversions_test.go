package drs

import "testing"

func TestDRSAccessMethodsFromInternalURLs_GCSAndAzure(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		wantTyp string
	}{
		{name: "gcs scheme", url: "gs://gcs-bucket/path/to/object.bin", wantTyp: "gs"},
		{name: "azure scheme", url: "azblob://az-container/path/to/object.bin", wantTyp: "azblob"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			methods, err := DRSAccessMethodsFromInternalURLs([]string{tc.url}, []string{"/programs/p1/projects/proj1"})
			if err != nil {
				t.Fatalf("DRSAccessMethodsFromInternalURLs returned error: %v", err)
			}
			if len(methods) != 1 {
				t.Fatalf("expected 1 access method, got %d", len(methods))
			}
			if got := methods[0].Type; got != tc.wantTyp {
				t.Fatalf("unexpected access method type: got %q want %q", got, tc.wantTyp)
			}
			if got := methods[0].AccessUrl.Url; got != tc.url {
				t.Fatalf("unexpected access URL: got %q want %q", got, tc.url)
			}
			if got := methods[0].Authorizations.BearerAuthIssuers; len(got) != 1 || got[0] != "/programs/p1/projects/proj1" {
				t.Fatalf("unexpected authorizations: %+v", got)
			}
		})
	}
}
