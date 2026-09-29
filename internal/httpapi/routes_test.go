package httpapi

import "testing"

func TestDecodeStrictJSONAllowsOnlyWhitespaceAfterValue(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{name: "single value", body: `{"bucket":"bucket2"}`},
		{name: "trailing whitespace", body: "{\"bucket\":\"bucket2\"} \r\n\t"},
		{name: "second value", body: `{"bucket":"bucket2"} {}`, wantErr: true},
		{name: "malformed trailing bytes", body: `{"bucket":"bucket2"}garbage`, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var dst map[string]string
			err := decodeStrictJSON([]byte(test.body), &dst)
			if (err != nil) != test.wantErr {
				t.Fatalf("decodeStrictJSON error = %v, want error %t", err, test.wantErr)
			}
		})
	}
}
