package plugin

import "testing"

func TestRPCMapDecodeRejectsMalformedOrNonObjectJSON(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{name: "malformed", data: []byte("{")},
		{name: "array", data: []byte("[]")},
		{name: "null", data: []byte("null")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := (&AuthenticationRPCInput{Metadata: tt.data}).ToPlugin(); err == nil {
				t.Fatal("ToPlugin accepted invalid metadata JSON")
			}
		})
	}
}
