package hash

import (
	"encoding/json"
	"strings"
	"testing"

	drsapi "github.com/calypr/syfon/apigen/client/drs"
)

func TestChecksumTypeHelpers(t *testing.T) {
	t.Parallel()

	if !ChecksumTypeSHA256.IsValid() {
		t.Fatal("expected sha256 to be valid")
	}
	if ChecksumType("bogus").IsValid() {
		t.Fatal("expected bogus checksum type to be invalid")
	}
	if got := ChecksumTypeSHA256.String(); got != "sha256" {
		t.Fatalf("unexpected string form: %q", got)
	}
}

func TestHashInfoUnmarshalJSON(t *testing.T) {
	t.Parallel()

	t.Run("null", func(t *testing.T) {
		var got HashInfo
		if err := json.Unmarshal([]byte("null"), &got); err != nil {
			t.Fatalf("unmarshal null: %v", err)
		}
		if got != (HashInfo{}) {
			t.Fatalf("expected zero hash info, got %+v", got)
		}
	})

	t.Run("map payload", func(t *testing.T) {
		payload := []byte(`{"md5":"m","sha1":"s1","sha256":"s256","sha512":"s512","crc32c":"crc","etag":"etag","ignored":"skip"}`)
		var got HashInfo
		if err := json.Unmarshal(payload, &got); err != nil {
			t.Fatalf("unmarshal map payload: %v", err)
		}
		want := HashInfo{MD5: "m", SHA: "s1", SHA256: "s256", SHA512: "s512", CRC: "crc", ETag: "etag"}
		if got != want {
			t.Fatalf("unexpected hash info: got %+v want %+v", got, want)
		}
	})

	t.Run("checksum array payload", func(t *testing.T) {
		payload := []byte(`[{"type":"sha256","checksum":"abc"},{"type":"md5","checksum":"def"}]`)
		var got HashInfo
		if err := json.Unmarshal(payload, &got); err != nil {
			t.Fatalf("unmarshal array payload: %v", err)
		}
		want := HashInfo{MD5: "def", SHA256: "abc"}
		if got != want {
			t.Fatalf("unexpected hash info: got %+v want %+v", got, want)
		}
	})

	t.Run("unsupported payload", func(t *testing.T) {
		var got HashInfo
		err := json.Unmarshal([]byte(`123`), &got)
		if err == nil || !strings.Contains(err.Error(), "unsupported HashInfo payload") {
			t.Fatalf("expected unsupported payload error, got %v", err)
		}
	})
}

func TestHashConversions(t *testing.T) {
	t.Parallel()

	h := HashInfo{
		MD5:    "md5-value",
		SHA:    "sha1-value",
		SHA256: "sha256-value",
		SHA512: "sha512-value",
		CRC:    "crc-value",
		ETag:   "etag-value",
	}

	gotMap := ConvertHashInfoToMap(h)
	wantMap := map[string]string{
		"md5":    "md5-value",
		"sha":    "sha1-value",
		"sha256": "sha256-value",
		"sha512": "sha512-value",
		"crc":    "crc-value",
		"etag":   "etag-value",
	}
	if len(gotMap) != len(wantMap) {
		t.Fatalf("unexpected map size: got %d want %d", len(gotMap), len(wantMap))
	}
	for k, want := range wantMap {
		if got := gotMap[k]; got != want {
			t.Fatalf("unexpected map value for %s: got %q want %q", k, got, want)
		}
	}

	checksums := []Checksum{{Type: "sha256", Checksum: "abc"}, {Type: "md5", Checksum: "def"}}
	if got := ConvertChecksumsToMap(checksums); got["sha256"] != "abc" || got["md5"] != "def" {
		t.Fatalf("unexpected checksum map: %+v", got)
	}
	if got := ConvertChecksumsToHashInfo(checksums); got != (HashInfo{MD5: "def", SHA256: "abc"}) {
		t.Fatalf("unexpected checksum hash info: %+v", got)
	}

	drsChecksums := []drsapi.Checksum{{Type: "sha1", Checksum: "aaa"}, {Type: "etag", Checksum: "bbb"}}
	if got := ConvertDrsChecksumsToMap(drsChecksums); got["sha1"] != "aaa" || got["etag"] != "bbb" {
		t.Fatalf("unexpected drs checksum map: %+v", got)
	}
	if got := ConvertDrsChecksumsToHashInfo(drsChecksums); got != (HashInfo{SHA: "aaa", ETag: "bbb"}) {
		t.Fatalf("unexpected drs hash info: %+v", got)
	}

	roundTrip := ConvertMapToDrsChecksums(wantMap)
	if got := ConvertDrsChecksumsToMap(roundTrip); len(got) != len(wantMap) {
		t.Fatalf("unexpected round-trip map size: got %d want %d", len(got), len(wantMap))
	} else {
		for k, want := range wantMap {
			if got[k] != want {
				t.Fatalf("unexpected round-trip value for %s: got %q want %q", k, got[k], want)
			}
		}
	}
}

func TestNormalizeChecksumType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
		want ChecksumType
	}{
		{name: "sha256", raw: " sha256 ", want: ChecksumTypeSHA256},
		{name: "sha alias", raw: "SHA", want: ChecksumTypeSHA1},
		{name: "crc32c", raw: "CRC32C", want: ChecksumTypeCRC32C},
		{name: "unknown preserved", raw: " custom ", want: ChecksumType(" custom ")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := NormalizeChecksumType(tc.raw); got != tc.want {
				t.Fatalf("NormalizeChecksumType(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestValidateChecksum(t *testing.T) {
	t.Parallel()

	valid := []Checksum{
		{Type: "sha256", Checksum: strings.Repeat("a", 64)},
		{Type: "sha512", Checksum: strings.Repeat("b", 128)},
		{Type: "sha", Checksum: strings.Repeat("c", 40)},
		{Type: "md5", Checksum: strings.Repeat("d", 32)},
		{Type: "etag", Checksum: "quoted-etag"},
	}
	for _, checksum := range valid {
		checksum := checksum
		t.Run("valid_"+checksum.Type, func(t *testing.T) {
			if err := ValidateChecksum(checksum); err != nil {
				t.Fatalf("expected valid checksum, got %v", err)
			}
		})
	}

	invalid := []struct {
		name string
		c    Checksum
		msg  string
	}{
		{name: "empty", c: Checksum{Type: "sha256", Checksum: "   "}, msg: "checksum value is required"},
		{name: "unsupported type", c: Checksum{Type: "nope", Checksum: "abc"}, msg: "unsupported checksum type"},
		{name: "bad sha256", c: Checksum{Type: "sha256", Checksum: strings.Repeat("g", 64)}, msg: "invalid sha256 checksum"},
		{name: "short md5", c: Checksum{Type: "md5", Checksum: "abc"}, msg: "invalid md5 checksum"},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateChecksum(tc.c)
			if err == nil || !strings.Contains(err.Error(), tc.msg) {
				t.Fatalf("expected error containing %q, got %v", tc.msg, err)
			}
		})
	}
}

func TestIsHexDigest(t *testing.T) {
	t.Parallel()

	if !isHexDigest(strings.Repeat("a", 8), 8) {
		t.Fatal("expected valid hex digest")
	}
	if isHexDigest("zzzz", 4) {
		t.Fatal("expected invalid hex digest for non-hex characters")
	}
	if isHexDigest("abcd", 8) {
		t.Fatal("expected invalid hex digest for wrong length")
	}
}


