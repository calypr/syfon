package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/calypr/syfon/client/transfer"
)

func testDownloadIdentity(data []byte) string {
	return fmt.Sprintf("sha256:%x", sha256.Sum256(data))
}

type downloadIdentitySource struct {
	data         []byte
	identity     string
	streams      int
	ranges       int
	reportedSize *int64
	sizeKnown    bool
}

func (s *downloadIdentitySource) Logger() transfer.TransferLogger { return transfer.NoOpLogger{} }

func (s *downloadIdentitySource) Stat(context.Context, string) (*transfer.ObjectMetadata, error) {
	size := int64(len(s.data))
	if s.reportedSize != nil {
		size = *s.reportedSize
	}
	return &transfer.ObjectMetadata{Size: size, SizeKnown: s.sizeKnown, AcceptRanges: true, Identity: s.identity}, nil
}

func (s *downloadIdentitySource) GetReader(context.Context, string) (io.ReadCloser, error) {
	s.streams++
	return io.NopCloser(bytes.NewReader(s.data)), nil
}

func (s *downloadIdentitySource) GetRangeReader(_ context.Context, _ string, start, length int64) (io.ReadCloser, error) {
	s.ranges++
	return io.NopCloser(bytes.NewReader(s.data[start : start+length])), nil
}

func TestDownloadDoesNotTrustUnverifiedDestinationBytes(t *testing.T) {
	for _, prior := range []string{"old-data", "old"} {
		t.Run(prior, func(t *testing.T) {
			destination := filepath.Join(t.TempDir(), "output.bin")
			if err := os.WriteFile(destination, []byte(prior), 0o600); err != nil {
				t.Fatal(err)
			}
			data := []byte("new-data")
			source := &downloadIdentitySource{data: data, identity: testDownloadIdentity(data)}
			if err := Download(context.Background(), source, "new-object", destination, DownloadOptions{MultipartThreshold: 1024 * 1024}); err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(destination)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != "new-data" {
				t.Fatalf("download returned bytes %q, want new-data; full reads=%d, range reads=%d", got, source.streams, source.ranges)
			}
		})
	}
}

func TestDownloadReusesOnlyMatchingCompletedIdentity(t *testing.T) {
	destination := filepath.Join(t.TempDir(), "output.bin")
	firstData := []byte("first")
	firstIdentity := testDownloadIdentity(firstData)
	first := &downloadIdentitySource{data: firstData, identity: firstIdentity}
	if err := Download(context.Background(), first, "alias-one", destination, DownloadOptions{MultipartThreshold: 1024 * 1024}); err != nil {
		t.Fatal(err)
	}
	matching := &downloadIdentitySource{data: firstData, identity: firstIdentity}
	if err := Download(context.Background(), matching, "another-alias", destination, DownloadOptions{MultipartThreshold: 1024 * 1024}); err != nil {
		t.Fatal(err)
	}
	if matching.streams != 0 || matching.ranges != 0 {
		t.Fatalf("matching completed identity was downloaded again: full=%d range=%d", matching.streams, matching.ranges)
	}
	replacementData := []byte("newer")
	replacement := &downloadIdentitySource{data: replacementData, identity: testDownloadIdentity(replacementData)}
	if err := Download(context.Background(), replacement, "alias-one", destination, DownloadOptions{MultipartThreshold: 1024 * 1024}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "newer" || replacement.streams != 1 {
		t.Fatalf("replacement download = %q, full reads=%d", got, replacement.streams)
	}
}

func TestDownloadDoesNotTrustTamperedCompletedDestination(t *testing.T) {
	destination := filepath.Join(t.TempDir(), "output.bin")
	data := []byte("first")
	identity := testDownloadIdentity(data)
	first := &downloadIdentitySource{data: data, identity: identity}
	if err := Download(context.Background(), first, "object", destination, DownloadOptions{MultipartThreshold: 1024 * 1024}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, []byte("other"), 0o600); err != nil {
		t.Fatal(err)
	}
	repair := &downloadIdentitySource{data: data, identity: identity}
	if err := Download(context.Background(), repair, "object", destination, DownloadOptions{MultipartThreshold: 1024 * 1024}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) || repair.streams != 1 {
		t.Fatalf("repaired download = %q, full reads=%d", got, repair.streams)
	}
}

func TestDownloadEphemeralDestinationRemovesResumeState(t *testing.T) {
	destination := filepath.Join(t.TempDir(), "output.bin")
	data := []byte("temporary")
	source := &downloadIdentitySource{data: data, identity: testDownloadIdentity(data)}
	if err := Download(context.Background(), source, "object", destination, DownloadOptions{MultipartThreshold: 1024 * 1024, EphemeralDestination: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(downloadResumeStatePath(destination)); !os.IsNotExist(err) {
		t.Fatalf("ephemeral download checkpoint stat error = %v, want not found", err)
	}
}

func TestDownloadValidatesZeroAndUnknownSizes(t *testing.T) {
	t.Run("known zero rejects a nonempty body", func(t *testing.T) {
		destination := filepath.Join(t.TempDir(), "output.bin")
		zero := int64(0)
		source := &downloadIdentitySource{
			data:         []byte("unexpected"),
			reportedSize: &zero,
			sizeKnown:    true,
		}
		if err := Download(context.Background(), source, "empty-object", destination, DownloadOptions{MultipartThreshold: 1024 * 1024}); err == nil {
			t.Fatal("download accepted a nonempty body for a known empty object")
		}
		data, err := os.ReadFile(destination)
		if err != nil {
			t.Fatal(err)
		}
		if len(data) != 0 {
			t.Fatalf("known-empty download wrote %d bytes", len(data))
		}
	})

	t.Run("unknown size still verifies a supplied checksum", func(t *testing.T) {
		destination := filepath.Join(t.TempDir(), "output.bin")
		zero := int64(0)
		source := &downloadIdentitySource{
			data:         []byte("unexpected"),
			identity:     testDownloadIdentity([]byte("expected")),
			reportedSize: &zero,
		}
		if err := Download(context.Background(), source, "unknown-size-object", destination, DownloadOptions{MultipartThreshold: 1024 * 1024}); err == nil || !strings.Contains(err.Error(), "checksum") {
			t.Fatalf("download error = %v, want checksum mismatch", err)
		}
	})

	t.Run("empty known object with its checksum succeeds", func(t *testing.T) {
		destination := filepath.Join(t.TempDir(), "output.bin")
		zero := int64(0)
		source := &downloadIdentitySource{
			identity:     testDownloadIdentity(nil),
			reportedSize: &zero,
			sizeKnown:    true,
		}
		if err := Download(context.Background(), source, "empty-object", destination, DownloadOptions{MultipartThreshold: 1024 * 1024}); err != nil {
			t.Fatalf("Download returned error for empty object: %v", err)
		}
		data, err := os.ReadFile(destination)
		if err != nil || len(data) != 0 {
			t.Fatalf("downloaded empty object data=%q err=%v", data, err)
		}
	})

	t.Run("known zero size still verifies a supplied checksum", func(t *testing.T) {
		destination := filepath.Join(t.TempDir(), "output.bin")
		zero := int64(0)
		source := &downloadIdentitySource{
			identity:     testDownloadIdentity([]byte("expected")),
			reportedSize: &zero,
			sizeKnown:    true,
		}
		if err := Download(context.Background(), source, "empty-object", destination, DownloadOptions{MultipartThreshold: 1024 * 1024}); err == nil || !strings.Contains(err.Error(), "checksum") {
			t.Fatalf("download error = %v, want checksum mismatch", err)
		}
	})
}
