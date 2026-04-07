package transfer

import (
	"bytes"
	"io"
	"testing"

	"github.com/calypr/syfon/client/pkg/common"
)

func TestProgressReaderFinalizes(t *testing.T) {
	payload := bytes.Repeat([]byte("a"), 16)
	var events []common.ProgressEvent

	reader := newProgressReader(bytes.NewReader(payload), func(event common.ProgressEvent) error {
		events = append(events, event)
		return nil
	}, "oid-123", int64(len(payload)))

	if _, err := io.Copy(io.Discard, reader); err != nil {
		t.Fatalf("copy failed: %v", err)
	}
	if err := reader.Finalize(); err != nil {
		t.Fatalf("finalize failed: %v", err)
	}

	if len(events) == 0 {
		t.Fatal("expected progress events, got none")
	}

	var total int64
	for _, event := range events {
		if event.Event != "progress" {
			t.Fatalf("unexpected event type: %s", event.Event)
		}
		total += event.BytesSinceLast
	}

	last := events[len(events)-1]
	if last.BytesSoFar != int64(len(payload)) {
		t.Fatalf("expected final bytesSoFar %d, got %d", len(payload), last.BytesSoFar)
	}
	if total != int64(len(payload)) {
		t.Fatalf("expected bytesSinceLast sum %d, got %d", len(payload), total)
	}
}
