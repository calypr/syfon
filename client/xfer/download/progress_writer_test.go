package download

import (
	"bytes"
	"io"
	"testing"

	"github.com/calypr/syfon/client/common"
)

func TestProgressWriterFinalizes(t *testing.T) {
	payload := bytes.Repeat([]byte("b"), 20)
	var events []common.ProgressEvent

	writer := newProgressWriter(io.Discard, func(event common.ProgressEvent) error {
		events = append(events, event)
		return nil
	}, "oid-456", int64(len(payload)))

	if _, err := writer.Write(payload); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if err := writer.Finalize(); err != nil {
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
