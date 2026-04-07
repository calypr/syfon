package upload

import (
	"testing"

	"github.com/calypr/syfon/client/pkg/common"
)

func TestOptimalChunkSize(t *testing.T) {
	tests := []struct {
		name          string
		fileSize      int64
		wantChunkSize int64
		wantParts     int64
	}{
		{
			name:          "0 bytes",
			fileSize:      0,
			wantChunkSize: 1 * common.MB,
			wantParts:     0,
		},
		{
			name:          "1MB",
			fileSize:      1 * common.MB,
			wantChunkSize: 1 * common.MB,
			wantParts:     1,
		},
		{
			name:          "100MB",
			fileSize:      100 * common.MB,
			wantChunkSize: 100 * common.MB,
			wantParts:     1,
		},
		{
			name:          "100MB+1B",
			fileSize:      100*common.MB + 1,
			wantChunkSize: 10 * common.MB,
			wantParts:     11,
		},
		{
			name:          "500MB",
			fileSize:      500 * common.MB,
			wantChunkSize: 10 * common.MB,
			wantParts:     50,
		},
		{
			name:          "1GB",
			fileSize:      1 * common.GB,
			wantChunkSize: 10 * common.MB,
			wantParts:     103,
		},
		{
			name:          "1GB+1B",
			fileSize:      1*common.GB + 1,
			wantChunkSize: 25 * common.MB,
			wantParts:     41,
		},
		{
			name:          "5GB",
			fileSize:      5 * common.GB,
			wantChunkSize: 70 * common.MB,
			wantParts:     74,
		},
		{
			name:          "10GB",
			fileSize:      10 * common.GB,
			wantChunkSize: 128 * common.MB,
			wantParts:     80,
		},
		{
			name:          "10GB+1B",
			fileSize:      10*common.GB + 1,
			wantChunkSize: 256 * common.MB,
			wantParts:     41,
		},
		{
			name:          "50GB",
			fileSize:      50 * common.GB,
			wantChunkSize: 256 * common.MB,
			wantParts:     200,
		},
		{
			name:          "100GB",
			fileSize:      100 * common.GB,
			wantChunkSize: 256 * common.MB,
			wantParts:     400,
		},
		{
			name:          "100GB+1B",
			fileSize:      100*common.GB + 1,
			wantChunkSize: 512 * common.MB,
			wantParts:     201,
		},
		{
			name:          "500GB",
			fileSize:      500 * common.GB,
			wantChunkSize: 739 * common.MB,
			wantParts:     693,
		},
		{
			name:          "1TB",
			fileSize:      1 * common.TB,
			wantChunkSize: 1 * common.GB,
			wantParts:     1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunkSize := OptimalChunkSize(tt.fileSize)
			if chunkSize != tt.wantChunkSize {
				t.Fatalf("chunk size = %d, want %d", chunkSize, tt.wantChunkSize)
			}

			parts := int64(0)
			if tt.fileSize > 0 && chunkSize > 0 {
				parts = (tt.fileSize + chunkSize - 1) / chunkSize
			}
			if parts != tt.wantParts {
				t.Fatalf("parts = %d, want %d", parts, tt.wantParts)
			}
		})
	}
}
