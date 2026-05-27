package transferprogress

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	clientcommon "github.com/calypr/syfon/client/common"
)

var spinnerFrames = []string{"|", "/", "-", `\`}

const minRenderInterval = 200 * time.Millisecond

type Renderer struct {
	out          io.Writer
	label        string
	total        int64
	current      int64
	completed    bool
	spinnerIndex int
	startedAt    time.Time
	lastAt       time.Time
	lastBytes    int64
	speedBps     float64
	lastRenderAt time.Time
	mu           sync.Mutex
}

func New(out io.Writer, label string, total int64) *Renderer {
	return &Renderer{
		out:   out,
		label: trimLabel(strings.TrimSpace(label), 24),
		total: total,
	}
}

func (r *Renderer) ProgressCallback() clientcommon.ProgressCallback {
	return func(evt clientcommon.ProgressEvent) error {
		if evt.Event != "progress" {
			return nil
		}
		r.SetCurrent(evt.BytesSoFar)
		return nil
	}
}

func (r *Renderer) Start() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.renderLocked(true)
}

func (r *Renderer) SetCurrent(current int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	if r.startedAt.IsZero() {
		r.startedAt = now
		r.lastAt = now
		r.lastBytes = r.current
	}
	if current > r.current {
		r.current = current
	}
	if !r.lastAt.IsZero() && current >= r.lastBytes {
		dt := now.Sub(r.lastAt).Seconds()
		if dt > 0 {
			delta := float64(current - r.lastBytes)
			inst := delta / dt
			if r.speedBps == 0 {
				r.speedBps = inst
			} else {
				r.speedBps = (r.speedBps * 0.7) + (inst * 0.3)
			}
		}
	}
	r.lastAt = now
	r.lastBytes = current
	r.renderLocked(false)
}

func (r *Renderer) Finish() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.completed = true
	if r.total > 0 && r.current < r.total {
		r.current = r.total
	}
	r.renderLocked(true)
	_, _ = fmt.Fprintln(r.out)
}

func (r *Renderer) Abort() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.renderLocked(true)
	_, _ = fmt.Fprintln(r.out)
}

func (r *Renderer) renderLocked(force bool) {
	if r.total <= 0 {
		return
	}
	now := time.Now()
	if !force && !r.lastRenderAt.IsZero() && now.Sub(r.lastRenderAt) < minRenderInterval {
		return
	}
	r.lastRenderAt = now
	r.spinnerIndex = (r.spinnerIndex + 1) % len(spinnerFrames)

	line := r.renderLineLocked()
	_, _ = fmt.Fprintf(r.out, "\r\x1b[2K%s", line)
}

func (r *Renderer) renderLineLocked() string {
	current := visibleProgressBytes(r.current, r.total, r.completed)
	spinner := ""
	if !r.completed && current < r.total {
		spinner = spinnerFrames[r.spinnerIndex] + " "
	}
	speed := renderSpeed(r.speedBps)
	return fmt.Sprintf(
		"%s%s %s / %s %s %s %s",
		spinner,
		r.label,
		formatBinaryBytes(current),
		formatBinaryBytes(r.total),
		renderProgressBar(current, r.total, 32),
		renderPercent(current, r.total, r.completed),
		speed,
	)
}

func WithProgress(ctx context.Context, did string, r *Renderer) context.Context {
	ctx = clientcommon.WithOid(ctx, did)
	return clientcommon.WithProgress(ctx, r.ProgressCallback())
}

func trimLabel(s string, max int) string {
	if max <= 3 || len(s) <= max {
		return s
	}
	return "..." + s[len(s)-(max-3):]
}

func renderProgressBar(current, total int64, width int) string {
	if width <= 0 {
		return "[]"
	}
	if total <= 0 {
		return "[" + strings.Repeat(" ", width) + "]"
	}
	if current < 0 {
		current = 0
	}
	if current > total {
		current = total
	}
	filled := int((current * int64(width)) / total)
	if filled > width {
		filled = width
	}
	return "[" + strings.Repeat("=", filled) + strings.Repeat(" ", width-filled) + "]"
}

func renderPercent(current, total int64, completed bool) string {
	if total <= 0 {
		return "0 %"
	}
	if current < 0 {
		current = 0
	}
	if current > total {
		current = total
	}
	value := (float64(current) * 100.0) / float64(total)
	if !completed && current < total && value > 99.9 {
		value = 99.9
	}
	return fmt.Sprintf("%.0f %%", value)
}

func formatBinaryBytes(n int64) string {
	if n <= 0 {
		return "0 B"
	}
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for value := n / unit; value >= unit && exp < 5; value /= unit {
		div *= unit
		exp++
	}
	suffix := []string{"KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}[exp]
	return fmt.Sprintf("%.2f %s", float64(n)/float64(div), suffix)
}

func renderSpeed(bps float64) string {
	if bps <= 0 {
		return ""
	}
	return fmt.Sprintf("%s/s", formatBinaryBytes(int64(bps)))
}

func visibleProgressBytes(current, total int64, completed bool) int64 {
	if current < 0 {
		current = 0
	}
	if total <= 0 {
		return current
	}
	if current > total {
		current = total
	}
	if !completed && current >= total {
		return total - 1
	}
	return current
}
