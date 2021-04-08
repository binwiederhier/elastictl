package util

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

var spinner = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

type ProgressBar struct {
	started     time.Time
	writer io.Writer
	count       int
	size        int64
	rendered    time.Time
	rendercount int64
	prevlen     int
	mu          sync.Mutex
}

func NewProgressBar(writer io.Writer) *ProgressBar {
	return &ProgressBar{
		started: time.Now(),
		writer: writer,
	}
}

func (p *ProgressBar) Add(size int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.count++
	p.size += size
	if time.Since(p.rendered) > 65*time.Millisecond {
		p.render(false)
	}
}

func (p *ProgressBar) Done() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.render(true)
}

func (p *ProgressBar) render(done bool) {
	spin := spinner[p.rendercount%int64(len(spinner))]
	count := p.count
	countPerSec := float64(p.count) / time.Since(p.started).Seconds()
	size := bytesToHuman(p.size)
	sizePerSec := bytesToHuman(int64(float64(p.size) / time.Since(p.started).Seconds()))
	if done {
		line := fmt.Sprintf("\rindexing finished: %d docs (%.2f docs/s), %s (%s/s)", count, countPerSec, size, sizePerSec)
		fmt.Fprint(p.writer, line)
		if p.prevlen > len(line) {
			fmt.Fprint(p.writer, strings.Repeat(" ", p.prevlen-len(line)))
		}
		fmt.Fprintln(p.writer)
		p.prevlen = len(line)
	} else {
		line := fmt.Sprintf("\r%s indexing: %d docs (%.2f docs/s), %s (%s/s)", spin, count, countPerSec, size, sizePerSec)
		fmt.Fprint(p.writer, line)
		if p.prevlen > len(line) {
			fmt.Fprint(p.writer, strings.Repeat(" ", p.prevlen-len(line)))
		}
		p.prevlen = len(line)
	}
	p.rendered = time.Now()
	p.rendercount++
}

func bytesToHuman(b int64) string {
	// From: https://yourbasic.org/golang/formatting-byte-size-to-human-readable-format/
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB",
		float64(b)/float64(div), "kMGTPE"[exp])
}
