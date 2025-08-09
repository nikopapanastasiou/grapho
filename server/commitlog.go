package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type CommitLog struct {
	path    string
	file    *os.File
	w       *bufio.Writer
	mu      sync.Mutex
	queue   chan string
	closed  chan struct{}
	started bool
	done    chan struct{}
	format  LogFormat
}

// LogFormat controls how entries are encoded on disk
type LogFormat int

const (
	LogFormatText LogFormat = iota
	LogFormatBinary
)

// OpenCommitLog opens or creates an append-only commit log at dataDir/commit.log using text format
func OpenCommitLog(dataDir string) (*CommitLog, error) {
	return OpenCommitLogWithFormat(dataDir, LogFormatText)
}

// OpenCommitLogWithFormat opens or creates a commit log with the specified format
func OpenCommitLogWithFormat(dataDir string, format LogFormat) (*CommitLog, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir data dir: %w", err)
	}
	p := filepath.Join(dataDir, "commit.log")
	f, err := os.OpenFile(p, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open commit log: %w", err)
	}
	cl := &CommitLog{
		path:   p,
		file:   f,
		w:      bufio.NewWriterSize(f, 64<<10),
		queue:  make(chan string, 1024),
		closed: make(chan struct{}),
		done:   make(chan struct{}),
		format: format,
	}
	return cl, nil
}

// Start begins the background writer goroutine
func (cl *CommitLog) Start() {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	if cl.started {
		return
	}
	cl.started = true
	go cl.run()
}

// Stop flushes and closes the log
func (cl *CommitLog) Stop() error {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	if !cl.started {
		return nil
	}
	close(cl.closed)
	// wait for run() to finish draining
	<-cl.done
	if err := cl.w.Flush(); err != nil {
		return err
	}
	if err := cl.file.Sync(); err != nil {
		return err
	}
	return cl.file.Close()
}

func (cl *CommitLog) run() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-cl.closed:
			// Drain remaining queued entries before exiting
			for {
				select {
				case line := <-cl.queue:
					cl.writeEntry(line)
				default:
					_ = cl.w.Flush()
					_ = cl.file.Sync()
					close(cl.done)
					return
				}
			}
		case line := <-cl.queue:
			// each line is a full command; write with newline
			cl.writeEntry(line)
		case <-ticker.C:
			_ = cl.w.Flush()
			_ = cl.file.Sync()
		}
	}
}

// writeEntry encodes a single command according to the configured format
func (cl *CommitLog) writeEntry(line string) {
	switch cl.format {
	case LogFormatBinary:
		// Binary encoding: 4-byte big-endian length, followed by bytes
		b := []byte(line)
		var hdr [4]byte
		n := len(b)
		hdr[0] = byte(n >> 24)
		hdr[1] = byte(n >> 16)
		hdr[2] = byte(n >> 8)
		hdr[3] = byte(n)
		_, _ = cl.w.Write(hdr[:])
		_, _ = cl.w.Write(b)
	default:
		// Text format: one command per line
		_, _ = cl.w.WriteString(line)
		if len(line) == 0 || line[len(line)-1] != '\n' {
			_ = cl.w.WriteByte('\n')
		}
	}
}

// Append enqueues a command to be written. Ordering is preserved by the single writer.
func (cl *CommitLog) Append(command string) error {
	if command == "" {
		return errors.New("empty command")
	}
	select {
	case cl.queue <- command:
		return nil
	default:
		// queue is full; do a synchronous write to avoid losing entries
		cl.mu.Lock()
		defer cl.mu.Unlock()
		cl.writeEntry(command)
		return cl.w.Flush()
	}
}

// Replay reads the log from the beginning and invokes apply for each line.
// apply should execute the command without re-appending to the log.
func (cl *CommitLog) Replay(apply func(line string) error) error {
	f, err := os.Open(cl.path)
	if err != nil {
		return fmt.Errorf("open for replay: %w", err)
	}
	defer f.Close()
	switch cl.format {
	case LogFormatBinary:
		r := bufio.NewReader(f)
		for {
			var hdr [4]byte
			if _, err := io.ReadFull(r, hdr[:]); err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					return nil
				}
				return fmt.Errorf("replay read header: %w", err)
			}
			n := int(hdr[0])<<24 | int(hdr[1])<<16 | int(hdr[2])<<8 | int(hdr[3])
			if n < 0 || n > 10<<20 { // 10MB guard
				return fmt.Errorf("invalid record length: %d", n)
			}
			buf := make([]byte, n)
			if _, err := io.ReadFull(r, buf); err != nil {
				return fmt.Errorf("replay read body: %w", err)
			}
			line := strings.TrimSpace(string(buf))
			if line == "" {
				continue
			}
			if err := apply(line); err != nil {
				return fmt.Errorf("replay apply failed: %w", err)
			}
		}
	default:
		s := bufio.NewScanner(f)
		s.Buffer(make([]byte, 0, 64<<10), 10<<20) // allow reasonably long commands
		for s.Scan() {
			line := s.Text()
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			if err := apply(line); err != nil {
				return fmt.Errorf("replay apply failed: %w", err)
			}
		}
		return s.Err()
	}
}
