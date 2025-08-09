package server

import (
	"bufio"
	"errors"
	"fmt"
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
}

// OpenCommitLog opens or creates an append-only commit log at dataDir/commit.log
func OpenCommitLog(dataDir string) (*CommitLog, error) {
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
	// allow run() to exit and then close
	// best-effort small wait
	t := time.NewTimer(500 * time.Millisecond)
	<-t.C
	if err := cl.w.Flush(); err != nil {
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
			_ = cl.w.Flush()
			return
		case line := <-cl.queue:
			// each line is a full command; write with newline
			_, _ = cl.w.WriteString(line)
			if len(line) == 0 || line[len(line)-1] != '\n' {
				_ = cl.w.WriteByte('\n')
			}
		case <-ticker.C:
			_ = cl.w.Flush()
			_ = cl.file.Sync()
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
		if _, err := cl.w.WriteString(command); err != nil {
			return err
		}
		if command[len(command)-1] != '\n' {
			if err := cl.w.WriteByte('\n'); err != nil {
				return err
			}
		}
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
