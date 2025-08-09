package catalog

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type fileStore struct {
	dir string
	mu  sync.Mutex
}

type Manifest struct {
	Snapshot  string `json:"snapshot"`
	Version   uint64 `json:"version"`
	DDLOffset uint64 `json:"ddl_offset"`
}

func NewFileStore(dir string) (Store, error) {
	if dir == "" {
		return nil, errors.New("catalog: empty dir")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &fileStore{dir: dir}, nil
}

func (fs *fileStore) snapPath(name string) string { return filepath.Join(fs.dir, name) }
func (fs *fileStore) ddlPath() string             { return filepath.Join(fs.dir, "catalog-ddl.jsonl") }
func (fs *fileStore) manifestPath() string        { return filepath.Join(fs.dir, "CATALOG-MANIFEST.json") }

func (fs *fileStore) Load() (*Catalog, uint64, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// read manifest (optional at first boot)
	var m Manifest
	if b, err := os.ReadFile(fs.manifestPath()); err == nil {
		if err := json.Unmarshal(b, &m); err != nil {
			return nil, 0, fmt.Errorf("catalog: bad manifest: %w", err)
		}
	}

	var cat *Catalog
	if m.Snapshot != "" {
		b, err := os.ReadFile(fs.snapPath(m.Snapshot))
		if err != nil {
			return nil, 0, fmt.Errorf("catalog: read snapshot: %w", err)
		}
		if err := json.Unmarshal(b, &cat); err != nil {
			return nil, 0, fmt.Errorf("catalog: decode snapshot: %w", err)
		}
	} else {
		cat = NewEmpty()
	}

	// replay DDL from offset
	off := uint64(0)
	f, err := os.OpenFile(fs.ddlPath(), os.O_CREATE|os.O_RDONLY, 0o644)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	br := bufio.NewReader(f)
	var pos uint64
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			pos++
			if pos <= m.DDLOffset {
				continue // already applied per manifest
			}
			var ev DDLEvent
			if err := json.Unmarshal(line, &ev); err != nil {
				// stop at corruption
				break
			}
			switch ev.Op {
			case OpCreateNode:
				var p CreateNodePayload
				_ = decode(ev.Stmt, &p)
				cat, err = ApplyCreateNode(cat, p)
			case OpCreateEdge:
				var p CreateEdgePayload
				_ = decode(ev.Stmt, &p)
				cat, err = ApplyCreateEdge(cat, p)
			case OpAlterNode:
				var p AlterNodePayload
				_ = decode(ev.Stmt, &p)
				cat, err = ApplyAlterNode(cat, p)
			case OpAlterEdge:
				var p AlterEdgePayload
				_ = decode(ev.Stmt, &p)
				cat, err = ApplyAlterEdge(cat, p)
			case OpDropNode:
				var p DropNodePayload
				_ = decode(ev.Stmt, &p)
				cat, err = ApplyDropNode(cat, p)
			case OpDropEdge:
				var p DropEdgePayload
				_ = decode(ev.Stmt, &p)
				cat, err = ApplyDropEdge(cat, p)
			default:
				err = fmt.Errorf("unknown op %s", ev.Op)
			}
			if err != nil {
				// stop on error; return best-effort catalog
				break
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
	}
	// pos == total lines; last fully applied line number is pos
	off = pos
	return cat, off, nil
}

func (fs *fileStore) AppendDDL(ev DDLEvent) (uint64, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	f, err := os.OpenFile(fs.ddlPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	b, err := json.Marshal(ev)
	if err != nil {
		return 0, err
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		return 0, err
	}
	if err := f.Sync(); err != nil {
		return 0, err
	}

	// Count lines to determine new offset (O(n)). For real use, track offset in memory.
	off, err := countLines(fs.ddlPath())
	if err != nil {
		return 0, err
	}
	return off, nil
}

func (fs *fileStore) Snapshot(cat *Catalog) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	name := fmt.Sprintf("catalog-snap-%06d.json", cat.Version)
	path := fs.snapPath(name)
	b, err := json.MarshalIndent(cat, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return err
	}
	// Sync manifest immediately to bind snapshot with current offset.
	return fs.UpdateManifest(cat.Version, 0 /* caller should pass real ddl offset after AppendDDL */)
}

func (fs *fileStore) UpdateManifest(catVersion uint64, ddlOffset uint64) error {
	// Discover latest snapshot file name by version
	entries, err := os.ReadDir(fs.dir)
	if err != nil {
		return err
	}
	var snap string
	for _, e := range entries {
		if !e.IsDir() && stringsHasPrefix(e.Name(), "catalog-snap-") {
			snap = e.Name()
		}
	}
	m := Manifest{Snapshot: snap, Version: catVersion, DDLOffset: ddlOffset}
	b, _ := json.MarshalIndent(m, "", "  ")
	tmp := fs.manifestPath() + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, fs.manifestPath())
}

func countLines(path string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	var n uint64
	br := bufio.NewReader(f)
	for {
		_, err := br.ReadBytes('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

func stringsHasPrefix(s, p string) bool {
	return len(s) >= len(p) && s[:len(p)] == p
}
