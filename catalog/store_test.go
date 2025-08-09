package catalog

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileStoreNewFileStore(t *testing.T) {
	tmpDir := t.TempDir()

	store, err := NewFileStore(tmpDir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if store == nil {
		t.Fatal("store is nil")
	}

	// Verify directory was created
	if _, err := os.Stat(tmpDir); os.IsNotExist(err) {
		t.Error("directory was not created")
	}
}

func TestFileStoreNewFileStoreEmptyDir(t *testing.T) {
	_, err := NewFileStore("")
	if err == nil {
		t.Fatal("expected error for empty dir but got none")
	}
	if !strings.Contains(err.Error(), "empty dir") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestFileStoreLoadEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := NewFileStore(tmpDir)

	cat, offset, err := store.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cat == nil {
		t.Fatal("catalog is nil")
	}

	if cat.Version != 0 {
		t.Errorf("expected version 0, got %d", cat.Version)
	}

	if offset != 0 {
		t.Errorf("expected offset 0, got %d", offset)
	}

	if len(cat.Nodes) != 0 || len(cat.Edges) != 0 {
		t.Error("expected empty catalog")
	}
}

func TestFileStoreAppendDDLAndLoad(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := NewFileStore(tmpDir)

	// Append a DDL event
	ev := DDLEvent{
		Op: OpCreateNode,
		Stmt: CreateNodePayload{
			Name: "Person",
			Fields: []FieldPayload{
				{Name: "id", Type: TypeSpec{Base: BaseUUID}, PrimaryKey: true},
				{Name: "name", Type: TypeSpec{Base: BaseString}},
			},
		},
	}

	offset, err := store.AppendDDL(ev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if offset != 1 {
		t.Errorf("expected offset 1, got %d", offset)
	}

	// Verify DDL file was created and contains the event
	ddlPath := filepath.Join(tmpDir, "catalog-ddl.jsonl")
	content, err := os.ReadFile(ddlPath)
	if err != nil {
		t.Fatalf("failed to read DDL file: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) != 1 {
		t.Errorf("expected 1 line, got %d", len(lines))
	}

	var readEv DDLEvent
	if err := json.Unmarshal([]byte(lines[0]), &readEv); err != nil {
		t.Fatalf("failed to unmarshal DDL event: %v", err)
	}

	if readEv.Op != OpCreateNode {
		t.Errorf("expected OpCreateNode, got %s", readEv.Op)
	}

	// Now load and verify replay
	cat, loadOffset, err := store.Load()
	if err != nil {
		t.Fatalf("unexpected load error: %v", err)
	}

	if loadOffset != 1 {
		t.Errorf("expected load offset 1, got %d", loadOffset)
	}

	if cat.Version != 1 {
		t.Errorf("expected catalog version 1, got %d", cat.Version)
	}

	if _, exists := cat.Nodes["Person"]; !exists {
		t.Error("Person node not found after replay")
	}
}

func TestFileStoreMultipleDDLEvents(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := NewFileStore(tmpDir)

	// Append multiple events
	events := []DDLEvent{
		{
			Op: OpCreateNode,
			Stmt: CreateNodePayload{
				Name: "Person",
				Fields: []FieldPayload{
					{Name: "id", Type: TypeSpec{Base: BaseString}, PrimaryKey: true},
				},
			},
		},
		{
			Op: OpCreateNode,
			Stmt: CreateNodePayload{
				Name: "Company",
				Fields: []FieldPayload{
					{Name: "id", Type: TypeSpec{Base: BaseString}, PrimaryKey: true},
				},
			},
		},
		{
			Op: OpCreateEdge,
			Stmt: CreateEdgePayload{
				Name: "WORKS_AT",
				From: EdgeEndpoint{Label: "Person", Card: Many},
				To:   EdgeEndpoint{Label: "Company", Card: One},
			},
		},
	}

	for i, ev := range events {
		offset, err := store.AppendDDL(ev)
		if err != nil {
			t.Fatalf("failed to append event %d: %v", i, err)
		}
		if offset != uint64(i+1) {
			t.Errorf("event %d: expected offset %d, got %d", i, i+1, offset)
		}
	}

	// Load and verify all events were replayed
	cat, offset, err := store.Load()
	if err != nil {
		t.Fatalf("unexpected load error: %v", err)
	}

	if offset != 3 {
		t.Errorf("expected offset 3, got %d", offset)
	}

	if cat.Version != 3 {
		t.Errorf("expected version 3, got %d", cat.Version)
	}

	if len(cat.Nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(cat.Nodes))
	}

	if len(cat.Edges) != 1 {
		t.Errorf("expected 1 edge, got %d", len(cat.Edges))
	}
}

func TestFileStoreSnapshot(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := NewFileStore(tmpDir)

	// Create a catalog to snapshot
	cat := &Catalog{
		Version: 5,
		Nodes: map[string]*NodeType{
			"Person": {
				Name: "Person",
				Fields: map[string]FieldSpec{
					"id": {Name: "id", Type: TypeSpec{Base: BaseUUID}},
				},
				PK:      "id",
				Indexes: map[string]IndexSpec{},
			},
		},
		Edges: map[string]*EdgeType{},
	}

	err := store.Snapshot(cat)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify snapshot file was created
	snapPath := filepath.Join(tmpDir, "catalog-snap-000005.json")
	if _, err := os.Stat(snapPath); os.IsNotExist(err) {
		t.Error("snapshot file was not created")
	}

	// Verify manifest was updated
	manifestPath := filepath.Join(tmpDir, "CATALOG-MANIFEST.json")
	manifestContent, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("failed to read manifest: %v", err)
	}

	var manifest Manifest
	if err := json.Unmarshal(manifestContent, &manifest); err != nil {
		t.Fatalf("failed to unmarshal manifest: %v", err)
	}

	if manifest.Snapshot != "catalog-snap-000005.json" {
		t.Errorf("unexpected snapshot name: %s", manifest.Snapshot)
	}

	if manifest.Version != 5 {
		t.Errorf("expected version 5, got %d", manifest.Version)
	}
}

func TestFileStoreLoadWithSnapshot(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := NewFileStore(tmpDir)

	// Create and save a snapshot
	originalCat := &Catalog{
		Version: 3,
		Nodes: map[string]*NodeType{
			"Person": {
				Name: "Person",
				Fields: map[string]FieldSpec{
					"id": {Name: "id", Type: TypeSpec{Base: BaseString}},
				},
			},
		},
		Edges: map[string]*EdgeType{},
	}

	store.Snapshot(originalCat)

	// Add some DDL events after the snapshot
	ev := DDLEvent{
		Op: OpCreateNode,
		Stmt: CreateNodePayload{
			Name: "Company",
			Fields: []FieldPayload{
				{Name: "id", Type: TypeSpec{Base: BaseString}},
			},
		},
	}
	store.AppendDDL(ev)

	// Update manifest to point to snapshot at offset 0
	store.UpdateManifest(3, 0)

	// Load should start from snapshot and replay DDL
	cat, offset, err := store.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if offset != 1 {
		t.Errorf("expected offset 1, got %d", offset)
	}

	if cat.Version != 4 { // 3 from snapshot + 1 from DDL replay
		t.Errorf("expected version 4, got %d", cat.Version)
	}

	if len(cat.Nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(cat.Nodes))
	}

	if _, exists := cat.Nodes["Person"]; !exists {
		t.Error("Person node not found (should be from snapshot)")
	}

	if _, exists := cat.Nodes["Company"]; !exists {
		t.Error("Company node not found (should be from DDL replay)")
	}
}

func TestFileStoreUpdateManifest(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := NewFileStore(tmpDir)

	err := store.UpdateManifest(10, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	manifestPath := filepath.Join(tmpDir, "CATALOG-MANIFEST.json")
	content, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("failed to read manifest: %v", err)
	}

	var manifest Manifest
	if err := json.Unmarshal(content, &manifest); err != nil {
		t.Fatalf("failed to unmarshal manifest: %v", err)
	}

	if manifest.Version != 10 {
		t.Errorf("expected version 10, got %d", manifest.Version)
	}

	if manifest.DDLOffset != 5 {
		t.Errorf("expected DDL offset 5, got %d", manifest.DDLOffset)
	}
}

func TestFileStoreCorruptedDDL(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := NewFileStore(tmpDir)

	// Write valid DDL event first
	validEv := DDLEvent{
		Op: OpCreateNode,
		Stmt: CreateNodePayload{
			Name: "Person",
			Fields: []FieldPayload{
				{Name: "id", Type: TypeSpec{Base: BaseString}},
			},
		},
	}
	store.AppendDDL(validEv)

	// Manually append corrupted line to DDL file
	ddlPath := filepath.Join(tmpDir, "catalog-ddl.jsonl")
	f, err := os.OpenFile(ddlPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("failed to open DDL file: %v", err)
	}
	f.WriteString("invalid json line\n")
	f.Close()

	// Load should stop at corruption but return best-effort catalog
	cat, offset, err := store.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have processed the first valid event
	if cat.Version != 1 {
		t.Errorf("expected version 1, got %d", cat.Version)
	}

	if _, exists := cat.Nodes["Person"]; !exists {
		t.Error("Person node not found")
	}

	// Offset should reflect total lines processed (including corrupted)
	if offset != 2 {
		t.Errorf("expected offset 2, got %d", offset)
	}
}

func TestFileStorePathMethods(t *testing.T) {
	tmpDir := t.TempDir()
	fs := &fileStore{dir: tmpDir}

	snapPath := fs.snapPath("test.json")
	expectedSnap := filepath.Join(tmpDir, "test.json")
	if snapPath != expectedSnap {
		t.Errorf("snapPath: expected %s, got %s", expectedSnap, snapPath)
	}

	ddlPath := fs.ddlPath()
	expectedDDL := filepath.Join(tmpDir, "catalog-ddl.jsonl")
	if ddlPath != expectedDDL {
		t.Errorf("ddlPath: expected %s, got %s", expectedDDL, ddlPath)
	}

	manifestPath := fs.manifestPath()
	expectedManifest := filepath.Join(tmpDir, "CATALOG-MANIFEST.json")
	if manifestPath != expectedManifest {
		t.Errorf("manifestPath: expected %s, got %s", expectedManifest, manifestPath)
	}
}

func TestCountLines(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	// Empty file
	os.WriteFile(testFile, []byte(""), 0644)
	count, err := countLines(testFile)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 lines, got %d", count)
	}

	// File with lines
	content := "line1\nline2\nline3\n"
	os.WriteFile(testFile, []byte(content), 0644)
	count, err = countLines(testFile)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 lines, got %d", count)
	}

	// File without trailing newline
	content = "line1\nline2\nline3"
	os.WriteFile(testFile, []byte(content), 0644)
	count, err = countLines(testFile)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 lines, got %d", count)
	}
}

func TestCountLinesNonexistentFile(t *testing.T) {
	_, err := countLines("/nonexistent/file")
	if err == nil {
		t.Fatal("expected error for nonexistent file but got none")
	}
}

func TestStringsHasPrefix(t *testing.T) {
	tests := []struct {
		s, p string
		want bool
	}{
		{"hello world", "hello", true},
		{"hello world", "world", false},
		{"hello", "hello world", false},
		{"hello", "hello", true},
		{"", "", true},
		{"hello", "", true},
		{"", "hello", false},
	}

	for _, tt := range tests {
		got := stringsHasPrefix(tt.s, tt.p)
		if got != tt.want {
			t.Errorf("stringsHasPrefix(%q, %q) = %v, want %v", tt.s, tt.p, got, tt.want)
		}
	}
}

func TestFileStoreConcurrency(t *testing.T) {
	tmpDir := t.TempDir()
	store, _ := NewFileStore(tmpDir)

	// Test concurrent AppendDDL calls
	const numGoroutines = 10
	results := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			ev := DDLEvent{
				Op: OpCreateNode,
				Stmt: CreateNodePayload{
					Name: "Person" + string(rune('A'+id)),
					Fields: []FieldPayload{
						{Name: "id", Type: TypeSpec{Base: BaseString}},
					},
				},
			}
			_, err := store.AppendDDL(ev)
			results <- err
		}(i)
	}

	// Collect results
	for i := 0; i < numGoroutines; i++ {
		if err := <-results; err != nil {
			t.Errorf("goroutine %d failed: %v", i, err)
		}
	}

	// Verify all events were persisted
	cat, offset, err := store.Load()
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}

	if offset != numGoroutines {
		t.Errorf("expected offset %d, got %d", numGoroutines, offset)
	}

	if len(cat.Nodes) != numGoroutines {
		t.Errorf("expected %d nodes, got %d", numGoroutines, len(cat.Nodes))
	}
}
