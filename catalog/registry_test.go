package catalog

import (
	"errors"
	"sync"
	"testing"
)

// Mock store for testing registry without file I/O
type mockStore struct {
	mu        sync.Mutex
	catalog   *Catalog
	ddlOffset uint64
	ddlLog    []DDLEvent
	snapshots map[uint64]*Catalog
	
	// Error injection
	loadErr      error
	appendErr    error
	snapshotErr  error
	manifestErr  error
}

func newMockStore() *mockStore {
	return &mockStore{
		snapshots: make(map[uint64]*Catalog),
	}
}

func (m *mockStore) Load() (*Catalog, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.loadErr != nil {
		return nil, 0, m.loadErr
	}
	
	if m.catalog == nil {
		return NewEmpty(), 0, nil
	}
	
	return m.catalog.Clone(), m.ddlOffset, nil
}

func (m *mockStore) AppendDDL(ev DDLEvent) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.appendErr != nil {
		return 0, m.appendErr
	}
	
	m.ddlLog = append(m.ddlLog, ev)
	m.ddlOffset = uint64(len(m.ddlLog))
	return m.ddlOffset, nil
}

func (m *mockStore) Snapshot(cat *Catalog) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.snapshotErr != nil {
		return m.snapshotErr
	}
	
	m.snapshots[cat.Version] = cat.Clone()
	return nil
}

func (m *mockStore) UpdateManifest(catVersion uint64, ddlOffset uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.manifestErr != nil {
		return m.manifestErr
	}
	
	// Update internal state to reflect manifest
	if snap, exists := m.snapshots[catVersion]; exists {
		m.catalog = snap
		m.ddlOffset = ddlOffset
	}
	return nil
}

func TestRegistryOpen(t *testing.T) {
	store := newMockStore()
	
	reg, err := Open(store)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	
	if reg == nil {
		t.Fatal("registry is nil")
	}
	
	current := reg.Current()
	if current == nil {
		t.Fatal("current catalog is nil")
	}
	
	if current.Version != 0 {
		t.Errorf("expected version 0, got %d", current.Version)
	}
}

func TestRegistryOpenWithExistingCatalog(t *testing.T) {
	store := newMockStore()
	
	// Pre-populate store with a catalog
	existingCat := &Catalog{
		Version: 5,
		Nodes:   map[string]*NodeType{},
		Edges:   map[string]*EdgeType{},
	}
	store.catalog = existingCat
	store.ddlOffset = 10
	
	reg, err := Open(store)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	
	current := reg.Current()
	if current.Version != 5 {
		t.Errorf("expected version 5, got %d", current.Version)
	}
}

func TestRegistryOpenLoadError(t *testing.T) {
	store := newMockStore()
	store.loadErr = errors.New("load failed")
	
	_, err := Open(store)
	if err == nil {
		t.Fatal("expected error but got none")
	}
	if err.Error() != "load failed" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRegistryApplyCreateNode(t *testing.T) {
	store := newMockStore()
	reg, _ := Open(store)
	
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
	
	newCat, err := reg.Apply(ev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	
	if newCat.Version != 1 {
		t.Errorf("expected version 1, got %d", newCat.Version)
	}
	
	if _, exists := newCat.Nodes["Person"]; !exists {
		t.Error("Person node not found in new catalog")
	}
	
	// Verify current catalog is updated
	current := reg.Current()
	if current.Version != 1 {
		t.Errorf("current catalog version not updated: got %d", current.Version)
	}
	
	// Verify DDL was persisted
	if len(store.ddlLog) != 1 {
		t.Errorf("expected 1 DDL event, got %d", len(store.ddlLog))
	}
}

func TestRegistryApplyCreateEdge(t *testing.T) {
	store := newMockStore()
	reg, _ := Open(store)
	
	// First create a node
	nodeEv := DDLEvent{
		Op: OpCreateNode,
		Stmt: CreateNodePayload{
			Name: "Person",
			Fields: []FieldPayload{
				{Name: "id", Type: TypeSpec{Base: BaseUUID}, PrimaryKey: true},
			},
		},
	}
	reg.Apply(nodeEv)
	
	// Then create an edge
	edgeEv := DDLEvent{
		Op: OpCreateEdge,
		Stmt: CreateEdgePayload{
			Name: "KNOWS",
			From: EdgeEndpoint{Label: "Person", Card: Many},
			To:   EdgeEndpoint{Label: "Person", Card: Many},
		},
	}
	
	newCat, err := reg.Apply(edgeEv)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	
	if newCat.Version != 2 {
		t.Errorf("expected version 2, got %d", newCat.Version)
	}
	
	if _, exists := newCat.Edges["KNOWS"]; !exists {
		t.Error("KNOWS edge not found in new catalog")
	}
}

func TestRegistryApplyValidationError(t *testing.T) {
	store := newMockStore()
	reg, _ := Open(store)
	
	ev := DDLEvent{
		Op: OpCreateNode,
		Stmt: CreateNodePayload{
			Name:   "", // Invalid: empty name
			Fields: []FieldPayload{},
		},
	}
	
	_, err := reg.Apply(ev)
	if err == nil {
		t.Fatal("expected validation error but got none")
	}
	
	// Verify catalog wasn't changed
	current := reg.Current()
	if current.Version != 0 {
		t.Errorf("catalog version changed despite error: got %d", current.Version)
	}
	
	// Verify no DDL was persisted
	if len(store.ddlLog) != 0 {
		t.Errorf("DDL was persisted despite error: %d events", len(store.ddlLog))
	}
}

func TestRegistryApplyPersistenceError(t *testing.T) {
	store := newMockStore()
	store.appendErr = errors.New("disk full")
	reg, _ := Open(store)
	
	ev := DDLEvent{
		Op: OpCreateNode,
		Stmt: CreateNodePayload{
			Name: "Person",
			Fields: []FieldPayload{
				{Name: "id", Type: TypeSpec{Base: BaseString}},
			},
		},
	}
	
	_, err := reg.Apply(ev)
	if err == nil {
		t.Fatal("expected persistence error but got none")
	}
	if err.Error() != "disk full" {
		t.Errorf("unexpected error: %v", err)
	}
	
	// Verify catalog wasn't changed
	current := reg.Current()
	if current.Version != 0 {
		t.Errorf("catalog version changed despite error: got %d", current.Version)
	}
}

func TestRegistryApplyManifestError(t *testing.T) {
	store := newMockStore()
	store.manifestErr = errors.New("manifest write failed")
	reg, _ := Open(store)
	
	ev := DDLEvent{
		Op: OpCreateNode,
		Stmt: CreateNodePayload{
			Name: "Person",
			Fields: []FieldPayload{
				{Name: "id", Type: TypeSpec{Base: BaseString}},
			},
		},
	}
	
	_, err := reg.Apply(ev)
	if err == nil {
		t.Fatal("expected manifest error but got none")
	}
	if err.Error() != "manifest write failed" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRegistryApplyUnsupportedOp(t *testing.T) {
	store := newMockStore()
	reg, _ := Open(store)
	
	ev := DDLEvent{
		Op:   "UNSUPPORTED_OP",
		Stmt: map[string]any{},
	}
	
	_, err := reg.Apply(ev)
	if err == nil {
		t.Fatal("expected error for unsupported op but got none")
	}
	if err.Error() != "unsupported DDL op UNSUPPORTED_OP" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRegistrySnapshot(t *testing.T) {
	store := newMockStore()
	reg, _ := Open(store)
	
	// Apply some changes first
	ev := DDLEvent{
		Op: OpCreateNode,
		Stmt: CreateNodePayload{
			Name: "Person",
			Fields: []FieldPayload{
				{Name: "id", Type: TypeSpec{Base: BaseString}},
			},
		},
	}
	reg.Apply(ev)
	
	err := reg.Snapshot()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	
	// Verify snapshot was created
	if len(store.snapshots) != 1 {
		t.Errorf("expected 1 snapshot, got %d", len(store.snapshots))
	}
	
	snap, exists := store.snapshots[1]
	if !exists {
		t.Fatal("snapshot for version 1 not found")
	}
	
	if _, exists := snap.Nodes["Person"]; !exists {
		t.Error("Person node not found in snapshot")
	}
}

func TestRegistrySnapshotError(t *testing.T) {
	store := newMockStore()
	store.snapshotErr = errors.New("snapshot failed")
	reg, _ := Open(store)
	
	err := reg.Snapshot()
	if err == nil {
		t.Fatal("expected snapshot error but got none")
	}
	if err.Error() != "snapshot failed" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRegistryConcurrentReads(t *testing.T) {
	store := newMockStore()
	reg, _ := Open(store)
	
	// Apply initial change
	ev := DDLEvent{
		Op: OpCreateNode,
		Stmt: CreateNodePayload{
			Name: "Person",
			Fields: []FieldPayload{
				{Name: "id", Type: TypeSpec{Base: BaseString}},
			},
		},
	}
	reg.Apply(ev)
	
	// Concurrent reads should all see consistent state
	const numReaders = 10
	results := make(chan uint64, numReaders)
	
	for i := 0; i < numReaders; i++ {
		go func() {
			cat := reg.Current()
			results <- cat.Version
		}()
	}
	
	for i := 0; i < numReaders; i++ {
		version := <-results
		if version != 1 {
			t.Errorf("reader %d got version %d, expected 1", i, version)
		}
	}
}

func TestDecodeFunction(t *testing.T) {
	tests := []struct {
		name    string
		src     any
		wantErr bool
	}{
		{
			name:    "nil input",
			src:     nil,
			wantErr: true,
		},
		{
			name: "map input",
			src: map[string]any{
				"Name": "Person",
				"Fields": []any{
					map[string]any{
						"Name": "id",
						"Type": map[string]any{"Base": 0},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "struct input",
			src: CreateNodePayload{
				Name: "Person",
				Fields: []FieldPayload{
					{Name: "id", Type: TypeSpec{Base: BaseString}},
				},
			},
			wantErr: false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dst CreateNodePayload
			err := decode(tt.src, &dst)
			
			if tt.wantErr && err == nil {
				t.Error("expected error but got none")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			
			if !tt.wantErr && dst.Name != "Person" {
				t.Errorf("decode failed: got name %q", dst.Name)
			}
		})
	}
}
