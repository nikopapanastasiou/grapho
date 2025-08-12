package executor

import (
	"bytes"
	"testing"

	"grapho/catalog"
	"grapho/parser"
)

// mockStore implements catalog.Store in-memory for tests.
type mockStore struct {
	cat       *catalog.Catalog
	ddlEvents []catalog.DDLEvent
}

func (m *mockStore) Load() (*catalog.Catalog, uint64, error) {
	if m.cat == nil {
		m.cat = catalog.NewEmpty()
	}
	return m.cat, 0, nil
}
func (m *mockStore) AppendDDL(ev catalog.DDLEvent) (uint64, error) {
	m.ddlEvents = append(m.ddlEvents, ev)
	return uint64(len(m.ddlEvents)), nil
}
func (m *mockStore) Snapshot(cat *catalog.Catalog) error { m.cat = cat; return nil }
func (m *mockStore) UpdateManifest(catVersion uint64, ddlOffset uint64) error { return nil }

func newTestRegistry(t *testing.T) *catalog.Registry {
	t.Helper()
	ms := &mockStore{}
	r, err := catalog.Open(ms)
	if err != nil { t.Fatalf("open registry: %v", err) }
	return r
}

func TestCreateAndInsertNode(t *testing.T) {
	r := newTestRegistry(t)
	ex := New(r)

	// CREATE NODE Person
	script := "CREATE NODE Person(name: STRING, age: INT);"
	p := parser.NewParser(script)
	stmts, errs := p.ParseScript()
	if len(errs) > 0 { t.Fatalf("parse errs: %v", errs) }
	mut, err := ex.ExecuteStatements(nil, stmts)
	if err != nil { t.Fatalf("exec ddl: %v", err) }
	if !mut { t.Fatalf("expected mutation on DDL") }

	// INSERT NODE Person
	script = "INSERT NODE Person (name: 'Alice', age: 30);"
	p = parser.NewParser(script)
	stmts, errs = p.ParseScript()
	if len(errs) > 0 { t.Fatalf("parse errs: %v", errs) }
	buf := &bytes.Buffer{}
	mut, err = ex.ExecuteStatements(buf, stmts)
	if err != nil { t.Fatalf("exec insert: %v", err) }
	if !mut { t.Fatalf("expected mutation on insert") }
	// verify one node stored
	nodes := ex.graph.Nodes["Person"]
	if nodes == nil || len(nodes) != 1 {
		t.Fatalf("expected 1 Person node, have: %v", len(nodes))
	}
}

func TestCreateEdgeAndInsert(t *testing.T) {
	r := newTestRegistry(t)
	ex := New(r)

	// DDL: create nodes and edge
	script := "CREATE NODE Person(name: STRING);" +
		"CREATE NODE City(name: STRING);" +
		"CREATE EDGE LIVES_IN(FROM Person ONE, TO City ONE, PROPS (since: INT));"
	p := parser.NewParser(script)
	stmts, errs := p.ParseScript()
	if len(errs) > 0 { t.Fatalf("parse errs: %v", errs) }
	if _, err := ex.ExecuteStatements(nil, stmts); err != nil { t.Fatalf("exec ddl: %v", err) }

	// Insert nodes
	p = parser.NewParser("INSERT NODE Person (name: 'Alice');INSERT NODE City (name: 'Paris');")
	stmts, errs = p.ParseScript()
	if len(errs) > 0 { t.Fatalf("parse errs: %v", errs) }
	if _, err := ex.ExecuteStatements(nil, stmts); err != nil { t.Fatalf("insert nodes: %v", err) }

	// Insert edge via property refs
	script = "INSERT EDGE LIVES_IN FROM Person(name: 'Alice') TO City(name: 'Paris') (since: 2020);"
	p = parser.NewParser(script)
	stmts, errs = p.ParseScript()
	if len(errs) > 0 { t.Fatalf("parse errs: %v", errs) }
	buf := &bytes.Buffer{}
	mut, err := ex.ExecuteStatements(buf, stmts)
	if err != nil { t.Fatalf("exec edge insert: %v", err) }
	if !mut { t.Fatalf("expected mutation on edge insert") }
	if len(ex.graph.Edges["LIVES_IN"]) != 1 {
		t.Fatalf("expected 1 LIVES_IN edge, got %d", len(ex.graph.Edges["LIVES_IN"]))
	}
}
