package parser

import (
	"fmt"
	"testing"
)

func TestInsertNodeParsing(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "basic insert node",
			input:   "INSERT NODE User (name: 'John', age: 25);",
			wantErr: false,
		},
		{
			name:    "insert node without properties",
			input:   "INSERT NODE User;",
			wantErr: false,
		},
		{
			name:    "insert node with multiple properties",
			input:   "INSERT NODE Product (name: 'Laptop', price: 999.99, available: true);",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.input)
			stmts, errs := p.ParseScript()

			if tt.wantErr {
				if len(errs) == 0 {
					t.Errorf("expected error but got none")
				}
				return
			}

			if len(errs) > 0 {
				t.Errorf("unexpected errors: %v", errs)
				return
			}

			if len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
				return
			}

			stmt, ok := stmts[0].(*InsertNodeStmt)
			if !ok {
				t.Errorf("expected InsertNodeStmt, got %T", stmts[0])
				return
			}

			if stmt.NodeType == "" {
				t.Errorf("expected non-empty node type")
			}
		})
	}
}

func TestInsertEdgeParsing(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "basic insert edge",
			input:   "INSERT EDGE FOLLOWS FROM User(1) TO User(2);",
			wantErr: false,
		},
		{
			name:    "insert edge with properties",
			input:   "INSERT EDGE LIKES FROM User(name: 'John') TO Product(id: '123') (rating: 5, comment: 'Great!');",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.input)
			stmts, errs := p.ParseScript()

			if tt.wantErr {
				if len(errs) == 0 {
					t.Errorf("expected error but got none")
				}
				return
			}

			if len(errs) > 0 {
				t.Errorf("unexpected errors: %v", errs)
				return
			}

			if len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
				return
			}

			stmt, ok := stmts[0].(*InsertEdgeStmt)
			if !ok {
				t.Errorf("expected InsertEdgeStmt, got %T", stmts[0])
				return
			}

			if stmt.EdgeType == "" {
				t.Errorf("expected non-empty edge type")
			}

			if stmt.FromNode == nil || stmt.ToNode == nil {
				t.Errorf("expected FROM and TO nodes to be specified")
			}
		})
	}
}

func TestUpdateNodeParsing(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "basic update node",
			input:   "UPDATE NODE User SET name: 'Jane' WHERE id: '1';",
			wantErr: false,
		},
		{
			name:    "update node without where",
			input:   "UPDATE NODE User SET active: true;",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.input)
			stmts, errs := p.ParseScript()

			if tt.wantErr {
				if len(errs) == 0 {
					t.Errorf("expected error but got none")
				}
				return
			}

			if len(errs) > 0 {
				t.Errorf("unexpected errors: %v", errs)
				return
			}

			if len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
				return
			}

			stmt, ok := stmts[0].(*UpdateNodeStmt)
			if !ok {
				t.Errorf("expected UpdateNodeStmt, got %T", stmts[0])
				return
			}

			if len(stmt.Set) == 0 {
				t.Errorf("expected SET properties")
			}
		})
	}
}

func TestDeleteNodeParsing(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "basic delete node",
			input:   "DELETE NODE User WHERE id: '1';",
			wantErr: false,
		},
		{
			name:    "delete node missing where",
			input:   "DELETE NODE User;",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.input)
			stmts, errs := p.ParseScript()

			if tt.wantErr {
				if len(errs) == 0 {
					t.Errorf("expected error but got none")
				}
				return
			}

			if len(errs) > 0 {
				t.Errorf("unexpected errors: %v", errs)
				return
			}

			if len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
				return
			}

			stmt, ok := stmts[0].(*DeleteNodeStmt)
			if !ok {
				t.Errorf("expected DeleteNodeStmt, got %T", stmts[0])
				return
			}

			if len(stmt.Where) == 0 {
				t.Errorf("expected WHERE conditions")
			}
		})
	}
}

func TestMatchParsing(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "basic match",
			input:   "MATCH User RETURN name;",
			wantErr: false,
		},
		{
			name:    "match with where",
			input:   "MATCH User WHERE age: 25 RETURN name, email;",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.input)
			stmts, errs := p.ParseScript()

			if tt.wantErr {
				if len(errs) == 0 {
					t.Errorf("expected error but got none")
				}
				return
			}

			if len(errs) > 0 {
				t.Errorf("unexpected errors: %v", errs)
				return
			}

			if len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
				return
			}

			stmt, ok := stmts[0].(*MatchStmt)
			if !ok {
				t.Errorf("expected MatchStmt, got %T", stmts[0])
				return
			}

			if len(stmt.Pattern) == 0 {
				t.Errorf("expected pattern elements")
			}
		})
	}
}

func TestMixedDMLStatements(t *testing.T) {
	input := `
		INSERT NODE User (name: 'John', age: 25);
		INSERT NODE Product (name: 'Laptop', price: 999.99);
		INSERT EDGE LIKES FROM User(1) TO Product(1) (rating: 5);
		UPDATE NODE User SET age: 26 WHERE name: 'John';
		MATCH User WHERE age: 26 RETURN name;
		DELETE EDGE LIKES WHERE rating: 5;
	`

	p := NewParser(input)
	stmts, errs := p.ParseScript()

	if len(errs) > 0 {
		t.Errorf("unexpected errors: %v", errs)
		return
	}

	if len(stmts) != 6 {
		t.Errorf("expected 6 statements, got %d", len(stmts))
		return
	}

	// Verify statement types
	expectedTypes := []string{
		"*parser.InsertNodeStmt",
		"*parser.InsertNodeStmt",
		"*parser.InsertEdgeStmt",
		"*parser.UpdateNodeStmt",
		"*parser.MatchStmt",
		"*parser.DeleteEdgeStmt",
	}

	for i, stmt := range stmts {
		actualType := fmt.Sprintf("%T", stmt)
		if actualType != expectedTypes[i] {
			t.Errorf("statement %d: expected %s, got %s", i, expectedTypes[i], actualType)
		}
	}
}
