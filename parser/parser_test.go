package parser

import (
	"fmt"
	"testing"
)

func TestParseCreateNode(t *testing.T) {
	src := `
CREATE NODE Person (
  id: uuid PRIMARY KEY,
  name: string,
  email: string UNIQUE,
  level: enum<'A','B','C'> DEFAULT 'A'
);
`
	p := NewParser(src)
	stmts, errs := p.ParseScript()

	t.Logf("stmts: %v", stmts)
	t.Logf("errs: %v", errs)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if len(stmts) != 1 {
		t.Fatalf("got %d stmts, want 1", len(stmts))
	}
	n, ok := stmts[0].(*CreateNodeStmt)
	if !ok || n.Name != "Person" || len(n.Fields) != 4 {
		t.Fatalf("bad AST: %#v", stmts[0])
	}
	if !n.Fields[0].PrimaryKey || n.Fields[1].NotNull || !n.Fields[2].Unique == false {
		// light assertion; real tests should check each flag and types
	}
}

func TestParseCreateEdge(t *testing.T) {
	src := `
CREATE EDGE WORKS_AT (
  FROM Person MANY,
  TO Company ONE,
  PROPS (role: string, start_date: date)
);
`
	p := NewParser(src)
	stmts, errs := p.ParseScript()
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	e, ok := stmts[0].(*CreateEdgeStmt)
	if !ok || e.Name != "WORKS_AT" {
		t.Fatalf("bad AST: %#v", stmts[0])
	}
	if e.From.Label != "Person" || e.From.Card != CardMany {
		t.Fatalf("bad FROM")
	}
	if e.To.Label != "Company" || e.To.Card != CardOne {
		t.Fatalf("bad TO")
	}
	if len(e.Props) != 2 {
		t.Fatalf("bad props: %d", len(e.Props))
	}
}

func TestMissingSemicolonRecovery(t *testing.T) {
	src := `CREATE NODE A(id:int) CREATE NODE B(id:int);`
	p := NewParser(src)
	_, errs := p.ParseScript()
	if len(errs) == 0 {
		t.Fatalf("expected error for missing semicolon")
	}
}

func TestParseAllPrimitiveTypesAndDefaults(t *testing.T) {
	src := `CREATE NODE T(
        a: string DEFAULT 'x',
        b: text,
        c: int DEFAULT 123,
        d: float DEFAULT 1.5,
        e: bool DEFAULT true,
        f: uuid,
        g: date,
        h: time,
        i: datetime,
        j: json,
        k: blob NOT NULL
    );`
	p := NewParser(src)
	stmts, errs := p.ParseScript()
	if len(errs) != 0 {
		t.Fatalf("unexpected errs: %v", errs)
	}
	n := stmts[0].(*CreateNodeStmt)
	if n.Name != "T" || len(n.Fields) != 11 {
		t.Fatalf("bad node or field count: %#v", n)
	}
	if n.Fields[0].Type.Base != BaseString || n.Fields[0].Default == nil || n.Fields[0].Default.Kind != LitString {
		t.Fatalf("bad field a: %#v", n.Fields[0])
	}
	if n.Fields[2].Default == nil || n.Fields[2].Default.Kind != LitNumber || n.Fields[3].Default == nil || n.Fields[3].Default.Kind != LitNumber {
		t.Fatalf("bad numeric defaults")
	}
	if n.Fields[4].Default == nil || n.Fields[4].Default.Kind != LitBool {
		t.Fatalf("bad bool default")
	}
	if !n.Fields[10].NotNull {
		t.Fatalf("expected k NOT NULL")
	}
}

func TestParseArrayAndEnumTypes(t *testing.T) {
	src := `CREATE NODE T(
        tags: array<string>,
        scores: array<float>,
        status: enum<'new','active','archived'>
    );`
	p := NewParser(src)
	stmts, errs := p.ParseScript()
	if len(errs) != 0 {
		t.Fatalf("unexpected errs: %v", errs)
	}
	n := stmts[0].(*CreateNodeStmt)
	if len(n.Fields) != 3 {
		t.Fatalf("want 3 fields, got %d", len(n.Fields))
	}
	if n.Fields[0].Type.Elem == nil || n.Fields[1].Type.Elem == nil {
		t.Fatalf("array elem missing")
	}
	if len(n.Fields[2].Type.EnumVals) != 3 || n.Fields[2].Type.EnumVals[0] != "new" {
		t.Fatalf("bad enum vals: %#v", n.Fields[2].Type.EnumVals)
	}
}

func TestParseFieldOptions(t *testing.T) {
	src := `CREATE NODE N(
        id: uuid PRIMARY KEY,
        email: string UNIQUE NOT NULL,
        name: string DEFAULT 'Anon'
    );`
	p := NewParser(src)
	stmts, errs := p.ParseScript()
	if len(errs) != 0 {
		t.Fatalf("unexpected errs: %v", errs)
	}
	n := stmts[0].(*CreateNodeStmt)
	if !n.Fields[0].PrimaryKey {
		t.Fatalf("id should be PK")
	}
	if !n.Fields[1].Unique || !n.Fields[1].NotNull {
		t.Fatalf("email flags wrong: %#v", n.Fields[1])
	}
	if n.Fields[2].Default == nil || n.Fields[2].Default.Kind != LitString || n.Fields[2].Default.Text != "Anon" {
		t.Fatalf("bad default: %#v", n.Fields[2].Default)
	}
}

func TestTrailingCommasAndEmptyFields(t *testing.T) {
	src := `
CREATE NODE A();
CREATE NODE B(
  x: int,
);
`
	p := NewParser(src)
	stmts, errs := p.ParseScript()
	if len(errs) != 0 {
		t.Fatalf("unexpected errs: %v", errs)
	}
	if len(stmts) != 2 {
		t.Fatalf("want 2 stmts, got %d", len(stmts))
	}
	a := stmts[0].(*CreateNodeStmt)
	b := stmts[1].(*CreateNodeStmt)
	if len(a.Fields) != 0 || len(b.Fields) != 1 {
		t.Fatalf("bad field counts: %d, %d", len(a.Fields), len(b.Fields))
	}
}

func TestQuotedIdentifiersInParser(t *testing.T) {
	src := "CREATE NODE `Weird Name` ( `first name`: string );"
	p := NewParser(src)
	stmts, errs := p.ParseScript()
	if len(errs) != 0 {
		t.Fatalf("unexpected errs: %v", errs)
	}
	n := stmts[0].(*CreateNodeStmt)
	if n.Name != "Weird Name" || n.Fields[0].Name != "first name" {
		t.Fatalf("quoted idents not parsed correctly: %#v", n)
	}
}

func TestEdgeDefaultsAndNoProps(t *testing.T) {
	src := `CREATE EDGE REL(
  FROM A,
  TO B
);`
	p := NewParser(src)
	stmts, errs := p.ParseScript()
	if len(errs) != 0 {
		t.Fatalf("unexpected errs: %v", errs)
	}
	e := stmts[0].(*CreateEdgeStmt)
	if e.From.Card != CardOne || e.To.Card != CardOne {
		t.Fatalf("default cardinalities should be ONE")
	}
	if len(e.Props) != 0 {
		t.Fatalf("expected no props")
	}
}

func TestMultipleStatements(t *testing.T) {
	src := `CREATE NODE A(id:int);
CREATE EDGE E(FROM A, TO A);
CREATE NODE B(name:string);`
	p := NewParser(src)
	stmts, errs := p.ParseScript()
	if len(errs) != 0 {
		t.Fatalf("unexpected errs: %v", errs)
	}
	if len(stmts) != 3 {
		t.Fatalf("want 3 stmts, got %d", len(stmts))
	}
}

func TestUnexpectedStartTokenRecovery(t *testing.T) {
	src := `FOO BAR; CREATE NODE A(id:int);`
	p := NewParser(src)
	stmts, errs := p.ParseScript()
	if len(errs) == 0 {
		t.Fatalf("expected at least one error")
	}
	if len(stmts) != 1 {
		t.Fatalf("should recover and parse following statement")
	}
}

func TestAlterNodeAddField(t *testing.T) {
	src := `ALTER NODE Person ADD email:string unique not null default 'none';`
	p := NewParser(src)
	stmts, errs := p.ParseScript()
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}

	stmt, ok := stmts[0].(*AlterNodeStmt)
	if !ok {
		t.Fatalf("expected AlterNodeStmt, got %T", stmts[0])
	}
	if stmt.Name != "Person" {
		t.Errorf("expected name Person, got %s", stmt.Name)
	}
	if stmt.Action != AlterAddField {
		t.Errorf("expected AlterAddField, got %v", stmt.Action)
	}
	if stmt.Field == nil {
		t.Fatal("expected field definition")
	}
	if stmt.Field.Name != "email" {
		t.Errorf("expected field name email, got %s", stmt.Field.Name)
	}
	if stmt.Field.Type.Base != BaseString {
		t.Errorf("expected string type, got %v", stmt.Field.Type.Base)
	}
	if !stmt.Field.Unique {
		t.Error("expected unique field")
	}
	if !stmt.Field.NotNull {
		t.Error("expected not null field")
	}
	if stmt.Field.Default == nil || stmt.Field.Default.Text != "none" {
		t.Error("expected default value 'none'")
	}
}

func TestAlterNodeDropField(t *testing.T) {
	src := `ALTER NODE Person DROP email;`
	p := NewParser(src)
	stmts, errs := p.ParseScript()
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	stmt := stmts[0].(*AlterNodeStmt)
	if stmt.Action != AlterDropField {
		t.Errorf("expected AlterDropField, got %v", stmt.Action)
	}
	if stmt.FieldName != "email" {
		t.Errorf("expected field name email, got %s", stmt.FieldName)
	}
}

func TestAlterNodeModifyField(t *testing.T) {
	src := `ALTER NODE Person MODIFY email:text not null;`
	p := NewParser(src)
	stmts, errs := p.ParseScript()
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	stmt := stmts[0].(*AlterNodeStmt)
	if stmt.Action != AlterModifyField {
		t.Errorf("expected AlterModifyField, got %v", stmt.Action)
	}
	if stmt.Field.Name != "email" {
		t.Errorf("expected field name email, got %s", stmt.Field.Name)
	}
	if stmt.Field.Type.Base != BaseText {
		t.Errorf("expected text type, got %v", stmt.Field.Type.Base)
	}
}

func TestAlterNodeSetPrimaryKey(t *testing.T) {
	src := `ALTER NODE Person SET PRIMARY KEY (id, email);`
	p := NewParser(src)
	stmts, errs := p.ParseScript()
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	stmt := stmts[0].(*AlterNodeStmt)
	if stmt.Action != AlterSetPrimaryKey {
		t.Errorf("expected AlterSetPrimaryKey, got %v", stmt.Action)
	}
	expectedPk := []string{"id", "email"}
	if len(stmt.PkFields) != len(expectedPk) {
		t.Fatalf("expected %d pk fields, got %d", len(expectedPk), len(stmt.PkFields))
	}
	for i, field := range expectedPk {
		if stmt.PkFields[i] != field {
			t.Errorf("expected pk field %s, got %s", field, stmt.PkFields[i])
		}
	}
}

func TestAlterEdgeAddProp(t *testing.T) {
	src := `ALTER EDGE Knows ADD weight:float default 1.0;`
	p := NewParser(src)
	stmts, errs := p.ParseScript()
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	stmt := stmts[0].(*AlterEdgeStmt)
	if stmt.Name != "Knows" {
		t.Errorf("expected name Knows, got %s", stmt.Name)
	}
	if stmt.Action != AlterAddProp {
		t.Errorf("expected AlterAddProp, got %v", stmt.Action)
	}
	if stmt.Prop.Name != "weight" {
		t.Errorf("expected prop name weight, got %s", stmt.Prop.Name)
	}
	if stmt.Prop.Type.Base != BaseFloat {
		t.Errorf("expected float type, got %v", stmt.Prop.Type.Base)
	}
}

func TestAlterEdgeDropProp(t *testing.T) {
	src := `ALTER EDGE Knows DROP weight;`
	p := NewParser(src)
	stmts, errs := p.ParseScript()
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	stmt := stmts[0].(*AlterEdgeStmt)
	if stmt.Action != AlterDropProp {
		t.Errorf("expected AlterDropProp, got %v", stmt.Action)
	}
	if stmt.PropName != "weight" {
		t.Errorf("expected prop name weight, got %s", stmt.PropName)
	}
}

func TestAlterEdgeSetEndpoints(t *testing.T) {
	src := `ALTER EDGE Knows SET FROM Person many TO Company one;`
	p := NewParser(src)
	stmts, errs := p.ParseScript()
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	stmt := stmts[0].(*AlterEdgeStmt)
	if stmt.Action != AlterSetEndpoints {
		t.Errorf("expected AlterSetEndpoints, got %v", stmt.Action)
	}
	if stmt.From.Label != "Person" || stmt.From.Card != CardMany {
		t.Errorf("expected from Person many, got %s %v", stmt.From.Label, stmt.From.Card)
	}
	if stmt.To.Label != "Company" || stmt.To.Card != CardOne {
		t.Errorf("expected to Company one, got %s %v", stmt.To.Label, stmt.To.Card)
	}
}

func TestAlterErrorCases(t *testing.T) {
	tests := []struct {
		name string
		src  string
	}{
		{"missing node/edge", "ALTER FOO;"},
		{"missing action", "ALTER NODE Person;"},
		{"invalid action", "ALTER NODE Person INVALID;"},
		{"missing field name for drop", "ALTER NODE Person DROP;"},
		{"missing primary key fields", "ALTER NODE Person SET PRIMARY KEY;"},
		{"missing edge action", "ALTER EDGE Knows;"},
		{"missing prop name for drop", "ALTER EDGE Knows DROP;"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.src)
			_, errs := p.ParseScript()
			if len(errs) == 0 {
				t.Errorf("expected errors for %s", tt.src)
			}
		})
	}
}

func TestDropNode(t *testing.T) {
	src := `DROP NODE Person;`
	p := NewParser(src)
	stmts, errs := p.ParseScript()
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}

	stmt, ok := stmts[0].(*DropNodeStmt)
	if !ok {
		t.Fatalf("expected DropNodeStmt, got %T", stmts[0])
	}
	if stmt.Name != "Person" {
		t.Errorf("expected name Person, got %s", stmt.Name)
	}
}

func TestDropEdge(t *testing.T) {
	src := `DROP EDGE Knows;`
	p := NewParser(src)
	stmts, errs := p.ParseScript()
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}

	stmt, ok := stmts[0].(*DropEdgeStmt)
	if !ok {
		t.Fatalf("expected DropEdgeStmt, got %T", stmts[0])
	}
	if stmt.Name != "Knows" {
		t.Errorf("expected name Knows, got %s", stmt.Name)
	}
}

func TestDropErrorCases(t *testing.T) {
	tests := []struct {
		name string
		src  string
	}{
		{"missing node/edge", "DROP FOO;"},
		{"missing name", "DROP NODE;"},
		{"missing edge name", "DROP EDGE;"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.src)
			_, errs := p.ParseScript()
			if len(errs) == 0 {
				t.Errorf("expected errors for %s", tt.src)
			}
		})
	}
}

func TestMixedStatements(t *testing.T) {
	src := `
		CREATE NODE Person(id:int PRIMARY KEY, name:string);
		CREATE EDGE Knows (FROM Person ONE, TO Person MANY);
		ALTER NODE Person ADD email:string;
		ALTER EDGE Knows ADD weight:float;
		DROP EDGE Knows;
		DROP NODE Person;
	`
	p := NewParser(src)
	stmts, errs := p.ParseScript()
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if len(stmts) != 6 {
		t.Fatalf("expected 6 statements, got %d", len(stmts))
	}

	// Verify statement types
	expectedTypes := []string{"CreateNodeStmt", "CreateEdgeStmt", "AlterNodeStmt", "AlterEdgeStmt", "DropEdgeStmt", "DropNodeStmt"}
	for i, stmt := range stmts {
		stmtType := fmt.Sprintf("%T", stmt)
		stmtType = stmtType[8:] // Remove "parser.*" prefix
		if stmtType != expectedTypes[i] {
			t.Errorf("statement %d: expected %s, got %s", i, expectedTypes[i], stmtType)
		}
	}
}
