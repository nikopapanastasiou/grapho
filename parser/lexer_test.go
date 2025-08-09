package parser

import (
	"strings"
	"testing"
)

func collectTokens(input string) []Token {
	l := NewLexer(input)
	var toks []Token
	for {
		tok := l.NextToken()
		toks = append(toks, tok)
		if tok.Type == EOF || tok.Type == ILLEGAL {
			break
		}
	}
	return toks
}

func assertTokens(t *testing.T, input string, want []Token) {
	got := collectTokens(input)
	if len(got) != len(want) {
		t.Fatalf("token count mismatch: got %d, want %d\nGOT: %#v\nWANT: %#v",
			len(got), len(want), got, want)
	}
	for i := range want {
		if got[i].Type != want[i].Type || got[i].Lit != want[i].Lit {
			t.Errorf("token %d mismatch: got (%v,%q), want (%v,%q)",
				i, got[i].Type, got[i].Lit, want[i].Type, want[i].Lit)
		}
	}
}

func TestSimpleCreateNode(t *testing.T) {
	input := `CREATE NODE Person (id: uuid PRIMARY KEY);`

	want := []Token{
		{Type: CREATE, Lit: "CREATE"},
		{Type: NODE, Lit: "NODE"},
		{Type: IDENT, Lit: "Person"},
		{Type: LPAREN, Lit: "("},
		{Type: IDENT, Lit: "id"},
		{Type: COLON, Lit: ":"},
		{Type: UUID, Lit: "uuid"},
		{Type: PRIMARY, Lit: "PRIMARY"},
		{Type: KEY, Lit: "KEY"},
		{Type: RPAREN, Lit: ")"},
		{Type: SEMI, Lit: ";"},
		{Type: EOF, Lit: ""},
	}
	assertTokens(t, input, want)
}

func TestStringLiteral(t *testing.T) {
	input := `name: string DEFAULT 'Alice''s account'`
	want := []Token{
		{Type: IDENT, Lit: "name"},
		{Type: COLON, Lit: ":"},
		{Type: STRINGKW, Lit: "string"},
		{Type: DEFAULT, Lit: "DEFAULT"},
		{Type: STRING, Lit: "Alice's account"},
		{Type: EOF, Lit: ""},
	}
	assertTokens(t, input, want)
}

func TestCommentsAndWhitespace(t *testing.T) {
	input := `
	-- this is a comment
	CREATE NODE X /* inline comment */ (y: int);
	`
	want := []Token{
		{Type: CREATE, Lit: "CREATE"},
		{Type: NODE, Lit: "NODE"},
		{Type: IDENT, Lit: "X"},
		{Type: LPAREN, Lit: "("},
		{Type: IDENT, Lit: "y"},
		{Type: COLON, Lit: ":"},
		{Type: INT, Lit: "int"},
		{Type: RPAREN, Lit: ")"},
		{Type: SEMI, Lit: ";"},
		{Type: EOF, Lit: ""},
	}
	assertTokens(t, input, want)
}

func TestQuotedIdentifier(t *testing.T) {
	input := "CREATE NODE `Weird Name` (id: uuid PRIMARY KEY);"
	want := []Token{
		{Type: CREATE, Lit: "CREATE"},
		{Type: NODE, Lit: "NODE"},
		{Type: IDENT, Lit: "Weird Name"},
		{Type: LPAREN, Lit: "("},
		{Type: IDENT, Lit: "id"},
		{Type: COLON, Lit: ":"},
		{Type: UUID, Lit: "uuid"},
		{Type: PRIMARY, Lit: "PRIMARY"},
		{Type: KEY, Lit: "KEY"},
		{Type: RPAREN, Lit: ")"},
		{Type: SEMI, Lit: ";"},
		{Type: EOF, Lit: ""},
	}
	assertTokens(t, input, want)
}

func TestIllegalToken(t *testing.T) {
	input := "CREATE NODE Person 💥"
	toks := collectTokens(input)
	last := toks[len(toks)-1]
	if last.Type != ILLEGAL {
		t.Fatalf("expected ILLEGAL, got %v", last.Type)
	}
}

func TestNumbers(t *testing.T) {
	input := `x: int, y: float, z: 123, w: 45.67;`
	want := []Token{
		{Type: IDENT, Lit: "x"},
		{Type: COLON, Lit: ":"},
		{Type: INT, Lit: "int"},
		{Type: COMMA, Lit: ","},
		{Type: IDENT, Lit: "y"},
		{Type: COLON, Lit: ":"},
		{Type: FLOAT, Lit: "float"},
		{Type: COMMA, Lit: ","},
		{Type: IDENT, Lit: "z"},
		{Type: COLON, Lit: ":"},
		{Type: NUMBER, Lit: "123"},
		{Type: COMMA, Lit: ","},
		{Type: IDENT, Lit: "w"},
		{Type: COLON, Lit: ":"},
		{Type: NUMBER, Lit: "45.67"},
		{Type: SEMI, Lit: ";"},
		{Type: EOF, Lit: ""},
	}
	assertTokens(t, input, want)
}

func TestBooleansAndNullNormalization(t *testing.T) {
	input := `flag: bool DEFAULT TRUE, n: int DEFAULT null, f: bool DEFAULT false;`
	want := []Token{
		{Type: IDENT, Lit: "flag"},
		{Type: COLON, Lit: ":"},
		{Type: BOOLKW, Lit: "bool"},
		{Type: DEFAULT, Lit: "DEFAULT"},
		{Type: BOOL, Lit: "true"},
		{Type: COMMA, Lit: ","},
		{Type: IDENT, Lit: "n"},
		{Type: COLON, Lit: ":"},
		{Type: INT, Lit: "int"},
		{Type: DEFAULT, Lit: "DEFAULT"},
		{Type: NULL, Lit: "null"},
		{Type: COMMA, Lit: ","},
		{Type: IDENT, Lit: "f"},
		{Type: COLON, Lit: ":"},
		{Type: BOOLKW, Lit: "bool"},
		{Type: DEFAULT, Lit: "DEFAULT"},
		{Type: BOOL, Lit: "false"},
		{Type: SEMI, Lit: ";"},
		{Type: EOF, Lit: ""},
	}
	assertTokens(t, input, want)
}

func TestSymbols(t *testing.T) {
	input := `( ) < > , ; :`
	want := []Token{
		{Type: LPAREN, Lit: "("},
		{Type: RPAREN, Lit: ")"},
		{Type: LT, Lit: "<"},
		{Type: GT, Lit: ">"},
		{Type: COMMA, Lit: ","},
		{Type: SEMI, Lit: ";"},
		{Type: COLON, Lit: ":"},
		{Type: EOF, Lit: ""},
	}
	assertTokens(t, input, want)
}

func TestUnterminatedString(t *testing.T) {
	input := `'abc`
	toks := collectTokens(input)
	last := toks[len(toks)-1]
	if last.Type != ILLEGAL {
		t.Fatalf("expected ILLEGAL, got %v", last.Type)
	}
	if !strings.Contains(strings.ToLower(last.Lit), "unterminated string") {
		t.Fatalf("unexpected error message: %q", last.Lit)
	}
}

func TestUnterminatedBlockComment(t *testing.T) {
	input := `/* comment`
	toks := collectTokens(input)
	last := toks[len(toks)-1]
	if last.Type != ILLEGAL {
		t.Fatalf("expected ILLEGAL, got %v", last.Type)
	}
	if !strings.Contains(strings.ToLower(last.Lit), "unterminated block comment") {
		t.Fatalf("unexpected error message: %q", last.Lit)
	}
}

func TestUnterminatedQuotedIdent(t *testing.T) {
	input := "`abc"
	toks := collectTokens(input)
	last := toks[len(toks)-1]
	if last.Type != ILLEGAL {
		t.Fatalf("expected ILLEGAL, got %v", last.Type)
	}
	if !strings.Contains(strings.ToLower(last.Lit), "unterminated quoted identifier") {
		t.Fatalf("unexpected error message: %q", last.Lit)
	}
}
