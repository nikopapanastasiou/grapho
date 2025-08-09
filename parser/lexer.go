package parser

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

type Lexer struct {
	input string
	pos   int
	start int
	width int
	line  int
	col   int
}

func NewLexer(input string) *Lexer {
	return &Lexer{
		input: input,
		line:  1,
		col:   1,
	}
}

func (l *Lexer) NextToken() Token {
	l.skipWhitespace()
	l.start = l.pos

	if l.pos >= len(l.input) {
		return l.makeToken(EOF, "")
	}

	// Handle comments
	if l.peek() == '-' && l.peekN(1) == '-' {
		l.skipLineComment()
		return l.NextToken()
	}
	if l.peek() == '/' && l.peekN(1) == '*' {
		if err := l.skipBlockComment(); err != nil {
			return l.errorToken(err.Error())
		}
		return l.NextToken()
	}

	ch := l.peek()

	// Symbols & punctuation
	switch ch {
	case '(':
		l.advance()
		return l.makeToken(LPAREN, "(")
	case ')':
		l.advance()
		return l.makeToken(RPAREN, ")")
	case '<':
		l.advance()
		return l.makeToken(LT, "<")
	case '>':
		l.advance()
		return l.makeToken(GT, ">")
	case ',':
		l.advance()
		return l.makeToken(COMMA, ",")
	case ';':
		l.advance()
		return l.makeToken(SEMI, ";")
	case ':':
		l.advance()
		return l.makeToken(COLON, ":")
	case '`':
		return l.lexQuotedIdent()
	case '\'':
		return l.lexString()
	}

	// Identifiers / keywords / booleans / null
	if isIdentStart(ch) {
		return l.lexIdentOrKeyword()
	}

	// Numbers
	if unicode.IsDigit(ch) {
		return l.lexNumber()
	}

	// Unknown
	l.advance()
	return l.errorToken(fmt.Sprintf("unexpected character: %q", ch))
}

func (l *Lexer) lexIdentOrKeyword() Token {
	for isIdentPart(l.peek()) {
		l.advance()
	}
	lit := l.input[l.start:l.pos]
	tokType := LookupIdent(lit)
	if tokType == BOOL {
		return l.makeToken(BOOL, strings.ToLower(lit))
	}
	if tokType == NULLKW {
		return l.makeToken(NULL, strings.ToLower(lit))
	}
	return l.makeToken(tokType, lit)
}

func (l *Lexer) lexQuotedIdent() Token {
	l.advance() // skip opening backtick
	for {
		if l.pos >= len(l.input) {
			return l.errorToken("unterminated quoted identifier")
		}
		if l.peek() == '`' {
			break
		}
		l.advance()
	}
	lit := l.input[l.start+1 : l.pos]
	l.advance() // skip closing backtick
	return l.makeToken(IDENT, lit)
}

func (l *Lexer) lexString() Token {
	l.advance() // skip opening quote
	var val []rune
	for {
		if l.pos >= len(l.input) {
			return l.errorToken("unterminated string literal")
		}
		if l.peek() == '\'' {
			if l.peekN(1) == '\'' { // escaped single quote
				val = append(val, '\'')
				l.advance()
				l.advance()
				continue
			}
			break
		}
		val = append(val, l.peek())
		l.advance()
	}
	l.advance() // skip closing quote
	return l.makeToken(STRING, string(val))
}

func (l *Lexer) lexNumber() Token {
	for unicode.IsDigit(l.peek()) {
		l.advance()
	}
	if l.peek() == '.' {
		l.advance()
		for unicode.IsDigit(l.peek()) {
			l.advance()
		}
	}
	return l.makeToken(NUMBER, l.input[l.start:l.pos])
}

func (l *Lexer) skipWhitespace() {
	for {
		if l.pos >= len(l.input) {
			return
		}
		ch := l.peek()
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
			l.advance()
			continue
		}
		return
	}
}

func (l *Lexer) skipLineComment() {
	for l.pos < len(l.input) && l.peek() != '\n' {
		l.advance()
	}
}

func (l *Lexer) skipBlockComment() error {
	l.advance() // skip '/'
	l.advance() // skip '*'
	for {
		if l.pos >= len(l.input) {
			return fmt.Errorf("unterminated block comment")
		}
		if l.peek() == '*' && l.peekN(1) == '/' {
			l.advance()
			l.advance()
			return nil
		}
		l.advance()
	}
}

func (l *Lexer) makeToken(t TokenType, lit string) Token {
	return Token{
		Type:   t,
		Lit:    lit,
		Line:   l.line,
		Column: l.col - (l.pos - l.start),
	}
}

func (l *Lexer) errorToken(msg string) Token {
	return Token{
		Type:   ILLEGAL,
		Lit:    msg,
		Line:   l.line,
		Column: l.col,
	}
}

func (l *Lexer) advance() {
	if l.pos >= len(l.input) {
		return
	}
	r, w := utf8.DecodeRuneInString(l.input[l.pos:])
	l.pos += w
	l.width = w
	if r == '\n' {
		l.line++
		l.col = 1
	} else {
		l.col++
	}
}

func (l *Lexer) peek() rune {
	if l.pos >= len(l.input) {
		return 0
	}
	r, _ := utf8.DecodeRuneInString(l.input[l.pos:])
	return r
}

func (l *Lexer) peekN(n int) rune {
	p := l.pos
	for i := 0; i < n; i++ {
		if p >= len(l.input) {
			return 0
		}
		_, w := utf8.DecodeRuneInString(l.input[p:])
		p += w
	}
	if p >= len(l.input) {
		return 0
	}
	r, _ := utf8.DecodeRuneInString(l.input[p:])
	return r
}

func isIdentStart(r rune) bool {
	return unicode.IsLetter(r) || r == '_'
}

func isIdentPart(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_'
}
