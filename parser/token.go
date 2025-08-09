package parser

type TokenType int

const (
	// Special
	EOF TokenType = iota
	ILLEGAL

	// Identifiers + literals
	IDENT  // Person, email, ...
	NUMBER // 42, 3.14
	STRING // 'hello'
	BOOL   // true, false
	NULL   // null

	// Keywords (normalized to upper case)
	CREATE
	NODE
	EDGE
	FROM
	TO
	PROPS
	PRIMARY
	KEY
	UNIQUE
	NOT
	NULLKW
	DEFAULT
	CHECK
	ALTER
	DROP
	INDEX
	ON
	ONE
	MANY
	ARRAY
	ENUM
	SHOW
	DESCRIBE
	TYPEKW // TYPE
	DATE
	TIME
	DATETIME
	JSON
	BLOB
	INT
	FLOAT
	STRINGKW
	TEXT
	BOOLKW
	UUID

	// Symbols
	LPAREN // (
	RPAREN // )
	LT     // <
	GT     // >
	COMMA  // ,
	SEMI   // ;
	COLON  // :
	QUOTE  // `
)

type Token struct {
	Type   TokenType
	Lit    string
	Line   int
	Column int
}
