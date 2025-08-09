package parser

import "fmt"

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
	ADD
	MODIFY
	SET
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
	
	// DML keywords
	INSERT
	UPDATE
	DELETE
	MATCH
	WHERE
	RETURN

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

// String returns a human-readable name for the token type
func (tt TokenType) String() string {
	switch tt {
	case EOF:
		return "EOF"
	case ILLEGAL:
		return "ILLEGAL"
	case IDENT:
		return "identifier"
	case NUMBER:
		return "number"
	case STRING:
		return "string"
	case BOOL:
		return "boolean"
	case NULL:
		return "null"
	case CREATE:
		return "CREATE"
	case NODE:
		return "NODE"
	case EDGE:
		return "EDGE"
	case FROM:
		return "FROM"
	case TO:
		return "TO"
	case PROPS:
		return "PROPS"
	case PRIMARY:
		return "PRIMARY"
	case KEY:
		return "KEY"
	case UNIQUE:
		return "UNIQUE"
	case NOT:
		return "NOT"
	case NULLKW:
		return "NULL"
	case DEFAULT:
		return "DEFAULT"
	case CHECK:
		return "CHECK"
	case ALTER:
		return "ALTER"
	case DROP:
		return "DROP"
	case ADD:
		return "ADD"
	case MODIFY:
		return "MODIFY"
	case SET:
		return "SET"
	case INDEX:
		return "INDEX"
	case ON:
		return "ON"
	case ONE:
		return "ONE"
	case MANY:
		return "MANY"
	case ARRAY:
		return "ARRAY"
	case ENUM:
		return "ENUM"
	case SHOW:
		return "SHOW"
	case DESCRIBE:
		return "DESCRIBE"
	case TYPEKW:
		return "TYPE"
	case DATE:
		return "DATE"
	case TIME:
		return "TIME"
	case DATETIME:
		return "DATETIME"
	case JSON:
		return "JSON"
	case BLOB:
		return "BLOB"
	case INT:
		return "INT"
	case FLOAT:
		return "FLOAT"
	case STRINGKW:
		return "STRING"
	case TEXT:
		return "TEXT"
	case BOOLKW:
		return "BOOL"
	case UUID:
		return "UUID"
	case INSERT:
		return "INSERT"
	case UPDATE:
		return "UPDATE"
	case DELETE:
		return "DELETE"
	case MATCH:
		return "MATCH"
	case WHERE:
		return "WHERE"
	case RETURN:
		return "RETURN"
	case LPAREN:
		return "("
	case RPAREN:
		return ")"
	case LT:
		return "<"
	case GT:
		return ">"
	case COMMA:
		return ","
	case SEMI:
		return ";"
	case COLON:
		return ":"
	case QUOTE:
		return "`"
	default:
		return fmt.Sprintf("TokenType(%d)", int(tt))
	}
}
