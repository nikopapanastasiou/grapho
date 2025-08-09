package parser

import "strings"

var keywords = map[string]TokenType{
	"CREATE":   CREATE,
	"NODE":     NODE,
	"EDGE":     EDGE,
	"FROM":     FROM,
	"TO":       TO,
	"PROPS":    PROPS,
	"PRIMARY":  PRIMARY,
	"KEY":      KEY,
	"UNIQUE":   UNIQUE,
	"NOT":      NOT,
	"NULL":     NULLKW,
	"DEFAULT":  DEFAULT,
	"CHECK":    CHECK,
	"ALTER":    ALTER,
	"DROP":     DROP,
	"ADD":      ADD,
	"MODIFY":   MODIFY,
	"SET":      SET,
	"INDEX":    INDEX,
	"ON":       ON,
	"ONE":      ONE,
	"MANY":     MANY,
	"ARRAY":    ARRAY,
	"ENUM":     ENUM,
	"SHOW":     SHOW,
	"DESCRIBE": DESCRIBE,
	"TYPE":     TYPEKW,
	"DATE":     DATE,
	"TIME":     TIME,
	"DATETIME": DATETIME,
	"JSON":     JSON,
	"BLOB":     BLOB,
	"INT":      INT,
	"FLOAT":    FLOAT,
	"STRING":   STRINGKW,
	"TEXT":     TEXT,
	"BOOL":     BOOLKW,
	"UUID":     UUID,
	"TRUE":     BOOL,
	"FALSE":    BOOL,
}

func LookupIdent(ident string) TokenType {
	up := strings.ToUpper(ident)
	if tok, ok := keywords[up]; ok {
		return tok
	}
	return IDENT
}
