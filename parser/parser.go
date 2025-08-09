package parser

import (
	"fmt"
)

type Parser struct {
	l   *Lexer
	tok Token
	// one-token lookahead only; lexer already provides tokens
	errors []ParseError
}

type ParseError struct {
	Line int
	Col  int
	Msg  string
}

func (e ParseError) Error() string { return fmt.Sprintf("%d:%d: %s", e.Line, e.Col, e.Msg) }

func NewParser(input string) *Parser {
	lex := NewLexer(input)
	p := &Parser{l: lex}
	p.next() // prime first token
	return p
}

func (p *Parser) next() {
	p.tok = p.l.NextToken()
}

func (p *Parser) expect(tt TokenType) Token {
	t := p.tok
	if t.Type != tt {
		p.errf(t.Line, t.Column, "expected %v, found %v (%q)", tt, t.Type, t.Lit)
	}
	p.next()
	return t
}

func (p *Parser) match(tt TokenType) bool {
	if p.tok.Type == tt {
		p.next()
		return true
	}
	return false
}

func (p *Parser) errf(line, col int, f string, args ...any) {
	p.errors = append(p.errors, ParseError{Line: line, Col: col, Msg: fmt.Sprintf(f, args...)})
	// best-effort recovery: advance to next ';' or EOF
	for p.tok.Type != SEMI && p.tok.Type != EOF {
		p.next()
	}
	if p.tok.Type == SEMI {
		p.next()
	}
}

func (p *Parser) Errors() []ParseError { return append([]ParseError(nil), p.errors...) }

/* ---------------------- entry points ---------------------- */

func (p *Parser) ParseScript() ([]Stmt, []ParseError) {
	var out []Stmt
	for p.tok.Type != EOF {
		// skip stray semicolons
		if p.match(SEMI) {
			continue
		}
		st := p.parseStmt()
		if st != nil {
			out = append(out, st)
			// require semicolon after each successful statement (recover if missing)
			if !p.match(SEMI) {
				t := p.tok
				p.errf(t.Line, t.Column, "missing ';' after statement")
			}
		}
	}
	return out, p.Errors()
}

func (p *Parser) parseStmt() Stmt {
	switch p.tok.Type {
	case CREATE:
		return p.parseCreate()
	case ALTER:
		return p.parseAlter()
	case DROP:
		return p.parseDrop()
	case INSERT:
		return p.parseInsert()
	case UPDATE:
		return p.parseUpdate()
	case DELETE:
		return p.parseDelete()
	case MATCH:
		return p.parseMatch()
	default:
		t := p.tok
		p.errf(t.Line, t.Column, "unexpected token %v at start of statement", t.Type)
		return nil
	}
}

func (p *Parser) parseCreate() Stmt {
	createTok := p.tok
	p.next()
	switch p.tok.Type {
	case NODE:
		p.next()
		return p.parseCreateNode(createTok.Line, createTok.Column)
	case EDGE:
		p.next()
		return p.parseCreateEdge(createTok.Line, createTok.Column)
	default:
		t := p.tok
		p.errf(t.Line, t.Column, "expected NODE or EDGE after CREATE")
		return nil
	}
}

/* ---------------------- CREATE NODE ----------------------- */

func (p *Parser) parseCreateNode(line, col int) *CreateNodeStmt {
	nameTok := p.expect(IDENT)
	stmt := &CreateNodeStmt{Name: nameTok.Lit, Line: line, Col: col}

	p.expect(LPAREN)
	// optional fields (allow empty list)
	if p.tok.Type != RPAREN {
		for {
			fd := p.parseFieldDef()
			if fd.Name != "" {
				stmt.Fields = append(stmt.Fields, fd)
			}
			if !p.match(COMMA) {
				break
			}
			// allow trailing comma before RPAREN
			if p.tok.Type == RPAREN {
				break
			}
		}
	}
	p.expect(RPAREN)
	return stmt
}

func (p *Parser) parseFieldDef() FieldDef {
	ident := p.expect(IDENT)
	fd := FieldDef{Name: ident.Lit, Line: ident.Line, Col: ident.Column}

	p.expect(COLON)
	ts := p.parseTypeSpec()
	fd.Type = ts

	// zero or more field options
loop:
	for {
		switch p.tok.Type {
		case PRIMARY:
			p.next()
			p.expect(KEY)
			fd.PrimaryKey = true
		case UNIQUE:
			p.next()
			fd.Unique = true
		case NOT:
			p.next()
			p.expect(NULL)
			fd.NotNull = true
		case DEFAULT:
			p.next()
			lit := p.parseLiteral()
			fd.Default = &lit
		default:
			break loop
		}
	}
	return fd
}

func (p *Parser) parseTypeSpec() TypeSpec {
	switch p.tok.Type {
	case STRINGKW:
		p.next()
		return TypeSpec{Base: BaseString}
	case TEXT:
		p.next()
		return TypeSpec{Base: BaseText}
	case INT:
		p.next()
		return TypeSpec{Base: BaseInt}
	case FLOAT:
		p.next()
		return TypeSpec{Base: BaseFloat}
	case BOOLKW:
		p.next()
		return TypeSpec{Base: BaseBool}
	case UUID:
		p.next()
		return TypeSpec{Base: BaseUUID}
	case DATE:
		p.next()
		return TypeSpec{Base: BaseDate}
	case TIME:
		p.next()
		return TypeSpec{Base: BaseTime}
	case DATETIME:
		p.next()
		return TypeSpec{Base: BaseDateTime}
	case JSON:
		p.next()
		return TypeSpec{Base: BaseJSON}
	case BLOB:
		p.next()
		return TypeSpec{Base: BaseBlob}

	case ARRAY:
		p.next()
		p.expect(LT)
		elem := p.parseTypeSpec()
		p.expect(GT)
		return TypeSpec{Base: BaseString, Elem: &elem} // BaseString placeholder: array-ness carried by Elem != nil

	case ENUM:
		p.next()
		p.expect(LT)
		var vals []string
		// at least one string
		s := p.expect(STRING)
		vals = append(vals, s.Lit)
		for p.match(COMMA) {
			s2 := p.expect(STRING)
			vals = append(vals, s2.Lit)
		}
		p.expect(GT)
		return TypeSpec{Base: BaseString, EnumVals: vals} // enums are strings with a closed set
	default:
		t := p.tok
		p.errf(t.Line, t.Column, "expected type, found %v", t.Type)
		// recover with a sentinel string type
		p.next()
		return TypeSpec{Base: BaseString}
	}
}

func (p *Parser) parseLiteral() Literal {
	t := p.tok
	switch t.Type {
	case STRING:
		p.next()
		return Literal{Kind: LitString, Text: t.Lit, Line: t.Line, Col: t.Column}
	case NUMBER:
		p.next()
		return Literal{Kind: LitNumber, Text: t.Lit, Line: t.Line, Col: t.Column}
	case BOOL:
		p.next()
		return Literal{Kind: LitBool, Text: t.Lit, Line: t.Line, Col: t.Column}
	case NULL:
		p.next()
		return Literal{Kind: LitNull, Text: "null", Line: t.Line, Col: t.Column}
	default:
		p.errf(t.Line, t.Column, "expected literal, found %v", t.Type)
		p.next()
		return Literal{Kind: LitNull, Text: "null", Line: t.Line, Col: t.Column}
	}
}

/* ---------------------- CREATE EDGE ----------------------- */

func (p *Parser) parseCreateEdge(line, col int) *CreateEdgeStmt {
	nameTok := p.expect(IDENT)
	stmt := &CreateEdgeStmt{Name: nameTok.Lit, Line: line, Col: col}

	p.expect(LPAREN)
	// FROM <label> [ONE|MANY] , TO <label> [ONE|MANY]
	p.expect(FROM)
	from := p.parseEndpoint()
	p.expect(COMMA)
	p.expect(TO)
	to := p.parseEndpoint()
	stmt.From, stmt.To = from, to

	// optional: , PROPS ( field_def, ... )
	if p.match(COMMA) {
		p.expect(PROPS)
		p.expect(LPAREN)
		if p.tok.Type != RPAREN {
			for {
				fd := p.parseFieldDef()
				if fd.Name != "" {
					stmt.Props = append(stmt.Props, fd)
				}
				if !p.match(COMMA) {
					break
				}
				if p.tok.Type == RPAREN {
					break
				}
			}
		}
		p.expect(RPAREN)
	}

	p.expect(RPAREN)
	return stmt
}

func (p *Parser) parseEndpoint() Endpoint {
	lbl := p.expect(IDENT)
	ep := Endpoint{Label: lbl.Lit, Card: CardOne}
	// optional multiplicity
	switch p.tok.Type {
	case ONE:
		p.next()
		ep.Card = CardOne
	case MANY:
		p.next()
		ep.Card = CardMany
	}
	return ep
}

/* ---------------------- ALTER statements ---------------------- */

func (p *Parser) parseAlter() Stmt {
	alterTok := p.tok
	p.next()
	switch p.tok.Type {
	case NODE:
		p.next()
		return p.parseAlterNode(alterTok.Line, alterTok.Column)
	case EDGE:
		p.next()
		return p.parseAlterEdge(alterTok.Line, alterTok.Column)
	default:
		t := p.tok
		p.errf(t.Line, t.Column, "expected NODE or EDGE after ALTER")
		return nil
	}
}

func (p *Parser) parseAlterNode(line, col int) *AlterNodeStmt {
	name := p.expect(IDENT)
	stmt := &AlterNodeStmt{
		Name: name.Lit,
		Line: line,
		Col:  col,
	}

	switch p.tok.Type {
	case ADD:
		p.next()
		stmt.Action = AlterAddField
		field := p.parseFieldDef()
		stmt.Field = &field
	case DROP:
		p.next()
		fieldName := p.expect(IDENT)
		stmt.Action = AlterDropField
		stmt.FieldName = fieldName.Lit
	case MODIFY:
		p.next()
		stmt.Action = AlterModifyField
		field := p.parseFieldDef()
		stmt.Field = &field
	case SET:
		p.next()
		p.expect(PRIMARY)
		p.expect(KEY)
		p.expect(LPAREN)

		// Parse primary key field list
		var pkFields []string
		for {
			fieldName := p.expect(IDENT)
			pkFields = append(pkFields, fieldName.Lit)
			if !p.match(COMMA) {
				break
			}
		}
		p.expect(RPAREN)

		stmt.Action = AlterSetPrimaryKey
		stmt.PkFields = pkFields
	default:
		t := p.tok
		p.errf(t.Line, t.Column, "expected ADD, DROP, MODIFY, or SET after ALTER NODE")
		return nil
	}

	return stmt
}

func (p *Parser) parseAlterEdge(line, col int) *AlterEdgeStmt {
	name := p.expect(IDENT)
	stmt := &AlterEdgeStmt{
		Name: name.Lit,
		Line: line,
		Col:  col,
	}

	switch p.tok.Type {
	case ADD:
		p.next()
		stmt.Action = AlterAddProp
		prop := p.parseFieldDef()
		stmt.Prop = &prop
	case DROP:
		p.next()
		propName := p.expect(IDENT)
		stmt.Action = AlterDropProp
		stmt.PropName = propName.Lit
	case MODIFY:
		p.next()
		stmt.Action = AlterModifyProp
		prop := p.parseFieldDef()
		stmt.Prop = &prop
	case SET:
		p.next()
		p.expect(FROM)
		from := p.parseEndpoint()
		p.expect(TO)
		to := p.parseEndpoint()

		stmt.Action = AlterSetEndpoints
		stmt.From = &from
		stmt.To = &to
	default:
		t := p.tok
		p.errf(t.Line, t.Column, "expected ADD, DROP, MODIFY, or SET after ALTER EDGE")
		return nil
	}

	return stmt
}

/* ---------------------- DROP statements ---------------------- */

func (p *Parser) parseDrop() Stmt {
	dropTok := p.tok
	p.next()
	switch p.tok.Type {
	case NODE:
		p.next()
		return p.parseDropNode(dropTok.Line, dropTok.Column)
	case EDGE:
		p.next()
		return p.parseDropEdge(dropTok.Line, dropTok.Column)
	default:
		t := p.tok
		p.errf(t.Line, t.Column, "expected NODE or EDGE after DROP")
		return nil
	}
}

func (p *Parser) parseDropNode(line, col int) *DropNodeStmt {
	name := p.expect(IDENT)
	return &DropNodeStmt{
		Name: name.Lit,
		Line: line,
		Col:  col,
	}
}

func (p *Parser) parseDropEdge(line, col int) *DropEdgeStmt {
	p.expect(IDENT) // edge name
	name := p.tok.Lit
	p.next()

	return &DropEdgeStmt{Name: name, Line: line, Col: col}
}

/* ---------------------- DML statements ---------------------- */

// parseInsert handles INSERT NODE and INSERT EDGE statements
func (p *Parser) parseInsert() Stmt {
	line, col := p.tok.Line, p.tok.Column
	p.expect(INSERT)

	switch p.tok.Type {
	case NODE:
		return p.parseInsertNode(line, col)
	case EDGE:
		return p.parseInsertEdge(line, col)
	default:
		p.errf(p.tok.Line, p.tok.Column, "expected NODE or EDGE after INSERT, found %v", p.tok.Type)
		return nil
	}
}

// parseInsertNode handles INSERT NODE statements
func (p *Parser) parseInsertNode(line, col int) *InsertNodeStmt {
	p.expect(NODE)

	// Parse node type
	nodeType := p.expect(IDENT).Lit

	// Parse optional properties
	var properties []Property
	if p.match(LPAREN) {
		properties = p.parsePropertyList()
		p.expect(RPAREN)
	}

	return &InsertNodeStmt{
		NodeType:   nodeType,
		Properties: properties,
		Line:       line,
		Col:        col,
	}
}

// parseInsertEdge handles INSERT EDGE statements
func (p *Parser) parseInsertEdge(line, col int) *InsertEdgeStmt {
	p.expect(EDGE)

	// Parse edge type
	edgeType := p.expect(IDENT).Lit

	// Parse FROM clause
	p.expect(FROM)
	fromNode := p.parseNodeRef()

	// Parse TO clause
	p.expect(TO)
	toNode := p.parseNodeRef()

	// Parse optional properties
	var properties []Property
	if p.match(LPAREN) {
		properties = p.parsePropertyList()
		p.expect(RPAREN)
	}

	return &InsertEdgeStmt{
		EdgeType:   edgeType,
		FromNode:   fromNode,
		ToNode:     toNode,
		Properties: properties,
		Line:       line,
		Col:        col,
	}
}

// parseUpdate handles UPDATE NODE and UPDATE EDGE statements
func (p *Parser) parseUpdate() Stmt {
	line, col := p.tok.Line, p.tok.Column
	p.expect(UPDATE)

	switch p.tok.Type {
	case NODE:
		return p.parseUpdateNode(line, col)
	case EDGE:
		return p.parseUpdateEdge(line, col)
	default:
		p.errf(p.tok.Line, p.tok.Column, "expected NODE or EDGE after UPDATE, found %v", p.tok.Type)
		return nil
	}
}

// parseUpdateNode handles UPDATE NODE statements
func (p *Parser) parseUpdateNode(line, col int) *UpdateNodeStmt {
	p.expect(NODE)

	// Parse node type
	nodeType := p.expect(IDENT).Lit

	// Parse SET clause
	p.expect(SET)
	setProps := p.parsePropertyList()

	// Parse optional WHERE clause
	var whereProps []Property
	if p.match(WHERE) {
		whereProps = p.parsePropertyList()
	}

	return &UpdateNodeStmt{
		NodeType: nodeType,
		Set:      setProps,
		Where:    whereProps,
		Line:     line,
		Col:      col,
	}
}

// parseUpdateEdge handles UPDATE EDGE statements
func (p *Parser) parseUpdateEdge(line, col int) *UpdateEdgeStmt {
	p.expect(EDGE)

	// Parse edge type
	edgeType := p.expect(IDENT).Lit

	// Parse SET clause
	p.expect(SET)
	setProps := p.parsePropertyList()

	// Parse optional WHERE clause
	var whereProps []Property
	if p.match(WHERE) {
		whereProps = p.parsePropertyList()
	}

	return &UpdateEdgeStmt{
		EdgeType: edgeType,
		Set:      setProps,
		Where:    whereProps,
		Line:     line,
		Col:      col,
	}
}

// parseDelete handles DELETE NODE and DELETE EDGE statements
func (p *Parser) parseDelete() Stmt {
	line, col := p.tok.Line, p.tok.Column
	p.expect(DELETE)

	switch p.tok.Type {
	case NODE:
		return p.parseDeleteNode(line, col)
	case EDGE:
		return p.parseDeleteEdge(line, col)
	default:
		p.errf(p.tok.Line, p.tok.Column, "expected NODE or EDGE after DELETE, found %v", p.tok.Type)
		return nil
	}
}

// parseDeleteNode handles DELETE NODE statements
func (p *Parser) parseDeleteNode(line, col int) *DeleteNodeStmt {
	p.expect(NODE)

	// Parse node type
	nodeType := p.expect(IDENT).Lit

	// Parse WHERE clause
	p.expect(WHERE)
	whereProps := p.parsePropertyList()

	return &DeleteNodeStmt{
		NodeType: nodeType,
		Where:    whereProps,
		Line:     line,
		Col:      col,
	}
}

// parseDeleteEdge handles DELETE EDGE statements
func (p *Parser) parseDeleteEdge(line, col int) *DeleteEdgeStmt {
	p.expect(EDGE)

	// Parse edge type
	edgeType := p.expect(IDENT).Lit

	// Parse WHERE clause
	p.expect(WHERE)
	whereProps := p.parsePropertyList()

	return &DeleteEdgeStmt{
		EdgeType: edgeType,
		Where:    whereProps,
		Line:     line,
		Col:      col,
	}
}

// parseMatch handles MATCH statements for querying
func (p *Parser) parseMatch() *MatchStmt {
	line, col := p.tok.Line, p.tok.Column
	p.expect(MATCH)

	// Parse pattern elements
	var pattern []MatchElement

	// Simple pattern parsing - can be extended for more complex patterns
	for p.tok.Type == IDENT {
		element := MatchElement{
			Type:   p.tok.Lit,
			IsEdge: false, // Simplified - assume nodes for now
			Line:   p.tok.Line,
			Col:    p.tok.Column,
		}
		p.next()

		// Optional alias
		if p.tok.Type == IDENT {
			element.Alias = p.tok.Lit
			p.next()
		}

		pattern = append(pattern, element)

		if !p.match(COMMA) {
			break
		}
	}

	// Parse optional WHERE clause
	var whereProps []Property
	if p.match(WHERE) {
		whereProps = p.parsePropertyList()
	}

	// Parse RETURN clause
	var returnFields []string
	if p.match(RETURN) {
		for {
			returnFields = append(returnFields, p.expect(IDENT).Lit)
			if !p.match(COMMA) {
				break
			}
		}
	}

	return &MatchStmt{
		Pattern: pattern,
		Where:   whereProps,
		Return:  returnFields,
		Line:    line,
		Col:     col,
	}
}

/* ---------------------- Helper functions ---------------------- */

// parsePropertyList parses a comma-separated list of property assignments
func (p *Parser) parsePropertyList() []Property {
	var properties []Property

	for {
		prop := Property{
			Name: p.expect(IDENT).Lit,
			Line: p.tok.Line,
			Col:  p.tok.Column,
		}

		p.expect(COLON)
		lit := p.parseLiteral()
		prop.Value = &lit

		properties = append(properties, prop)

		if !p.match(COMMA) {
			break
		}
	}

	return properties
}

// parseNodeRef parses a node reference (by ID or properties)
func (p *Parser) parseNodeRef() *NodeRef {
	nodeRef := &NodeRef{
		Line: p.tok.Line,
		Col:  p.tok.Column,
	}

	// Parse node type
	nodeRef.NodeType = p.expect(IDENT).Lit

	// Parse reference - either direct ID or property match
	if p.match(LPAREN) {
		if p.tok.Type == NUMBER || p.tok.Type == STRING {
			// Direct ID reference
			lit := p.parseLiteral()
			nodeRef.ID = &lit
		} else {
			// Property-based match
			nodeRef.Properties = p.parsePropertyList()
		}
		p.expect(RPAREN)
	}

	return nodeRef
}
