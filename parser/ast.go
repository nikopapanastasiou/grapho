package parser

type Stmt interface {
	node()
	Pos() (line, col int)
}

type BaseType int

const (
	BaseString BaseType = iota
	BaseText
	BaseInt
	BaseFloat
	BaseBool
	BaseUUID
	BaseDate
	BaseTime
	BaseDateTime
	BaseJSON
	BaseBlob
)

type TypeSpec struct {
	Base     BaseType
	Elem     *TypeSpec // for array<>
	EnumVals []string  // for enum<>
}

type FieldDef struct {
	Name       string
	Type       TypeSpec
	PrimaryKey bool
	Unique     bool
	NotNull    bool
	Default    *Literal
	Line, Col  int
}

type CreateNodeStmt struct {
	Name      string
	Fields    []FieldDef
	Line, Col int
}

func (*CreateNodeStmt) node()             {}
func (s *CreateNodeStmt) Pos() (int, int) { return s.Line, s.Col }

type Cardinality int

const (
	CardOne Cardinality = iota
	CardMany
)

type Endpoint struct {
	Label string
	Card  Cardinality
}

type CreateEdgeStmt struct {
	Name      string
	From      Endpoint
	To        Endpoint
	Props     []FieldDef // optional
	Line, Col int
}

func (*CreateEdgeStmt) node()             {}
func (s *CreateEdgeStmt) Pos() (int, int) { return s.Line, s.Col }

type LiteralKind int

const (
	LitString LiteralKind = iota
	LitNumber
	LitBool
	LitNull
)

type Literal struct {
	Kind      LiteralKind
	Text      string // original text (already unescaped for strings)
	Line, Col int
}

// ALTER statement types

type AlterAction int

const (
	AlterAddField AlterAction = iota
	AlterDropField
	AlterModifyField
	AlterSetPrimaryKey
	AlterAddProp
	AlterDropProp
	AlterModifyProp
	AlterSetEndpoints
)

type AlterNodeStmt struct {
	Name      string
	Action    AlterAction
	Field     *FieldDef // for add/modify field
	FieldName string    // for drop field
	PkFields  []string  // for set primary key
	Line, Col int
}

func (*AlterNodeStmt) node()             {}
func (s *AlterNodeStmt) Pos() (int, int) { return s.Line, s.Col }

type AlterEdgeStmt struct {
	Name      string
	Action    AlterAction
	Prop      *FieldDef  // for add/modify prop
	PropName  string     // for drop prop
	From      *Endpoint  // for set endpoints
	To        *Endpoint  // for set endpoints
	Line, Col int
}

func (*AlterEdgeStmt) node()             {}
func (s *AlterEdgeStmt) Pos() (int, int) { return s.Line, s.Col }

// DROP statement types

type DropNodeStmt struct {
	Name      string
	Line, Col int
}

func (*DropNodeStmt) node()             {}
func (s *DropNodeStmt) Pos() (int, int) { return s.Line, s.Col }

type DropEdgeStmt struct {
	Name      string
	Line, Col int
}

func (*DropEdgeStmt) node()             {}
func (s *DropEdgeStmt) Pos() (int, int) { return s.Line, s.Col }

// DML statement types

// Property represents a key-value pair for node/edge properties
type Property struct {
	Name      string
	Value     *Literal
	Line, Col int
}

// InsertNodeStmt represents INSERT NODE statement
type InsertNodeStmt struct {
	NodeType   string
	Properties []Property
	Line, Col  int
}

func (*InsertNodeStmt) node()             {}
func (s *InsertNodeStmt) Pos() (int, int) { return s.Line, s.Col }

// InsertEdgeStmt represents INSERT EDGE statement
type InsertEdgeStmt struct {
	EdgeType   string
	FromNode   *NodeRef
	ToNode     *NodeRef
	Properties []Property
	Line, Col  int
}

func (*InsertEdgeStmt) node()             {}
func (s *InsertEdgeStmt) Pos() (int, int) { return s.Line, s.Col }

// NodeRef represents a reference to a node (by ID or property match)
type NodeRef struct {
	NodeType   string
	ID         *Literal // Direct ID reference
	Properties []Property // Property-based match
	Line, Col  int
}

// UpdateNodeStmt represents UPDATE NODE statement
type UpdateNodeStmt struct {
	NodeType   string
	Where      []Property // WHERE conditions
	Set        []Property // SET assignments
	Line, Col  int
}

func (*UpdateNodeStmt) node()             {}
func (s *UpdateNodeStmt) Pos() (int, int) { return s.Line, s.Col }

// UpdateEdgeStmt represents UPDATE EDGE statement
type UpdateEdgeStmt struct {
	EdgeType   string
	Where      []Property // WHERE conditions
	Set        []Property // SET assignments
	Line, Col  int
}

func (*UpdateEdgeStmt) node()             {}
func (s *UpdateEdgeStmt) Pos() (int, int) { return s.Line, s.Col }

// DeleteNodeStmt represents DELETE NODE statement
type DeleteNodeStmt struct {
	NodeType   string
	Where      []Property // WHERE conditions
	Line, Col  int
}

func (*DeleteNodeStmt) node()             {}
func (s *DeleteNodeStmt) Pos() (int, int) { return s.Line, s.Col }

// DeleteEdgeStmt represents DELETE EDGE statement
type DeleteEdgeStmt struct {
	EdgeType   string
	Where      []Property // WHERE conditions
	Line, Col  int
}

func (*DeleteEdgeStmt) node()             {}
func (s *DeleteEdgeStmt) Pos() (int, int) { return s.Line, s.Col }

// MatchStmt represents MATCH statement for querying
type MatchStmt struct {
	Pattern    []MatchElement
	Where      []Property // Optional WHERE conditions
	Return     []string   // RETURN fields
	Line, Col  int
}

func (*MatchStmt) node()             {}
func (s *MatchStmt) Pos() (int, int) { return s.Line, s.Col }

// MatchElement represents a node or edge pattern in MATCH
type MatchElement struct {
	Type       string     // Node or edge type
	Alias      string     // Optional alias
	Properties []Property // Property constraints
	IsEdge     bool       // true for edges, false for nodes
	Line, Col  int
}
