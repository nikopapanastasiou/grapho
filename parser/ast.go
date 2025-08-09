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
