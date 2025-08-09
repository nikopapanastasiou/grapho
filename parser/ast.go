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
