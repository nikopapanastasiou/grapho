package catalog

import "slices"

type Cardinality int

const (
	One Cardinality = iota
	Many
)

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
	BaseArray // Elem != nil defines the element
	BaseEnum  // EnumVals non-empty
)

type TypeSpec struct {
	Base     BaseType
	Elem     *TypeSpec // for arrays
	EnumVals []string  // for enums
}

type FieldSpec struct {
	Name    string
	Type    TypeSpec
	Unique  bool
	NotNull bool
	// NOTE: Defaults are stored as raw string form for now; coercion happens in semantic/DML layer
	DefaultRaw *string
}

type NodeType struct {
	Name   string
	Fields map[string]FieldSpec
	PK     string // "" => internal ID
	// Index metadata (runtime index handles live elsewhere)
	Indexes map[string]IndexSpec // by field name
}

type EdgeEndpoint struct {
	Label string
	Card  Cardinality
}

type EdgeType struct {
	Name  string
	From  EdgeEndpoint
	To    EdgeEndpoint
	Props map[string]FieldSpec
	// Multiplicity/uniqueness rules could be expanded later
}

type IndexSpec struct {
	Field  string
	Unique bool
}

type Catalog struct {
	Version uint64
	Nodes   map[string]*NodeType
	Edges   map[string]*EdgeType
}

func (c *Catalog) Clone() *Catalog {
	nn := make(map[string]*NodeType, len(c.Nodes))
	for k, v := range c.Nodes {
		nn[k] = cloneNodeType(v)
	}
	ee := make(map[string]*EdgeType, len(c.Edges))
	for k, v := range c.Edges {
		ee[k] = cloneEdgeType(v)
	}
	return &Catalog{
		Version: c.Version,
		Nodes:   nn,
		Edges:   ee,
	}
}

func cloneNodeType(n *NodeType) *NodeType {
	if n == nil {
		return nil
	}
	f := make(map[string]FieldSpec, len(n.Fields))
	for k, v := range n.Fields {
		// copy default pointer
		var d *string
		if v.DefaultRaw != nil {
			tmp := *v.DefaultRaw
			d = &tmp
		}
		f[k] = FieldSpec{
			Name:       v.Name,
			Type:       cloneType(v.Type),
			Unique:     v.Unique,
			NotNull:    v.NotNull,
			DefaultRaw: d,
		}
	}
	idx := make(map[string]IndexSpec, len(n.Indexes))
	for k, v := range n.Indexes {
		idx[k] = v
	}
	return &NodeType{
		Name:    n.Name,
		Fields:  f,
		PK:      n.PK,
		Indexes: idx,
	}
}

func cloneEdgeType(e *EdgeType) *EdgeType {
	if e == nil {
		return nil
	}
	props := make(map[string]FieldSpec, len(e.Props))
	for k, v := range e.Props {
		var d *string
		if v.DefaultRaw != nil {
			tmp := *v.DefaultRaw
			d = &tmp
		}
		props[k] = FieldSpec{
			Name:       v.Name,
			Type:       cloneType(v.Type),
			Unique:     v.Unique,
			NotNull:    v.NotNull,
			DefaultRaw: d,
		}
	}
	return &EdgeType{
		Name:  e.Name,
		From:  e.From,
		To:    e.To,
		Props: props,
	}
}

func cloneType(t TypeSpec) TypeSpec {
	var elem *TypeSpec
	if t.Elem != nil {
		cp := cloneType(*t.Elem)
		elem = &cp
	}
	return TypeSpec{
		Base:     t.Base,
		Elem:     elem,
		EnumVals: slices.Clone(t.EnumVals),
	}
}

// NewEmpty returns an initial empty catalog (Version=0).
func NewEmpty() *Catalog {
	return &Catalog{
		Version: 0,
		Nodes:   map[string]*NodeType{},
		Edges:   map[string]*EdgeType{},
	}
}
