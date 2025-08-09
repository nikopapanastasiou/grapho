package catalog

import (
	"errors"
	"fmt"
	"strings"
)

type DDLOp string

const (
	OpCreateNode DDLOp = "CREATE_NODE"
	OpCreateEdge DDLOp = "CREATE_EDGE"
	OpAlterNode  DDLOp = "ALTER_NODE"
	OpAlterEdge  DDLOp = "ALTER_EDGE"
	OpDropNode   DDLOp = "DROP_NODE"
	OpDropEdge   DDLOp = "DROP_EDGE"
	// (later) OpCreateIndex, OpDropIndex, ...
)

// Events are persisted to the catalog DDL log (JSON lines).
type DDLEvent struct {
	Op   DDLOp
	Stmt any // one of the payload structs below
}

// Payloads (mirror your RDCL AST structs but trimmed to what the catalog needs)
type CreateNodePayload struct {
	Name   string
	Fields []FieldPayload
}

type FieldPayload struct {
	Name       string
	Type       TypeSpec
	PrimaryKey bool
	Unique     bool
	NotNull    bool
	DefaultRaw *string
}

type CreateEdgePayload struct {
	Name  string
	From  EdgeEndpoint
	To    EdgeEndpoint
	Props []FieldPayload
}

// ALTER NODE payloads
type AlterNodePayload struct {
	Name    string
	Actions []NodeAlterAction
}

type NodeAlterAction struct {
	Type string // "ADD_FIELD", "DROP_FIELD", "MODIFY_FIELD", "SET_PRIMARY_KEY"

	// For ADD_FIELD and MODIFY_FIELD
	Field *FieldPayload

	// For DROP_FIELD and SET_PRIMARY_KEY
	FieldName string
}

// ALTER EDGE payloads
type AlterEdgePayload struct {
	Name    string
	Actions []EdgeAlterAction
}

type EdgeAlterAction struct {
	Type string // "ADD_PROP", "DROP_PROP", "MODIFY_PROP", "CHANGE_ENDPOINT"

	// For ADD_PROP and MODIFY_PROP
	Prop *FieldPayload

	// For DROP_PROP
	PropName string

	// For CHANGE_ENDPOINT
	Endpoint    string // "FROM" or "TO"
	NewEndpoint *EdgeEndpoint
}

// DROP payloads
type DropNodePayload struct {
	Name string
}

type DropEdgePayload struct {
	Name string
}

/* -------------------- Pure functional apply with validation -------------------- */

// ApplyCreateNode returns a new catalog (copy-on-write) with the node type added.
func ApplyCreateNode(c *Catalog, p CreateNodePayload) (*Catalog, error) {
	if err := validateCreateNode(c, p); err != nil {
		return nil, err
	}
	out := c.Clone()
	nt := &NodeType{
		Name:    p.Name,
		Fields:  map[string]FieldSpec{},
		PK:      "",
		Indexes: map[string]IndexSpec{},
	}
	for _, f := range p.Fields {
		if _, exists := nt.Fields[f.Name]; exists {
			return nil, fmt.Errorf("duplicate field %q", f.Name)
		}
		fs := FieldSpec{
			Name:       f.Name,
			Type:       f.Type,
			Unique:     f.Unique,
			NotNull:    f.NotNull,
			DefaultRaw: f.DefaultRaw,
		}
		nt.Fields[f.Name] = fs
		if f.PrimaryKey {
			nt.PK = f.Name
			nt.Indexes[f.Name] = IndexSpec{Field: f.Name, Unique: true}
		} else if f.Unique {
			nt.Indexes[f.Name] = IndexSpec{Field: f.Name, Unique: true}
		}
	}
	out.Nodes[p.Name] = nt
	out.Version++
	return out, nil
}

func validateCreateNode(c *Catalog, p CreateNodePayload) error {
	if p.Name == "" {
		return errors.New("node name required")
	}
	if _, ok := c.Nodes[p.Name]; ok {
		return fmt.Errorf("node %q already exists", p.Name)
	}
	if len(p.Fields) == 0 {
		return errors.New("node must define at least one field")
	}
	var pkCount int
	seen := map[string]struct{}{}
	for _, f := range p.Fields {
		if f.Name == "" {
			return errors.New("field with empty name")
		}
		if _, dup := seen[f.Name]; dup {
			return fmt.Errorf("duplicate field %q", f.Name)
		}
		seen[f.Name] = struct{}{}
		if f.PrimaryKey {
			pkCount++
			if !isScalarType(f.Type) {
				return fmt.Errorf("primary key %q must be scalar", f.Name)
			}
		}
		if f.NotNull && f.DefaultRaw != nil && strings.EqualFold(*f.DefaultRaw, "null") {
			return fmt.Errorf("field %q NOT NULL but default null", f.Name)
		}
		if f.Type.Base == BaseEnum && len(f.Type.EnumVals) == 0 {
			return fmt.Errorf("enum field %q must have values", f.Name)
		}
	}
	if pkCount > 1 {
		return errors.New("multiple PRIMARY KEY fields")
	}
	return nil
}

func isScalarType(t TypeSpec) bool {
	switch t.Base {
	case BaseString, BaseText, BaseInt, BaseFloat, BaseBool, BaseUUID, BaseDate, BaseTime, BaseDateTime:
		return true
	default:
		return false
	}
}

func ApplyCreateEdge(c *Catalog, p CreateEdgePayload) (*Catalog, error) {
	if err := validateCreateEdge(c, p); err != nil {
		return nil, err
	}
	out := c.Clone()
	et := &EdgeType{
		Name:  p.Name,
		From:  p.From,
		To:    p.To,
		Props: map[string]FieldSpec{},
	}
	for _, f := range p.Props {
		if _, exists := et.Props[f.Name]; exists {
			return nil, fmt.Errorf("duplicate edge prop %q", f.Name)
		}
		et.Props[f.Name] = FieldSpec{
			Name:       f.Name,
			Type:       f.Type,
			Unique:     f.Unique, // (rare on edges, but allowed)
			NotNull:    f.NotNull,
			DefaultRaw: f.DefaultRaw,
		}
	}
	out.Edges[p.Name] = et
	out.Version++
	return out, nil
}

func validateCreateEdge(c *Catalog, p CreateEdgePayload) error {
	if p.Name == "" {
		return errors.New("edge name required")
	}
	if _, ok := c.Edges[p.Name]; ok {
		return fmt.Errorf("edge %q already exists", p.Name)
	}
	// endpoints must exist
	if _, ok := c.Nodes[p.From.Label]; !ok {
		return fmt.Errorf("FROM node type %q not found", p.From.Label)
	}
	if _, ok := c.Nodes[p.To.Label]; !ok {
		return fmt.Errorf("TO node type %q not found", p.To.Label)
	}
	// props sanity
	seen := map[string]struct{}{}
	for _, f := range p.Props {
		if f.Name == "" {
			return errors.New("edge prop with empty name")
		}
		if _, dup := seen[f.Name]; dup {
			return fmt.Errorf("duplicate edge prop %q", f.Name)
		}
		seen[f.Name] = struct{}{}
		if f.Type.Base == BaseEnum && len(f.Type.EnumVals) == 0 {
			return fmt.Errorf("enum prop %q must have values", f.Name)
		}
	}
	return nil
}

/* -------------------- ALTER NODE -------------------- */

// ApplyAlterNode returns a new catalog with the node type modified.
func ApplyAlterNode(c *Catalog, p AlterNodePayload) (*Catalog, error) {
	if err := validateAlterNode(c, p); err != nil {
		return nil, err
	}

	out := c.Clone()
	nt := out.Nodes[p.Name] // validated to exist

	for _, action := range p.Actions {
		switch action.Type {
		case "ADD_FIELD":
			if _, exists := nt.Fields[action.Field.Name]; exists {
				return nil, fmt.Errorf("field %q already exists", action.Field.Name)
			}
			fs := FieldSpec{
				Name:       action.Field.Name,
				Type:       action.Field.Type,
				Unique:     action.Field.Unique,
				NotNull:    action.Field.NotNull,
				DefaultRaw: action.Field.DefaultRaw,
			}
			nt.Fields[action.Field.Name] = fs

			if action.Field.PrimaryKey {
				if nt.PK != "" {
					return nil, errors.New("node already has a primary key")
				}
				nt.PK = action.Field.Name
				nt.Indexes[action.Field.Name] = IndexSpec{Field: action.Field.Name, Unique: true}
			} else if action.Field.Unique {
				nt.Indexes[action.Field.Name] = IndexSpec{Field: action.Field.Name, Unique: true}
			}

		case "DROP_FIELD":
			if _, exists := nt.Fields[action.FieldName]; !exists {
				return nil, fmt.Errorf("field %q does not exist", action.FieldName)
			}
			if nt.PK == action.FieldName {
				return nil, fmt.Errorf("cannot drop primary key field %q", action.FieldName)
			}
			delete(nt.Fields, action.FieldName)
			delete(nt.Indexes, action.FieldName)

		case "MODIFY_FIELD":
			if _, exists := nt.Fields[action.Field.Name]; !exists {
				return nil, fmt.Errorf("field %q does not exist", action.Field.Name)
			}
			if nt.PK == action.Field.Name && action.Field.PrimaryKey {
				// Modifying existing PK field - validate it remains scalar
				if !isScalarType(action.Field.Type) {
					return nil, fmt.Errorf("primary key %q must be scalar", action.Field.Name)
				}
			} else if nt.PK == action.Field.Name && !action.Field.PrimaryKey {
				return nil, fmt.Errorf("cannot remove primary key from field %q", action.Field.Name)
			} else if nt.PK != action.Field.Name && action.Field.PrimaryKey {
				return nil, fmt.Errorf("cannot set primary key on field %q when %q is already primary key", action.Field.Name, nt.PK)
			}

			fs := FieldSpec{
				Name:       action.Field.Name,
				Type:       action.Field.Type,
				Unique:     action.Field.Unique,
				NotNull:    action.Field.NotNull,
				DefaultRaw: action.Field.DefaultRaw,
			}
			nt.Fields[action.Field.Name] = fs

			// Update indexes
			if action.Field.Unique || action.Field.PrimaryKey {
				nt.Indexes[action.Field.Name] = IndexSpec{Field: action.Field.Name, Unique: true}
			} else {
				delete(nt.Indexes, action.Field.Name)
			}

		case "SET_PRIMARY_KEY":
			if _, exists := nt.Fields[action.FieldName]; !exists {
				return nil, fmt.Errorf("field %q does not exist", action.FieldName)
			}
			field := nt.Fields[action.FieldName]
			if !isScalarType(field.Type) {
				return nil, fmt.Errorf("primary key %q must be scalar", action.FieldName)
			}

			// Remove old PK index if exists
			if nt.PK != "" {
				oldField := nt.Fields[nt.PK]
				if !oldField.Unique {
					delete(nt.Indexes, nt.PK)
				}
			}

			nt.PK = action.FieldName
			nt.Indexes[action.FieldName] = IndexSpec{Field: action.FieldName, Unique: true}

		default:
			return nil, fmt.Errorf("unknown alter node action: %s", action.Type)
		}
	}

	out.Version++
	return out, nil
}

func validateAlterNode(c *Catalog, p AlterNodePayload) error {
	if p.Name == "" {
		return errors.New("node name required")
	}
	if _, ok := c.Nodes[p.Name]; !ok {
		return fmt.Errorf("node %q does not exist", p.Name)
	}
	if len(p.Actions) == 0 {
		return errors.New("at least one action required")
	}

	for _, action := range p.Actions {
		switch action.Type {
		case "ADD_FIELD", "MODIFY_FIELD":
			if action.Field == nil {
				return fmt.Errorf("field required for action %s", action.Type)
			}
			if action.Field.Name == "" {
				return errors.New("field name required")
			}
			if action.Field.Type.Base == BaseEnum && len(action.Field.Type.EnumVals) == 0 {
				return fmt.Errorf("enum field %q must have values", action.Field.Name)
			}
			if action.Field.NotNull && action.Field.DefaultRaw != nil && strings.EqualFold(*action.Field.DefaultRaw, "null") {
				return fmt.Errorf("field %q NOT NULL but default null", action.Field.Name)
			}
			if action.Field.PrimaryKey && !isScalarType(action.Field.Type) {
				return fmt.Errorf("primary key %q must be scalar", action.Field.Name)
			}
		case "DROP_FIELD", "SET_PRIMARY_KEY":
			if action.FieldName == "" {
				return fmt.Errorf("field name required for action %s", action.Type)
			}
		default:
			return fmt.Errorf("unknown alter node action: %s", action.Type)
		}
	}

	return nil
}

/* -------------------- ALTER EDGE -------------------- */

// ApplyAlterEdge returns a new catalog with the edge type modified.
func ApplyAlterEdge(c *Catalog, p AlterEdgePayload) (*Catalog, error) {
	if err := validateAlterEdge(c, p); err != nil {
		return nil, err
	}

	out := c.Clone()
	et := out.Edges[p.Name] // validated to exist

	for _, action := range p.Actions {
		switch action.Type {
		case "ADD_PROP":
			if _, exists := et.Props[action.Prop.Name]; exists {
				return nil, fmt.Errorf("prop %q already exists", action.Prop.Name)
			}
			et.Props[action.Prop.Name] = FieldSpec{
				Name:       action.Prop.Name,
				Type:       action.Prop.Type,
				Unique:     action.Prop.Unique,
				NotNull:    action.Prop.NotNull,
				DefaultRaw: action.Prop.DefaultRaw,
			}

		case "DROP_PROP":
			if _, exists := et.Props[action.PropName]; !exists {
				return nil, fmt.Errorf("prop %q does not exist", action.PropName)
			}
			delete(et.Props, action.PropName)

		case "MODIFY_PROP":
			if _, exists := et.Props[action.Prop.Name]; !exists {
				return nil, fmt.Errorf("prop %q does not exist", action.Prop.Name)
			}
			et.Props[action.Prop.Name] = FieldSpec{
				Name:       action.Prop.Name,
				Type:       action.Prop.Type,
				Unique:     action.Prop.Unique,
				NotNull:    action.Prop.NotNull,
				DefaultRaw: action.Prop.DefaultRaw,
			}

		case "CHANGE_ENDPOINT":
			if action.Endpoint == "FROM" {
				if _, ok := c.Nodes[action.NewEndpoint.Label]; !ok {
					return nil, fmt.Errorf("FROM node type %q not found", action.NewEndpoint.Label)
				}
				et.From = *action.NewEndpoint
			} else if action.Endpoint == "TO" {
				if _, ok := c.Nodes[action.NewEndpoint.Label]; !ok {
					return nil, fmt.Errorf("TO node type %q not found", action.NewEndpoint.Label)
				}
				et.To = *action.NewEndpoint
			} else {
				return nil, fmt.Errorf("invalid endpoint %q", action.Endpoint)
			}

		default:
			return nil, fmt.Errorf("unknown alter edge action: %s", action.Type)
		}
	}

	out.Version++
	return out, nil
}

func validateAlterEdge(c *Catalog, p AlterEdgePayload) error {
	if p.Name == "" {
		return errors.New("edge name required")
	}
	if _, ok := c.Edges[p.Name]; !ok {
		return fmt.Errorf("edge %q does not exist", p.Name)
	}
	if len(p.Actions) == 0 {
		return errors.New("at least one action required")
	}

	for _, action := range p.Actions {
		switch action.Type {
		case "ADD_PROP", "MODIFY_PROP":
			if action.Prop == nil {
				return fmt.Errorf("prop required for action %s", action.Type)
			}
			if action.Prop.Name == "" {
				return errors.New("prop name required")
			}
			if action.Prop.Type.Base == BaseEnum && len(action.Prop.Type.EnumVals) == 0 {
				return fmt.Errorf("enum prop %q must have values", action.Prop.Name)
			}
			if action.Prop.NotNull && action.Prop.DefaultRaw != nil && strings.EqualFold(*action.Prop.DefaultRaw, "null") {
				return fmt.Errorf("prop %q NOT NULL but default null", action.Prop.Name)
			}
		case "DROP_PROP":
			if action.PropName == "" {
				return fmt.Errorf("prop name required for action %s", action.Type)
			}
		case "CHANGE_ENDPOINT":
			if action.Endpoint != "FROM" && action.Endpoint != "TO" {
				return fmt.Errorf("endpoint must be FROM or TO, got %q", action.Endpoint)
			}
			if action.NewEndpoint == nil {
				return errors.New("new endpoint required for CHANGE_ENDPOINT")
			}
			if action.NewEndpoint.Label == "" {
				return errors.New("endpoint label required")
			}
			if _, ok := c.Nodes[action.NewEndpoint.Label]; !ok {
				return fmt.Errorf("endpoint node type %q not found", action.NewEndpoint.Label)
			}
		default:
			return fmt.Errorf("unknown alter edge action: %s", action.Type)
		}
	}

	return nil
}

/* -------------------- DROP NODE -------------------- */

// ApplyDropNode returns a new catalog with the node type removed.
func ApplyDropNode(c *Catalog, p DropNodePayload) (*Catalog, error) {
	if err := validateDropNode(c, p); err != nil {
		return nil, err
	}

	out := c.Clone()
	delete(out.Nodes, p.Name)
	out.Version++
	return out, nil
}

func validateDropNode(c *Catalog, p DropNodePayload) error {
	if p.Name == "" {
		return errors.New("node name required")
	}
	if _, ok := c.Nodes[p.Name]; !ok {
		return fmt.Errorf("node %q does not exist", p.Name)
	}

	// Check if any edges reference this node
	for edgeName, edge := range c.Edges {
		if edge.From.Label == p.Name || edge.To.Label == p.Name {
			return fmt.Errorf("cannot drop node %q: referenced by edge %q", p.Name, edgeName)
		}
	}

	return nil
}

/* -------------------- DROP EDGE -------------------- */

// ApplyDropEdge returns a new catalog with the edge type removed.
func ApplyDropEdge(c *Catalog, p DropEdgePayload) (*Catalog, error) {
	if err := validateDropEdge(c, p); err != nil {
		return nil, err
	}

	out := c.Clone()
	delete(out.Edges, p.Name)
	out.Version++
	return out, nil
}

func validateDropEdge(c *Catalog, p DropEdgePayload) error {
	if p.Name == "" {
		return errors.New("edge name required")
	}
	if _, ok := c.Edges[p.Name]; !ok {
		return fmt.Errorf("edge %q does not exist", p.Name)
	}

	return nil
}
