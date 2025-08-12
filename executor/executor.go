package executor

import (
	"fmt"
	"io"
	"strings"

	"grapho/catalog"
	"grapho/parser"
)

// Executor encapsulates execution logic for DDL/DML statements.
type Executor struct {
	registry *catalog.Registry
	graph    *GraphData
}

func New(reg *catalog.Registry) *Executor {
	return &Executor{
		registry: reg,
		graph: &GraphData{
			Nodes:  make(map[string]map[string]interface{}),
			Edges:  make(map[string][]EdgeInstance),
			NextID: 1,
		},
	}
}

// ExecuteStatements runs a batch of parsed statements. It returns whether any statement mutated state.
func (e *Executor) ExecuteStatements(w io.Writer, stmts []parser.Stmt) (bool, error) {
	mutated := false
	for i, st := range stmts {
		if err := e.ExecuteStatement(w, st); err != nil {
			if w != nil {
				fmt.Fprintf(w, "Error executing statement %d: %s\n", i+1, err.Error())
			}
			return mutated, err
		}
		switch st.(type) {
		case *parser.CreateNodeStmt, *parser.CreateEdgeStmt,
			*parser.AlterNodeStmt, *parser.AlterEdgeStmt,
			*parser.DropNodeStmt, *parser.DropEdgeStmt,
			*parser.InsertNodeStmt, *parser.InsertEdgeStmt,
			*parser.UpdateNodeStmt, *parser.UpdateEdgeStmt,
			*parser.DeleteNodeStmt, *parser.DeleteEdgeStmt:
			mutated = true
		}
	}
	if w != nil {
		fmt.Fprintf(w, "OK - %d statement(s) executed successfully\n\n", len(stmts))
	}
	return mutated, nil
}

// ExecuteStatement executes a single parsed statement.
func (e *Executor) ExecuteStatement(w io.Writer, stmt parser.Stmt) error {
	switch st := stmt.(type) {
	case *parser.CreateNodeStmt:
		return e.executeCreateNode(st)
	case *parser.CreateEdgeStmt:
		return e.executeCreateEdge(st)
	case *parser.AlterNodeStmt:
		return e.executeAlterNode(st)
	case *parser.AlterEdgeStmt:
		return e.executeAlterEdge(st)
	case *parser.DropNodeStmt:
		return e.executeDropNode(st)
	case *parser.DropEdgeStmt:
		return e.executeDropEdge(st)
	case *parser.InsertNodeStmt:
		return e.executeInsertNode(w, st)
	case *parser.InsertEdgeStmt:
		return e.executeInsertEdge(w, st)
	case *parser.UpdateNodeStmt:
		return e.executeUpdateNode(w, st)
	case *parser.UpdateEdgeStmt:
		return e.executeUpdateEdge(w, st)
	case *parser.DeleteNodeStmt:
		return e.executeDeleteNode(w, st)
	case *parser.DeleteEdgeStmt:
		return e.executeDeleteEdge(w, st)
	case *parser.MatchStmt:
		return e.executeMatch(w, st)
	default:
		return fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

// DDL helpers
func (e *Executor) executeCreateNode(stmt *parser.CreateNodeStmt) error {
	fields := make([]catalog.FieldPayload, len(stmt.Fields))
	for i, field := range stmt.Fields {
		fields[i] = catalog.FieldPayload{
			Name:       field.Name,
			Type:       convertTypeSpec(field.Type),
			PrimaryKey: field.PrimaryKey,
			Unique:     field.Unique,
			NotNull:    field.NotNull,
		}
		if field.Default != nil {
			defaultVal := field.Default.Text
			fields[i].DefaultRaw = &defaultVal
		}
	}
	payload := catalog.CreateNodePayload{Name: stmt.Name, Fields: fields}
	_, err := e.registry.Apply(catalog.DDLEvent{Op: catalog.OpCreateNode, Stmt: payload})
	return err
}

func (e *Executor) executeCreateEdge(stmt *parser.CreateEdgeStmt) error {
	props := make([]catalog.FieldPayload, len(stmt.Props))
	for i, prop := range stmt.Props {
		props[i] = catalog.FieldPayload{
			Name:    prop.Name,
			Type:    convertTypeSpec(prop.Type),
			Unique:  prop.Unique,
			NotNull: prop.NotNull,
		}
		if prop.Default != nil {
			defaultVal := prop.Default.Text
			props[i].DefaultRaw = &defaultVal
		}
	}
	payload := catalog.CreateEdgePayload{
		Name: stmt.Name,
		From: catalog.EdgeEndpoint{Label: stmt.From.Label, Card: convertCardinality(stmt.From.Card)},
		To:   catalog.EdgeEndpoint{Label: stmt.To.Label, Card: convertCardinality(stmt.To.Card)},
		Props: props,
	}
	_, err := e.registry.Apply(catalog.DDLEvent{Op: catalog.OpCreateEdge, Stmt: payload})
	return err
}

func (e *Executor) executeAlterNode(stmt *parser.AlterNodeStmt) error {
	var action catalog.NodeAlterAction
	switch stmt.Action {
	case parser.AlterAddField:
		action.Type = "ADD_FIELD"
		action.Field = &catalog.FieldPayload{
			Name:    stmt.Field.Name,
			Type:    convertTypeSpec(stmt.Field.Type),
			Unique:  stmt.Field.Unique,
			NotNull: stmt.Field.NotNull,
		}
		if stmt.Field.Default != nil {
			defaultVal := stmt.Field.Default.Text
			action.Field.DefaultRaw = &defaultVal
		}
	case parser.AlterDropField:
		action.Type = "DROP_FIELD"
		action.FieldName = stmt.FieldName
	case parser.AlterModifyField:
		action.Type = "MODIFY_FIELD"
		action.Field = &catalog.FieldPayload{
			Name:    stmt.Field.Name,
			Type:    convertTypeSpec(stmt.Field.Type),
			Unique:  stmt.Field.Unique,
			NotNull: stmt.Field.NotNull,
		}
		if stmt.Field.Default != nil {
			defaultVal := stmt.Field.Default.Text
			action.Field.DefaultRaw = &defaultVal
		}
	case parser.AlterSetPrimaryKey:
		action.Type = "SET_PRIMARY_KEY"
		action.FieldName = strings.Join(stmt.PkFields, ",")
	default:
		return fmt.Errorf("unsupported alter node action: %v", stmt.Action)
	}
	payload := catalog.AlterNodePayload{Name: stmt.Name, Actions: []catalog.NodeAlterAction{action}}
	_, err := e.registry.Apply(catalog.DDLEvent{Op: catalog.OpAlterNode, Stmt: payload})
	return err
}

func (e *Executor) executeAlterEdge(stmt *parser.AlterEdgeStmt) error {
	var action catalog.EdgeAlterAction
	switch stmt.Action {
	case parser.AlterAddProp:
		action.Type = "ADD_PROP"
		action.Prop = &catalog.FieldPayload{
			Name:    stmt.Prop.Name,
			Type:    convertTypeSpec(stmt.Prop.Type),
			Unique:  stmt.Prop.Unique,
			NotNull: stmt.Prop.NotNull,
		}
		if stmt.Prop.Default != nil {
			defaultVal := stmt.Prop.Default.Text
			action.Prop.DefaultRaw = &defaultVal
		}
	case parser.AlterDropProp:
		action.Type = "DROP_PROP"
		action.PropName = stmt.PropName
	case parser.AlterModifyProp:
		action.Type = "MODIFY_PROP"
		action.Prop = &catalog.FieldPayload{
			Name:    stmt.Prop.Name,
			Type:    convertTypeSpec(stmt.Prop.Type),
			Unique:  stmt.Prop.Unique,
			NotNull: stmt.Prop.NotNull,
		}
		if stmt.Prop.Default != nil {
			defaultVal := stmt.Prop.Default.Text
			action.Prop.DefaultRaw = &defaultVal
		}
	case parser.AlterSetEndpoints:
		if stmt.From != nil {
			action.Type = "CHANGE_ENDPOINT"
			action.Endpoint = "FROM"
			action.NewEndpoint = &catalog.EdgeEndpoint{Label: stmt.From.Label, Card: convertCardinality(stmt.From.Card)}
		} else if stmt.To != nil {
			action.Type = "CHANGE_ENDPOINT"
			action.Endpoint = "TO"
			action.NewEndpoint = &catalog.EdgeEndpoint{Label: stmt.To.Label, Card: convertCardinality(stmt.To.Card)}
		}
	default:
		return fmt.Errorf("unsupported alter edge action: %v", stmt.Action)
	}
	payload := catalog.AlterEdgePayload{Name: stmt.Name, Actions: []catalog.EdgeAlterAction{action}}
	_, err := e.registry.Apply(catalog.DDLEvent{Op: catalog.OpAlterEdge, Stmt: payload})
	return err
}

func (e *Executor) executeDropNode(stmt *parser.DropNodeStmt) error {
	payload := catalog.DropNodePayload{Name: stmt.Name}
	_, err := e.registry.Apply(catalog.DDLEvent{Op: catalog.OpDropNode, Stmt: payload})
	return err
}

func (e *Executor) executeDropEdge(stmt *parser.DropEdgeStmt) error {
	payload := catalog.DropEdgePayload{Name: stmt.Name}
	_, err := e.registry.Apply(catalog.DDLEvent{Op: catalog.OpDropEdge, Stmt: payload})
	return err
}

// ---------------------- DML and Query ----------------------

type GraphData struct {
	Nodes  map[string]map[string]interface{}
	Edges  map[string][]EdgeInstance
	NextID int64
}

type EdgeInstance struct {
	ID         string
	FromNodeID string
	ToNodeID   string
	Properties map[string]interface{}
}

func (e *Executor) executeInsertNode(w io.Writer, stmt *parser.InsertNodeStmt) error {
	// Validate node type exists in catalog
	cat := e.registry.Current()
	nodeType, exists := cat.Nodes[stmt.NodeType]
	if !exists {
		return fmt.Errorf("node type '%s' does not exist", stmt.NodeType)
	}
	// Generate new node ID
	nodeID := fmt.Sprintf("%d", e.graph.NextID)
	e.graph.NextID++
	// Initialize storage for this node type
	if e.graph.Nodes[stmt.NodeType] == nil {
		e.graph.Nodes[stmt.NodeType] = make(map[string]interface{})
	}
	// Build properties
	properties := make(map[string]interface{})
	for _, prop := range stmt.Properties {
		switch prop.Value.Kind {
		case parser.LitString:
			properties[prop.Name] = prop.Value.Text
		case parser.LitNumber:
			properties[prop.Name] = prop.Value.Text
		case parser.LitBool:
			properties[prop.Name] = prop.Value.Text == "true"
		case parser.LitNull:
			properties[prop.Name] = nil
		}
	}
	// Simple required field check
	for fieldName, fieldSpec := range nodeType.Fields {
		if fieldSpec.NotNull {
			if _, ok := properties[fieldName]; !ok {
				return fmt.Errorf("required field '%s' is missing", fieldName)
			}
		}
	}
	// Add synthetic ID
	properties["_id"] = nodeID
	// Store the node
	e.graph.Nodes[stmt.NodeType][nodeID] = properties
	if w != nil {
		fmt.Fprintf(w, "Node inserted with ID: %s\n", nodeID)
	}
	return nil
}

func (e *Executor) executeInsertEdge(w io.Writer, stmt *parser.InsertEdgeStmt) error {
	// Validate edge type exists
	cat := e.registry.Current()
	edgeType, exists := cat.Edges[stmt.EdgeType]
	if !exists {
		return fmt.Errorf("edge type '%s' does not exist", stmt.EdgeType)
	}
	// Resolve endpoints
	fromNodeID, err := e.findNodeID(stmt.FromNode)
	if err != nil {
		return fmt.Errorf("FROM node not found: %v", err)
	}
	toNodeID, err := e.findNodeID(stmt.ToNode)
	if err != nil {
		return fmt.Errorf("TO node not found: %v", err)
	}
	if stmt.FromNode.NodeType != edgeType.From.Label {
		return fmt.Errorf("FROM node type '%s' does not match edge FROM type '%s'", stmt.FromNode.NodeType, edgeType.From.Label)
	}
	if stmt.ToNode.NodeType != edgeType.To.Label {
		return fmt.Errorf("TO node type '%s' does not match edge TO type '%s'", stmt.ToNode.NodeType, edgeType.To.Label)
	}
	// Generate ID
	edgeID := fmt.Sprintf("edge_%d", e.graph.NextID)
	e.graph.NextID++
	// Properties
	properties := make(map[string]interface{})
	for _, prop := range stmt.Properties {
		switch prop.Value.Kind {
		case parser.LitString:
			properties[prop.Name] = prop.Value.Text
		case parser.LitNumber:
			properties[prop.Name] = prop.Value.Text
		case parser.LitBool:
			properties[prop.Name] = prop.Value.Text == "true"
		case parser.LitNull:
			properties[prop.Name] = nil
		}
	}
	edge := EdgeInstance{ID: edgeID, FromNodeID: fromNodeID, ToNodeID: toNodeID, Properties: properties}
	e.graph.Edges[stmt.EdgeType] = append(e.graph.Edges[stmt.EdgeType], edge)
	if w != nil {
		fmt.Fprintf(w, "Edge inserted with ID: %s\n", edgeID)
	}
	return nil
}

func (e *Executor) executeUpdateNode(w io.Writer, stmt *parser.UpdateNodeStmt) error {
	nodes := e.graph.Nodes[stmt.NodeType]
	if nodes == nil {
		return fmt.Errorf("no nodes of type '%s' found", stmt.NodeType)
	}
	updated := 0
	for _, nodeProps := range nodes {
		if e.matchesConditions(nodeProps, stmt.Where) {
			for _, setProp := range stmt.Set {
				switch setProp.Value.Kind {
				case parser.LitString:
					nodeProps.(map[string]interface{})[setProp.Name] = setProp.Value.Text
				case parser.LitNumber:
					nodeProps.(map[string]interface{})[setProp.Name] = setProp.Value.Text
				case parser.LitBool:
					nodeProps.(map[string]interface{})[setProp.Name] = setProp.Value.Text == "true"
				case parser.LitNull:
					nodeProps.(map[string]interface{})[setProp.Name] = nil
				}
			}
			updated++
		}
	}
	if w != nil {
		fmt.Fprintf(w, "Updated %d node(s)\n", updated)
	}
	return nil
}

func (e *Executor) executeUpdateEdge(w io.Writer, stmt *parser.UpdateEdgeStmt) error {
	edges := e.graph.Edges[stmt.EdgeType]
	updated := 0
	for i := range edges {
		if e.matchesConditions(edges[i].Properties, stmt.Where) {
			for _, setProp := range stmt.Set {
				switch setProp.Value.Kind {
				case parser.LitString:
					edges[i].Properties[setProp.Name] = setProp.Value.Text
				case parser.LitNumber:
					edges[i].Properties[setProp.Name] = setProp.Value.Text
				case parser.LitBool:
					edges[i].Properties[setProp.Name] = setProp.Value.Text == "true"
				case parser.LitNull:
					edges[i].Properties[setProp.Name] = nil
				}
			}
			updated++
		}
	}
	if w != nil {
		fmt.Fprintf(w, "Updated %d edge(s)\n", updated)
	}
	return nil
}

func (e *Executor) executeDeleteNode(w io.Writer, stmt *parser.DeleteNodeStmt) error {
	nodes := e.graph.Nodes[stmt.NodeType]
	if nodes == nil {
		return fmt.Errorf("no nodes of type '%s' found", stmt.NodeType)
	}
	deleted := 0
	for nodeID, nodeProps := range nodes {
		if e.matchesConditions(nodeProps, stmt.Where) {
			delete(nodes, nodeID)
			deleted++
		}
	}
	if w != nil {
		fmt.Fprintf(w, "Deleted %d node(s)\n", deleted)
	}
	return nil
}

func (e *Executor) executeDeleteEdge(w io.Writer, stmt *parser.DeleteEdgeStmt) error {
	edges := e.graph.Edges[stmt.EdgeType]
	var remaining []EdgeInstance
	deleted := 0
	for _, edge := range edges {
		if e.matchesConditions(edge.Properties, stmt.Where) {
			deleted++
		} else {
			remaining = append(remaining, edge)
		}
	}
	e.graph.Edges[stmt.EdgeType] = remaining
	if w != nil {
		fmt.Fprintf(w, "Deleted %d edge(s)\n", deleted)
	}
	return nil
}

func (e *Executor) executeMatch(w io.Writer, stmt *parser.MatchStmt) error {
	if w != nil {
		fmt.Fprintf(w, "MATCH Results:\n")
	}
	for _, element := range stmt.Pattern {
		if !element.IsEdge {
			nodes := e.graph.Nodes[element.Type]
			if nodes != nil {
				if w != nil {
					fmt.Fprintf(w, "\nNodes of type '%s':\n", element.Type)
				}
				for nodeID, props := range nodes {
					if len(stmt.Where) == 0 || e.matchesConditions(props, stmt.Where) {
						if w != nil {
							fmt.Fprintf(w, "  ID: %s, Properties: %v\n", nodeID, props)
						}
					}
				}
			}
		}
	}
	return nil
}

// Helpers
func (e *Executor) findNodeID(nodeRef *parser.NodeRef) (string, error) {
	nodes := e.graph.Nodes[nodeRef.NodeType]
	if nodes == nil {
		return "", fmt.Errorf("no nodes of type '%s' found", nodeRef.NodeType)
	}
	if nodeRef.ID != nil {
		nodeID := nodeRef.ID.Text
		if _, exists := nodes[nodeID]; exists {
			return nodeID, nil
		}
		return "", fmt.Errorf("node with ID '%s' not found", nodeID)
	}
	for nodeID, nodeProps := range nodes {
		if e.matchesConditions(nodeProps, nodeRef.Properties) {
			return nodeID, nil
		}
	}
	return "", fmt.Errorf("no matching node found")
}

func (e *Executor) matchesConditions(properties interface{}, conditions []parser.Property) bool {
	if len(conditions) == 0 {
		return true
	}
	props, ok := properties.(map[string]interface{})
	if !ok {
		return false
	}
	for _, condition := range conditions {
		propValue, exists := props[condition.Name]
		if !exists {
			return false
		}
		var expectedValue interface{}
		switch condition.Value.Kind {
		case parser.LitString:
			expectedValue = condition.Value.Text
		case parser.LitNumber:
			expectedValue = condition.Value.Text
		case parser.LitBool:
			expectedValue = condition.Value.Text == "true"
		case parser.LitNull:
			expectedValue = nil
		}
		if propValue != expectedValue {
			return false
		}
	}
	return true
}

// Converters moved from server
func convertTypeSpec(t parser.TypeSpec) catalog.TypeSpec {
	spec := catalog.TypeSpec{Base: convertBaseType(t.Base)}
	if t.Elem != nil {
		elem := convertTypeSpec(*t.Elem)
		spec.Elem = &elem
	}
	if len(t.EnumVals) > 0 {
		spec.EnumVals = make([]string, len(t.EnumVals))
		copy(spec.EnumVals, t.EnumVals)
	}
	return spec
}

func convertBaseType(bt parser.BaseType) catalog.BaseType {
	switch bt {
	case parser.BaseString:
		return catalog.BaseString
	case parser.BaseText:
		return catalog.BaseText
	case parser.BaseInt:
		return catalog.BaseInt
	case parser.BaseFloat:
		return catalog.BaseFloat
	case parser.BaseBool:
		return catalog.BaseBool
	case parser.BaseUUID:
		return catalog.BaseUUID
	case parser.BaseDate:
		return catalog.BaseDate
	case parser.BaseTime:
		return catalog.BaseTime
	case parser.BaseDateTime:
		return catalog.BaseDateTime
	case parser.BaseJSON:
		return catalog.BaseJSON
	case parser.BaseBlob:
		return catalog.BaseBlob
	default:
		return catalog.BaseString
	}
}

func convertCardinality(c parser.Cardinality) catalog.Cardinality {
	switch c {
	case parser.CardOne:
		return catalog.One
	case parser.CardMany:
		return catalog.Many
	default:
		return catalog.One
	}
}
