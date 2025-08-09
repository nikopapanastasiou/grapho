package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"

	"grapho/catalog"
	"grapho/parser"
)

// Server represents a TCP server that executes DDL commands
type Server struct {
	addr     string
	registry *catalog.Registry
	listener net.Listener
	mu       sync.RWMutex
	clients  map[net.Conn]bool
	commitLog *CommitLog
	replaying bool
}

// NewServer creates a new server instance
func NewServer(addr string, registry *catalog.Registry) *Server {
	return &Server{
		addr:     addr,
		registry: registry,
		clients:  make(map[net.Conn]bool),
	}
}

// AttachCommitLog associates a commit log with the server
func (s *Server) AttachCommitLog(cl *CommitLog) {
	s.commitLog = cl
}

// Start begins listening for connections
func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.addr, err)
	}

	// On startup, replay commit log if present
	if s.commitLog != nil {
		s.replaying = true
		if err := s.commitLog.Replay(func(line string) error {
			// Apply without emitting to any client and without re-appending
			p := parser.NewParser(line)
			stmts, errs := p.ParseScript()
			if len(errs) > 0 {
				// stop replay on parse error to avoid corrupting state
				return fmt.Errorf("replay parse error: %v", errs)
			}
			for _, st := range stmts {
				if err := s.executeStatement(nil, st); err != nil {
					return fmt.Errorf("replay exec error: %w", err)
				}
			}
			return nil
		}); err != nil {
			return fmt.Errorf("replay commit log failed: %w", err)
		}
		s.replaying = false
	}

	s.listener = listener
	fmt.Printf("Server listening on %s\n", s.addr)
	
	for {
		conn, err := listener.Accept()
		if err != nil {
			// Check if server was stopped
			select {
			case <-make(chan struct{}):
				return nil
			default:
				fmt.Printf("Failed to accept connection: %v\n", err)
				continue
			}
		}
		
		s.mu.Lock()
		s.clients[conn] = true
		s.mu.Unlock()
		
		go s.handleConnection(conn)
	}
}

// Stop shuts down the server
func (s *Server) Stop() error {
	if s.listener != nil {
		s.listener.Close()
	}
	
	s.mu.Lock()
	for conn := range s.clients {
		conn.Close()
	}
	s.clients = make(map[net.Conn]bool)
	s.mu.Unlock()
	
	return nil
}

// handleConnection processes commands from a single client
func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		s.mu.Lock()
		delete(s.clients, conn)
		s.mu.Unlock()
		conn.Close()
	}()
	
	fmt.Printf("Client connected: %s\n", conn.RemoteAddr())
	
	// Send welcome message
	fmt.Fprintf(conn, "Welcome to Grapho DDL Server\n")
	fmt.Fprintf(conn, "Enter DDL commands (CREATE, ALTER, DROP) followed by semicolon\n")
	fmt.Fprintf(conn, "Type 'quit' to exit\n\n")
	
	scanner := bufio.NewScanner(conn)
	var commandBuffer strings.Builder
	
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		
		if line == "quit" || line == "exit" {
			fmt.Fprintf(conn, "Goodbye!\n")
			return
		}
		
		if line == "" {
			continue
		}
		
		// Add line to command buffer
		commandBuffer.WriteString(line)
		commandBuffer.WriteString(" ")
		
		// Check if command is complete (ends with semicolon)
		if strings.HasSuffix(line, ";") {
			command := commandBuffer.String()
			commandBuffer.Reset()
			
			s.executeCommand(conn, command)
		}
	}
	
	if err := scanner.Err(); err != nil && err != io.EOF {
		fmt.Printf("Error reading from client %s: %v\n", conn.RemoteAddr(), err)
	}
	
	fmt.Printf("Client disconnected: %s\n", conn.RemoteAddr())
}

// executeCommand parses and executes a DDL command
func (s *Server) executeCommand(conn net.Conn, command string) {
	command = strings.TrimSpace(command)
	if command == "" {
		return
	}
	
	fmt.Printf("Executing command: %s\n", command)
	
	// Parse the command
	p := parser.NewParser(command)
	stmts, errs := p.ParseScript()
	
	if len(errs) > 0 {
		fmt.Fprintf(conn, "Parse errors:\n")
		for _, err := range errs {
			fmt.Fprintf(conn, "  %s\n", err.Error())
		}
		fmt.Fprintf(conn, "\n")
		return
	}
	
	if len(stmts) == 0 {
		fmt.Fprintf(conn, "No statements to execute\n\n")
		return
	}
	
    // Execute each statement and track whether any mutates state
    mutated := false
    for i, stmt := range stmts {
        if err := s.executeStatement(conn, stmt); err != nil {
            fmt.Fprintf(conn, "Error executing statement %d: %s\n", i+1, err.Error())
            return
        }
        switch stmt.(type) {
        case *parser.CreateNodeStmt, *parser.CreateEdgeStmt,
            *parser.AlterNodeStmt, *parser.AlterEdgeStmt,
            *parser.DropNodeStmt, *parser.DropEdgeStmt,
            *parser.InsertNodeStmt, *parser.InsertEdgeStmt,
            *parser.UpdateNodeStmt, *parser.UpdateEdgeStmt,
            *parser.DeleteNodeStmt, *parser.DeleteEdgeStmt:
            mutated = true
        }
    }
    
    fmt.Fprintf(conn, "OK - %d statement(s) executed successfully\n\n", len(stmts))

    // Append the original command to the commit log only if there was a mutation
    if mutated && s.commitLog != nil && !s.replaying {
        toAppend := strings.TrimSpace(command)
        if !strings.HasSuffix(toAppend, ";") {
            toAppend += ";"
        }
        _ = s.commitLog.Append(toAppend)
    }
}

// executeStatement executes a single parsed statement
func (s *Server) executeStatement(conn net.Conn, stmt parser.Stmt) error {
	switch st := stmt.(type) {
	case *parser.CreateNodeStmt:
		return s.executeCreateNode(st)
	case *parser.CreateEdgeStmt:
		return s.executeCreateEdge(st)
	case *parser.AlterNodeStmt:
		return s.executeAlterNode(st)
	case *parser.AlterEdgeStmt:
		return s.executeAlterEdge(st)
	case *parser.DropNodeStmt:
		return s.executeDropNode(st)
	case *parser.DropEdgeStmt:
		return s.executeDropEdge(st)
	case *parser.InsertNodeStmt:
		return s.executeInsertNode(conn, st)
	case *parser.InsertEdgeStmt:
		return s.executeInsertEdge(conn, st)
	case *parser.UpdateNodeStmt:
		return s.executeUpdateNode(conn, st)
	case *parser.UpdateEdgeStmt:
		return s.executeUpdateEdge(conn, st)
	case *parser.DeleteNodeStmt:
		return s.executeDeleteNode(conn, st)
	case *parser.DeleteEdgeStmt:
		return s.executeDeleteEdge(conn, st)
	case *parser.MatchStmt:
		return s.executeMatch(conn, st)
	default:
		return fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

// executeCreateNode executes a CREATE NODE statement
func (s *Server) executeCreateNode(stmt *parser.CreateNodeStmt) error {
	// Convert parser types to catalog types
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
	
	payload := catalog.CreateNodePayload{
		Name:   stmt.Name,
		Fields: fields,
	}
	
	_, err := s.registry.Apply(catalog.DDLEvent{
		Op:   catalog.OpCreateNode,
		Stmt: payload,
	})
	return err
}

// executeCreateEdge executes a CREATE EDGE statement
func (s *Server) executeCreateEdge(stmt *parser.CreateEdgeStmt) error {
	// Convert parser types to catalog types
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
		From: catalog.EdgeEndpoint{
			Label: stmt.From.Label,
			Card:  convertCardinality(stmt.From.Card),
		},
		To: catalog.EdgeEndpoint{
			Label: stmt.To.Label,
			Card:  convertCardinality(stmt.To.Card),
		},
		Props: props,
	}
	
	_, err := s.registry.Apply(catalog.DDLEvent{
		Op:   catalog.OpCreateEdge,
		Stmt: payload,
	})
	return err
}

// executeAlterNode executes an ALTER NODE statement
func (s *Server) executeAlterNode(stmt *parser.AlterNodeStmt) error {
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
	
	payload := catalog.AlterNodePayload{
		Name:    stmt.Name,
		Actions: []catalog.NodeAlterAction{action},
	}
	
	_, err := s.registry.Apply(catalog.DDLEvent{
		Op:   catalog.OpAlterNode,
		Stmt: payload,
	})
	return err
}

// executeAlterEdge executes an ALTER EDGE statement
func (s *Server) executeAlterEdge(stmt *parser.AlterEdgeStmt) error {
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
		// For SET FROM/TO, we need separate actions
		// This is a simplification - in reality we might need to handle both endpoints
		if stmt.From != nil {
			action.Type = "CHANGE_ENDPOINT"
			action.Endpoint = "FROM"
			action.NewEndpoint = &catalog.EdgeEndpoint{
				Label: stmt.From.Label,
				Card:  convertCardinality(stmt.From.Card),
			}
		} else if stmt.To != nil {
			action.Type = "CHANGE_ENDPOINT"
			action.Endpoint = "TO"
			action.NewEndpoint = &catalog.EdgeEndpoint{
				Label: stmt.To.Label,
				Card:  convertCardinality(stmt.To.Card),
			}
		}
	default:
		return fmt.Errorf("unsupported alter edge action: %v", stmt.Action)
	}
	
	payload := catalog.AlterEdgePayload{
		Name:    stmt.Name,
		Actions: []catalog.EdgeAlterAction{action},
	}
	
	_, err := s.registry.Apply(catalog.DDLEvent{
		Op:   catalog.OpAlterEdge,
		Stmt: payload,
	})
	return err
}

// executeDropNode executes a DROP NODE statement
func (s *Server) executeDropNode(stmt *parser.DropNodeStmt) error {
	payload := catalog.DropNodePayload{
		Name: stmt.Name,
	}
	
	_, err := s.registry.Apply(catalog.DDLEvent{
		Op:   catalog.OpDropNode,
		Stmt: payload,
	})
	return err
}

// executeDropEdge executes a DROP EDGE statement
func (s *Server) executeDropEdge(stmt *parser.DropEdgeStmt) error {
	payload := catalog.DropEdgePayload{
		Name: stmt.Name,
	}
	
	_, err := s.registry.Apply(catalog.DDLEvent{
		Op:   catalog.OpDropEdge,
		Stmt: payload,
	})
	return err
}

// Helper functions to convert between parser and catalog types

func convertTypeSpec(t parser.TypeSpec) catalog.TypeSpec {
	spec := catalog.TypeSpec{
		Base: convertBaseType(t.Base),
	}
	
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
		return catalog.BaseString // fallback
	}
}

func convertCardinality(c parser.Cardinality) catalog.Cardinality {
	switch c {
	case parser.CardOne:
		return catalog.One
	case parser.CardMany:
		return catalog.Many
	default:
		return catalog.One // fallback
	}
}

/* ---------------------- DML execution methods ---------------------- */

// Simple in-memory data store for demonstration
// In a real implementation, this would be a proper graph database
type GraphData struct {
	Nodes  map[string]map[string]interface{} // nodeType -> nodeID -> properties
	Edges  map[string][]EdgeInstance         // edgeType -> list of edge instances
	NextID int64                             // Simple ID generator
}

type EdgeInstance struct {
	ID         string
	FromNodeID string
	ToNodeID   string
	Properties map[string]interface{}
}

var graphData = &GraphData{
	Nodes:  make(map[string]map[string]interface{}),
	Edges:  make(map[string][]EdgeInstance),
	NextID: 1,
}

// executeInsertNode executes an INSERT NODE statement
func (s *Server) executeInsertNode(conn net.Conn, stmt *parser.InsertNodeStmt) error {
    // Validate node type exists in catalog
    cat := s.registry.Current()
    nodeType, exists := cat.Nodes[stmt.NodeType]
    if !exists {
        return fmt.Errorf("node type '%s' does not exist", stmt.NodeType)
    }
    // Generate new node ID
    nodeID := fmt.Sprintf("%d", graphData.NextID)
    graphData.NextID++
    // Initialize storage for this node type
    if graphData.Nodes[stmt.NodeType] == nil {
        graphData.Nodes[stmt.NodeType] = make(map[string]interface{})
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
    graphData.Nodes[stmt.NodeType][nodeID] = properties
    if conn != nil {
        fmt.Fprintf(conn, "Node inserted with ID: %s\n", nodeID)
    }
    return nil
}

// executeInsertEdge executes an INSERT EDGE statement
func (s *Server) executeInsertEdge(conn net.Conn, stmt *parser.InsertEdgeStmt) error {
    // Validate edge type exists
    cat := s.registry.Current()
    edgeType, exists := cat.Edges[stmt.EdgeType]
    if !exists {
        return fmt.Errorf("edge type '%s' does not exist", stmt.EdgeType)
    }
    // Resolve endpoints
    fromNodeID, err := s.findNodeID(stmt.FromNode)
    if err != nil { return fmt.Errorf("FROM node not found: %v", err) }
    toNodeID, err := s.findNodeID(stmt.ToNode)
    if err != nil { return fmt.Errorf("TO node not found: %v", err) }
    if stmt.FromNode.NodeType != edgeType.From.Label {
        return fmt.Errorf("FROM node type '%s' does not match edge FROM type '%s'", stmt.FromNode.NodeType, edgeType.From.Label)
    }
    if stmt.ToNode.NodeType != edgeType.To.Label {
        return fmt.Errorf("TO node type '%s' does not match edge TO type '%s'", stmt.ToNode.NodeType, edgeType.To.Label)
    }
    // Generate ID
    edgeID := fmt.Sprintf("edge_%d", graphData.NextID)
    graphData.NextID++
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
    edge := EdgeInstance{ ID: edgeID, FromNodeID: fromNodeID, ToNodeID: toNodeID, Properties: properties }
    graphData.Edges[stmt.EdgeType] = append(graphData.Edges[stmt.EdgeType], edge)
    if conn != nil {
        fmt.Fprintf(conn, "Edge inserted with ID: %s\n", edgeID)
    }
    return nil
}

// executeUpdateNode executes an UPDATE NODE statement
func (s *Server) executeUpdateNode(conn net.Conn, stmt *parser.UpdateNodeStmt) error {
    nodes := graphData.Nodes[stmt.NodeType]
    if nodes == nil { return fmt.Errorf("no nodes of type '%s' found", stmt.NodeType) }
    updated := 0
    for _, nodeProps := range nodes {
        if s.matchesConditions(nodeProps, stmt.Where) {
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
    if conn != nil { fmt.Fprintf(conn, "Updated %d node(s)\n", updated) }
    return nil
}

// executeUpdateEdge executes an UPDATE EDGE statement
func (s *Server) executeUpdateEdge(conn net.Conn, stmt *parser.UpdateEdgeStmt) error {
    edges := graphData.Edges[stmt.EdgeType]
    updated := 0
    for i := range edges {
        if s.matchesConditions(edges[i].Properties, stmt.Where) {
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
    if conn != nil { fmt.Fprintf(conn, "Updated %d edge(s)\n", updated) }
    return nil
}

// executeDeleteNode executes a DELETE NODE statement
func (s *Server) executeDeleteNode(conn net.Conn, stmt *parser.DeleteNodeStmt) error {
    nodes := graphData.Nodes[stmt.NodeType]
    if nodes == nil { return fmt.Errorf("no nodes of type '%s' found", stmt.NodeType) }
    deleted := 0
    for nodeID, nodeProps := range nodes {
        if s.matchesConditions(nodeProps, stmt.Where) {
            delete(nodes, nodeID)
            deleted++
        }
    }
    if conn != nil { fmt.Fprintf(conn, "Deleted %d node(s)\n", deleted) }
    return nil
}

// executeDeleteEdge executes a DELETE EDGE statement
func (s *Server) executeDeleteEdge(conn net.Conn, stmt *parser.DeleteEdgeStmt) error {
    edges := graphData.Edges[stmt.EdgeType]
    var remaining []EdgeInstance
    deleted := 0
    for _, edge := range edges {
        if s.matchesConditions(edge.Properties, stmt.Where) {
            deleted++
        } else {
            remaining = append(remaining, edge)
        }
    }
    graphData.Edges[stmt.EdgeType] = remaining
    if conn != nil { fmt.Fprintf(conn, "Deleted %d edge(s)\n", deleted) }
    return nil
}

// executeMatch executes a MATCH statement for querying
func (s *Server) executeMatch(conn net.Conn, stmt *parser.MatchStmt) error {
    if conn != nil { fmt.Fprintf(conn, "MATCH Results:\n") }
    for _, element := range stmt.Pattern {
        if !element.IsEdge {
            nodes := graphData.Nodes[element.Type]
            if nodes != nil {
                if conn != nil { fmt.Fprintf(conn, "\nNodes of type '%s':\n", element.Type) }
                for nodeID, props := range nodes {
                    if len(stmt.Where) == 0 || s.matchesConditions(props, stmt.Where) {
                        if conn != nil { fmt.Fprintf(conn, "  ID: %s, Properties: %v\n", nodeID, props) }
                    }
                }
            }
        }
    }
    return nil
}

/* ---------------------- Helper methods ---------------------- */

// findNodeID finds a node ID based on NodeRef (by direct ID or property match)
func (s *Server) findNodeID(nodeRef *parser.NodeRef) (string, error) {
    nodes := graphData.Nodes[nodeRef.NodeType]
    if nodes == nil {
        return "", fmt.Errorf("no nodes of type '%s' found", nodeRef.NodeType)
    }
    // Direct ID reference
    if nodeRef.ID != nil {
        nodeID := nodeRef.ID.Text
        if _, exists := nodes[nodeID]; exists {
            return nodeID, nil
        }
        return "", fmt.Errorf("node with ID '%s' not found", nodeID)
    }
    // Property-based search
    for nodeID, nodeProps := range nodes {
        if s.matchesConditions(nodeProps, nodeRef.Properties) {
            return nodeID, nil
        }
    }
    return "", fmt.Errorf("no matching node found")
}

// matchesConditions checks if properties match the given conditions
func (s *Server) matchesConditions(properties interface{}, conditions []parser.Property) bool {
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
		
		// Simple equality check
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
