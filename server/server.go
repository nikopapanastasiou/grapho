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
}

// NewServer creates a new server instance
func NewServer(addr string, registry *catalog.Registry) *Server {
	return &Server{
		addr:     addr,
		registry: registry,
		clients:  make(map[net.Conn]bool),
	}
}

// Start begins listening for connections
func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.addr, err)
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
	
	// Execute each statement
	for i, stmt := range stmts {
		if err := s.executeStatement(conn, stmt); err != nil {
			fmt.Fprintf(conn, "Error executing statement %d: %s\n", i+1, err.Error())
			return
		}
	}
	
	fmt.Fprintf(conn, "OK - %d statement(s) executed successfully\n\n", len(stmts))
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
