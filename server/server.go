package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"

	"grapho/catalog"
	"grapho/executor"
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
	exec     *executor.Executor
}

// NewServer creates a new server instance
func NewServer(addr string, registry *catalog.Registry) *Server {
	s := &Server{
		addr:     addr,
		registry: registry,
		clients:  make(map[net.Conn]bool),
	}
	// initialize executor
	s.exec = executor.New(registry)
	return s
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
				if err := s.exec.ExecuteStatement(nil, st); err != nil {
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
	
	// Delegate to executor
	mutated, err := s.exec.ExecuteStatements(conn, stmts)
	if err != nil {
		return
	}
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
// Note: All statement execution logic has been moved to the executor package.

// Execution of DDL/DML has moved to the executor package.
