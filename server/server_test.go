package server

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"grapho/catalog"
)

func TestServerBasicOperations(t *testing.T) {
	// Create temporary directory for test data
	tmpDir := t.TempDir()
	
	// Initialize catalog store and registry
	store, err := catalog.NewFileStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create catalog store: %v", err)
	}

	registry, err := catalog.Open(store)
	if err != nil {
		t.Fatalf("Failed to open catalog registry: %v", err)
	}

	// Create server
	srv := NewServer("localhost:0", registry) // Use port 0 for random available port
	
	// Start server in goroutine
	go func() {
		if err := srv.Start(); err != nil {
			t.Logf("Server stopped: %v", err)
		}
	}()
	
	// Give server time to start
	time.Sleep(100 * time.Millisecond)
	
	// Get the actual address the server is listening on
	addr := srv.listener.Addr().String()
	
	defer srv.Stop()
	
	// Connect to server
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()
	
	// Read welcome messages
	scanner := bufio.NewScanner(conn)
	for i := 0; i < 3; i++ { // Skip welcome messages
		if !scanner.Scan() {
			t.Fatal("Failed to read welcome message")
		}
	}
	
	// Test CREATE NODE
	fmt.Fprintf(conn, "CREATE NODE Person(id:int PRIMARY KEY, name:string);\n")
	if !scanner.Scan() {
		t.Fatal("Failed to read response")
	}
	response := scanner.Text()
	if !strings.Contains(response, "OK") {
		t.Errorf("Expected OK response, got: %s", response)
	}
	
	// Test CREATE EDGE
	fmt.Fprintf(conn, "CREATE EDGE Knows FROM Person ONE TO Person MANY;\n")
	if !scanner.Scan() {
		t.Fatal("Failed to read response")
	}
	response = scanner.Text()
	if !strings.Contains(response, "OK") {
		t.Errorf("Expected OK response, got: %s", response)
	}
	
	// Test ALTER NODE
	fmt.Fprintf(conn, "ALTER NODE Person ADD email:string;\n")
	if !scanner.Scan() {
		t.Fatal("Failed to read response")
	}
	response = scanner.Text()
	if !strings.Contains(response, "OK") {
		t.Errorf("Expected OK response, got: %s", response)
	}
	
	// Test error handling
	fmt.Fprintf(conn, "INVALID COMMAND;\n")
	if !scanner.Scan() {
		t.Fatal("Failed to read response")
	}
	response = scanner.Text()
	if !strings.Contains(response, "Parse errors") {
		t.Errorf("Expected parse error, got: %s", response)
	}
}

func TestServerMultipleClients(t *testing.T) {
	// Create temporary directory for test data
	tmpDir := t.TempDir()
	
	// Initialize catalog store and registry
	store, err := catalog.NewFileStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create catalog store: %v", err)
	}

	registry, err := catalog.Open(store)
	if err != nil {
		t.Fatalf("Failed to open catalog registry: %v", err)
	}

	// Create server
	srv := NewServer("localhost:0", registry)
	
	// Start server in goroutine
	go func() {
		if err := srv.Start(); err != nil {
			t.Logf("Server stopped: %v", err)
		}
	}()
	
	// Give server time to start
	time.Sleep(100 * time.Millisecond)
	
	addr := srv.listener.Addr().String()
	defer srv.Stop()
	
	// Connect multiple clients
	const numClients = 3
	clients := make([]net.Conn, numClients)
	
	for i := 0; i < numClients; i++ {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("Failed to connect client %d: %v", i, err)
		}
		clients[i] = conn
		
		// Skip welcome messages
		scanner := bufio.NewScanner(conn)
		for j := 0; j < 3; j++ {
			scanner.Scan()
		}
	}
	
	// Each client creates a different node
	for i, conn := range clients {
		nodeName := fmt.Sprintf("Node%d", i)
		fmt.Fprintf(conn, "CREATE NODE %s(id:int PRIMARY KEY);\n", nodeName)
		
		scanner := bufio.NewScanner(conn)
		if !scanner.Scan() {
			t.Fatalf("Failed to read response from client %d", i)
		}
		response := scanner.Text()
		if !strings.Contains(response, "OK") {
			t.Errorf("Client %d: Expected OK response, got: %s", i, response)
		}
		
		conn.Close()
	}
}

func TestServerCommandBuffer(t *testing.T) {
	// Create temporary directory for test data
	tmpDir := t.TempDir()
	
	// Initialize catalog store and registry
	store, err := catalog.NewFileStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create catalog store: %v", err)
	}

	registry, err := catalog.Open(store)
	if err != nil {
		t.Fatalf("Failed to open catalog registry: %v", err)
	}

	// Create server
	srv := NewServer("localhost:0", registry)
	
	// Start server in goroutine
	go func() {
		if err := srv.Start(); err != nil {
			t.Logf("Server stopped: %v", err)
		}
	}()
	
	// Give server time to start
	time.Sleep(100 * time.Millisecond)
	
	addr := srv.listener.Addr().String()
	defer srv.Stop()
	
	// Connect to server
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()
	
	// Read welcome messages
	scanner := bufio.NewScanner(conn)
	for i := 0; i < 3; i++ {
		scanner.Scan()
	}
	
	// Send multi-line command
	fmt.Fprintf(conn, "CREATE NODE Person(\n")
	fmt.Fprintf(conn, "  id:int PRIMARY KEY,\n")
	fmt.Fprintf(conn, "  name:string\n")
	fmt.Fprintf(conn, ");\n")
	
	if !scanner.Scan() {
		t.Fatal("Failed to read response")
	}
	response := scanner.Text()
	if !strings.Contains(response, "OK") {
		t.Errorf("Expected OK response, got: %s", response)
	}
}
