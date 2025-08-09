package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	var addr = flag.String("addr", "localhost:8080", "Server address to connect to")
	flag.Parse()

	// Connect to server
	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		fmt.Printf("Failed to connect to server: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Printf("Connected to Grapho server at %s\n", *addr)
	fmt.Println("Type DDL commands or 'quit' to exit")

	// Start goroutine to read server responses
	go func() {
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			fmt.Println(scanner.Text())
		}
	}()

	// Read user input and send to server
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		if line == "quit" || line == "exit" {
			fmt.Fprintf(conn, "quit\n")
			break
		}

		fmt.Fprintf(conn, "%s\n", line)
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading input: %v\n", err)
	}
}
