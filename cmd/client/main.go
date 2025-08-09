package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"regexp"
	"strings"
)

// matchRenderer holds temporary state while collecting a MATCH response
type matchRenderer struct {
	collecting bool
	lines      []string
}

func (mr *matchRenderer) start() {
	mr.collecting = true
	mr.lines = mr.lines[:0]
}

func (mr *matchRenderer) add(line string) {
	mr.lines = append(mr.lines, line)
}

func (mr *matchRenderer) reset() {
	mr.collecting = false
	mr.lines = mr.lines[:0]
}

// render pretty-prints collected MATCH response lines
func (mr *matchRenderer) render() {
	if len(mr.lines) == 0 {
		return
	}

	// Patterns: "Nodes of type 'X':" and "  ID: <id>, Properties: map[... ]"
	sectionRe := regexp.MustCompile(`^Nodes of type '([^']+)':$`)
	idLineRe := regexp.MustCompile(`^\s*ID:\s*([^,]+),\s*Properties:\s*(.*)$`)

	currentType := ""
	fmt.Println("MATCH Results (formatted):")

	for _, ln := range mr.lines {
		if m := sectionRe.FindStringSubmatch(strings.TrimSpace(ln)); m != nil {
			currentType = m[1]
			fmt.Printf("\nType: %s\n", currentType)
			fmt.Println("------------------------")
			continue
		}
		if m := idLineRe.FindStringSubmatch(strings.TrimSpace(ln)); m != nil {
			id := strings.TrimSpace(m[1])
			propsRaw := strings.TrimSpace(m[2])
			// Parse Go-style map[...] to key=value list (best-effort)
			props := parseProps(propsRaw)
			fmt.Printf("- id: %s", id)
			if currentType != "" {
				fmt.Printf("  (%s)", currentType)
			}
			fmt.Println()
			if len(props) > 0 {
				for _, kv := range props {
					fmt.Printf("    %s\n", kv)
				}
			}
			continue
		}
		// Fallback: print any other line inside block
		if strings.TrimSpace(ln) != "" {
			fmt.Println(ln)
		}
	}
	fmt.Println()
}

// parseProps converts a "map[k1:v1 k2:v2]" string into []"k=v"
func parseProps(s string) []string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "map[") && strings.HasSuffix(s, "]") {
		inner := s[4:len(s)-1]
		parts := strings.Fields(inner)
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			// keep as "k:v" -> "k=v"
			out = append(out, strings.ReplaceAll(p, ":", "="))
		}
		return out
	}
	return nil
}

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

	// Start goroutine to read and render server responses
	go func() {
		scanner := bufio.NewScanner(conn)
		mr := &matchRenderer{}
		for scanner.Scan() {
			line := scanner.Text()

			// Detect start of MATCH block
			if strings.HasPrefix(line, "MATCH Results:") {
				mr.start()
				// Don't print the raw header; we'll print our own later
				continue
			}

			// If collecting MATCH lines, buffer until command completion
			if mr.collecting {
				// End conditions: OK -, Error executing, or blank line after OK
				if strings.HasPrefix(line, "OK - ") || strings.HasPrefix(line, "Error executing") {
					// First render the collected MATCH output
					mr.render()
					mr.reset()
					// Then print the completion line
					fmt.Println(line)
					continue
				}
				// Still part of MATCH block
				mr.add(line)
				continue
			}

			// Default: echo line as-is
			fmt.Println(line)
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
