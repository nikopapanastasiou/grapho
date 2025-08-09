package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"grapho/catalog"
	"grapho/server"
)

func main() {
	var (
		addr      = flag.String("addr", ":8080", "TCP address to listen on")
		dataDir   = flag.String("data", "./data", "Directory to store catalog data")
		logFormat = flag.String("log-format", "binary", "Commit log format: text|binary")
	)
	flag.Parse()

	// Create data directory if it doesn't exist
	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// Initialize catalog store and registry
	store, err := catalog.NewFileStore(*dataDir)
	if err != nil {
		log.Fatalf("Failed to create catalog store: %v", err)
	}

	registry, err := catalog.Open(store)
	if err != nil {
		log.Fatalf("Failed to open catalog registry: %v", err)
	}

	// Create and start server
	srv := server.NewServer(*addr, registry)

	// Open and start commit log with selected format, attach to server
	var format server.LogFormat
	switch *logFormat {
	case "binary":
		format = server.LogFormatBinary
	default:
		format = server.LogFormatText
	}
	cl, err := server.OpenCommitLogWithFormat(*dataDir, format)
	if err != nil {
		log.Fatalf("Failed to open commit log: %v", err)
	}
	cl.Start()
	srv.AttachCommitLog(cl)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\nShutting down server...")
		if err := srv.Stop(); err != nil {
			log.Printf("Error stopping server: %v", err)
		}
		if err := cl.Stop(); err != nil {
			log.Printf("Error stopping commit log: %v", err)
		}
		os.Exit(0)
	}()

	// Start server (blocks until stopped)
	if err := srv.Start(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
