package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/e13815332/multiscan/internal/worker"
)

func main() {
	master := flag.String("master", "ws://localhost:8800/api/worker/ws", "Master WebSocket URL")
	name := flag.String("name", "", "Worker name (default: hostname)")
	uuid := flag.String("uuid", "", "Worker UUID (default: hostname)")
	flag.Parse()

	hostname, _ := os.Hostname()
	if *name == "" {
		*name = hostname
	}
	if *uuid == "" {
		*uuid = hostname
	}

	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Printf("[worker] Starting Worker %s...", *name)

	client := worker.NewClient(*master, *name, *uuid)

	if err := client.Run(); err != nil {
		log.Fatalf("[worker] Failed to connect: %v", err)
	}

	fmt.Printf("✅ Worker %s connected to %s\n", *name, *master)

	// Wait for signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("[worker] Shutting down...")
	client.Stop()
}
