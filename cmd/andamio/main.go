// Command andamio runs a membership node.
//
// Each node participates in a SWIM-style probe/gossip loop over UDP and
// exposes a small HTTP status endpoint. It can bootstrap a brand new
// cluster or join an existing one by pointing at a seed node.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/edxeme/andamio/internal/httpapi"
	"github.com/edxeme/andamio/internal/node"
)

func main() {
	bindAddr := flag.String("bind", "0.0.0.0", "address to bind probe/gossip traffic on")
	bindPort := flag.Int("port", 7946, "udp port for probe/gossip traffic")
	httpAddr := flag.String("http", "0.0.0.0:7373", "listen address for the status endpoint")
	name := flag.String("name", "", "node name (defaults to hostname)")
	seed := flag.String("seed", "", "address of an existing member to join, host:port")
	probeInterval := flag.Duration("probe-interval", time.Second, "probe cadence")
	probeTimeout := flag.Duration("probe-timeout", 500*time.Millisecond, "per-probe ack window")

	flag.Parse()

	if *name == "" {
		host, err := os.Hostname()
		if err != nil {
			fmt.Fprintf(os.Stderr, "andamio: resolve hostname: %v\n", err)
			os.Exit(1)
		}
		*name = host
	}

	logger := log.New(os.Stderr, fmt.Sprintf("andamio[%s] ", *name), log.LstdFlags|log.Lmsgprefix)

	n, err := node.New(node.Config{
		Name:          *name,
		BindHost:      *bindAddr,
		BindPort:      *bindPort,
		ProbeInterval: *probeInterval,
		ProbeTimeout:  *probeTimeout,
		Logger:        logger,
	})
	if err != nil {
		logger.Fatalf("start node: %v", err)
	}
	if err := n.Start(*seed); err != nil {
		logger.Fatalf("join cluster: %v", err)
	}

	status := httpapi.New(n)
	go func() {
		if err := status.ListenAndServe(*httpAddr); err != nil {
			logger.Printf("status endpoint: %v", err)
		}
	}()

	logger.Printf("node up (udp %s, http %s, seed %q)", n.Addr(), *httpAddr, *seed)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig

	logger.Printf("shutting down gracefully")
	n.Leave()
}
