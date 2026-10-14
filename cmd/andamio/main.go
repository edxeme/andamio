// Command andamio runs a membership node.
//
// Each node participates in a SWIM-style probe/gossip loop over UDP and
// exposes a small HTTP status endpoint. It can bootstrap a brand new
// cluster or join an existing one by pointing at a seed node.
package main

import (
	"flag"
	"fmt"
	"os"
)

const usage = `andamio — scaffolding for peer-to-peer clusters

Usage:
  andamio [flags]

Flags:
`

func main() {
	bindAddr := flag.String("bind", "0.0.0.0", "address to bind probe/gossip traffic on")
	bindPort := flag.Int("port", 7946, "udp port for probe/gossip traffic")
	httpAddr := flag.String("http", "0.0.0.0:7373", "listen address for the status endpoint")
	name := flag.String("name", "", "node name (defaults to hostname)")
	seed := flag.String("seed", "", "address of an existing member to join, host:port")

	flag.Usage = func() {
		fmt.Fprint(os.Stderr, usage)
		flag.PrintDefaults()
	}
	flag.Parse()

	if *name == "" {
		host, err := os.Hostname()
		if err != nil {
			fmt.Fprintf(os.Stderr, "andamio: resolve hostname: %v\n", err)
			os.Exit(1)
		}
		*name = host
	}

	fmt.Printf("andamio: node %q would listen on %s:%d (http %s, seed %q)\n",
		*name, *bindAddr, *bindPort, *httpAddr, *seed)

	// The probe loop and status server land in follow-up work; for now the
	// binary validates its wiring and configuration surface.
}
