// Command andamioctl inspects an andamio node's status endpoint.
//
//	andamioctl members <addr>   print the node's membership table
//	andamioctl health <addr>    check /healthz
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/edxeme/andamio/internal/proto"
)

const usage = `andamioctl — inspect andamio clusters

Usage:
  andamioctl members <addr>
  andamioctl health <addr>

Addr is host:port of a node's status endpoint.
`

func main() {
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()

	args := flag.Args()
	if len(args) != 2 {
		flag.Usage()
		os.Exit(2)
	}

	client := &http.Client{Timeout: 3 * time.Second}

	var err error
	switch args[0] {
	case "members":
		err = members(client, args[1])
	case "health":
		err = health(client, args[1])
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "andamioctl: %v\n", err)
		os.Exit(1)
	}
}

func members(client *http.Client, addr string) error {
	url := fmt.Sprintf("http://%s/members", strings.TrimPrefix(addr, "http://"))
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("members: http %s", resp.Status)
	}

	var table []proto.Member
	if err := json.NewDecoder(resp.Body).Decode(&table); err != nil {
		return err
	}

	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tADDR\tSTATE\tINCARNATION")
	for _, m := range table {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\n", m.Name, m.Addr, m.State, m.Incarnation)
	}
	return tw.Flush()
}

func health(client *http.Client, addr string) error {
	url := fmt.Sprintf("http://%s/healthz", strings.TrimPrefix(addr, "http://"))
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	fmt.Printf("%s %s\n", addr, resp.Status)
	return nil
}
