package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/edxeme/andamio/internal/node"
)

func TestMembersEndpoint(t *testing.T) {
	n, err := node.New(node.Config{Name: "solo", BindHost: "127.0.0.1"})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	defer n.Shutdown()

	srv := httptest.NewServer(New(n).Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/members")
	if err != nil {
		t.Fatalf("get /members: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var table []protoMember
	if err := json.NewDecoder(resp.Body).Decode(&table); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(table) != 1 || table[0].Name != "solo" || table[0].State != "alive" {
		t.Errorf("unexpected table: %+v", table)
	}
}

func TestHealthzEndpoint(t *testing.T) {
	n, err := node.New(node.Config{Name: "solo", BindHost: "127.0.0.1"})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	defer n.Shutdown()

	srv := httptest.NewServer(New(n).Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("get /healthz: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

// protoMember mirrors the json shape of proto.Member without importing it.
type protoMember struct {
	Name  string
	State string
}
