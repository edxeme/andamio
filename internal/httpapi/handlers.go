// Package httpapi exposes a node's membership state over HTTP.
//
// Endpoints:
//
//	GET /members  — the node's view of the cluster, as JSON
//	GET /self     — this node's own member record
//	GET /healthz  — liveness for load balancers and probes
package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/edxeme/andamio/internal/node"
)

// Server serves status traffic for one node.
type Server struct {
	node *node.Node
	mux  *http.ServeMux
}

// New wires the status endpoints for a node.
func New(n *node.Node) *Server {
	s := &Server{node: n, mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /members", s.members)
	s.mux.HandleFunc("GET /self", s.self)
	s.mux.HandleFunc("GET /healthz", s.healthz)
	return s
}

// Handler exposes the routes for http.Server or httptest use.
func (s *Server) Handler() http.Handler { return s.mux }

// ListenAndServe blocks serving the status endpoints.
func (s *Server) ListenAndServe(addr string) error {
	return http.ListenAndServe(addr, s.Handler())
}

func (s *Server) members(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.node.Members())
}

func (s *Server) self(w http.ResponseWriter, _ *http.Request) {
	for _, m := range s.node.Members() {
		if m.Name == s.node.Name() {
			writeJSON(w, http.StatusOK, m)
			return
		}
	}
	http.Error(w, "self not in table", http.StatusInternalServerError)
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
