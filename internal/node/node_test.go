package node

import (
	"testing"
	"time"

	"github.com/edxeme/andamio/internal/proto"
)

func TestSupersedesRules(t *testing.T) {
	alive := proto.Member{Name: "n", Incarnation: 3, State: proto.Alive}
	suspect := proto.Member{Name: "n", Incarnation: 3, State: proto.Suspect}
	dead := proto.Member{Name: "n", Incarnation: 3, State: proto.Dead}

	cases := []struct {
		report proto.Member
		cur    proto.Member
		want   bool
	}{
		{report: proto.Member{Incarnation: 4, State: proto.Alive}, cur: alive, want: true},
		{report: proto.Member{Incarnation: 2, State: proto.Dead}, cur: alive, want: false},
		{report: suspect, cur: alive, want: true},
		{report: alive, cur: suspect, want: false},
		{report: dead, cur: suspect, want: true},
		{report: alive, cur: alive, want: false},
	}
	for i, tc := range cases {
		if got := supersedes(tc.report, tc.cur); got != tc.want {
			t.Errorf("case %d: supersedes(%v, %v) = %v, want %v", i, tc.report.State, tc.cur.State, got, tc.want)
		}
	}
}

func TestMergeIgnoresDeadStranger(t *testing.T) {
	n := newTestNode(t, "solo")
	defer n.Shutdown()

	n.merge(proto.Member{Name: "ghost", Addr: "127.0.0.1:1", State: proto.Dead})
	if got := len(n.Members()); got != 1 {
		t.Errorf("table has %d members, want 1 (self only)", got)
	}
}

func TestClusterFormation(t *testing.T) {
	a := newTestNode(t, "a")
	defer a.Shutdown()
	if err := a.Start(""); err != nil {
		t.Fatalf("start a: %v", err)
	}

	b := newTestNode(t, "b")
	defer b.Shutdown()
	c := newTestNode(t, "c")
	defer c.Shutdown()

	for _, joiner := range []*Node{b, c} {
		if err := joiner.Start(a.Addr()); err != nil {
			t.Fatalf("start %s: %v", joiner.Name(), err)
		}
	}

	// Every node must eventually see the full table alive.
	for _, n := range []*Node{a, b, c} {
		waitFor(t, n, 3, proto.Alive)
	}
}

func TestFailureDetection(t *testing.T) {
	a := newTestNode(t, "a")
	defer a.Shutdown()
	if err := a.Start(""); err != nil {
		t.Fatalf("start a: %v", err)
	}

	b := newTestNode(t, "b")
	defer b.Shutdown()
	if err := b.Start(a.Addr()); err != nil {
		t.Fatalf("start b: %v", err)
	}
	waitFor(t, a, 2, proto.Alive)
	waitFor(t, b, 2, proto.Alive)

	// Hard kill: no leave message, the cluster must notice on its own.
	b.Shutdown()
	waitForState(t, a, "b", proto.Dead)
}

func TestGracefulLeave(t *testing.T) {
	a := newTestNode(t, "a")
	defer a.Shutdown()
	if err := a.Start(""); err != nil {
		t.Fatalf("start a: %v", err)
	}

	b := newTestNode(t, "b")
	if err := b.Start(a.Addr()); err != nil {
		t.Fatalf("start b: %v", err)
	}
	waitFor(t, a, 2, proto.Alive)

	b.Leave() // graceful: stops itself
	waitForState(t, a, "b", proto.Left)
}

func TestSelfRefutation(t *testing.T) {
	n := newTestNode(t, "under-attack")
	defer n.Shutdown()

	self := n.Members()[0]
	n.merge(proto.Member{Name: self.Name, Addr: self.Addr, Incarnation: self.Incarnation, State: proto.Suspect})

	after := n.Members()[0]
	if after.Incarnation != self.Incarnation+1 {
		t.Fatalf("incarnation = %d, want %d", after.Incarnation, self.Incarnation+1)
	}
	if after.State != proto.Alive {
		t.Errorf("state = %s, want alive", after.State)
	}

	// The refutation must be queued for gossip, not kept private.
	var rebroadcast bool
	for _, g := range n.takeGossip() {
		if g.Name == self.Name && g.State == proto.Alive && g.Incarnation == after.Incarnation {
			rebroadcast = true
		}
	}
	if !rebroadcast {
		t.Error("refutation was not queued for gossip")
	}
}

func newTestNode(t *testing.T, name string) *Node {
	t.Helper()
	n, err := New(Config{
		Name:          name,
		BindHost:      "127.0.0.1",
		ProbeInterval: 100 * time.Millisecond,
		ProbeTimeout:  80 * time.Millisecond,
		SuspectMult:   3,
		Witnesses:     2,
	})
	if err != nil {
		t.Fatalf("new node %s: %v", name, err)
	}
	return n
}

// waitFor polls until node n knows exactly want members in the given state.
func waitFor(t *testing.T, n *Node, want int, state proto.State) {
	t.Helper()
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		count := 0
		for _, m := range n.Members() {
			if m.State == state {
				count++
			}
		}
		if count == want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("node %s: timed out waiting for %d member(s) in state %s; table: %v",
		n.Name(), want, state, n.Members())
}

// waitForState polls until node n holds member name in the given state.
func waitForState(t *testing.T, n *Node, name string, state proto.State) {
	t.Helper()
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		for _, m := range n.Members() {
			if m.Name == name && m.State == state {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("node %s: timed out waiting for %s to be %s; table: %v",
		n.Name(), name, state, n.Members())
}
