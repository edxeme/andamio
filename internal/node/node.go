// Package node implements a SWIM-style membership node.
//
// Each node runs a periodic probe loop over UDP, piggybacks membership
// updates on every outbound message, and merges incoming reports with
// incarnation-ordered rules. Failed direct probes are retried through
// witnesses before a member is marked suspect.
package node

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/edxeme/andamio/internal/proto"
	"github.com/edxeme/andamio/internal/transport"
)

const (
	defaultProbeInterval = time.Second
	defaultProbeTimeout  = 500 * time.Millisecond
	defaultSuspectMult   = 4
	defaultWitnesses     = 2
	gossipFanout         = 8
	changeRetransmits    = 6
	joinTimeout          = 3 * time.Second
)

// Config configures a node.
type Config struct {
	Name          string
	BindHost      string
	BindPort      int
	ProbeInterval time.Duration
	ProbeTimeout  time.Duration
	// SuspectMult is how many probe intervals a suspect has to refute
	// itself before it is declared dead.
	SuspectMult int
	// Witnesses is how many peers relay an indirect probe after a miss.
	Witnesses int
	Logger    *log.Logger
}

func (c *Config) setDefaults() {
	if c.ProbeInterval <= 0 {
		c.ProbeInterval = defaultProbeInterval
	}
	if c.ProbeTimeout <= 0 {
		c.ProbeTimeout = defaultProbeTimeout
	}
	if c.SuspectMult <= 0 {
		c.SuspectMult = defaultSuspectMult
	}
	if c.Witnesses <= 0 {
		c.Witnesses = defaultWitnesses
	}
	if c.Logger == nil {
		c.Logger = log.Default()
	}
}

// Metrics are monotonic counters exposed over the status endpoint.
type Metrics struct {
	PingsSent     atomic.Uint64
	PingsRecv     atomic.Uint64
	AcksRecv      atomic.Uint64
	IndirectSent  atomic.Uint64
	GossipReports atomic.Uint64
	Suspicions    atomic.Uint64
	Deaths        atomic.Uint64
	Refutations   atomic.Uint64
}

type pendingChange struct {
	Member      proto.Member
	Retransmits int
}

// Node is one membership participant.
type Node struct {
	cfg     Config
	udp     *transport.UDP
	Metrics Metrics

	mu      sync.Mutex
	members map[string]proto.Member
	since   map[string]time.Time // when each member last changed state
	queue   []pendingChange
	seq     uint64
	acks    map[uint64]chan struct{}
	joinCh  chan []proto.Member

	stop     chan struct{}
	stopOnce sync.Once
}

// New binds the UDP socket and seeds the table with this node alone.
func New(cfg Config) (*Node, error) {
	cfg.setDefaults()
	n := &Node{
		cfg:     cfg,
		members: make(map[string]proto.Member),
		since:   make(map[string]time.Time),
		acks:    make(map[uint64]chan struct{}),
		joinCh:  make(chan []proto.Member, 1),
		stop:    make(chan struct{}),
	}

	udp, err := transport.Listen(cfg.BindHost, cfg.BindPort, n.handle)
	if err != nil {
		return nil, err
	}
	n.udp = udp

	_, port, _ := net.SplitHostPort(udp.LocalAddr())
	// Boot time seeds the incarnation so a restarted node with the same
	// name outranks the stale records the cluster still holds about it.
	n.members[cfg.Name] = proto.Member{
		Name:        cfg.Name,
		Addr:        net.JoinHostPort(advertiseHost(cfg.BindHost), port),
		Incarnation: uint64(time.Now().Unix()),
		State:       proto.Alive,
	}
	return n, nil
}

func advertiseHost(bindHost string) string {
	if bindHost == "" || bindHost == "0.0.0.0" || bindHost == "::" {
		return "127.0.0.1"
	}
	return bindHost
}

// Name is the node's cluster-unique name.
func (n *Node) Name() string { return n.cfg.Name }

// Addr is the advertised udp endpoint.
func (n *Node) Addr() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.members[n.cfg.Name].Addr
}

// Start joins the cluster through seed (when set) and launches the probe loop.
func (n *Node) Start(seed string) error {
	if seed != "" {
		if err := n.join(seed); err != nil {
			n.Shutdown()
			return err
		}
	}
	go n.probeLoop()
	return nil
}

// Leave announces a graceful departure to alive peers and shuts down.
func (n *Node) Leave() {
	n.mu.Lock()
	self := n.members[n.cfg.Name]
	self.State = proto.Left
	n.members[n.cfg.Name] = self
	n.enqueueLocked(self)
	n.mu.Unlock()

	for _, m := range n.Members() {
		if m.Name == n.cfg.Name || m.State != proto.Alive {
			continue
		}
		msg := &proto.Message{Leave: &proto.Leave{Member: self}, Gossip: n.takeGossip()}
		if err := n.udp.Send(m.Addr, msg); err != nil {
			n.cfg.Logger.Printf("leave: notify %s: %v", m.Name, err)
		}
	}
	n.Shutdown()
}

// Shutdown stops the probe loop and closes the socket. It is idempotent.
func (n *Node) Shutdown() {
	n.stopOnce.Do(func() { close(n.stop) })
	n.udp.Close()
}

// Members returns a name-sorted copy of the membership table.
func (n *Node) Members() []proto.Member {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]proto.Member, 0, len(n.members))
	for _, m := range n.members {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (n *Node) join(seed string) error {
	n.mu.Lock()
	self := n.members[n.cfg.Name]
	n.mu.Unlock()

	if err := n.udp.Send(seed, &proto.Message{Join: &proto.Join{Member: self}}); err != nil {
		return fmt.Errorf("node: join %s: %w", seed, err)
	}
	select {
	case members := <-n.joinCh:
		for _, m := range members {
			n.merge(m)
		}
		n.cfg.Logger.Printf("joined via %s with %d members", seed, len(members))
		return nil
	case <-time.After(joinTimeout):
		return fmt.Errorf("node: join %s: no join-ack within %s", seed, joinTimeout)
	}
}

func (n *Node) probeLoop() {
	ticker := time.NewTicker(n.cfg.ProbeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.stop:
			return
		case <-ticker.C:
			n.expireSuspects()
			if target, ok := n.pickTarget(); ok {
				n.probe(target)
			}
		}
	}
}

// pickTarget chooses a random live peer to probe this round.
func (n *Node) pickTarget() (proto.Member, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	var candidates []proto.Member
	for _, m := range n.members {
		if m.Name != n.cfg.Name && m.State != proto.Dead && m.State != proto.Left {
			candidates = append(candidates, m)
		}
	}
	if len(candidates) == 0 {
		return proto.Member{}, false
	}
	return candidates[rand.Intn(len(candidates))], true
}

// probe runs one SWIM round: direct ping, then indirect ping-req through
// witnesses, and finally a suspicion if nobody heard back.
func (n *Node) probe(target proto.Member) {
	seq := n.nextSeq()
	if n.ping(target.Addr, seq, n.cfg.ProbeTimeout) {
		return
	}

	if n.indirectPing(target, seq) {
		return
	}

	n.cfg.Logger.Printf("suspecting %s after failed direct and indirect probes", target.Name)
	n.Metrics.Suspicions.Add(1)
	n.merge(proto.Member{Name: target.Name, Addr: target.Addr, Incarnation: target.Incarnation, State: proto.Suspect})
}

func (n *Node) ping(addr string, seq uint64, timeout time.Duration) bool {
	ch := n.register(seq)
	defer n.unregister(seq)

	n.Metrics.PingsSent.Add(1)
	msg := &proto.Message{Ping: &proto.Ping{Seq: seq}, Gossip: n.takeGossip()}
	if err := n.udp.Send(addr, msg); err != nil {
		n.cfg.Logger.Printf("ping %s: %v", addr, err)
		return false
	}

	select {
	case <-ch:
		return true
	case <-time.After(timeout):
		return false
	case <-n.stop:
		return false
	}
}

// indirectPing asks up to Witnesses random peers to probe target and
// report back on the same sequence number.
func (n *Node) indirectPing(target proto.Member, seq uint64) bool {
	n.mu.Lock()
	var witnesses []proto.Member
	for _, m := range n.members {
		if m.Name != n.cfg.Name && m.Name != target.Name && m.State == proto.Alive {
			witnesses = append(witnesses, m)
		}
	}
	n.mu.Unlock()

	rand.Shuffle(len(witnesses), func(i, j int) { witnesses[i], witnesses[j] = witnesses[j], witnesses[i] })
	if len(witnesses) > n.cfg.Witnesses {
		witnesses = witnesses[:n.cfg.Witnesses]
	}
	if len(witnesses) == 0 {
		return false
	}

	ch := n.register(seq)
	defer n.unregister(seq)

	for _, w := range witnesses {
		n.Metrics.IndirectSent.Add(1)
		req := &proto.Message{
			PingReq: &proto.PingReq{Seq: seq, Target: target.Name, TargetAddr: target.Addr},
			Gossip:  n.takeGossip(),
		}
		if err := n.udp.Send(w.Addr, req); err != nil {
			n.cfg.Logger.Printf("ping-req via %s: %v", w.Name, err)
		}
	}

	select {
	case <-ch:
		return true
	case <-time.After(2 * n.cfg.ProbeTimeout):
		return false
	case <-n.stop:
		return false
	}
}

func (n *Node) nextSeq() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.seq++
	return n.seq
}

func (n *Node) register(seq uint64) <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	ch := make(chan struct{}, 1)
	n.acks[seq] = ch
	return ch
}

func (n *Node) unregister(seq uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.acks, seq)
}

// expireSuspects promotes suspects that failed to refute within the window.
func (n *Node) expireSuspects() {
	cutoff := time.Now().Add(-time.Duration(n.cfg.SuspectMult) * n.cfg.ProbeInterval)

	n.mu.Lock()
	defer n.mu.Unlock()
	for name, m := range n.members {
		if m.State != proto.Suspect {
			continue
		}
		if t, ok := n.since[name]; !ok || t.After(cutoff) {
			continue
		}
		m.State = proto.Dead
		n.members[name] = m
		n.since[name] = time.Now()
		n.enqueueLocked(m)
		n.Metrics.Deaths.Add(1)
		n.cfg.Logger.Printf("declared %s dead", name)
	}
}

// handle is the transport callback. It runs in its own goroutine per datagram.
func (n *Node) handle(msg *proto.Message, from *net.UDPAddr) {
	n.applyGossip(msg.Gossip)

	switch {
	case msg.Ping != nil:
		n.Metrics.PingsRecv.Add(1)
		n.send(from.String(), &proto.Message{Ack: &proto.Ack{Seq: msg.Ping.Seq}})

	case msg.PingReq != nil:
		// Probe the target on the requester's behalf and relay the result.
		req := msg.PingReq
		go func() {
			if n.ping(req.TargetAddr, req.Seq, n.cfg.ProbeTimeout) {
				n.send(from.String(), &proto.Message{Ack: &proto.Ack{Seq: req.Seq}})
			}
		}()

	case msg.Ack != nil:
		n.Metrics.AcksRecv.Add(1)
		n.resolve(msg.Ack.Seq)

	case msg.Join != nil:
		n.merge(msg.Join.Member)
		n.send(from.String(), &proto.Message{JoinAck: &proto.JoinAck{Members: n.Members()}})

	case msg.JoinAck != nil:
		select {
		case n.joinCh <- msg.JoinAck.Members:
		default:
		}

	case msg.Leave != nil:
		n.merge(msg.Leave.Member)
	}
}

func (n *Node) send(addr string, msg *proto.Message) {
	msg.Gossip = append(msg.Gossip, n.takeGossip()...)
	if err := n.udp.Send(addr, msg); err != nil {
		n.cfg.Logger.Printf("send %s to %s: %v", msg, addr, err)
	}
}

func (n *Node) resolve(seq uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if ch, ok := n.acks[seq]; ok {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (n *Node) applyGossip(reports []proto.Member) {
	for _, r := range reports {
		n.Metrics.GossipReports.Add(1)
		n.merge(r)
	}
}

// merge folds one report into the table. Higher incarnation wins; on a tie,
// suspect beats alive and dead beats suspect. A suspicion or death report
// about ourselves is refuted by bumping our incarnation.
func (n *Node) merge(report proto.Member) {
	if report.Name == n.cfg.Name {
		if report.State == proto.Suspect || report.State == proto.Dead {
			n.refute(report.Incarnation)
		}
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	cur, exists := n.members[report.Name]
	if !exists {
		if report.State == proto.Dead || report.State == proto.Left {
			return // never saw this member alive, nothing to retire
		}
		n.members[report.Name] = report
		n.since[report.Name] = time.Now()
		n.enqueueLocked(report)
		return
	}

	if !supersedes(report, cur) {
		return
	}
	if report.State != cur.State || report.Incarnation != cur.Incarnation {
		n.since[report.Name] = time.Now()
		n.enqueueLocked(report)
	}
	n.members[report.Name] = report
}

// refute answers a suspicion about ourselves with a higher incarnation.
func (n *Node) refute(suspectedAt uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	self := n.members[n.cfg.Name]
	if self.State == proto.Left || suspectedAt < self.Incarnation {
		return
	}
	self.Incarnation = suspectedAt + 1
	self.State = proto.Alive
	n.members[n.cfg.Name] = self
	n.enqueueLocked(self)
	n.Metrics.Refutations.Add(1)
	n.cfg.Logger.Printf("refuted suspicion at incarnation %d", self.Incarnation)
}

// supersedes reports whether report should replace cur.
func supersedes(report, cur proto.Member) bool {
	if report.Incarnation != cur.Incarnation {
		return report.Incarnation > cur.Incarnation
	}
	return stateRank(report.State) > stateRank(cur.State)
}

func stateRank(s proto.State) int {
	switch s {
	case proto.Alive:
		return 0
	case proto.Suspect:
		return 1
	case proto.Dead, proto.Left:
		return 2
	default:
		return -1
	}
}

func (n *Node) enqueueLocked(m proto.Member) {
	for i := range n.queue {
		if n.queue[i].Member.Name == m.Name {
			n.queue[i] = pendingChange{Member: m, Retransmits: changeRetransmits}
			return
		}
	}
	n.queue = append(n.queue, pendingChange{Member: m, Retransmits: changeRetransmits})
}

// takeGossip drains up to gossipFanout pending changes for one outbound
// message and spends one retransmit from each.
func (n *Node) takeGossip() []proto.Member {
	n.mu.Lock()
	defer n.mu.Unlock()

	if len(n.queue) == 0 {
		return nil
	}
	batch := make([]proto.Member, 0, gossipFanout)
	keep := n.queue[:0]
	for i := range n.queue {
		c := n.queue[i]
		if len(batch) < gossipFanout {
			batch = append(batch, c.Member)
			c.Retransmits--
		}
		if c.Retransmits > 0 {
			keep = append(keep, c)
		}
	}
	n.queue = keep
	return batch
}
