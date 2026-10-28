// Package proto defines the wire structs exchanged between andamio members.
//
// The protocol is deliberately tiny: every datagram is a single gob-encoded
// Message. Membership updates ride along on any message (including pings and
// acks) so that healthy rounds double as gossip rounds.
package proto

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

// State is the liveness state of a member as seen by the cluster.
type State uint8

const (
	Alive State = iota
	Suspect
	Dead
	Left
)

func (s State) String() string {
	switch s {
	case Alive:
		return "alive"
	case Suspect:
		return "suspect"
	case Dead:
		return "dead"
	case Left:
		return "left"
	default:
		return "unknown"
	}
}

// MarshalJSON renders the state as its lowercase name so status
// endpoints stay readable by humans and curl.
func (s State) MarshalJSON() ([]byte, error) {
	return []byte(`"` + s.String() + `"`), nil
}

// Member is one node's view of a cluster participant. Incarnation numbers
// order conflicting reports about the same node; higher wins.
type Member struct {
	Name        string
	Addr        string
	Incarnation uint64
	State       State
}

// Ping starts a probe round. The responder must echo it in an Ack.
type Ping struct {
	Seq uint64
}

// Ack completes a probe round.
type Ack struct {
	Seq uint64
}

// PingReq asks a witness to probe a target on the sender's behalf. The
// witness answers with a plain Ack carrying the same sequence number, which
// the prober treats as an indirect ack for the target.
type PingReq struct {
	Seq        uint64
	Target     string
	TargetAddr string
}

// Join advertises a newcomer. The recipient replies with JoinAck carrying
// its current membership table.
type Join struct {
	Member Member
}

// JoinAck is the membership table handed to a joining node.
type JoinAck struct {
	Members []Member
}

// Leave marks the sender as gracefully departing.
type Leave struct {
	Member Member
}

// Message is the single envelope for all traffic. Exactly one payload field
// besides Gossip should be set; Gossip rides along with any of them.
type Message struct {
	Ping    *Ping
	Ack     *Ack
	PingReq *PingReq
	Join    *Join
	JoinAck *JoinAck
	Leave   *Leave

	Gossip []Member
}

// String renders the payload kind for logs.
func (m *Message) String() string {
	switch {
	case m.Ping != nil:
		return fmt.Sprintf("ping(seq=%d, gossip=%d)", m.Ping.Seq, len(m.Gossip))
	case m.Ack != nil:
		return fmt.Sprintf("ack(seq=%d, gossip=%d)", m.Ack.Seq, len(m.Gossip))
	case m.PingReq != nil:
		return fmt.Sprintf("ping-req(target=%s, gossip=%d)", m.PingReq.Target, len(m.Gossip))
	case m.Join != nil:
		return fmt.Sprintf("join(%s, gossip=%d)", m.Join.Member.Name, len(m.Gossip))
	case m.JoinAck != nil:
		return fmt.Sprintf("join-ack(%d members)", len(m.JoinAck.Members))
	case m.Leave != nil:
		return fmt.Sprintf("leave(%s, gossip=%d)", m.Leave.Member.Name, len(m.Gossip))
	default:
		return "empty"
	}
}

// Encode serializes a message into a single datagram payload.
func Encode(m *Message) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(m); err != nil {
		return nil, fmt.Errorf("proto: encode: %w", err)
	}
	return buf.Bytes(), nil
}

// Decode parses a datagram payload back into a message.
func Decode(b []byte) (*Message, error) {
	m := &Message{}
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(m); err != nil {
		return nil, fmt.Errorf("proto: decode: %w", err)
	}
	return m, nil
}
