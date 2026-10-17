package proto

import "testing"

func TestEncodeDecodeRoundTrip(t *testing.T) {
	msgs := []*Message{
		{Ping: &Ping{Seq: 7}},
		{Ack: &Ack{Seq: 7}},
		{PingReq: &PingReq{Seq: 9, Target: "b", TargetAddr: "10.0.0.2:7946"}},
		{Join: &Join{Member: Member{Name: "c", Addr: "10.0.0.3:7946", State: Alive}}},
		{JoinAck: &JoinAck{Members: []Member{
			{Name: "a", Addr: "10.0.0.1:7946", State: Alive},
			{Name: "b", Addr: "10.0.0.2:7946", Incarnation: 2, State: Suspect},
		}}},
		{Leave: &Leave{Member: Member{Name: "d", State: Left}}},
		{Gossip: []Member{{Name: "e", Incarnation: 4, State: Alive}}},
	}

	for i, want := range msgs {
		b, err := Encode(want)
		if err != nil {
			t.Fatalf("case %d: encode: %v", i, err)
		}
		got, err := Decode(b)
		if err != nil {
			t.Fatalf("case %d: decode: %v", i, err)
		}
		if got.String() != want.String() {
			t.Errorf("case %d: round trip mismatch: got %q, want %q", i, got.String(), want.String())
		}
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	if _, err := Decode([]byte("not a gob stream")); err == nil {
		t.Fatal("expected error decoding garbage")
	}
}

func TestStateString(t *testing.T) {
	cases := map[State]string{
		Alive:    "alive",
		Suspect:  "suspect",
		Dead:     "dead",
		Left:     "left",
		State(9): "unknown",
	}
	for s, want := range cases {
		if got := s.String(); got != want {
			t.Errorf("State(%d): got %q, want %q", s, got, want)
		}
	}
}
