package transport

import (
	"net"
	"testing"
	"time"

	"github.com/edxeme/andamio/internal/proto"
)

func TestPingAckRoundTrip(t *testing.T) {
	acks := make(chan struct{}, 1)

	var a *UDP
	a = mustListen(t, func(msg *proto.Message, from *net.UDPAddr) {
		if msg.Ping != nil {
			if err := a.Send(from.String(), &proto.Message{Ack: &proto.Ack{Seq: msg.Ping.Seq}}); err != nil {
				t.Errorf("send ack: %v", err)
			}
		}
	})
	defer a.Close()

	b := mustListen(t, func(msg *proto.Message, from *net.UDPAddr) {
		if msg.Ack != nil && msg.Ack.Seq == 42 {
			acks <- struct{}{}
		}
	})
	defer b.Close()

	if err := b.Send(a.LocalAddr(), &proto.Message{Ping: &proto.Ping{Seq: 42}}); err != nil {
		t.Fatalf("send ping: %v", err)
	}

	select {
	case <-acks:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for ack")
	}
}

func TestGarbageDatagramIsDropped(t *testing.T) {
	received := make(chan struct{}, 1)
	a := mustListen(t, func(*proto.Message, *net.UDPAddr) { received <- struct{}{} })
	defer a.Close()

	conn, err := net.Dial("udp", a.LocalAddr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if _, err := conn.Write([]byte("garbage")); err != nil {
		t.Fatalf("write: %v", err)
	}

	select {
	case <-received:
		t.Fatal("garbage datagram should not reach the handler")
	case <-time.After(300 * time.Millisecond):
	}
}

func mustListen(t *testing.T, h Handler) *UDP {
	t.Helper()
	u, err := Listen("127.0.0.1", 0, h)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return u
}
