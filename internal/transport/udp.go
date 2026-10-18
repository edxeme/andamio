// Package transport moves encoded messages between members over UDP.
//
// One datagram carries exactly one proto.Message. The listener dispatches
// each datagram to its handler in its own goroutine, so handlers must be
// safe for concurrent use and must not block on slow work.
package transport

import (
	"fmt"
	"net"

	"github.com/edxeme/andamio/internal/proto"
)

const maxDatagramSize = 64 * 1024

// Handler receives one decoded message per datagram.
type Handler func(msg *proto.Message, from *net.UDPAddr)

// UDP is a datagram endpoint for probe/gossip traffic.
type UDP struct {
	conn    *net.UDPConn
	handler Handler

	// done closes the read loop; tests rely on Close being idempotent.
	done chan struct{}
}

// Listen binds a UDP socket and starts its read loop.
func Listen(bindHost string, bindPort int, handler Handler) (*UDP, error) {
	addr := &net.UDPAddr{IP: net.ParseIP(bindHost), Port: bindPort}
	if addr.IP == nil {
		// Allow hostnames such as localhost.
		var err error
		addr, err = net.ResolveUDPAddr("udp", net.JoinHostPort(bindHost, fmt.Sprint(bindPort)))
		if err != nil {
			return nil, fmt.Errorf("transport: resolve %s:%d: %w", bindHost, bindPort, err)
		}
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("transport: listen udp %s: %w", addr, err)
	}

	u := &UDP{conn: conn, handler: handler, done: make(chan struct{})}
	go u.readLoop()
	return u, nil
}

// LocalAddr reports the bound host:port, useful when binding to port 0.
func (u *UDP) LocalAddr() string {
	return u.conn.LocalAddr().String()
}

// Send encodes and delivers one message.
func (u *UDP) Send(addr string, msg *proto.Message) error {
	payload, err := proto.Encode(msg)
	if err != nil {
		return err
	}
	dst, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return fmt.Errorf("transport: resolve %s: %w", addr, err)
	}
	if _, err := u.conn.WriteToUDP(payload, dst); err != nil {
		return fmt.Errorf("transport: send to %s: %w", addr, err)
	}
	return nil
}

// Close stops the read loop and releases the socket.
func (u *UDP) Close() error {
	select {
	case <-u.done:
	default:
		close(u.done)
	}
	return u.conn.Close()
}

func (u *UDP) readLoop() {
	buf := make([]byte, maxDatagramSize)
	for {
		n, from, err := u.conn.ReadFromUDP(buf)
		select {
		case <-u.done:
			return
		default:
		}
		if err != nil {
			// A closed socket is the normal shutdown path.
			return
		}
		if n == 0 {
			continue
		}

		payload := make([]byte, n)
		copy(payload, buf[:n])
		go u.dispatch(payload, from)
	}
}

func (u *UDP) dispatch(payload []byte, from *net.UDPAddr) {
	msg, err := proto.Decode(payload)
	if err != nil {
		// Corrupt or foreign datagrams are dropped, not fatal.
		return
	}
	u.handler(msg, from)
}
