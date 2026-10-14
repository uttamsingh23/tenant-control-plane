// Package tcpproxy is a TCP proxy for tests that need to break the
// network between a client and a server on demand: refuse connections,
// cut established ones, or accept connections and never answer.
package tcpproxy

import (
	"io"
	"net"
	"sync"
	"testing"
)

// Mode is how the proxy treats connections.
type Mode int

// Proxy modes.
const (
	// Forward relays traffic to the target.
	Forward Mode = iota
	// Refuse closes every new connection as soon as it is accepted, like a
	// server that is down.
	Refuse
	// Blackhole accepts connections and never forwards or answers, like an
	// unresponsive server.
	Blackhole
)

// Proxy listens on a local port and relays to a target address.
type Proxy struct {
	target string
	ln     net.Listener

	mu    sync.Mutex
	mode  Mode
	conns map[net.Conn]struct{}
	wg    sync.WaitGroup
}

// Start starts a proxy to target in the given mode. It is stopped, and
// every connection closed, when the test ends.
func Start(t *testing.T, target string, mode Mode) *Proxy {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &Proxy{target: target, ln: ln, mode: mode, conns: map[net.Conn]struct{}{}}
	p.wg.Go(p.accept)
	t.Cleanup(func() {
		_ = ln.Close()
		p.Cut()
		p.wg.Wait()
	})
	return p
}

// Addr is the host:port clients connect to.
func (p *Proxy) Addr() string { return p.ln.Addr().String() }

// SetMode changes how new connections are treated. Leaving Forward also
// cuts established connections.
func (p *Proxy) SetMode(m Mode) {
	p.mu.Lock()
	p.mode = m
	p.mu.Unlock()
	if m != Forward {
		p.Cut()
	}
}

// Cut closes every established connection; new ones are still accepted.
func (p *Proxy) Cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for c := range p.conns {
		_ = c.Close()
	}
	clear(p.conns)
}

func (p *Proxy) accept() {
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return // listener closed
		}
		p.mu.Lock()
		mode := p.mode
		if mode != Refuse {
			p.conns[client] = struct{}{}
		}
		p.mu.Unlock()
		switch mode {
		case Refuse:
			_ = client.Close()
		case Blackhole:
			// Held open until cut; nothing is read or written.
		case Forward:
			p.wg.Go(func() { p.forward(client) })
		}
	}
}

func (p *Proxy) forward(client net.Conn) {
	server, err := net.Dial("tcp", p.target) //nolint:noctx // closed via Cut, not a context
	if err != nil {
		_ = client.Close()
		return
	}
	p.mu.Lock()
	if _, open := p.conns[client]; !open { // cut while dialling
		p.mu.Unlock()
		_ = client.Close()
		_ = server.Close()
		return
	}
	p.conns[server] = struct{}{}
	p.mu.Unlock()

	var once sync.Once
	closeBoth := func() {
		once.Do(func() {
			_ = client.Close()
			_ = server.Close()
		})
	}
	pipe := func(dst, src net.Conn) {
		defer closeBoth()
		_, _ = io.Copy(dst, src) // ends when either side closes
	}
	p.wg.Go(func() { pipe(server, client) })
	pipe(client, server)
}
