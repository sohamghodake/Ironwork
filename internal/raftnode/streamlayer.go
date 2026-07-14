package raftnode

import (
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

// tlsStreamLayer runs the Raft transport over the cluster's mutual TLS
// material — the same trust domain as service gRPC. Peers verify each other
// against the cluster CA in both directions.
type tlsStreamLayer struct {
	net.Listener
	advertise string
	clientTLS *tls.Config
}

// newTLSStreamLayer listens on bindAddr with the server TLS config and
// advertises advertiseAddr to peers.
func newTLSStreamLayer(bindAddr, advertiseAddr string, serverTLS, clientTLS *tls.Config) (*tlsStreamLayer, error) {
	ln, err := tls.Listen("tcp", bindAddr, serverTLS)
	if err != nil {
		return nil, fmt.Errorf("raftnode: listen %s: %w", bindAddr, err)
	}
	return &tlsStreamLayer{Listener: ln, advertise: advertiseAddr, clientTLS: clientTLS}, nil
}

// Addr returns the advertised address: peers must dial a hostname the shared
// certificate covers, not this node's bind address.
func (l *tlsStreamLayer) Addr() net.Addr { return advertisedAddr(l.advertise) }

// Dial opens an mTLS connection to a peer, verifying its cert against the
// cluster CA with ServerName set to the peer's hostname.
func (l *tlsStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	host, _, err := net.SplitHostPort(string(address))
	if err != nil {
		return nil, fmt.Errorf("raftnode: bad peer address %q: %w", address, err)
	}
	cfg := l.clientTLS.Clone()
	cfg.ServerName = host
	dialer := &net.Dialer{Timeout: timeout}
	return tls.DialWithDialer(dialer, "tcp", string(address), cfg)
}

// advertisedAddr implements net.Addr for an advertised host:port.
type advertisedAddr string

func (a advertisedAddr) Network() string { return "tcp" }
func (a advertisedAddr) String() string  { return string(a) }
