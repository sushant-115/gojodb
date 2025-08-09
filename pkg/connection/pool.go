// Package connection provides a robust and thread-safe TCP connection pool.
// It is designed to manage and reuse connections to multiple remote hosts,
// which is ideal for a primary node sending logs to many replicas.
package connection

import (
	"fmt"
	"net"
	"sync"
	"time"
)

// PooledConn is a wrapper around net.Conn that includes a reference to the pool
// it belongs to. This allows for easy connection releasing.
type PooledConn struct {
	net.Conn
	pool *replicaPool
}

// Close returns the connection to the pool. It doesn't actually close the underlying
// TCP connection. To force-close, use ForceClose().
func (c *PooledConn) Close() error {
	if c.pool == nil {
		return fmt.Errorf("connection is already closed or detached from pool")
	}
	c.pool.put(c.Conn)
	c.pool = nil // Mark as closed
	return nil
}

// ForceClose closes the underlying TCP connection permanently and does not return it to the pool.
func (c *PooledConn) ForceClose() error {
	return c.Conn.Close()
}

// replicaPool manages a pool of connections for a single remote address.
type replicaPool struct {
	mu       sync.Mutex
	conns    chan net.Conn
	factory  func() (net.Conn, error)
	maxSize  int
	numConns int // Current number of connections created
	address  string
}

// ConnectionPoolManager manages multiple replicaPools, one for each remote host.
type ConnectionPoolManager struct {
	mu      sync.RWMutex
	pools   map[string]*replicaPool
	maxSize int // Default max size for new pools
	timeout time.Duration
}

// NewConnectionPoolManager creates a new manager for connection pools.
// maxSize is the maximum number of open connections per replica.
// timeout is the connection timeout for creating new connections.
func NewConnectionPoolManager(maxSize int, timeout time.Duration) *ConnectionPoolManager {
	return &ConnectionPoolManager{
		pools:   make(map[string]*replicaPool),
		maxSize: maxSize,
		timeout: timeout,
	}
}

// Get retrieves a connection from the pool for the specified address.
// If no pool exists for the address, it creates one.
func (m *ConnectionPoolManager) Get(address string) (*PooledConn, error) {
	m.mu.RLock()
	pool, ok := m.pools[address]
	m.mu.RUnlock()

	if !ok {
		m.mu.Lock()
		// Double-check after acquiring write lock
		pool, ok = m.pools[address]
		if !ok {
			factory := func() (net.Conn, error) {
				return net.DialTimeout("tcp", address, m.timeout)
			}
			pool = &replicaPool{
				conns:   make(chan net.Conn, m.maxSize),
				factory: factory,
				maxSize: m.maxSize,
				address: address,
			}
			m.pools[address] = pool
		}
		m.mu.Unlock()
	}

	conn, err := pool.get()
	if err != nil {
		return nil, err
	}

	return &PooledConn{Conn: conn, pool: pool}, nil
}

// get retrieves a connection from a specific replica's pool.
func (p *replicaPool) get() (net.Conn, error) {
	// Try to get an existing connection from the channel
	select {
	case conn := <-p.conns:
		return conn, nil
	default:
		// Channel is empty, try to create a new connection if not at max size
		p.mu.Lock()
		defer p.mu.Unlock()

		if p.numConns < p.maxSize {
			conn, err := p.factory()
			if err != nil {
				return nil, err
			}
			p.numConns++
			return conn, nil
		}
		// Pool is full, block and wait for a connection to be returned
		return <-p.conns, nil
	}
}

// put returns a connection to the pool.
func (p *replicaPool) put(conn net.Conn) {
	if conn == nil {
		return
	}

	select {
	case p.conns <- conn:
		// Connection returned to pool
	default:
		// Pool is full, close the connection
		p.mu.Lock()
		conn.Close()
		p.numConns--
		p.mu.Unlock()
	}
}

// Close shuts down the entire connection pool manager, closing all connections.
func (m *ConnectionPoolManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, pool := range m.pools {
		pool.close()
	}
	m.pools = make(map[string]*replicaPool)
}

// close shuts down a specific replica's pool.
func (p *replicaPool) close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	close(p.conns)
	for conn := range p.conns {
		conn.Close()
	}
	p.numConns = 0
}
