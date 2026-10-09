package conn

// SetClientIDForTest sets a connection's client identifier without a
// handshake, for tests of what observes connections outside this package.
func SetClientIDForTest(c *Conn, id string) { c.clientID = id }
