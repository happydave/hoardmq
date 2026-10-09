package listener

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// The subset of RFC 6455 MQTT needs: the server side of the handshake with
// the "mqtt" subprotocol, masked binary data frames from the client,
// unmasked binary frames to it, fragmentation, ping, pong and close. No
// extensions, no text frames.

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

const (
	opContinuation = 0x0
	opText         = 0x1
	opBinary       = 0x2
	opClose        = 0x8
	opPing         = 0x9
	opPong         = 0xA
)

// Close status codes used.
const (
	closeNormal        = 1000
	closeProtocolError = 1002
	closeUnsupported   = 1003
	closeTooLarge      = 1009
)

func (l *Listener) upgrade(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet ||
		!strings.EqualFold(r.Header.Get("Upgrade"), "websocket") ||
		!headerContainsToken(r.Header, "Connection", "upgrade") ||
		r.Header.Get("Sec-WebSocket-Version") != "13" ||
		r.Header.Get("Sec-WebSocket-Key") == "" {
		http.Error(w, "websocket upgrade required", http.StatusBadRequest)
		return
	}
	if !headerContainsToken(r.Header, "Sec-WebSocket-Protocol", "mqtt") {
		http.Error(w, "subprotocol mqtt required", http.StatusBadRequest)
		return
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "upgrade not supported", http.StatusInternalServerError)
		return
	}
	accept := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + wsGUID))
	raw, rw, err := hj.Hijack()
	if err != nil {
		return
	}
	resp := "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " +
		base64.StdEncoding.EncodeToString(accept[:]) + "\r\nSec-WebSocket-Protocol: mqtt\r\n\r\n"
	if _, err := rw.WriteString(resp); err != nil || rw.Flush() != nil {
		_ = raw.Close()
		return
	}
	c := newWSConn(raw, rw.Reader, l.MaxFrame)
	select {
	case l.upgraded <- c:
	case <-l.closeCh:
		_ = c.Close()
	}
}

func headerContainsToken(h http.Header, name, token string) bool {
	for _, v := range h.Values(name) {
		for _, part := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(part), token) {
				return true
			}
		}
	}
	return false
}

// wsConn presents the payload stream of binary frames as a net.Conn.
type wsConn struct {
	raw      net.Conn
	r        io.Reader
	maxFrame int

	readMu    sync.Mutex
	leftover  []byte
	inMessage bool
	closed    bool

	writeMu sync.Mutex
	hdr     [10]byte
}

func newWSConn(raw net.Conn, br *bufio.Reader, maxFrame int) *wsConn {
	c := &wsConn{raw: raw, maxFrame: maxFrame}
	// The hijacked reader may hold bytes the client sent after the
	// handshake; drain those first, then read the socket directly so no
	// bufio buffer outlives the handshake.
	if n := br.Buffered(); n > 0 {
		c.r = io.MultiReader(io.LimitReader(br, int64(n)), raw)
	} else {
		c.r = raw
	}
	return c
}

func (c *wsConn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	for len(c.leftover) == 0 {
		if c.closed {
			return 0, io.EOF
		}
		if err := c.readFrame(); err != nil {
			return 0, err
		}
	}
	n := copy(p, c.leftover)
	c.leftover = c.leftover[n:]
	return n, nil
}

// readFrame reads one frame, handling control frames inline and appending a
// data frame's unmasked payload to leftover.
func (c *wsConn) readFrame() error {
	var h [2]byte
	if _, err := io.ReadFull(c.r, h[:]); err != nil {
		return err
	}
	fin := h[0]&0x80 != 0
	if h[0]&0x70 != 0 {
		return c.fail(closeProtocolError, "reserved bits set")
	}
	op := h[0] & 0x0F
	masked := h[1]&0x80 != 0
	length := uint64(h[1] & 0x7F)
	switch length {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(c.r, ext[:]); err != nil {
			return err
		}
		length = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(c.r, ext[:]); err != nil {
			return err
		}
		length = binary.BigEndian.Uint64(ext[:])
	}
	if !masked {
		return c.fail(closeProtocolError, "client frame not masked")
	}
	var mask [4]byte
	if _, err := io.ReadFull(c.r, mask[:]); err != nil {
		return err
	}
	isControl := op&0x8 != 0
	if isControl && (length > 125 || !fin) {
		return c.fail(closeProtocolError, "invalid control frame")
	}
	if c.maxFrame > 0 && length > uint64(c.maxFrame) {
		return c.fail(closeTooLarge, "frame too large")
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(c.r, payload); err != nil {
		return err
	}
	for i := range payload {
		payload[i] ^= mask[i%4]
	}
	switch op {
	case opBinary:
		if c.inMessage {
			return c.fail(closeProtocolError, "new data frame inside a fragmented message")
		}
		c.inMessage = !fin
		c.leftover = append(c.leftover, payload...)
	case opContinuation:
		if !c.inMessage {
			return c.fail(closeProtocolError, "continuation frame without a message")
		}
		c.inMessage = !fin
		c.leftover = append(c.leftover, payload...)
	case opText:
		return c.fail(closeUnsupported, "text frames are not accepted")
	case opPing:
		return c.writeFrame(opPong, payload)
	case opPong:
	case opClose:
		code := uint16(closeNormal)
		if len(payload) >= 2 {
			code = binary.BigEndian.Uint16(payload)
		}
		_ = c.writeClose(code, "")
		c.closed = true
		return io.EOF
	default:
		return c.fail(closeProtocolError, "unknown opcode")
	}
	return nil
}

// fail sends a close frame with the status, marks the connection closed and
// returns an error carrying the reason.
func (c *wsConn) fail(code uint16, reason string) error {
	_ = c.writeClose(code, reason)
	c.closed = true
	_ = c.raw.Close()
	return errors.New("websocket: " + reason)
}

func (c *wsConn) writeClose(code uint16, reason string) error {
	payload := make([]byte, 2, 2+len(reason))
	binary.BigEndian.PutUint16(payload, code)
	payload = append(payload, reason...)
	return c.writeFrame(opClose, payload)
}

func (c *wsConn) Write(p []byte) (int, error) {
	if err := c.writeFrame(opBinary, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *wsConn) writeFrame(op byte, payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	h := c.hdr[:0]
	h = append(h, 0x80|op)
	switch n := len(payload); {
	case n < 126:
		h = append(h, byte(n))
	case n < 65536:
		h = append(h, 126, byte(n>>8), byte(n))
	default:
		h = append(h, 127)
		h = binary.BigEndian.AppendUint64(h, uint64(n))
	}
	buf := make([]byte, 0, len(h)+len(payload))
	buf = append(buf, h...)
	buf = append(buf, payload...)
	_, err := c.raw.Write(buf)
	return err
}

// Close sends a normal close frame on a best-effort basis and closes the
// socket.
func (c *wsConn) Close() error {
	_ = c.raw.SetWriteDeadline(time.Now().Add(time.Second))
	_ = c.writeClose(closeNormal, "")
	return c.raw.Close()
}

func (c *wsConn) LocalAddr() net.Addr                { return c.raw.LocalAddr() }
func (c *wsConn) RemoteAddr() net.Addr               { return c.raw.RemoteAddr() }
func (c *wsConn) SetDeadline(t time.Time) error      { return c.raw.SetDeadline(t) }
func (c *wsConn) SetReadDeadline(t time.Time) error  { return c.raw.SetReadDeadline(t) }
func (c *wsConn) SetWriteDeadline(t time.Time) error { return c.raw.SetWriteDeadline(t) }
