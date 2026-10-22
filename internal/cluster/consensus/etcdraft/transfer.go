package etcdraft

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// A snapshot travels as a transfer on the data connection: the sender's
// offer, the file in chunks with a bounded number unacknowledged, and an
// end; the receiver writes it to a file, checks it, hands the message to its
// loop and answers done. Nothing on either side holds the whole snapshot.
const (
	// snapshotChunk is one chunk's file bytes: the bound of one raft append.
	snapshotChunk = maxAppendBytes
	// snapshotWindow is how many chunks may be unacknowledged, so anything
	// admitted to the data connection after a chunk waits behind at most
	// this many.
	snapshotWindow = 4
	// ackTimeoutElections: a transfer with no reply for this many election
	// timeouts has failed; one that keeps progressing has no deadline.
	ackTimeoutElections = 10
	// backoffCapElections bounds the wait before a failed transfer's report,
	// which is what keeps a failing transfer from repeating every heartbeat.
	backoffCapElections = 30
)

// Message kinds, the first byte of a payload on each class.
const (
	xOffer  byte = 1
	xChunk  byte = 2
	xEnd    byte = 3
	xCancel byte = 4

	rAccept  byte = 1
	rAck     byte = 2
	rStaging byte = 3
	rDone    byte = 4
	rRefuse  byte = 5
)

// Reasons a transfer ends, the closed set the counter's label takes.
const (
	reasonOK          = "ok"
	reasonPeerVersion = "peer-version"
	reasonLinkDown    = "link-down"
	reasonAckTimeout  = "ack-timeout"
	reasonReadError   = "read-error"
	reasonCancelled   = "cancelled"
	reasonReplaced    = "replaced" // internal: a newer transfer to the follower owes the report
	reasonFormat      = "format"
	reasonEncoding    = "encoding"
	reasonSpace       = "space"
	reasonOrder       = "order"
	reasonSize        = "size"
	reasonDigest      = "digest"
	reasonAborted     = "aborted"
	reasonStage       = "stage"
)

// refusedPfx marks a sender's end by the receiver's refusal, whose reason
// follows it.
const refusedPfx = "refused:"

// transferHooks are faults for tests; nil in the product.
type transferHooks struct {
	corruptChunk func(offset uint64, data []byte) []byte     // receiver, before it hashes and writes
	dropAcks     func() bool                                 // receiver answers nothing from the first chunk on
	backoff      func(failures int) time.Duration            // sender, in place of the doubling
	stageDelay   func()                                      // receiver, inside staging
	onReport     func(to uint64, status raft.SnapshotStatus) // the loop, as it tells the library
}

type offerMsg struct {
	id        uint64
	size      uint64
	digest    [32]byte
	format    byte
	encodings []byte
	chunk     uint32
	msg       []byte // the marshalled MsgSnap, its data the sender's descriptor
}

func (o offerMsg) encode() []byte {
	b := []byte{xOffer}
	b = binary.BigEndian.AppendUint64(b, o.id)
	b = binary.BigEndian.AppendUint64(b, o.size)
	b = append(b, o.digest[:]...)
	b = append(b, o.format, byte(len(o.encodings)))
	b = append(b, o.encodings...)
	b = binary.BigEndian.AppendUint32(b, o.chunk)
	b = binary.BigEndian.AppendUint32(b, uint32(len(o.msg)))
	return append(b, o.msg...)
}

func decodeOffer(p []byte) (offerMsg, error) {
	var o offerMsg
	if len(p) < 1+8+8+32+2 {
		return o, errors.New("short offer")
	}
	o.id = binary.BigEndian.Uint64(p[1:9])
	o.size = binary.BigEndian.Uint64(p[9:17])
	copy(o.digest[:], p[17:49])
	o.format = p[49]
	n := int(p[50])
	p = p[51:]
	if len(p) < n+8 {
		return o, errors.New("short offer")
	}
	o.encodings = append([]byte(nil), p[:n]...)
	o.chunk = binary.BigEndian.Uint32(p[n : n+4])
	m := int(binary.BigEndian.Uint32(p[n+4 : n+8]))
	p = p[n+8:]
	if len(p) != m {
		return o, errors.New("offer message length")
	}
	o.msg = p
	return o, nil
}

// idMsg is every other message: a kind, the transfer, an integer and, for a
// chunk, its bytes or, for a refusal or cancel, the reason.
func idMsg(kind byte, id, v uint64, rest []byte) []byte {
	b := []byte{kind}
	b = binary.BigEndian.AppendUint64(b, id)
	b = binary.BigEndian.AppendUint64(b, v)
	return append(b, rest...)
}

func decodeIDMsg(p []byte) (kind byte, id, v uint64, rest []byte, err error) {
	if len(p) < 17 {
		return 0, 0, 0, nil, errors.New("short transfer message")
	}
	return p[0], binary.BigEndian.Uint64(p[1:9]), binary.BigEndian.Uint64(p[9:17]), p[17:], nil
}

type reply struct {
	kind   byte
	v      uint64
	reason string
}

type outTransfer struct {
	id      uint64
	to      uint64
	peer    string
	desc    snapDesc
	msg     *pb.Message
	began   time.Time
	sent    uint64
	replies chan reply
	cancel  chan string // the reason, from outside the transfer's goroutine
	done    chan struct{}
}

type snapReport struct {
	to     uint64
	status raft.SnapshotStatus
}

func (c *Core) electionTimeout() time.Duration {
	return c.o.Tick * time.Duration(c.o.ElectionTicks)
}

// startTransfer begins sending a snapshot message's file to its follower,
// off the loop. Called on the loop with the message the library emitted.
func (c *Core) startTransfer(peer string, m *pb.Message) {
	to := m.GetTo()
	snap := m.GetSnapshot()
	d, _, err := decodeDesc(snap.GetData())
	if err != nil {
		// A snapshot stored inline by an older build: build a file now, and
		// report this attempt failed so the library asks again.
		c.forceBuild = true
		c.endOut(&outTransfer{to: to, peer: peer, began: time.Now(), done: make(chan struct{})}, reasonReadError, err)
		return
	}
	x := &outTransfer{id: rand.Uint64(), to: to, peer: peer, desc: d, msg: m, began: time.Now(), replies: make(chan reply, 64), cancel: make(chan string, 1), done: make(chan struct{})}
	c.xferMu.Lock()
	prev := c.outByPeer[to]
	c.outByPeer[to] = x
	c.outByID[x.id] = x
	c.xferMu.Unlock()
	if prev != nil {
		prev.stop(reasonReplaced)
	}
	c.pinFile(d.Name)
	c.set("hoardmq_raft_snapshot_transfers_active", map[string]string{"direction": "out"}, float64(c.activeOut()))
	c.log.Info("snapshot transfer started", "peer", peer, "transfer", x.id, "index", d.Index, "bytes", d.Size)
	c.bg.Add(1)
	go func() {
		defer c.bg.Done()
		reason, err := c.sendFile(x)
		c.endOut(x, reason, err)
	}()
}

func (x *outTransfer) stop(reason string) {
	select {
	case x.cancel <- reason:
	default:
	}
}

func (c *Core) activeOut() int {
	c.xferMu.Lock()
	defer c.xferMu.Unlock()
	return len(c.outByID)
}

// sendFile runs one transfer to its end and returns the reason it ended.
func (c *Core) sendFile(x *outTransfer) (string, error) {
	f, err := os.Open(filepath.Join(c.snapDir, x.desc.Name))
	if err != nil {
		return reasonReadError, err
	}
	defer func() { _ = f.Close() }()
	msg, err := proto.Marshal(x.msg)
	if err != nil {
		return reasonReadError, err
	}
	send := func(p []byte) (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), c.ackTimeout())
		defer cancel()
		err := c.o.Transport.Send(ctx, x.peer, transport.SnapshotData, p)
		switch {
		case err == nil:
			return "", nil
		case errors.Is(err, transport.ErrUnsupportedClass):
			return reasonPeerVersion, err
		default:
			return reasonLinkDown, err
		}
	}
	timer := time.NewTimer(c.ackTimeout())
	defer timer.Stop()
	// wait takes the next reply; a refusal, a cancel, the core's stop or the
	// timeout ends the transfer.
	wait := func() (reply, string, error) {
		select {
		case r := <-x.replies:
			if r.kind == rRefuse {
				return r, refusedPfx + r.reason, fmt.Errorf("refused: %s", r.reason)
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(c.ackTimeout())
			return r, "", nil
		case why := <-x.cancel:
			return reply{}, why, errors.New(why)
		case <-c.stop:
			return reply{}, reasonCancelled, errStopped
		case <-timer.C:
			return reply{}, reasonAckTimeout, fmt.Errorf("no answer for %s", c.ackTimeout())
		}
	}
	offer := offerMsg{id: x.id, size: x.desc.Size, digest: x.desc.Digest, format: consensus.FileFormat,
		encodings: []byte{byte(consensus.EncodingBlob), byte(consensus.EncodingJSON), byte(consensus.EncodingCompact)}, chunk: snapshotChunk, msg: msg}
	if why, err := send(offer.encode()); err != nil {
		return why, err
	}
	r, why, err := wait()
	if err != nil {
		return why, err
	}
	if r.kind != rAccept || r.v > x.desc.Size {
		return refusedPfx + reasonFormat, fmt.Errorf("answer %d to the offer", r.kind)
	}
	sent, acked := r.v, r.v
	if _, err := f.Seek(int64(sent), io.SeekStart); err != nil {
		return reasonReadError, err
	}
	for acked < x.desc.Size {
		for sent < x.desc.Size && sent-acked < snapshotWindow*snapshotChunk {
			n := min(uint64(snapshotChunk), x.desc.Size-sent)
			buf := make([]byte, n)
			if _, err := io.ReadFull(f, buf); err != nil {
				return reasonReadError, err
			}
			if why, err := send(idMsg(xChunk, x.id, sent, buf)); err != nil {
				return why, err
			}
			sent += n
			x.sent = sent
			c.count("hoardmq_raft_snapshot_transfer_bytes_total", map[string]string{"direction": "out"}, float64(n))
		}
		r, why, err := wait()
		if err != nil {
			return why, err
		}
		if r.kind == rAck && r.v > acked && r.v <= sent {
			acked = r.v
		}
	}
	if why, err := send(idMsg(xEnd, x.id, x.desc.Size, nil)); err != nil {
		return why, err
	}
	for {
		r, why, err := wait()
		if err != nil {
			return why, err
		}
		if r.kind == rDone {
			return reasonOK, nil
		}
	}
}

func (c *Core) ackTimeout() time.Duration {
	return ackTimeoutElections * c.electionTimeout()
}

// endOut finishes a transfer: it releases the file and the compaction floor
// at once, counts and logs the outcome, and reports it to the library through
// the loop, a failure after the peer's backoff.
func (c *Core) endOut(x *outTransfer, reason string, err error) {
	pinned := c.pinnedEntries()
	close(x.done)
	c.xferMu.Lock()
	if x.id != 0 {
		delete(c.outByID, x.id)
		if c.outByPeer[x.to] == x {
			delete(c.outByPeer, x.to)
		}
	}
	failures := 0
	if reason == reasonOK {
		delete(c.failures, x.to)
	} else if reason != reasonCancelled && reason != reasonReplaced {
		c.failures[x.to]++
		failures = c.failures[x.to]
	}
	c.xferMu.Unlock()
	if x.desc.Name != "" {
		c.unpinFile(x.desc.Name)
		c.removeSnapshotFiles(false)
	}
	select {
	case c.floorFreed <- struct{}{}:
	default:
	}
	took := time.Since(x.began)
	c.set("hoardmq_raft_snapshot_transfers_active", map[string]string{"direction": "out"}, float64(c.activeOut()))
	c.count("hoardmq_raft_snapshot_transfer_seconds_total", map[string]string{"direction": "out"}, took.Seconds())
	outcome, label := "failed", reason
	switch {
	case reason == reasonOK:
		outcome = "ok"
	case reason == reasonReplaced:
		label = reasonCancelled
	case strings.HasPrefix(reason, refusedPfx):
		outcome, label = "refused", strings.TrimPrefix(reason, refusedPfx)
	}
	c.count("hoardmq_raft_snapshot_transfers_total", map[string]string{"direction": "out", "outcome": outcome, "reason": label}, 1)
	attrs := []any{"peer", x.peer, "transfer", x.id, "index", x.desc.Index, "bytes", x.desc.Size, "sent", x.sent, "took", took.Round(time.Millisecond), "pinnedEntries", pinned}
	if reason == reasonOK {
		c.log.Info("snapshot transfer completed", attrs...)
		c.report(x.to, raft.SnapshotFinish, 0)
		return
	}
	c.log.Warn("snapshot transfer failed", append(attrs, "reason", reason, "err", err)...)
	if errors.Is(err, errStopped) || reason == reasonReplaced {
		return
	}
	c.report(x.to, raft.SnapshotFailure, c.backoffFor(failures))
}

// backoffFor is the wait before a failure's report: one election timeout,
// doubling with each consecutive failure, capped.
func (c *Core) backoffFor(failures int) time.Duration {
	if h := c.o.transferHooks; h != nil && h.backoff != nil {
		return h.backoff(failures)
	}
	if failures < 1 {
		failures = 1
	}
	d := c.electionTimeout()
	for i := 1; i < failures && d < backoffCapElections*c.electionTimeout(); i++ {
		d *= 2
	}
	return min(d, backoffCapElections*c.electionTimeout())
}

// report hands a transfer's outcome to the loop after delay.
func (c *Core) report(to uint64, status raft.SnapshotStatus, delay time.Duration) {
	c.bg.Add(1)
	go func() {
		defer c.bg.Done()
		if delay > 0 {
			t := time.NewTimer(delay)
			defer t.Stop()
			select {
			case <-t.C:
			case <-c.stop:
				return
			}
		}
		select {
		case c.reports <- snapReport{to: to, status: status}:
		case <-c.stop:
		}
	}()
}

// floors is the snapshot index of every transfer in flight.
func (c *Core) floors() []uint64 {
	c.xferMu.Lock()
	defer c.xferMu.Unlock()
	out := make([]uint64, 0, len(c.outByID))
	for _, x := range c.outByID {
		out = append(out, x.desc.Index)
	}
	return out
}

// pinnedEntries is how many entries the floors keep beyond the trailing
// window, which is what a slow transfer costs the log.
func (c *Core) pinnedEntries() uint64 {
	stored, trailing := c.o.Store.SnapshotIndex(), c.o.SnapshotTrailing
	free := compactTarget(stored, trailing, nil)
	held := compactTarget(stored, trailing, c.floors())
	if free > held {
		return free - held
	}
	return 0
}

// cancelOut ends the transfers to the peers keep refuses, with reason.
func (c *Core) cancelOut(reason string, keep func(x *outTransfer) bool) {
	c.xferMu.Lock()
	var stop []*outTransfer
	for _, x := range c.outByID {
		if !keep(x) {
			stop = append(stop, x)
		}
	}
	c.xferMu.Unlock()
	for _, x := range stop {
		x.stop(reason)
	}
}

// onReply routes a receiver's answer to its transfer.
func (c *Core) onReply(peer string, p []byte) {
	kind, id, v, rest, err := decodeIDMsg(p)
	if err != nil {
		return
	}
	c.xferMu.Lock()
	x := c.outByID[id]
	c.xferMu.Unlock()
	if x == nil || x.peer != peer {
		return
	}
	select {
	case x.replies <- reply{kind: kind, v: v, reason: string(rest)}:
	case <-x.done:
	case <-c.stop:
	}
}

type inTransfer struct {
	id      uint64
	peer    string
	offer   offerMsg
	msg     *pb.Message
	desc    snapDesc
	f       *os.File
	part    string
	written uint64
	hash    hash.Hash // of the file before its trailer, which is the digest itself
	trailer []byte
	began   time.Time
	ended   sync.Once
	// mu is held while a message of the transfer is handled. An abort from
	// elsewhere (a newer offer, the connection down, the core's stop) closes
	// cancelled and cleans up itself only when no handler holds mu; else the
	// handler does when it next looks.
	mu         sync.Mutex
	cancelled  chan struct{}
	cancelOnce sync.Once
	cancelWhy  string
	cancelErr  error
}

// cancelIn ends an incoming transfer from outside its handler.
func (c *Core) cancelIn(in *inTransfer, reason string, err error) {
	in.cancelOnce.Do(func() {
		in.cancelWhy, in.cancelErr = reason, err
		close(in.cancelled)
	})
	if in.mu.TryLock() {
		c.abortIn(in, reason, err)
		in.mu.Unlock()
	}
}

// wasCancelled finishes a transfer cancelled while its handler ran.
func (c *Core) wasCancelled(in *inTransfer) bool {
	select {
	case <-in.cancelled:
		c.abortIn(in, in.cancelWhy, in.cancelErr)
		return true
	default:
		return false
	}
}

func (c *Core) replyTo(peer string, p []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), c.ackTimeout())
	defer cancel()
	_ = c.o.Transport.Send(ctx, peer, transport.SnapshotReply, p)
}

// onData handles the sender's side of a transfer. It runs on the data
// connection's drainer for this class, one message at a time.
func (c *Core) onData(peer string, p []byte) {
	if len(p) == 0 {
		return
	}
	if p[0] == xOffer {
		c.onOffer(peer, p)
		return
	}
	kind, id, v, rest, err := decodeIDMsg(p)
	if err != nil {
		return
	}
	c.xferMu.Lock()
	in := c.in
	c.xferMu.Unlock()
	if in == nil || in.id != id || in.peer != peer {
		if kind != xCancel {
			c.replyTo(peer, idMsg(rRefuse, id, 0, []byte(reasonAborted)))
		}
		return
	}
	in.mu.Lock()
	defer in.mu.Unlock()
	if c.wasCancelled(in) {
		return
	}
	defer c.wasCancelled(in)
	switch kind {
	case xChunk:
		if v != in.written {
			c.refuseIn(in, reasonOrder, fmt.Errorf("chunk at %d, expected %d", v, in.written))
			return
		}
		if in.written+uint64(len(rest)) > in.offer.size {
			c.refuseIn(in, reasonSize, fmt.Errorf("chunk reaches %d past the offered %d", in.written+uint64(len(rest)), in.offer.size))
			return
		}
		data := rest
		if h := c.o.transferHooks; h != nil && h.corruptChunk != nil {
			data = h.corruptChunk(v, append([]byte(nil), rest...))
		}
		if _, err := in.f.Write(data); err != nil {
			c.refuseIn(in, reasonSpace, err)
			return
		}
		in.digestChunk(data)
		in.written += uint64(len(data))
		c.count("hoardmq_raft_snapshot_transfer_bytes_total", map[string]string{"direction": "in"}, float64(len(data)))
		if h := c.o.transferHooks; h != nil && h.dropAcks != nil && h.dropAcks() {
			return
		}
		c.replyTo(peer, idMsg(rAck, id, in.written, nil))
	case xEnd:
		c.finishIn(in)
	case xCancel:
		c.abortIn(in, reasonAborted, errors.New("cancelled by the sender: "+string(rest)))
	}
}

func (c *Core) onOffer(peer string, p []byte) {
	o, err := decodeOffer(p)
	if err != nil {
		return
	}
	c.xferMu.Lock()
	prev := c.in
	c.in = nil
	c.staged = nil
	c.xferMu.Unlock()
	if prev != nil {
		c.cancelIn(prev, reasonAborted, errors.New("a newer offer arrived"))
	}
	in := &inTransfer{id: o.id, peer: peer, offer: o, began: time.Now(), hash: sha256.New(), cancelled: make(chan struct{})}
	c.set("hoardmq_raft_snapshot_transfers_active", map[string]string{"direction": "in"}, 1)
	if o.format != consensus.FileFormat {
		c.refuseIn(in, reasonFormat, fmt.Errorf("format %d", o.format))
		return
	}
	for _, e := range o.encodings {
		if e < byte(consensus.EncodingBlob) || e > byte(consensus.EncodingCompact) {
			c.refuseIn(in, reasonEncoding, fmt.Errorf("section encoding %d", e))
			return
		}
	}
	m := &pb.Message{}
	if err := proto.Unmarshal(o.msg, m); err != nil || m.GetType() != pb.MsgSnap || m.GetSnapshot() == nil {
		c.refuseIn(in, reasonFormat, fmt.Errorf("offer message: %v", err))
		return
	}
	d, _, err := decodeDesc(m.GetSnapshot().GetData())
	if err != nil {
		c.refuseIn(in, reasonFormat, err)
		return
	}
	in.msg, in.desc = m, d
	if err := os.MkdirAll(c.snapDir, 0o755); err != nil {
		c.refuseIn(in, reasonSpace, err)
		return
	}
	in.part = filepath.Join(c.snapDir, fmt.Sprintf("%s%d%s", recvPfx, o.id, partialEx))
	if in.f, err = os.Create(in.part); err != nil {
		c.refuseIn(in, reasonSpace, err)
		return
	}
	c.xferMu.Lock()
	c.in = in
	c.xferMu.Unlock()
	c.replyTo(peer, idMsg(rAccept, o.id, 0, nil))
}

// digestChunk hashes a chunk's bytes that come before the file's trailer
// and keeps the trailer's.
func (in *inTransfer) digestChunk(data []byte) {
	body := in.offer.size - uint64(sha256.Size)
	if in.offer.size < uint64(sha256.Size) {
		body = 0
	}
	n := uint64(len(data))
	if in.written < body {
		k := min(n, body-in.written)
		in.hash.Write(data[:k])
		data = data[k:]
	}
	in.trailer = append(in.trailer, data...)
}

// finishIn checks a complete file, makes it durable under its own name and
// hands the snapshot message, naming it, to the loop.
func (c *Core) finishIn(in *inTransfer) {
	if in.written != in.offer.size {
		c.refuseIn(in, reasonSize, fmt.Errorf("%d bytes, the offer said %d", in.written, in.offer.size))
		return
	}
	var sum [32]byte
	copy(sum[:], in.hash.Sum(nil))
	if sum != in.offer.digest || sum != in.desc.Digest || !bytes.Equal(in.trailer, sum[:]) {
		c.refuseIn(in, reasonDigest, errors.New("digest does not match the offer"))
		return
	}
	if err := in.f.Sync(); err != nil {
		c.refuseIn(in, reasonSpace, err)
		return
	}
	if err := in.f.Close(); err != nil {
		in.f = nil
		c.refuseIn(in, reasonSpace, err)
		return
	}
	in.f = nil
	local := in.desc
	local.Name = receivedName(local.Index, local.Term, time.Now())
	if err := os.Rename(in.part, filepath.Join(c.snapDir, local.Name)); err != nil {
		c.refuseIn(in, reasonSpace, err)
		return
	}
	if err := syncDir(c.snapDir); err != nil {
		c.refuseIn(in, reasonSpace, err)
		return
	}
	in.part = ""
	if err := c.stageReceived(in, local); err != nil {
		_ = os.Remove(filepath.Join(c.snapDir, local.Name))
		c.refuseIn(in, reasonStage, err)
		return
	}
	m := proto.Clone(in.msg).(*pb.Message)
	m.Snapshot.Data = local.encode()
	c.holdReceived(local)
	c.mu.Lock()
	c.idNames[m.GetFrom()] = in.peer
	c.mu.Unlock()
	c.offer(m)
	c.replyTo(in.peer, idMsg(rDone, in.id, in.written, nil))
	c.endIn(in, reasonOK, nil)
}

// stageReceived decodes a received file into new machine state off the
// loop, so the loop's install is a swap. The sender hears progress while
// it runs, since a large decode outlasts the ack timeout.
func (c *Core) stageReceived(in *inTransfer, local snapDesc) error {
	type staged struct {
		st  *consensus.StagedMachine
		err error
	}
	out := make(chan staged, 1)
	if !c.bgAdd() {
		return errStopped
	}
	go func() {
		defer c.bg.Done()
		st, err := c.stageSnapshot(local)
		out <- staged{st, err}
	}()
	tick := time.NewTicker(c.ackTimeout() / 4)
	defer tick.Stop()
	var progress uint64
	for {
		select {
		case r := <-out:
			if r.err != nil {
				return r.err
			}
			c.xferMu.Lock()
			c.staged = &stagedSnapshot{desc: local, st: r.st}
			c.xferMu.Unlock()
			return nil
		case <-tick.C:
			progress++
			c.replyTo(in.peer, idMsg(rStaging, in.id, progress, nil))
		case <-in.cancelled:
			return errors.New("cancelled while staging")
		case <-c.stop:
			return errStopped
		}
	}
}

// bgAdd counts a goroutine Stop waits for, refusing once Stop has begun
// waiting: a transport handler can still run after it.
func (c *Core) bgAdd() bool {
	c.xferMu.Lock()
	defer c.xferMu.Unlock()
	if c.closing {
		return false
	}
	c.bg.Add(1)
	return true
}

// stagedSnapshot is a received snapshot decoded and waiting for the loop.
type stagedSnapshot struct {
	desc snapDesc
	st   *consensus.StagedMachine
}

// takeStaged returns the staged state for a descriptor, emptying the slot.
func (c *Core) takeStaged(d snapDesc) *consensus.StagedMachine {
	c.xferMu.Lock()
	defer c.xferMu.Unlock()
	s := c.staged
	if s == nil || s.desc.Name != d.Name || s.desc.Index != d.Index || s.desc.Term != d.Term {
		return nil
	}
	c.staged = nil
	return s.st
}

// dropStaged lets staged state go once the applied index has reached it:
// installed, or the library ignored the snapshot because the log held it.
func (c *Core) dropStaged(applied uint64) {
	c.xferMu.Lock()
	if c.staged != nil && c.staged.desc.Index <= applied {
		c.staged = nil
	}
	c.xferMu.Unlock()
}

func (c *Core) refuseIn(in *inTransfer, reason string, err error) {
	c.replyTo(in.peer, idMsg(rRefuse, in.id, 0, []byte(reason)))
	c.abortIn(in, reason, err)
}

// abortIn ends an incoming transfer that will not complete, removing its
// partial file.
func (c *Core) abortIn(in *inTransfer, reason string, err error) {
	if in.f != nil {
		_ = in.f.Close()
	}
	if in.part != "" {
		_ = os.Remove(in.part)
	}
	c.endIn(in, reason, err)
}

func (c *Core) endIn(in *inTransfer, reason string, err error) {
	in.ended.Do(func() { c.countIn(in, reason, err) })
}

func (c *Core) countIn(in *inTransfer, reason string, err error) {
	c.xferMu.Lock()
	if c.in == in {
		c.in = nil
	}
	c.xferMu.Unlock()
	c.set("hoardmq_raft_snapshot_transfers_active", map[string]string{"direction": "in"}, 0)
	took := time.Since(in.began)
	c.count("hoardmq_raft_snapshot_transfer_seconds_total", map[string]string{"direction": "in"}, took.Seconds())
	outcome := "refused"
	switch reason {
	case reasonOK:
		outcome = "ok"
	case reasonAborted:
		outcome = "failed"
	}
	c.count("hoardmq_raft_snapshot_transfers_total", map[string]string{"direction": "in", "outcome": outcome, "reason": reason}, 1)
	attrs := []any{"peer", in.peer, "transfer", in.id, "index", in.desc.Index, "bytes", in.offer.size, "received", in.written, "took", took.Round(time.Millisecond)}
	if reason == reasonOK {
		c.log.Info("snapshot received", attrs...)
		return
	}
	c.log.Warn("snapshot transfer refused or aborted", append(attrs, "reason", reason, "err", err)...)
}

// abortInFrom ends an incoming transfer from peer, whose data connection
// went down.
func (c *Core) abortInFrom(peer string) {
	c.xferMu.Lock()
	in := c.in
	if in != nil && in.peer == peer {
		c.in = nil
	} else {
		in = nil
	}
	c.xferMu.Unlock()
	if in != nil {
		c.cancelIn(in, reasonAborted, errors.New("the data connection went down"))
	}
}

// onDataState follows the peers' data connections: a transfer either way
// on one that went down has lost frames and ends.
func (c *Core) onDataState(peer string, st transport.State) {
	if st != transport.Down {
		return
	}
	c.cancelOut(reasonLinkDown, func(x *outTransfer) bool { return x.peer != peer })
	c.abortInFrom(peer)
}
