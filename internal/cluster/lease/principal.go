package lease

import (
	"crypto/sha256"
	"encoding/binary"
)

// PrincipalDigest names a session's principal on its lease without the
// username: the first 8 bytes of SHA-256 over the length-prefixed namespace
// and username, never 0, so 0 always means none. SHA-256 makes a crafted
// colliding username impractical; a collision would only leave the exact
// comparison the session manager makes at connect to refuse, as it did before
// leases carried a digest.
func PrincipalDigest(namespace, username string) uint64 {
	b := make([]byte, 0, 2*binary.MaxVarintLen64+len(namespace)+len(username))
	b = binary.AppendUvarint(b, uint64(len(namespace)))
	b = append(b, namespace...)
	b = binary.AppendUvarint(b, uint64(len(username)))
	b = append(b, username...)
	sum := sha256.Sum256(b)
	if d := binary.BigEndian.Uint64(sum[:8]); d != 0 {
		return d
	}
	return 1
}

// principalRefusedEarly is the decision on a CONNECT for a lease another
// node may hold: refused when sessions are bound to their principal and the
// lease records a principal other than the CONNECT's. A lease or a CONNECT
// with no digest is not refused here; the session manager's comparison still
// applies after the acquire.
func principalRefusedEarly(bind bool, lease, connecting uint64) bool {
	return bind && lease != 0 && connecting != 0 && lease != connecting
}
