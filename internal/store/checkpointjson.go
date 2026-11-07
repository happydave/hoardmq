package store

import (
	"bytes"
	"encoding/json"
	"errors"
)

// sessionsHead is how json.Marshal begins a State with no sessions.
const sessionsHead = `{"sessions":null`

// encodeCheckpoint writes the bytes json.Marshal(st) gives, one session at a
// time into a buffer of size hint, and clears each session's slot once it is
// written. json.Marshal holds the whole payload in a buffer that grows by
// doubling and then copies it, most of a checkpoint's heap peak at scale. The
// state must be the checkpoint's own.
func encodeCheckpoint(st *State, hint int) ([]byte, error) {
	buf := make([]byte, 0, hint)
	buf = append(buf, sessionsHead[:len(sessionsHead)-len("null")]...)
	if st.Sessions == nil {
		buf = append(buf, "null"...)
	} else {
		buf = append(buf, '[')
		for i, ss := range st.Sessions {
			if i > 0 {
				buf = append(buf, ',')
			}
			b, err := json.Marshal(ss)
			if err != nil {
				return nil, err
			}
			buf = append(buf, b...)
			st.Sessions[i] = nil
		}
		buf = append(buf, ']')
	}
	// Every other member as json.Marshal writes it, read from the state with
	// its sessions left out, so a member added to State is never missed.
	rest := *st
	rest.Sessions = nil
	tail, err := json.Marshal(&rest)
	if err != nil {
		return nil, err
	}
	if !bytes.HasPrefix(tail, []byte(sessionsHead)) {
		return nil, errors.New("store: a checkpoint state does not encode its sessions first")
	}
	return append(buf, tail[len(sessionsHead):]...), nil
}
