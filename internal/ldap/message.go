package ldap

import (
	"bufio"
	"fmt"
	"io"
)

// Protocol operations: their application tags (RFC 4511 § 4.2 to § 4.12).
const (
	opBindRequest    = 0
	opBindResponse   = 1
	opUnbindRequest  = 2
	opSearchRequest  = 3
	opSearchEntry    = 4
	opSearchDone     = 5
	opSearchRef      = 19
	opExtendedReq    = 23
	opExtendedResult = 24
)

// Result codes this package names (RFC 4511 Appendix A).
const (
	Success             = 0
	SizeLimitExceeded   = 4
	NoSuchObject        = 32
	InvalidCredentials  = 49
	Busy                = 51
	Unavailable         = 52
	ProtocolError       = 2
	UnwillingToPerform  = 53
	startTLSOID         = "1.3.6.1.4.1.1466.20037"
	noticeOfDisconnectN = "1.3.6.1.4.1.1466.20036"
)

// Scope is a search's scope.
type Scope int

// Scopes (RFC 4511 § 4.5.1.2).
const (
	ScopeBase Scope = 0
	ScopeOne  Scope = 1
	ScopeSub  Scope = 2
)

func message(id int, op []byte) []byte {
	return seq(integer(tagInteger, classUniversal, int64(id)), op)
}

// BindRequest is a simple bind: version 3, the name, and the password as the
// simple credentials ([0] primitive).
func BindRequest(id int, name string, password []byte) []byte {
	op := element(classApplication, true, opBindRequest, concat([][]byte{
		integer(tagInteger, classUniversal, 3),
		octets([]byte(name)),
		element(classContext, false, 0, password),
	}))
	return message(id, op)
}

// SearchRequest asks for the DNs only (the attribute list "1.1"), never
// dereferencing aliases, with the given size and time limits.
func SearchRequest(id int, base string, scope Scope, sizeLimit, timeLimit int, filter []byte) []byte {
	op := element(classApplication, true, opSearchRequest, concat([][]byte{
		octets([]byte(base)),
		integer(tagEnumerated, classUniversal, int64(scope)),
		integer(tagEnumerated, classUniversal, 0),
		integer(tagInteger, classUniversal, int64(sizeLimit)),
		integer(tagInteger, classUniversal, int64(timeLimit)),
		boolean(false),
		filter,
		seq(octets([]byte("1.1"))),
	}))
	return message(id, op)
}

// StartTLSRequest is the StartTLS extended operation (RFC 4511 § 4.14).
func StartTLSRequest(id int) []byte {
	op := element(classApplication, true, opExtendedReq, element(classContext, false, 0, []byte(startTLSOID)))
	return message(id, op)
}

// UnbindRequest ends the session (a primitive NULL-valued operation).
func UnbindRequest(id int) []byte {
	return message(id, element(classApplication, false, opUnbindRequest, nil))
}

// Response is one decoded message from the directory.
type Response struct {
	ID int
	// Op is the operation's application tag.
	Op   int
	Code int
	// DN is a search entry's object name.
	DN string
	// Name is an extended response's name, when present.
	Name string
}

// ParseResponse decodes one LDAPMessage.
func ParseResponse(b []byte) (Response, error) {
	env, rest, err := expect(b, classUniversal, true, tagSequence)
	if err != nil {
		return Response{}, err
	}
	if len(rest) != 0 {
		return Response{}, fmt.Errorf("%w: bytes after the message", ErrMalformed)
	}
	idt, body, err := expect(env.value, classUniversal, false, tagInteger)
	if err != nil {
		return Response{}, err
	}
	id, err := parseInt(idt.value)
	if err != nil {
		return Response{}, err
	}
	op, _, err := readTLV(body)
	if err != nil {
		return Response{}, err
	}
	if op.class != classApplication {
		return Response{}, fmt.Errorf("%w: operation not application-tagged", ErrMalformed)
	}
	r := Response{ID: int(id), Op: op.tag}
	switch op.tag {
	case opSearchEntry:
		dn, _, err := expect(op.value, classUniversal, false, tagOctetString)
		if err != nil {
			return Response{}, err
		}
		r.DN = string(dn.value)
	case opSearchRef:
	case opBindResponse, opSearchDone, opExtendedResult:
		code, rest, err := expect(op.value, classUniversal, false, tagEnumerated)
		if err != nil {
			return Response{}, err
		}
		n, err := parseInt(code.value)
		if err != nil {
			return Response{}, err
		}
		r.Code = int(n)
		if op.tag == opExtendedResult {
			r.Name = extendedName(rest)
		}
	default:
		return Response{}, fmt.Errorf("%w: unexpected operation %d", ErrMalformed, op.tag)
	}
	return r, nil
}

// extendedName finds the responseName ([10]) after an LDAPResult's code.
func extendedName(b []byte) string {
	for len(b) > 0 {
		t, rest, err := readTLV(b)
		if err != nil {
			return ""
		}
		if t.class == classContext && t.tag == 10 {
			return string(t.value)
		}
		b = rest
	}
	return ""
}

// readMessage reads one whole BER element from r, refusing one over max.
func readMessage(r *bufio.Reader, max int) ([]byte, error) {
	id, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	first, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	hdr := []byte{id, first}
	if first&0x80 != 0 {
		count := int(first & 0x7f)
		if count == 0 || count > 4 {
			return nil, fmt.Errorf("%w: length octets", ErrMalformed)
		}
		more := make([]byte, count)
		if _, err := io.ReadFull(r, more); err != nil {
			return nil, err
		}
		hdr = append(hdr, more...)
	}
	n, _, err := decodeLength(hdr[1:])
	if err != nil {
		return nil, err
	}
	if n > max {
		return nil, fmt.Errorf("%w: a message of %d bytes", ErrMalformed, n)
	}
	body := make([]byte, len(hdr)+n)
	copy(body, hdr)
	if _, err := io.ReadFull(r, body[len(hdr):]); err != nil {
		return nil, err
	}
	return body, nil
}
