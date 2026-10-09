package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"
)

// CredentialsCollection is the replicated collection the in-broker database
// keeps client credentials in, keyed by username.
const CredentialsCollection = "auth.users"

// Bounds on a stored credential.
const (
	MaxCredentialUsername = 256
	MaxAttributes         = 32
	MaxAttributeBytes     = 256
)

// StoredCredential is a credential record's value: the password only as a
// PHC string of a supported form, and attributes for authorization.
type StoredCredential struct {
	Password   string            `json:"password"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// CheckCredential is the rule every stored credential meets.
func CheckCredential(username string, c StoredCredential) error {
	if username == "" || len(username) > MaxCredentialUsername || !utf8.ValidString(username) {
		return fmt.Errorf("username must be 1 to %d bytes of UTF-8", MaxCredentialUsername)
	}
	if err := CheckPHC(c.Password); errors.Is(err, ErrArgon2idUnavailable) {
		return err
	} else if err != nil {
		return errors.New("the password must be a PHC hash string (see hoardmq hash-password)")
	}
	if len(c.Attributes) > MaxAttributes {
		return fmt.Errorf("at most %d attributes", MaxAttributes)
	}
	for k, v := range c.Attributes {
		if k == "" || len(k) > MaxAttributeBytes || len(v) > MaxAttributeBytes || !utf8.ValidString(k) || !utf8.ValidString(v) {
			return fmt.Errorf("attribute names must be 1 to %d bytes and values at most %d, of UTF-8", MaxAttributeBytes, MaxAttributeBytes)
		}
	}
	return nil
}

// CredentialForm names a stored password's form.
func CredentialForm(phc string) string {
	if _, err := ParseSCRAM(phc); err == nil {
		return scramID
	}
	return phcID
}

// DBAuthenticator checks username and password against the in-broker
// database. Lookup returns the user's record value, whether one exists, and
// whether the database is open on this node yet.
type DBAuthenticator struct {
	Lookup func(username string) (value json.RawMessage, found, open bool)
	// Cache answers a repeated verification without deriving; nil derives.
	Cache *VerifyCache
}

// Authenticate implements Authenticator.
func (a *DBAuthenticator) Authenticate(_ context.Context, req *Request) Result {
	value, found, open := a.Lookup(req.Username)
	return dbDecide(req, value, found, open, a.Cache.Verify)
}

// dbDecide is the decision on a request against what the database holds for
// its username: an unknown user, or a database not open yet, passes; a known
// user is allowed only with the right password.
func dbDecide(req *Request, value json.RawMessage, found, open bool, verify func(stored string, password []byte) (bool, error)) Result {
	if !open || !found {
		return Result{Decision: Pass}
	}
	var c StoredCredential
	if err := json.Unmarshal(value, &c); err != nil {
		return Result{Decision: Deny, Reason: "db: stored credential unreadable"}
	}
	match, err := verify(c.Password, req.Password)
	if errors.Is(err, ErrArgon2Budget) {
		return Result{Decision: Deny, Reason: "db: " + ErrArgon2Budget.Error()}
	}
	if err != nil {
		return Result{Decision: Deny, Reason: "db: stored credential unreadable"}
	}
	if !match {
		return Result{Decision: Deny, Reason: "db: wrong password"}
	}
	return Result{Decision: Allow, Attributes: c.Attributes}
}
