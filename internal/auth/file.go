package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"

	"github.com/happydave/hoardmq/internal/topic"
)

// FileAuthenticator checks username and password against a JSON users file:
//
//	{"users": [{"username": "u", "password": "$pbkdf2-sha256$...", "attributes": {"k": "v"}}]}
//
// A known user with a wrong password is a deny; an unknown user, or a
// request without a username, passes to the next authenticator, so a token
// sent beside a username reaches the authenticator that claims it.
type FileAuthenticator struct {
	path  string
	mu    sync.RWMutex
	users map[string]fileUser
	// Cache answers a repeated verification without deriving; nil derives.
	Cache Verifier
}

type fileUser struct {
	Username   string            `json:"username"`
	Password   string            `json:"password"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

type usersFile struct {
	Users []fileUser `json:"users"`
}

// LoadUsersFile reads and validates a users file. Every password must be a
// supported PHC string; a plain-text password is an error naming the user.
func LoadUsersFile(path string) (*FileAuthenticator, error) {
	f := &FileAuthenticator{path: path}
	if err := f.Reload(); err != nil {
		return nil, err
	}
	return f, nil
}

// Reload re-reads the file. On error the previous users stay in force.
func (f *FileAuthenticator) Reload() error {
	data, err := os.ReadFile(f.path)
	if err != nil {
		return fmt.Errorf("users file: %w", err)
	}
	users, err := parseUsers(data)
	if err != nil {
		return fmt.Errorf("users file %s: %w", f.path, err)
	}
	f.mu.Lock()
	f.users = users
	f.mu.Unlock()
	return nil
}

func parseUsers(data []byte) (map[string]fileUser, error) {
	var doc usersFile
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&doc); err != nil {
		return nil, err
	}
	users := make(map[string]fileUser, len(doc.Users))
	for i, u := range doc.Users {
		if u.Username == "" {
			return nil, fmt.Errorf("users[%d]: username is empty", i)
		}
		if _, dup := users[u.Username]; dup {
			return nil, fmt.Errorf("users[%d]: duplicate username %q", i, u.Username)
		}
		if err := CheckPHC(u.Password); errors.Is(err, ErrSCRAMIterations) || errors.Is(err, ErrArgon2idUnavailable) {
			return nil, fmt.Errorf("users[%d] %q: %w", i, u.Username, err)
		} else if err != nil {
			return nil, fmt.Errorf("users[%d] %q: password must be a PHC hash string (see hoardmq hash-password)", i, u.Username)
		}
		users[u.Username] = u
	}
	return users, nil
}

// Authenticate implements Authenticator.
func (f *FileAuthenticator) Authenticate(_ context.Context, req *Request) Result {
	if !req.HasUsername {
		return Result{Decision: Pass}
	}
	f.mu.RLock()
	u, ok := f.users[req.Username]
	f.mu.RUnlock()
	if !ok {
		return Result{Decision: Pass}
	}
	match, err := verifyWith(f.Cache, u.Password, req.Password)
	if errors.Is(err, ErrArgon2Budget) {
		return Result{Decision: Deny, Reason: "file: " + ErrArgon2Budget.Error()}
	}
	if err != nil || !match {
		return Result{Decision: Deny, Reason: "file: wrong password"}
	}
	return Result{Decision: Allow, Attributes: u.Attributes}
}

// FileAuthorizer applies ACL rules from a JSON file:
//
//	{"rules": [{"principal": {"username": "u"}, "topic": "devices/${clientid}/#", "allow": ["publish", "subscribe"]}]}
//
// A rule applies when every stated principal member matches the identity and
// the topic filter, after template substitution (${clientid}, ${username},
// ${cn}, ${attr.<name>}), matches the request topic.
// The first applicable rule that mentions the action decides; "deny" lists
// refuse, "allow" lists grant. No applicable rule passes. A rule whose
// template value is empty or contains a wildcard or level separator, or
// names an attribute the identity was not given, is not applicable to that
// identity. A template only fills in a topic; an attribute grants nothing by
// itself.
type FileAuthorizer struct {
	path  string
	mu    sync.RWMutex
	rules []aclRule
}

type aclPrincipal struct {
	Username *string `json:"username,omitempty"`
	ClientID *string `json:"clientId,omitempty"`
	CN       *string `json:"cn,omitempty"`
}

type aclRule struct {
	Principal aclPrincipal `json:"principal"`
	Topic     string       `json:"topic"`
	Allow     []string     `json:"allow,omitempty"`
	Deny      []string     `json:"deny,omitempty"`
}

type aclFile struct {
	Rules []aclRule `json:"rules"`
}

// LoadACLFile reads and validates an ACL file.
func LoadACLFile(path string) (*FileAuthorizer, error) {
	a := &FileAuthorizer{path: path}
	if err := a.Reload(); err != nil {
		return nil, err
	}
	return a, nil
}

// Reload re-reads the file. On error the previous rules stay in force.
func (a *FileAuthorizer) Reload() error {
	data, err := os.ReadFile(a.path)
	if err != nil {
		return fmt.Errorf("acl file: %w", err)
	}
	rules, err := parseACL(data)
	if err != nil {
		return fmt.Errorf("acl file %s: %w", a.path, err)
	}
	a.mu.Lock()
	a.rules = rules
	a.mu.Unlock()
	return nil
}

func parseACL(data []byte) ([]aclRule, error) {
	var doc aclFile
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&doc); err != nil {
		return nil, err
	}
	for i, r := range doc.Rules {
		if r.Topic == "" {
			return nil, fmt.Errorf("rules[%d]: topic is empty", i)
		}
		if strings.Contains(templatePattern.ReplaceAllString(r.Topic, ""), "${attr.") {
			return nil, fmt.Errorf("rules[%d]: an attribute template is ${attr.<name>}, a name of letters, digits, '.', '_' or '-'", i)
		}
		if msg := topic.ValidateFilter(substitute(r.Topic, checkIdentity(r.Topic))); msg != "" {
			return nil, fmt.Errorf("rules[%d]: %s", i, msg)
		}
		if len(r.Allow) == 0 && len(r.Deny) == 0 {
			return nil, fmt.Errorf("rules[%d]: neither allow nor deny", i)
		}
		for _, act := range append(append([]string{}, r.Allow...), r.Deny...) {
			if _, ok := parseAction(act); !ok {
				return nil, fmt.Errorf("rules[%d]: unknown action %q", i, act)
			}
		}
	}
	return doc.Rules, nil
}

func parseAction(s string) (Action, bool) {
	switch s {
	case "publish":
		return Publish, true
	case "subscribe":
		return Subscribe, true
	case "unsubscribe":
		return Unsubscribe, true
	}
	return 0, false
}

// templatePattern finds every template in a rule topic: ${clientid},
// ${username}, ${cn} and ${attr.<name>}, an authenticator's attribute.
var templatePattern = regexp.MustCompile(`\$\{(clientid|username|cn|attr\.[A-Za-z0-9_.-]+)\}`)

// templateValue is the identity's value for one template's name, and
// whether it has one; an attribute it was not given has none.
func templateValue(name string, id *Identity) (string, bool) {
	switch name {
	case "clientid":
		return id.ClientID, true
	case "username":
		return id.Username, true
	case "cn":
		return id.CertCN, true
	}
	v, ok := id.Attributes[strings.TrimPrefix(name, "attr.")]
	return v, ok
}

// substitute replaces every template in a rule topic in one pass, so a value
// is never read again as a template.
func substitute(pattern string, id *Identity) string {
	return templatePattern.ReplaceAllStringFunc(pattern, func(m string) string {
		v, _ := templateValue(m[2:len(m)-1], id)
		return v
	})
}

// templateUsable reports whether every template the pattern uses has a
// usable value, so that no identity member, and no missing attribute, can
// widen a rule.
func templateUsable(pattern string, id *Identity) bool {
	for _, m := range templatePattern.FindAllStringSubmatch(pattern, -1) {
		if v, ok := templateValue(m[1], id); !ok || !templateValueUsable(v) {
			return false
		}
	}
	return true
}

// checkIdentity stands for every identity when a rule's topic is validated:
// each template, attributes included, as a plain value.
func checkIdentity(pattern string) *Identity {
	id := &Identity{ClientID: "x", Username: "x", CertCN: "x", Attributes: map[string]string{}}
	for _, m := range templatePattern.FindAllStringSubmatch(pattern, -1) {
		if name, ok := strings.CutPrefix(m[1], "attr."); ok {
			id.Attributes[name] = "x"
		}
	}
	return id
}

// templateValueUsable reports whether a value can stand in a rule topic as
// literal text. A wildcard ('+', '#') or level separator ('/') in it would
// make the rule match other identities' topics.
func templateValueUsable(v string) bool {
	return v != "" && !strings.ContainsAny(v, "+#/")
}

// Authorize implements Authorizer. For subscribe and unsubscribe the topic
// is a filter; it is authorized when the rule's filter covers it exactly or
// by wildcard (a request for "a/#" needs a rule for "a/#" or "#", not "a/b").
func (a *FileAuthorizer) Authorize(_ context.Context, action Action, t string, id *Identity) Decision {
	a.mu.RLock()
	defer a.mu.RUnlock()
	name := action.String()
	for _, r := range a.rules {
		if !principalMatches(r.Principal, id) || !templateUsable(r.Topic, id) {
			continue
		}
		filter := substitute(r.Topic, id)
		if !covers(filter, t) {
			continue
		}
		if contains(r.Deny, name) {
			return Deny
		}
		if contains(r.Allow, name) {
			return Allow
		}
	}
	return Pass
}

func principalMatches(p aclPrincipal, id *Identity) bool {
	if p.Username != nil && *p.Username != id.Username {
		return false
	}
	if p.ClientID != nil && *p.ClientID != id.ClientID {
		return false
	}
	if p.CN != nil && *p.CN != id.CertCN {
		return false
	}
	return true
}

// covers reports whether a rule filter authorizes a request topic or filter.
// A request without wildcards is a plain match. A request with wildcards is
// covered only when the rule filter matches every name the request could
// match, which holds when the rule is "#", or the rule equals the request,
// or the rule matches the request treated as a name with its wildcards as
// literal levels and the rule has '#' at or before the request's first
// wildcard level.
func covers(rule, request string) bool {
	if !topic.HasWildcard(request) {
		return topic.Match(rule, request)
	}
	if rule == request || rule == "#" {
		return true
	}
	rl := strings.Split(rule, "/")
	ql := strings.Split(request, "/")
	for i := range ql {
		if i >= len(rl) {
			return false
		}
		if rl[i] == "#" {
			return true
		}
		if ql[i] == "#" {
			return false
		}
		if ql[i] == "+" {
			if rl[i] != "+" {
				return false
			}
			continue
		}
		if rl[i] != "+" && rl[i] != ql[i] {
			return false
		}
	}
	return len(rl) == len(ql)
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// SCRAM returns a user's SCRAM credential: false for an unknown user and for
// one whose password is not stored in the SCRAM form.
func (f *FileAuthenticator) SCRAM(username string) (SCRAMCredential, bool) {
	f.mu.RLock()
	u, ok := f.users[username]
	f.mu.RUnlock()
	if !ok {
		return SCRAMCredential{}, false
	}
	c, err := ParseSCRAM(u.Password)
	return c, err == nil
}
