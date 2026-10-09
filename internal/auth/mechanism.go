package auth

import "context"

// Mechanism is an MQTT 5 enhanced-authentication method (§ 4.12): the
// connection begins one exchange per CONNECT or re-authentication that
// names its method.
type Mechanism interface {
	Methods() []string
	Begin(ctx context.Context, method string, req *Request) Exchange
}

// Exchange is one run of a mechanism. Step takes the client's authentication
// data and answers.
type Exchange interface {
	Step(data []byte) Step
}

// StepKind is what a step asks of the connection.
type StepKind int

// Step kinds.
const (
	// Continue sends the step's data in an AUTH with reason 0x18 and waits
	// for the client's next AUTH.
	Continue StepKind = iota
	// Succeed ends the exchange authenticated.
	Succeed
	// Fail ends the exchange refused.
	Fail
)

// Step is an exchange's answer. Data goes to the client (the server's
// challenge, or on success its final message). On success Username is the
// authenticated user. On failure Checked says whether a credential was
// examined (0x86) rather than the exchange being malformed (0x87), and Reason
// is for the audit log only.
type Step struct {
	Kind       StepKind
	Data       []byte
	Username   string
	Attributes map[string]string
	Checked    bool
	Reason     string
}
