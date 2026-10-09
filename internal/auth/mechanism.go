package auth

import extauth "github.com/happydave/hoardmq/ext/auth"

// The enhanced-authentication mechanism is the extension API's.
type (
	Mechanism = extauth.Mechanism
	Exchange  = extauth.Exchange
	Step      = extauth.Step
	StepKind  = extauth.StepKind
)

// Step kinds.
const (
	Continue = extauth.Continue
	Succeed  = extauth.Succeed
	Fail     = extauth.Fail
)
