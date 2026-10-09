//go:build race

package session

// raceEnabled: the race detector changes what escapes, so an allocation
// count measured without it is not a bound with it.
const raceEnabled = true
