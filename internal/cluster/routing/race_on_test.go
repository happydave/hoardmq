//go:build race

package routing

// raceEnabled: the race detector changes what escapes, so an allocation
// count measured without it is not a bound with it.
const raceEnabled = true
