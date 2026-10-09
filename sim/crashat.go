package sim

import (
	"fmt"
	"strconv"
	"time"
)

// crashAtEnv, when set, holds the crash until that many seconds after the
// run's start. The survivors' lease ticks run on a schedule set when their
// nodes start, and the crashed node is declared dead on one of them, so two
// builds whose fills take different times would otherwise crash at different
// points of that schedule and recover at different times for that alone.
const crashAtEnv = "HOARDMQ_SIM_CRASH_AT_SEC"

// crashAt reads crashAtEnv: zero when it is unset, an error when it is not a
// positive whole number of seconds.
func crashAt(getenv func(string) string) (time.Duration, error) {
	v := getenv(crashAtEnv)
	if v == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s=%q: want a positive whole number of seconds", crashAtEnv, v)
	}
	return time.Duration(n) * time.Second, nil
}

// crashWait is how long to hold the crash so it falls at start+at. A run that
// reaches the crash later than that is refused rather than crashed late,
// since its crash would not be at the asked point.
func crashWait(start, now time.Time, at time.Duration) (time.Duration, error) {
	due := start.Add(at)
	if now.After(due) {
		return 0, fmt.Errorf("%s=%d: the run reached the crash %.2fs after its start, later than asked", crashAtEnv, int(at/time.Second), now.Sub(start).Seconds())
	}
	return due.Sub(now), nil
}
