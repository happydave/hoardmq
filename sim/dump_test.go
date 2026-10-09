package sim

import (
	"fmt"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
	"time"
)

// A long measurement cannot be interrupted for a goroutine dump without
// losing its report. With HOARDMQ_SIM_DUMP=<prefix>, each SIGUSR1 writes the
// full goroutine profile to <prefix>-<time>.txt and the run continues.
// Collecting the profile stops the world for as long as it takes to walk
// every goroutine, which at a hundred thousand connections is seconds; the
// completion line on stderr says how long, so the pause can be read against
// the measurement.
func init() {
	prefix := os.Getenv("HOARDMQ_SIM_DUMP")
	if prefix == "" {
		return
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGUSR1)
	go func() {
		for range ch {
			start := time.Now()
			path := fmt.Sprintf("%s-%s.txt", prefix, start.Format("150405"))
			f, err := os.Create(path)
			if err != nil {
				fmt.Fprintf(os.Stderr, "goroutine dump: %v\n", err)
				continue
			}
			err = pprof.Lookup("goroutine").WriteTo(f, 2)
			_ = f.Close()
			fmt.Fprintf(os.Stderr, "goroutine dump %s written in %s (err=%v)\n", path, time.Since(start).Round(time.Millisecond), err)
		}
	}()
}
