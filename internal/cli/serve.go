package cli

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/happydave/hoardmq/internal/broker"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/datadir"
)

// serveSignals is the set of signals that stop the broker; tests replace the
// notifier to drive shutdown without a real signal.
var notifyContext = func(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
}

// notifyReload delivers the signal that reloads the configuration, SIGHUP,
// until stop is called; tests replace it to deliver one without a signal.
var notifyReload = func() (signals <-chan os.Signal, stop func()) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP)
	return ch, func() { signal.Stop(ch) }
}

// reloaded is told each signal reload's result; tests replace it to wait for
// one.
var reloaded = func(broker.ReloadResult) {}

// started is told the node once it has started; tests replace it to reach
// the node's listeners and its drain.
var started = func(*broker.Server) {}

// lockDataDir takes the data directory's lock; tests replace it to stand in
// for a filesystem that cannot lock.
var lockDataDir = datadir.Exclusive

// lockShared is wal verify's and a dry run's lock; tests replace it as they
// do lockDataDir.
var lockShared = datadir.Shared

// shutdownGrace bounds how long serve waits for connections to end.
const shutdownGrace = 5 * time.Second

func runServe(a *app, args []string) int {
	path, code := a.configFlags("serve", args)
	if code >= 0 {
		return code
	}
	doc, code := a.loadPath(path)
	if code >= 0 {
		return code
	}
	level := new(slog.LevelVar)
	level.Set(doc.Log.SlogLevel())
	logger := newLogger(a.stderr, doc.Log, level)
	srv, err := broker.New(doc, logger)
	if err != nil {
		fprintf(a.stderr, "hoardmq serve: %v\n", err)
		return ExitProblems
	}
	// The lock is taken once the listeners are bound, so a start that fails
	// to bind still writes nothing, and before any file of the node opens.
	var lock *datadir.Lock
	defer func() {
		if lock != nil {
			_ = lock.Release()
		}
	}()
	srv.SetDataDirGuard(func() error {
		lock = lockDataDir(doc.Node.DataDir)
		switch lock.Outcome {
		case datadir.HeldElsewhere:
			return fmt.Errorf("data directory %s is in use: %s", doc.Node.DataDir, lock.Describe())
		case datadir.Failed:
			return fmt.Errorf("data directory %s: %s", doc.Node.DataDir, lock.Describe())
		case datadir.Unsupported:
			logger.Warn("the data directory cannot be locked, so nothing stops a second node from using it", "dataDir", doc.Node.DataDir, "err", lock.Err)
		}
		srv.SetDataDirLocked(lock.Outcome == datadir.Held)
		return nil
	})
	srv.SetConfigSource(broker.ConfigSource{Path: path, Environ: a.environ, Level: level})
	// SIGHUP is caught from before Start, so one arriving while the node
	// starts does not end the process; it is acted on once Start returns.
	reloads, stopReloads := notifyReload()
	defer stopReloads()
	if err := srv.Start(); err != nil {
		fprintf(a.stderr, "hoardmq serve: %v\n", err)
		return ExitProblems
	}
	// Reloads run until serve returns, through a drain and the shutdown,
	// which refuses them.
	quit, reloaderDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(reloaderDone)
		for {
			select {
			case <-reloads:
				reloaded(srv.ReloadOnSignal("SIGHUP"))
			case <-quit:
				return
			}
		}
	}()
	defer func() {
		close(quit)
		<-reloaderDone
	}()
	ctx, stop := notifyContext(context.Background())
	defer stop()
	logger.Info("hoardmq serving", "version", Version(), "dataDir", doc.Node.DataDir)
	started(srv)
	select {
	case <-ctx.Done():
		if doc.Cluster.Drain.OnSignal && !srv.Draining() {
			// Setups without a preStop hook (bare containers, systemd) get
			// the graceful leave from the signal itself; a second signal
			// exits without waiting for the drain to finish.
			logger.Info("signal received; draining before exit")
			srv.Drain()
			again, stopAgain := notifyContext(context.Background())
			defer stopAgain()
			select {
			case <-srv.Drained():
				logger.Info("drain complete; exiting")
			case <-again.Done():
				logger.Info("second signal; exiting without finishing the drain")
			}
		}
	case <-srv.Drained():
		holdExit(ctx, logger, time.Duration(doc.Cluster.Drain.HoldExitMs)*time.Millisecond)
	}
	logger.Info("shutting down", "grace", shutdownGrace.String())
	sctx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		logger.Warn("shutdown incomplete", "err", err)
	}
	return ExitOK
}

// holdExit keeps the process up after a drain no signal started, until the
// stop signal or the end of the hold: a preStop hook waiting on the drain dies
// with this process, and the kubelet sends SIGTERM only once the hook returns.
func holdExit(stop context.Context, logger *slog.Logger, hold time.Duration) {
	if hold <= 0 {
		logger.Info("drain complete; exiting")
		return
	}
	logger.Info("drain complete; waiting for the stop signal", "hold", hold.String())
	timer := time.NewTimer(hold)
	defer timer.Stop()
	select {
	case <-stop.Done():
		logger.Info("stop signal after the drain; exiting")
	case <-timer.C:
		logger.Info("no stop signal within the hold; exiting")
	}
}

// newLogger writes at the level variable's level, which a reload sets.
func newLogger(w io.Writer, cfg config.Log, level *slog.LevelVar) *slog.Logger {
	opts := &slog.HandlerOptions{Level: level}
	if cfg.Format == "json" {
		return slog.New(slog.NewJSONHandler(w, opts))
	}
	return slog.New(slog.NewTextHandler(w, opts))
}
