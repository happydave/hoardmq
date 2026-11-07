// Package datadir holds a node's data directory: the lock a running node
// takes so that no second node, and no repair, works on its files at once.
package datadir

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// LockName is the lock file in a data directory.
const LockName = "LOCK"

// Outcome is what a lock attempt found.
type Outcome int

const (
	// Held: this process holds the lock.
	Held Outcome = iota
	// HeldElsewhere: another process holds it.
	HeldElsewhere
	// Unsupported: the filesystem cannot lock.
	Unsupported
	// Absent: a shared attempt found no lock file; nothing of this build has
	// held the directory.
	Absent
	// Failed: the lock file could not be opened or locked for another reason.
	Failed
)

func (o Outcome) String() string {
	switch o {
	case Held:
		return "held"
	case HeldElsewhere:
		return "held-elsewhere"
	case Unsupported:
		return "unsupported"
	case Absent:
		return "absent"
	}
	return "failed"
}

// Lock is one attempt on a data directory's lock.
type Lock struct {
	Outcome Outcome
	Path    string
	// PID is the process the file names when another holds the lock, 0 when
	// it names none.
	PID int
	// Err is the error behind Unsupported and Failed.
	Err error
	f   *os.File
}

// Exclusive takes the data directory's lock, creating the directory and the
// file as needed, without waiting. A holder writes its PID into the file.
func Exclusive(dir string) *Lock {
	dir = orCurrent(dir)
	l := &Lock{Path: filepath.Join(dir, LockName)}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		l.Outcome, l.Err = Failed, err
		return l
	}
	f, err := os.OpenFile(l.Path, os.O_RDWR|os.O_CREATE, 0o640)
	if err != nil {
		l.Outcome, l.Err = Failed, err
		return l
	}
	if l.take(f, true); l.Outcome != Held {
		return l
	}
	if err := writePID(f); err != nil {
		_ = f.Close()
		l.f, l.Outcome, l.Err = nil, Failed, err
	}
	return l
}

// Shared takes the lock beside other shared holders, without waiting and
// without creating or writing anything: a directory with no lock file is
// Absent.
func Shared(dir string) *Lock {
	l := &Lock{Path: filepath.Join(orCurrent(dir), LockName)}
	f, err := os.Open(l.Path)
	if os.IsNotExist(err) {
		l.Outcome = Absent
		return l
	}
	if err != nil {
		l.Outcome, l.Err = Failed, err
		return l
	}
	l.take(f, false)
	return l
}

// take locks f, keeping it when the lock is held and closing it otherwise.
func (l *Lock) take(f *os.File, exclusive bool) {
	err := flock(f, exclusive)
	l.Outcome = classify(err)
	if l.Outcome == Held {
		l.f = f
		return
	}
	if l.Outcome == HeldElsewhere {
		l.PID = readPID(f)
	} else {
		l.Err = err
	}
	_ = f.Close()
}

// Release gives the lock up.
func (l *Lock) Release() error {
	if l.f == nil {
		return nil
	}
	f := l.f
	l.f = nil
	return f.Close()
}

func orCurrent(dir string) string {
	if dir == "" {
		return "."
	}
	return dir
}

func writePID(f *os.File) error {
	if err := f.Truncate(0); err != nil {
		return err
	}
	if _, err := f.WriteAt([]byte(strconv.Itoa(os.Getpid())+"\n"), 0); err != nil {
		return err
	}
	return f.Sync()
}

func readPID(f *os.File) int {
	b := make([]byte, 32)
	n, _ := f.ReadAt(b, 0)
	pid, err := strconv.Atoi(string(bytes.TrimSpace(b[:n])))
	if err != nil {
		return 0
	}
	return pid
}

// Describe is the lock's state in a sentence, for messages.
func (l *Lock) Describe() string {
	switch l.Outcome {
	case Held:
		return "locked by this process"
	case HeldElsewhere:
		if l.PID > 0 {
			return fmt.Sprintf("%s is locked by another process (PID %d)", l.Path, l.PID)
		}
		return fmt.Sprintf("%s is locked by another process (no PID recorded)", l.Path)
	case Unsupported:
		return fmt.Sprintf("%s cannot be locked on this filesystem: %v", l.Path, l.Err)
	case Absent:
		return fmt.Sprintf("%s does not exist: no node of this build has held the directory", l.Path)
	}
	return fmt.Sprintf("%s: %v", l.Path, l.Err)
}
