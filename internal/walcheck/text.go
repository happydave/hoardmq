package walcheck

import (
	"fmt"
	"io"
)

// WriteText writes the report in the human form.
func WriteText(w io.Writer, r *Report) error {
	p := &printer{w: w}
	p.report(r)
	if part := r.Repair; part != nil {
		switch {
		case part.Refused != "":
			p.f("repair refused: %s\n", part.Refused)
		case len(part.Actions) == 0:
			p.f("repair: nothing to do\n")
		case part.DryRun:
			p.f("repair (dry run; -apply makes it):\n")
			p.actions(part.Actions)
		default:
			p.f("repair:\n")
			p.actions(part.Actions)
			if part.MovedAside != "" {
				p.f("  kept what it dropped in %s\n", part.MovedAside)
			} else {
				p.f("  kept nothing (-discard)\n")
			}
			if part.Error != "" {
				p.f("  failed: %s\n", part.Error)
			}
			if part.After != nil {
				p.f("after the repair: %s\n", verdictText(part.After))
			}
		}
	}
	return p.err
}

type printer struct {
	w   io.Writer
	err error
}

func (p *printer) f(format string, a ...any) {
	if p.err == nil {
		_, p.err = fmt.Fprintf(p.w, format, a...)
	}
}

func (p *printer) report(r *Report) {
	p.f("data directory %s: %s\n", r.DataDir, lockText(r.Lock))
	for _, l := range r.Logs {
		if !l.Present {
			p.f("log %s (%s): absent\n", l.Name, l.Dir)
			continue
		}
		p.f("log %s (%s)\n", l.Name, l.Dir)
		if n := len(l.Segments); n > 0 {
			p.f("  segments %d, indexes %d to %d", n, l.Segments[0].First, l.Segments[n-1].Last)
		} else {
			p.f("  no segment")
		}
		if l.Checkpoint != nil {
			p.f("; checkpoint %s at %d\n", l.Checkpoint.File, l.Checkpoint.Index)
		} else {
			p.f("; no checkpoint\n")
		}
		if len(l.Findings) == 0 {
			p.f("  clean\n")
		}
		for _, f := range l.Findings {
			switch f.Start {
			case StartRepairs:
				p.f("  start repairs: %s: %s\n", f.Kind, f.Detail)
			case StartRefuses:
				p.f("  start refuses: %s: %s\n    recovery: %s\n", f.Kind, f.Detail, f.Recovery)
			default:
				p.f("  note: %s: %s\n", f.Kind, f.Detail)
			}
		}
	}
	p.f("verdict: %s\n", verdictText(r))
}

func (p *printer) actions(as []Action) {
	for _, a := range as {
		switch a.Repair {
		case RepairTruncateTail:
			p.f("  log %s: truncate %s at offset %d, dropping %d bytes\n", a.Log, a.File, a.Offset, a.Bytes)
		default:
			p.f("  log %s: remove %s (%d bytes)\n", a.Log, a.File, a.Bytes)
		}
		if a.Copy != "" {
			p.f("    kept as %s\n", a.Copy)
		}
	}
}

func lockText(l LockState) string {
	switch l.State {
	case "held":
		return "locked by this command"
	case "held-elsewhere":
		if l.PID > 0 {
			return fmt.Sprintf("%s is locked by another process (PID %d)", l.Path, l.PID)
		}
		return fmt.Sprintf("%s is locked by another process", l.Path)
	case "absent":
		return fmt.Sprintf("not locked (%s does not exist)", l.Path)
	case "unsupported":
		return fmt.Sprintf("could not lock %s: this filesystem does not support it (%s)", l.Path, l.Error)
	}
	return fmt.Sprintf("could not lock %s: %s", l.Path, l.Error)
}

func verdictText(r *Report) string {
	n := 0
	for _, f := range r.Findings() {
		if (r.Verdict == VerdictStartRefuses && f.Start == StartRefuses) || (r.Verdict == VerdictStartRepairs && f.Start == StartRepairs) {
			n++
		}
	}
	switch r.Verdict {
	case VerdictStartRefuses:
		return fmt.Sprintf("a start would refuse (%d)", n)
	case VerdictStartRepairs:
		return fmt.Sprintf("a start would repair %d and proceed", n)
	}
	return "clean: a start would proceed"
}
