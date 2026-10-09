package sim

import (
	"os"
	"strings"
	"testing"
	"time"
)

// TestHostIntervalAnswersWhyTheProcessStopped is the question the work item
// asks: given an interval in which the process was not running, say how much
// of it was the machine waiting on io, how much on cpu, and how much on
// neither. Until this existed, three published rounds reasoned "not GC,
// therefore starvation" with disk never ruled in or out.
func TestHostIntervalAnswersWhyTheProcessStopped(t *testing.T) {
	base := time.Now()
	prev := hostSnapshot{At: base, PSIOK: true}
	cur := hostSnapshot{
		At: base.Add(10 * time.Second), PSIOK: true,
		// Of ten seconds, three were spent with every task stalled on io and
		// one with every task stalled on cpu.
		IOFull: 3_000_000, IOSome: 4_000_000,
		CPUFull: 1_000_000, CPUSome: 6_000_000,
	}
	d := hostInterval(prev, cur)

	if !d.PSIOK {
		t.Fatal("pressure should be available")
	}
	if d.IOFullSec != 3 || d.CPUFullSec != 1 {
		t.Errorf("io/cpu full = %vs/%vs, want 3s/1s", d.IOFullSec, d.CPUFullSec)
	}
	if d.IOFullPct != 30 || d.CPUFullPct != 10 {
		t.Errorf("io/cpu full = %v%%/%v%%, want 30/10", d.IOFullPct, d.CPUFullPct)
	}
	if d.IOSomePct != 40 || d.CPUSomePct != 60 {
		t.Errorf("io/cpu some = %v%%/%v%%, want 40/60", d.IOSomePct, d.CPUSomePct)
	}
}

// TestHostIntervalReportsAbsentNotZero: a zero reads as "no pressure", which
// is the opposite of "not measured".
func TestHostIntervalReportsAbsentNotZero(t *testing.T) {
	base := time.Now()
	d := hostInterval(
		hostSnapshot{At: base},
		hostSnapshot{At: base.Add(time.Second)},
	)
	if d.PSIOK || d.DiskOK || d.StatOK {
		t.Fatalf("nothing was available, so nothing may be reported: %+v", d)
	}
	if got := d.suffix(); got != " | host=unavailable" {
		t.Errorf("suffix = %q, want an explicit unavailable", got)
	}

	// Pressure available, disk not: each group stands on its own.
	partial := hostInterval(
		hostSnapshot{At: base, PSIOK: true},
		hostSnapshot{At: base.Add(time.Second), PSIOK: true, IOFull: 500_000},
	)
	if !partial.PSIOK || partial.DiskOK {
		t.Fatalf("groups must be independent: %+v", partial)
	}
	if s := partial.suffix(); !strings.Contains(s, "psi io=") || !strings.Contains(s, "disk=unavailable") {
		t.Errorf("suffix = %q, want pressure reported and disk named unavailable", s)
	}
}

// TestHostIntervalRefusesCountersThatWentBackwards: a wrap, or a device
// replaced under the run, must not be reported as a negative measurement.
func TestHostIntervalRefusesCountersThatWentBackwards(t *testing.T) {
	base := time.Now()
	d := hostInterval(
		hostSnapshot{At: base, PSIOK: true, IOFull: 5_000_000, DiskOK: true, IOTicks: 900},
		hostSnapshot{At: base.Add(time.Second), PSIOK: true, IOFull: 1_000_000, DiskOK: true, IOTicks: 100},
	)
	if d.PSIOK {
		t.Errorf("a pressure counter that went backwards must be unavailable, got %+v", d)
	}
	if d.DiskOK {
		t.Errorf("a disk counter that went backwards must be unavailable, got %+v", d)
	}
	if d.IOFullPct < 0 || d.DiskUtilPct < 0 {
		t.Errorf("no figure may be negative: %+v", d)
	}
}

func TestHostIntervalRefusesAZeroLengthInterval(t *testing.T) {
	at := time.Now()
	d := hostInterval(hostSnapshot{At: at, PSIOK: true}, hostSnapshot{At: at, PSIOK: true, IOFull: 1})
	if d.PSIOK || d.IOFullPct != 0 {
		t.Errorf("an interval of no duration has no rate: %+v", d)
	}
}

// TestHostDiskDeltaComputesUtilisationAndQueue: io_ticks is milliseconds the
// device was busy; weighted_io_ticks is queue-depth-milliseconds, so dividing
// by the interval gives the average requests in flight.
func TestHostDiskDeltaComputesUtilisationAndQueue(t *testing.T) {
	base := time.Now()
	d := hostInterval(
		hostSnapshot{At: base, DiskOK: true},
		hostSnapshot{
			At: base.Add(10 * time.Second), DiskOK: true, DiskDevice: "dm-0",
			IOTicks: 9_000, WeightedIOTicks: 45_000,
			WriteSectors: 10 * (1 << 20) / 512, // 10 MiB over ten seconds
		},
	)
	if !d.DiskOK || d.DiskDevice != "dm-0" {
		t.Fatalf("disk group: %+v", d)
	}
	if d.DiskUtilPct != 90 {
		t.Errorf("utilisation = %v%%, want 90", d.DiskUtilPct)
	}
	if d.DiskQueue != 4.5 {
		t.Errorf("queue = %v, want 4.5", d.DiskQueue)
	}
	if got := d.DiskWriteMBs; got < 0.99 || got > 1.01 {
		t.Errorf("write rate = %v MB/s, want about 1", got)
	}
}

// TestReadHostOnThisMachine is a smoke test against the real /proc. It
// asserts nothing about the values — they are whatever this machine is doing
// — only that reading them does not panic and that availability is reported
// honestly.
func TestReadHostOnThisMachine(t *testing.T) {
	h := readHost(os.TempDir())
	if h.At.IsZero() {
		t.Fatal("no timestamp")
	}
	if h.DiskOK && h.DiskDevice == "" {
		t.Error("disk reported available with no device name")
	}
	if h.PSIOK && h.IOSome == 0 && h.IOFull == 0 && h.CPUSome == 0 {
		t.Error("pressure reported available but every counter is zero; suspect a parse failure")
	}
	t.Logf("psi=%v disk=%v(%s) stat=%v mem=%v self=%v", h.PSIOK, h.DiskOK, h.DiskDevice, h.StatOK, h.MemOK, h.SelfOK)
}

// TestDeviceOfResolvesSomething: the disk figures must describe the device
// the broker writes to, so an unresolvable directory reports nothing rather
// than reading whichever device happens to be first.
func TestDeviceOfResolvesSomething(t *testing.T) {
	if _, _, ok := deviceOf(t.TempDir()); !ok {
		t.Error("a real directory should resolve to a device")
	}
	if _, _, ok := deviceOf(t.TempDir() + "/does-not-exist"); ok {
		t.Error("a missing directory must not resolve")
	}
	var h hostSnapshot
	readDisk(&h, t.TempDir()+"/does-not-exist")
	if h.DiskOK {
		t.Error("an unresolvable directory must leave the disk group unavailable")
	}
}
