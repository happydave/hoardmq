package sim

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Every published failover report carries this caveat: "The sampler reports
// how long the process was stopped and never why: a stop is equally
// consistent with stop-the-world, scheduler starvation, CPU steal on a
// shared host and swap." GC is measured and the Go scheduler's latency is
// measured; nothing measured the machine, so three rounds reasoned "not GC,
// therefore starvation" with disk neither ruled in nor out (WI 1375).
//
// This file measures the machine. The headline is Linux pressure stall
// information: for cpu, io and memory, the time some task was stalled on
// that resource and the time every runnable task was. "full" is the one
// that means the machine stopped, and it is directly comparable with the
// stall observer's figure for how long this process was not running.

// hostSnapshot is one reading of the machine's cumulative counters. Fields
// carry their own availability: a host that does not expose a file leaves
// that group unavailable rather than zero, because a zero reads as "no
// pressure" and that is the opposite of "not measured".
type hostSnapshot struct {
	At time.Time

	PSIOK                   bool
	CPUSome, IOSome, IOFull uint64 // microseconds, cumulative
	MemSome, MemFull        uint64
	CPUFull                 uint64

	StatOK               bool
	IOWait, Steal, Total uint64 // jiffies, cumulative

	DiskOK                    bool
	DiskDevice                string
	ReadIOs, WriteIOs         uint64
	ReadSectors, WriteSectors uint64
	IOTicks, WeightedIOTicks  uint64 // milliseconds, cumulative

	MemOK                                bool
	MemAvailableKB, DirtyKB, WritebackKB uint64

	SelfOK                bool
	SelfReadB, SelfWriteB uint64
	MajFlt                uint64
}

// hostDelta is what one interval of the machine looked like. Percentages are
// of the interval; a fraction above 100 is possible for "some" by PSI's own
// definition and for disk utilisation on a multi-queue device, and is
// reported rather than clamped, because a clamp would hide the saturation
// this exists to show.
type hostDelta struct {
	PSIOK      bool    `json:"psi_ok"`
	CPUSomePct float64 `json:"cpu_some_pct,omitempty"`
	CPUFullPct float64 `json:"cpu_full_pct,omitempty"`
	IOSomePct  float64 `json:"io_some_pct,omitempty"`
	IOFullPct  float64 `json:"io_full_pct,omitempty"`
	MemSomePct float64 `json:"mem_some_pct,omitempty"`
	MemFullPct float64 `json:"mem_full_pct,omitempty"`
	// IOFullSec is the same figure as IOFullPct in seconds, so it can be put
	// beside the stall observer's "how long the process was not running".
	IOFullSec  float64 `json:"io_full_sec,omitempty"`
	CPUFullSec float64 `json:"cpu_full_sec,omitempty"`

	StatOK    bool    `json:"stat_ok"`
	IOWaitPct float64 `json:"iowait_pct,omitempty"`
	StealPct  float64 `json:"steal_pct,omitempty"`

	DiskOK       bool    `json:"disk_ok"`
	DiskDevice   string  `json:"disk_device,omitempty"`
	DiskUtilPct  float64 `json:"disk_util_pct,omitempty"`
	DiskQueue    float64 `json:"disk_queue,omitempty"`
	DiskWriteMBs float64 `json:"disk_write_mbs,omitempty"`
	DiskReadMBs  float64 `json:"disk_read_mbs,omitempty"`

	MemOK          bool    `json:"mem_ok"`
	DirtyMB        float64 `json:"dirty_mb,omitempty"`
	WritebackMB    float64 `json:"writeback_mb,omitempty"`
	MemAvailableMB float64 `json:"mem_available_mb,omitempty"`

	SelfOK       bool    `json:"self_ok"`
	SelfWriteMBs float64 `json:"self_write_mbs,omitempty"`
	MajFlt       uint64  `json:"majflt,omitempty"`
}

// readHost takes one reading. dataDir names the directory whose device the
// disk figures should describe; an empty string, or a directory whose device
// has no diskstats line, leaves the disk group unavailable rather than
// reading a device the broker is not writing to.
func readHost(dataDir string) hostSnapshot {
	h := hostSnapshot{At: time.Now()}
	readPSI(&h)
	readStat(&h)
	readMeminfo(&h)
	readSelfIO(&h)
	readDisk(&h, dataDir)
	return h
}

func readPSI(h *hostSnapshot) {
	get := func(path string) (some, full uint64, ok bool) {
		data, err := os.ReadFile(path) //nolint:gosec // a fixed /proc path
		if err != nil {
			return 0, 0, false
		}
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) == 0 {
				continue
			}
			var target *uint64
			switch fields[0] {
			case "some":
				target = &some
			case "full":
				target = &full
			default:
				continue
			}
			for _, f := range fields[1:] {
				if v, found := strings.CutPrefix(f, "total="); found {
					n, err := strconv.ParseUint(v, 10, 64)
					if err == nil {
						*target = n
						ok = true
					}
				}
			}
		}
		return some, full, ok
	}
	cs, cf, ok1 := get("/proc/pressure/cpu")
	is, iff, ok2 := get("/proc/pressure/io")
	ms, mf, ok3 := get("/proc/pressure/memory")
	// Each file is parsed independently; a kernel that omits one still
	// reports the others. PSI is unavailable only if none of them read.
	h.PSIOK = ok1 || ok2 || ok3
	h.CPUSome, h.CPUFull = cs, cf
	h.IOSome, h.IOFull = is, iff
	h.MemSome, h.MemFull = ms, mf
}

func readStat(h *hostSnapshot) {
	data, err := os.ReadFile("/proc/stat") //nolint:gosec // a fixed /proc path
	if err != nil {
		return
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 9 || fields[0] != "cpu" {
			continue
		}
		var total uint64
		for i, f := range fields[1:] {
			n, err := strconv.ParseUint(f, 10, 64)
			if err != nil {
				return
			}
			total += n
			switch i {
			case 4:
				h.IOWait = n
			case 7:
				h.Steal = n
			}
		}
		h.Total, h.StatOK = total, true
		return
	}
}

func readMeminfo(h *hostSnapshot) {
	data, err := os.ReadFile("/proc/meminfo") //nolint:gosec // a fixed /proc path
	if err != nil {
		return
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		n, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		switch fields[0] {
		case "MemAvailable:":
			h.MemAvailableKB, h.MemOK = n, true
		case "Dirty:":
			h.DirtyKB = n
		case "Writeback:":
			h.WritebackKB = n
		}
	}
}

func readSelfIO(h *hostSnapshot) {
	data, err := os.ReadFile("/proc/self/io") //nolint:gosec // a fixed /proc path
	if err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) != 2 {
				continue
			}
			n, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				continue
			}
			switch fields[0] {
			case "read_bytes:":
				h.SelfReadB, h.SelfOK = n, true
			case "write_bytes:":
				h.SelfWriteB = n
			}
		}
	}
	if data, err := os.ReadFile("/proc/self/stat"); err == nil { //nolint:gosec // a fixed /proc path
		// Field 12 (1-based) is majflt. The comm field can contain spaces
		// and parentheses, so the fields are counted from the closing one.
		if i := strings.LastIndex(string(data), ") "); i >= 0 {
			rest := strings.Fields(string(data)[i+2:])
			if len(rest) > 9 {
				if n, err := strconv.ParseUint(rest[9], 10, 64); err == nil {
					h.MajFlt = n
				}
			}
		}
	}
}

// deviceOf resolves the block device backing a directory, as "major:minor".
// The encoding is glibc's: this is decoded here because golang.org/x/sys is
// not a dependency of this module.
func deviceOf(dir string) (major, minor uint32, ok bool) {
	var st syscall.Stat_t
	if err := syscall.Stat(dir, &st); err != nil {
		return 0, 0, false
	}
	dev := uint64(st.Dev) //nolint:unconvert // Dev is not uint64 on every GOARCH
	major = uint32((dev >> 8) & 0xfff)
	minor = uint32((dev & 0xff) | ((dev >> 12) & 0xfff00))
	return major, minor, true
}

func readDisk(h *hostSnapshot, dataDir string) {
	if dataDir == "" {
		return
	}
	major, minor, ok := deviceOf(dataDir)
	if !ok {
		return
	}
	data, err := os.ReadFile("/proc/diskstats") //nolint:gosec // a fixed /proc path
	if err != nil {
		return
	}
	for _, line := range strings.Split(string(data), "\n") {
		f := strings.Fields(line)
		if len(f) < 14 {
			continue
		}
		maj, err1 := strconv.ParseUint(f[0], 10, 32)
		min, err2 := strconv.ParseUint(f[1], 10, 32)
		if err1 != nil || err2 != nil || uint32(maj) != major || uint32(min) != minor {
			continue
		}
		n := func(i int) uint64 {
			v, _ := strconv.ParseUint(f[i], 10, 64)
			return v
		}
		h.DiskDevice = f[2]
		h.ReadIOs, h.ReadSectors = n(3), n(5)
		h.WriteIOs, h.WriteSectors = n(7), n(9)
		h.IOTicks, h.WeightedIOTicks = n(12), n(13)
		h.DiskOK = true
		return
	}
}

// hostInterval is the delta between two readings. A counter that went
// backwards — a wrap, or a device replaced under us — makes its group
// unavailable rather than negative.
func hostInterval(prev, cur hostSnapshot) hostDelta {
	var d hostDelta
	secs := cur.At.Sub(prev.At).Seconds()
	if secs <= 0 {
		return d
	}
	pct := func(deltaMicros uint64) float64 { return float64(deltaMicros) / 1e6 / secs * 100 }
	sub := func(a, b uint64) (uint64, bool) {
		if a < b {
			return 0, false
		}
		return a - b, true
	}

	if prev.PSIOK && cur.PSIOK {
		cs, ok1 := sub(cur.CPUSome, prev.CPUSome)
		cf, ok2 := sub(cur.CPUFull, prev.CPUFull)
		is, ok3 := sub(cur.IOSome, prev.IOSome)
		iff, ok4 := sub(cur.IOFull, prev.IOFull)
		ms, ok5 := sub(cur.MemSome, prev.MemSome)
		mf, ok6 := sub(cur.MemFull, prev.MemFull)
		if ok1 && ok2 && ok3 && ok4 && ok5 && ok6 {
			d.PSIOK = true
			d.CPUSomePct, d.CPUFullPct = pct(cs), pct(cf)
			d.IOSomePct, d.IOFullPct = pct(is), pct(iff)
			d.MemSomePct, d.MemFullPct = pct(ms), pct(mf)
			d.IOFullSec, d.CPUFullSec = float64(iff)/1e6, float64(cf)/1e6
		}
	}
	if prev.StatOK && cur.StatOK {
		iw, ok1 := sub(cur.IOWait, prev.IOWait)
		st, ok2 := sub(cur.Steal, prev.Steal)
		tot, ok3 := sub(cur.Total, prev.Total)
		if ok1 && ok2 && ok3 && tot > 0 {
			d.StatOK = true
			d.IOWaitPct = float64(iw) / float64(tot) * 100
			d.StealPct = float64(st) / float64(tot) * 100
		}
	}
	if prev.DiskOK && cur.DiskOK {
		ticks, ok1 := sub(cur.IOTicks, prev.IOTicks)
		weighted, ok2 := sub(cur.WeightedIOTicks, prev.WeightedIOTicks)
		ws, ok3 := sub(cur.WriteSectors, prev.WriteSectors)
		rs, ok4 := sub(cur.ReadSectors, prev.ReadSectors)
		if ok1 && ok2 && ok3 && ok4 {
			d.DiskOK, d.DiskDevice = true, cur.DiskDevice
			d.DiskUtilPct = float64(ticks) / 1000 / secs * 100
			// Weighted ticks are queue-depth-milliseconds, so dividing by the
			// interval gives the average number of requests in flight.
			d.DiskQueue = float64(weighted) / 1000 / secs
			d.DiskWriteMBs = float64(ws) * 512 / (1 << 20) / secs
			d.DiskReadMBs = float64(rs) * 512 / (1 << 20) / secs
		}
	}
	if cur.MemOK {
		d.MemOK = true
		d.DirtyMB = float64(cur.DirtyKB) / 1024
		d.WritebackMB = float64(cur.WritebackKB) / 1024
		d.MemAvailableMB = float64(cur.MemAvailableKB) / 1024
	}
	if prev.SelfOK && cur.SelfOK {
		if w, ok := sub(cur.SelfWriteB, prev.SelfWriteB); ok {
			d.SelfOK = true
			d.SelfWriteMBs = float64(w) / (1 << 20) / secs
		}
		if m, ok := sub(cur.MajFlt, prev.MajFlt); ok {
			d.MajFlt = m
		}
	}
	return d
}

// suffix is what the progress line carries. It leads with the two figures
// that answer the standing question — how much of this interval was the
// machine stalled on io, and how much on cpu — because those are directly
// comparable with the stall observer's "how long the process was not
// running", which until now had no such counterpart.
func (d hostDelta) suffix() string {
	if !d.PSIOK && !d.DiskOK && !d.StatOK {
		return " | host=unavailable"
	}
	out := " |"
	if d.PSIOK {
		out += fmt.Sprintf(" psi io=%.0f/%.0f%% cpu=%.0f/%.0f%% mem=%.0f/%.0f%%",
			d.IOSomePct, d.IOFullPct, d.CPUSomePct, d.CPUFullPct, d.MemSomePct, d.MemFullPct)
	} else {
		out += " psi=unavailable"
	}
	if d.StatOK {
		out += fmt.Sprintf(" iowait=%.0f%% steal=%.0f%%", d.IOWaitPct, d.StealPct)
	}
	if d.DiskOK {
		out += fmt.Sprintf(" %s util=%.0f%% qd=%.1f w=%.0fMB/s", d.DiskDevice, d.DiskUtilPct, d.DiskQueue, d.DiskWriteMBs)
	} else {
		out += " disk=unavailable"
	}
	if d.SelfOK {
		out += fmt.Sprintf(" self-w=%.0fMB/s", d.SelfWriteMBs)
	}
	if d.MemOK {
		out += fmt.Sprintf(" dirty=%.0fMB wb=%.0fMB", d.DirtyMB, d.WritebackMB)
	}
	if d.MajFlt > 0 {
		out += fmt.Sprintf(" majflt=%d", d.MajFlt)
	}
	return out
}
