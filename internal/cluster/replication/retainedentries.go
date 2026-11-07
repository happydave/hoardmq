package replication

import (
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// carried reports whether an entry may travel or be installed as a copy: a
// tombstone at the zero Version would read as a copy drop, removing whatever
// its receiver holds, so it is never carried.
func carried(e store.RetainedEntry) bool { return !e.Tombstone || !e.RV.Zero() }

// localEntries is this node's own retained entries that may travel
// (carried).
func (r *Replicator) localEntries() []store.RetainedEntry {
	es := r.o.Local.RetainedEntries()
	out := es[:0:0]
	for _, e := range es {
		if carried(e) {
			out = append(out, e)
		}
	}
	return out
}

// replicaEntries lists a replica state's retained entries, messages at their
// versions and tombstones (an empty payload at a stamped version), leaving out
// a tombstone that may not travel (carried).
func replicaEntries(st *store.State) []store.RetainedEntry {
	vers := make(map[[2]string]store.Version, len(st.RetainedVersions))
	for _, v := range st.RetainedVersions {
		vers[[2]string{v.Namespace, v.Topic}] = v.RV
	}
	out := make([]store.RetainedEntry, 0, len(st.Retained)+len(st.RetainedTombstones))
	for _, m := range st.Retained {
		out = append(out, store.RetainedEntry{Msg: m, RV: vers[[2]string{m.Namespace, m.Topic}]})
	}
	for _, t := range st.RetainedTombstones {
		if t.RV.Zero() {
			continue
		}
		out = append(out, store.RetainedEntry{Msg: &message.Message{Namespace: t.Namespace, Topic: t.Topic, Retain: true}, RV: t.RV, Tombstone: true})
	}
	return out
}

// entriesInShard is the entries of one shard.
func entriesInShard(r *Replicator, es []store.RetainedEntry, shard int) []store.RetainedEntry {
	var out []store.RetainedEntry
	for _, e := range es {
		if r.o.Map.ShardOf(e.Msg.Namespace, e.Msg.Topic) == shard {
			out = append(out, e)
		}
	}
	return out
}

// entryRecord is an entry's record: a message at its version, or a clear.
func entryRecord(e store.RetainedEntry) store.Record { return store.RetainRecordVersioned(e.Msg, e.RV) }

// rvOf is a frame's version member for an entry: nil when unversioned.
func rvOf(v store.Version) *store.Version {
	if v.Zero() {
		return nil
	}
	return &v
}

// frameVersion is a frame's version, the zero Version when it carries none.
func frameVersion(fr frame) store.Version {
	if fr.RV == nil {
		return store.Version{}
	}
	return *fr.RV
}
