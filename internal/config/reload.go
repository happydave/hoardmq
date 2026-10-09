package config

import (
	"encoding/json"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// reloadable are the members a running node puts into force on a reload.
// Every other member is read once, into a connection, a socket, the store or
// a module, and changes only with a restart.
var reloadable = map[string]bool{
	"/log/level":                 true,
	"/limits/clientMessageRate":  true,
	"/limits/clientMessageBurst": true,
	"/limits/clientByteRate":     true,
	"/limits/clientByteBurst":    true,
	"/limits/connectRate":        true,
	"/limits/connectBurst":       true,
	"/limits/maxQueuedMessages":  true,
	"/limits/queueDrop":          true,
	"/limits/maxSessionExpiry":   true,
	"/overload/queuedMessages":   true,
	"/overload/replicationLag":   true,
	"/overload/memoryBytes":      true,
	"/flapping/enabled":          true,
	"/flapping/windowSeconds":    true,
	"/flapping/maxCount":         true,
	"/flapping/banSeconds":       true,
}

// A listener's own limits are reloadable while the listener at that index is
// the same one: a new name, type or address is a restart-only change of its
// own, and an added or removed listener changes the whole element.
var reloadableListener = regexp.MustCompile(`^/listeners/[0-9]+/(connectRate|connectBurst|maxConnections)$`)

// Reloadable reports whether a change at pointer can be put into force by a
// reload.
func Reloadable(pointer string) bool {
	return reloadable[pointer] || reloadableListener.MatchString(pointer)
}

// Changes compares the document in force with a new one and returns the JSON
// pointer of every member that differs, and of those the ones only a restart
// can put into force. Both lists are sorted.
func Changes(running, next Document) (changed, restartOnly []string) {
	a, b := tree(running), tree(next)
	diff(a, b, "", &changed)
	sort.Strings(changed)
	for _, p := range changed {
		if !Reloadable(p) {
			restartOnly = append(restartOnly, p)
		}
	}
	return changed, restartOnly
}

func tree(d Document) any {
	data, err := json.Marshal(d)
	if err != nil {
		panic(err) // a Document always marshals
	}
	var t any
	if err := json.Unmarshal(data, &t); err != nil {
		panic(err)
	}
	return t
}

// diff appends the pointer of every leaf, or whole element present on one
// side only, at which a and b differ.
func diff(a, b any, at string, out *[]string) {
	am, aok := a.(map[string]any)
	bm, bok := b.(map[string]any)
	if aok && bok {
		keys := map[string]bool{}
		for k := range am {
			keys[k] = true
		}
		for k := range bm {
			keys[k] = true
		}
		for k := range keys {
			av, ain := am[k]
			bv, bin := bm[k]
			p := at + "/" + escape(k)
			if ain != bin {
				*out = append(*out, p)
				continue
			}
			diff(av, bv, p, out)
		}
		return
	}
	as, aok := a.([]any)
	bs, bok := b.([]any)
	if aok && bok {
		for i := 0; i < len(as) || i < len(bs); i++ {
			p := at + "/" + strconv.Itoa(i)
			if i >= len(as) || i >= len(bs) {
				*out = append(*out, p)
				continue
			}
			diff(as[i], bs[i], p, out)
		}
		return
	}
	if !reflect.DeepEqual(a, b) {
		if at == "" {
			at = "/"
		}
		*out = append(*out, at)
	}
}

// escape is RFC 6901's escaping of one reference token.
func escape(token string) string {
	return strings.ReplaceAll(strings.ReplaceAll(token, "~", "~0"), "/", "~1")
}
