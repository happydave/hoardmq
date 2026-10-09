package config

import (
	"encoding/json"
	"reflect"
	"testing"
)

// TestChangesClassifiesEachMember: a reload puts the safe set into force and
// refuses every other change, named by its pointer.
func TestChangesClassifiesEachMember(t *testing.T) {
	base := func() Document {
		d := Default()
		d.Node.DataDir = "/data"
		d.Listeners = []Listener{{Name: "tcp", Type: "tcp", Address: "127.0.0.1:1883"}}
		d.Modules = map[string]json.RawMessage{"auth.file": json.RawMessage(`{"path":"/etc/users"}`)}
		return d
	}
	for _, c := range []struct {
		name        string
		mutate      func(*Document)
		changed     []string
		restartOnly []string
	}{
		{"identical", func(*Document) {}, nil, nil},
		{"log level", func(d *Document) { d.Log.Level = "debug" }, []string{"/log/level"}, nil},
		{"log format", func(d *Document) { d.Log.Format = "json" }, []string{"/log/format"}, []string{"/log/format"}},
		{"listener rate", func(d *Document) { d.Listeners[0].ConnectRate = 50 }, []string{"/listeners/0/connectRate"}, nil},
		{"listener address", func(d *Document) { d.Listeners[0].Address = "127.0.0.1:1884" },
			[]string{"/listeners/0/address"}, []string{"/listeners/0/address"}},
		{"listener added", func(d *Document) {
			d.Listeners = append(d.Listeners, Listener{Name: "ws", Type: "ws", Address: "127.0.0.1:8080"})
		}, []string{"/listeners/1"}, []string{"/listeners/1"}},
		{"packet size", func(d *Document) { d.Limits.MaxPacketSize = 2048 },
			[]string{"/limits/maxPacketSize"}, []string{"/limits/maxPacketSize"}},
		{"queue bound", func(d *Document) { d.Limits.MaxQueuedMessages = 3 }, []string{"/limits/maxQueuedMessages"}, nil},
		{"module member", func(d *Document) { d.Modules["auth.file"] = json.RawMessage(`{"path":"/etc/other"}`) },
			[]string{"/modules/auth.file/path"}, []string{"/modules/auth.file/path"}},
		{"mixed", func(d *Document) { d.Log.Level = "debug"; d.Cluster.Replicas = 3 },
			[]string{"/cluster/replicas", "/log/level"}, []string{"/cluster/replicas"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			next := base()
			c.mutate(&next)
			changed, restartOnly := Changes(base(), next)
			if !reflect.DeepEqual(changed, c.changed) || !reflect.DeepEqual(restartOnly, c.restartOnly) {
				t.Fatalf("changed %q, restart-only %q; want %q and %q", changed, restartOnly, c.changed, c.restartOnly)
			}
		})
	}
}

// TestPointerTokensAreEscaped: a member name holding "/" or "~" is escaped as
// RFC 6901 requires, so the pointer names one member.
func TestPointerTokensAreEscaped(t *testing.T) {
	a, b := Default(), Default()
	a.Modules = map[string]json.RawMessage{"x.y": json.RawMessage(`{"a/b~c":1}`)}
	b.Modules = map[string]json.RawMessage{"x.y": json.RawMessage(`{"a/b~c":2}`)}
	changed, _ := Changes(a, b)
	if want := []string{"/modules/x.y/a~1b~0c"}; !reflect.DeepEqual(changed, want) {
		t.Fatalf("changed %q, want %q", changed, want)
	}
}
