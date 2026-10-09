package broker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// usersNode starts a node authenticating from a users file with body, with
// mutate applied to its configuration.
func usersNode(t *testing.T, body string, mutate func(*config.Document)) *fixture {
	t.Helper()
	users := filepath.Join(t.TempDir(), "users.json")
	if err := os.WriteFile(users, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return start(t, func(d *config.Document) {
		d.Auth.Anonymous = false
		d.Modules["auth.file"] = json.RawMessage(`{"path":"` + users + `"}`)
		if mutate != nil {
			mutate(d)
		}
	})
}

func login(f *fixture, user, password string) packet.ReasonCode {
	_, ack := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "c-" + user, CleanStart: true, HasUsername: true, Username: user,
		HasPassword: true, Password: []byte(password)}, false)
	return ack.ReasonCode
}

// TestArgon2idUserLogsIn: an Argon2id user and a PBKDF2 user in one users
// file each connect with their password, and not with a wrong one.
func TestArgon2idUserLogsIn(t *testing.T) {
	cheap, err := auth.HashArgon2id([]byte("argon-pw"), auth.Argon2idParams{MemoryKiB: 64, Time: 1, Lanes: 1})
	if err != nil {
		t.Fatal(err)
	}
	pbkdf2, err := auth.HashPassword([]byte("pbkdf2-pw"))
	if err != nil {
		t.Fatal(err)
	}
	f := usersNode(t, `{"users":[{"username":"a","password":"`+cheap+`"},{"username":"p","password":"`+pbkdf2+`"}]}`, nil)
	for _, c := range []struct {
		user, password string
		want           packet.ReasonCode
	}{
		{"a", "argon-pw", packet.Success},
		{"a", "wrong", packet.BadUserNameOrPassword},
		{"p", "pbkdf2-pw", packet.Success},
	} {
		if got := login(f, c.user, c.password); got != c.want {
			t.Errorf("%s with %q: %s, want %s", c.user, c.password, got, c.want)
		}
	}
}

// TestArgon2idBudgetFromConfig: the configured budget is the one verifications
// hold: a 2 MiB hash under a 1 MiB budget is refused, where the default
// budget would take it.
func TestArgon2idBudgetFromConfig(t *testing.T) {
	t.Cleanup(func() { auth.SetArgon2MemoryBytes(auth.DefaultArgon2MemoryBytes) })
	costly, err := auth.HashArgon2id([]byte("costly-pw"), auth.Argon2idParams{MemoryKiB: 2048, Time: 1, Lanes: 1})
	if err != nil {
		t.Fatal(err)
	}
	f := usersNode(t, `{"users":[{"username":"c","password":"`+costly+`"}]}`, func(d *config.Document) { d.Auth.Argon2MemoryBytes = 1 << 20 })
	if got := login(f, "c", "costly-pw"); got != packet.BadUserNameOrPassword {
		t.Errorf("a 2 MiB hash under a 1 MiB budget: %s, want refused", got)
	}
}
