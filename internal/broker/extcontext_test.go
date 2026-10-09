package broker

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/config"
)

// TestModuleContextPasswords: a module verifies a stored credential through
// its context the way the built-in authenticators do. Serving, the
// verification goes through the node's cache, so a repeat is a hit; at
// hoardmq check the verifier still verifies, caches nothing, and the context
// carries no services.
func TestModuleContextPasswords(t *testing.T) {
	phc, err := auth.HashPassword([]byte("pw"))
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.Default()

	cache := auth.NewVerifyCache(time.Minute, 10)
	serving := moduleContext(cfg, log, services{}, Hooks{}, cache)
	for i := 0; i < 2; i++ {
		if ok, err := serving.Passwords.Verify(phc, []byte("pw")); !ok || err != nil {
			t.Fatalf("serving verification %d: %v, %v", i, ok, err)
		}
	}
	if ok, _ := serving.Passwords.Verify(phc, []byte("wrong")); ok {
		t.Fatal("a wrong password verified")
	}
	if st := cache.Stats(); st.Hits != 1 || st.Derivations != 2 {
		t.Fatalf("cache stats %+v, want 1 hit and 2 derivations", st)
	}

	checking := moduleContext(cfg, log, nil, Hooks{}, nil)
	if checking.Passwords == nil {
		t.Fatal("hoardmq check's context has no password verifier")
	}
	if ok, err := checking.Passwords.Verify(phc, []byte("pw")); !ok || err != nil {
		t.Fatalf("check-time verification: %v, %v", ok, err)
	}
	if checking.Services != nil {
		t.Fatal("hoardmq check's context carries services")
	}
}
