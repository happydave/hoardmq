package auth

import (
	"errors"
	"fmt"
	"sync"
)

// memoryBudget is the memory the node's Argon2id verifications may hold at
// once. A verification reserves its cost before deriving and releases it
// after; a cost over the whole budget is refused rather than waited for.
type memoryBudget struct {
	mu    sync.Mutex
	freed *sync.Cond
	total int64
	used  int64
}

func newMemoryBudget(total int64) *memoryBudget {
	b := &memoryBudget{total: total}
	b.freed = sync.NewCond(&b.mu)
	return b
}

// DefaultArgon2MemoryBytes is the budget when none is configured: four
// verifications at the default cost of 64 MiB.
const DefaultArgon2MemoryBytes = 256 << 20

var argon2Budget = newMemoryBudget(DefaultArgon2MemoryBytes)

// SetArgon2MemoryBytes sets the node's budget for Argon2id verifications
// held at once. The broker calls it before it serves.
func SetArgon2MemoryBytes(n int64) {
	b := argon2Budget
	b.mu.Lock()
	b.total = n
	b.mu.Unlock()
	b.freed.Broadcast()
}

// ErrArgon2Budget is a hash whose memory cost exceeds the node's budget.
var ErrArgon2Budget = errors.New("the hash's memory cost exceeds auth.argon2MemoryBytes; it cannot be verified on this node")

// reserve waits until n bytes fit within the budget and takes them.
func (b *memoryBudget) reserve(n int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for {
		if n > b.total {
			return fmt.Errorf("%w (%d bytes needed, %d allowed)", ErrArgon2Budget, n, b.total)
		}
		if b.used+n <= b.total {
			b.used += n
			return nil
		}
		b.freed.Wait()
	}
}

func (b *memoryBudget) release(n int64) {
	b.mu.Lock()
	b.used -= n
	b.mu.Unlock()
	b.freed.Broadcast()
}
