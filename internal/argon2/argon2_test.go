//go:build !fips

package argon2

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func unhex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestBlake2b: RFC 7693 Appendix A's digest of "abc", and the reference
// implementation's digests of "" and of "abc" at 32 bytes (golang.org/x/crypto
// v0.57.0, computed outside this module).
func TestBlake2b(t *testing.T) {
	for _, c := range []struct {
		in   string
		size int
		want string
	}{
		{"abc", 64, "ba80a53f981c4d0d6a2797b69f12f6e94c212f14685ac4b74b12bb6fdbffa2d17d87c5392aab792dc252d5de4533cc9518d38aa8dbf1925ab92386edd4009923"},
		{"", 64, "786a02f742015903c6c6fd852552d272912f4740e15847618a86e217f71f5419d25e1031afee585313896444934eb04b903a685b1448b755d56f701afe9be2ce"},
		{"abc", 32, "bddd813c634239723171ef3fee98579b94964e3bb1cb3e427262c8c068d52319"},
	} {
		if got := blake2bSum(c.size, []byte(c.in)); !bytes.Equal(got, unhex(t, c.want)) {
			t.Errorf("BLAKE2b-%d(%q) = %x, want %s", 8*c.size, c.in, got, c.want)
		}
	}
	// A message over one block, written in pieces, is the same as written whole.
	long := bytes.Repeat([]byte("0123456789"), 40)
	d := newBlake2b(64)
	for i := 0; i < len(long); i += 7 {
		d.write(long[i:min(i+7, len(long))])
	}
	if !bytes.Equal(d.sum(), blake2bSum(64, long)) {
		t.Error("a message written in pieces hashes differently from the same message written whole")
	}
}

// RFC 9106 § 5's inputs: every Argon2 type from the same password, salt,
// secret and associated data, t=3, m=32, p=4, a 32-byte tag.
var (
	rfcPassword = bytes.Repeat([]byte{0x01}, 32)
	rfcSalt     = bytes.Repeat([]byte{0x02}, 16)
	rfcSecret   = bytes.Repeat([]byte{0x03}, 8)
	rfcData     = bytes.Repeat([]byte{0x04}, 12)
)

// TestRFC9106: the RFC's tag for each type. The three differ only in how a
// block's reference is chosen, so matching each one holds that choice.
func TestRFC9106(t *testing.T) {
	for _, c := range []struct {
		name string
		mode int
		want string
	}{
		{"argon2d", argon2d, "512b391b6f1162975371d30919734294f868e3be3984f3c1a13a4db9fabe4acb"},
		{"argon2i", argon2i, "c814d9d1dc7f37aa13f0d77f2494bda1c8de6b016dd388d29952a4c4672b6ce8"},
		{"argon2id", argon2id, "0d640df58d78766c08c037a34a8b53c9d01ef0452d75b65eb52520e96b01e659"},
	} {
		got := derive(c.mode, rfcPassword, rfcSalt, rfcSecret, rfcData, 3, 32, 4, 32)
		if !bytes.Equal(got, unhex(t, c.want)) {
			t.Errorf("%s: %x, want %s", c.name, got, c.want)
		}
	}
}

// TestReferenceVectors: the reference implementation's tags for password
// "password" and salt "somesalt", 24 bytes, across passes, memory and lanes,
// as golang.org/x/crypto's argon2 test lists them. Three and six lanes do not
// divide 64 and 1024 KiB evenly, so memory is rounded down; t=1 runs only the
// first pass, whose first half Argon2id addresses independently of the data.
func TestReferenceVectors(t *testing.T) {
	for _, v := range []struct {
		mode             int
		time, mem, lanes uint32
		hash             string
	}{
		{argon2i, 1, 64, 1, "b9c401d1844a67d50eae3967dc28870b22e508092e861a37"},
		{argon2d, 1, 64, 1, "8727405fd07c32c78d64f547f24150d3f2e703a89f981a19"},
		{argon2id, 1, 64, 1, "655ad15eac652dc59f7170a7332bf49b8469be1fdb9c28bb"},
		{argon2i, 2, 64, 1, "8cf3d8f76a6617afe35fac48eb0b7433a9a670ca4a07ed64"},
		{argon2d, 2, 64, 1, "3be9ec79a69b75d3752acb59a1fbb8b295a46529c48fbb75"},
		{argon2id, 2, 64, 1, "068d62b26455936aa6ebe60060b0a65870dbfa3ddf8d41f7"},
		{argon2i, 2, 64, 2, "2089f3e78a799720f80af806553128f29b132cafe40d059f"},
		{argon2d, 2, 64, 2, "68e2462c98b8bc6bb60ec68db418ae2c9ed24fc6748a40e9"},
		{argon2id, 2, 64, 2, "350ac37222f436ccb5c0972f1ebd3bf6b958bf2071841362"},
		{argon2i, 3, 256, 2, "f5bbf5d4c3836af13193053155b73ec7476a6a2eb93fd5e6"},
		{argon2d, 3, 256, 2, "f4f0669218eaf3641f39cc97efb915721102f4b128211ef2"},
		{argon2id, 3, 256, 2, "4668d30ac4187e6878eedeacf0fd83c5a0a30db2cc16ef0b"},
		{argon2i, 4, 4096, 4, "a11f7b7f3f93f02ad4bddb59ab62d121e278369288a0d0e7"},
		{argon2d, 4, 4096, 4, "935598181aa8dc2b720914aa6435ac8d3e3a4210c5b0fb2d"},
		{argon2id, 4, 4096, 4, "145db9733a9f4ee43edf33c509be96b934d505a4efb33c5a"},
		{argon2i, 4, 1024, 8, "0cdd3956aa35e6b475a7b0c63488822f774f15b43f6e6e17"},
		{argon2d, 4, 1024, 8, "83604fc2ad0589b9d055578f4d3cc55bc616df3578a896e9"},
		{argon2id, 4, 1024, 8, "8dafa8e004f8ea96bf7c0f93eecf67a6047476143d15577f"},
		{argon2i, 2, 64, 3, "5cab452fe6b8479c8661def8cd703b611a3905a6d5477fe6"},
		{argon2d, 2, 64, 3, "22474a423bda2ccd36ec9afd5119e5c8949798cadf659f51"},
		{argon2id, 2, 64, 3, "4a15b31aec7c2590b87d1f520be7d96f56658172deaa3079"},
		{argon2i, 3, 1024, 6, "d236b29c2b2a09babee842b0dec6aa1e83ccbdea8023dced"},
		{argon2d, 3, 1024, 6, "a3351b0319a53229152023d9206902f4ef59661cdca89481"},
		{argon2id, 3, 1024, 6, "1640b932f4b60e272f5d2207b9a9c626ffa1bd88d2349016"},
	} {
		want := unhex(t, v.hash)
		got := derive(v.mode, []byte("password"), []byte("somesalt"), nil, nil, v.time, v.mem, v.lanes, uint32(len(want)))
		if !bytes.Equal(got, want) {
			t.Errorf("type %d t=%d m=%d p=%d: %x, want %s", v.mode, v.time, v.mem, v.lanes, got, v.hash)
		}
	}
}
