package dcs

import (
	"bytes"
	"strings"
	"testing"
)

// FuzzSealedOpen feeds arbitrary stored values to the weft-seal:v1: parser. It
// must never panic, never accept what it did not seal, and never echo the
// stored body in its error. The committed corpus lives in testdata/fuzz.
func FuzzSealedOpen(f *testing.F) {
	key := strings.Repeat("k", 32)
	s, err := NewSealedStore(NewMemStore(), key)
	if err != nil {
		f.Fatal(err)
	}
	const p = "/weft/x/secrets/a"
	// A fixed nonce keeps `good` identical in the fuzzing worker processes,
	// which re-run this function.
	old := randReader
	randReader = bytes.NewReader(make([]byte, 12))
	good, err := s.current.seal(p, "plain")
	randReader = old
	if err != nil {
		f.Fatal(err)
	}
	f.Add(p, good)
	f.Add(p, sealPrefix)
	f.Add(p, sealPrefix+":")
	f.Add(p, sealPrefix+s.current.id+":")
	f.Add(p, sealPrefix+s.current.id+":!!!")
	f.Add(p, sealPrefix+"deadbeef:AAAA")
	f.Add("/other", good) // sealed for another path
	f.Add(p, "")
	f.Fuzz(func(t *testing.T, path, stored string) {
		plain, kid, err := s.open(path, stored)
		if err == nil {
			// Only a value this store sealed for this very path can open (the
			// caller checks the prefix before open, which tolerates its absence).
			if strings.TrimPrefix(stored, sealPrefix) != strings.TrimPrefix(good, sealPrefix) || path != p || plain != "plain" || kid != s.current.id {
				t.Fatalf("accepted %q at %q as %q", stored, path, plain)
			}
			return
		}
		// The key id is deliberately named in the error; the body never is.
		kid, body, _ := strings.Cut(strings.TrimPrefix(stored, sealPrefix), ":")
		if len(body) > 16 && !strings.Contains(kid, body) && !strings.Contains(path, body) && strings.Contains(err.Error(), body) {
			t.Fatalf("error echoes the sealed body: %v", err)
		}
	})
}

// Go's base64 decoder skips CR and LF; the parser must not, or one ciphertext
// has many stored spellings.
func TestOpenRefusesANonCanonicalEncoding(t *testing.T) {
	s, err := NewSealedStore(NewMemStore(), strings.Repeat("k", 32))
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := s.current.seal("/p", "plain")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.open("/p", sealed); err != nil {
		t.Fatal(err)
	}
	cut := len(sealPrefix) + len(s.current.id) + 1 + 5
	for _, ws := range []string{"\r", "\n", "\r\n"} {
		if _, _, err := s.open("/p", sealed[:cut]+ws+sealed[cut:]); err == nil {
			t.Errorf("accepted %q inside the body", ws)
		}
	}
}

func TestSealSealOpenRoundTripAnyPlaintext(t *testing.T) {
	s, err := NewSealedStore(NewMemStore(), strings.Repeat("k", 32))
	if err != nil {
		t.Fatal(err)
	}
	for _, plain := range []string{"", "a", "héllo\x00", strings.Repeat("z", 5000)} {
		sealed, err := s.current.seal("/p", plain)
		if err != nil {
			t.Fatal(err)
		}
		got, _, err := s.open("/p", sealed)
		if err != nil || got != plain {
			t.Fatalf("%q: got %q, %v", plain, got, err)
		}
	}
}

func TestBootstrapLockPathUsesTheNamespace(t *testing.T) {
	e, err := NewEtcdStore(nil, "/weft/forgejo/inst", 15, EtcdSecurity{})
	if err != nil {
		t.Fatal(err)
	}
	if got := e.bootstrapLockPath(); got != "/weft/forgejo/inst/bootstrap-lock" {
		t.Fatalf("lock path = %q", got)
	}
}
