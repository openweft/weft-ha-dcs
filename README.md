# weft-ha-dcs

The etcd client shared by the `weft-ha-*` operators whose replicas only need a
shared-secret store and a bootstrap lock (`weft-ha-forgejo`, `weft-ha-irods`).
It exists because both carried byte-identical copies of this code.

```go
import "github.com/openweft/weft-ha-dcs"   // package dcs

etcd, err := dcs.NewEtcdStore(endpoints, "/weft/forgejo/"+install, 15, dcs.EtcdSecurity{
    CAFile: ca, CertFile: cert, KeyPEM: keyPEM, Username: user, Password: pw,
})
store, err := dcs.NewSealedStore(etcd, sealKey, oldKeys...) // secrets sealed in etcd
```

- `Store`, `MemStore` (tests and single-node), `EtcdStore` (CAS key store and
  lease-bound bootstrap lock at `<namespace>/bootstrap-lock`).
- `EtcdSecurity`: TLS, mutual TLS and etcd user authentication; a plaintext
  endpoint is refused unless `AllowPlaintext` is set.
- `SealedStore`: AES-256-GCM, key derived with HKDF-SHA256 from a cluster key,
  value format `weft-seal:v1:<key id>:<base64url(nonce || ciphertext)>`, the etcd
  path as additional data, rotation through previous keys, legacy plaintext
  migrated by compare-and-swap then compacted. The `weft-seal:v1:` parser is
  fuzzed (`FuzzSealedOpen`, corpus in `testdata/fuzz`).

Flags that carry the key, the client key and the password are registered with
`github.com/go-authn/secretsource/secretflag` (`AddFrom`, `AddFromList`, v0.12.0):
`--etcd-seal-key-from`, `--etcd-seal-old-key-from` (repeatable),
`--etcd-client-key-from`, `--etcd-password-from`. No plain twin exists, so no
secret shows in `ps`.

Licence: BSD-3-Clause.
