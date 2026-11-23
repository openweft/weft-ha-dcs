# Changelog

## v0.2.0

- Added the optional `Deleter` interface (`DeleteKey(ctx, path) (bool, error)`),
  implemented by `MemStore`, `EtcdStore` (one Txn: delete only if the key
  exists) and `SealedStore`. `SealedStore.DeleteKey` removes a sealed or legacy
  plaintext value without opening it, then compacts the key history. Old
  revisions leave the disk only after a defragmentation of the etcd members.

## v0.1.0

- First release: `internal/dcs` of weft-ha-forgejo v0.8.0 and weft-ha-irods
  v0.7.0 extracted as one module. `NewEtcdStore` takes the full key namespace
  (e.g. `/weft/forgejo/<install>`) instead of an install or zone name.
- Added a fuzz test of the `weft-seal:v1:` parser with a committed corpus.
