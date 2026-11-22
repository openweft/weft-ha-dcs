# Changelog

## v0.1.0

- First release: `internal/dcs` of weft-ha-forgejo v0.8.0 and weft-ha-irods
  v0.7.0 extracted as one module. `NewEtcdStore` takes the full key namespace
  (e.g. `/weft/forgejo/<install>`) instead of an install or zone name.
- Added a fuzz test of the `weft-seal:v1:` parser with a committed corpus.
