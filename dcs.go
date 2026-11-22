// Package dcs is the Distributed Configuration Store layer — etcd
// today, but the interface stays implementation-agnostic so the
// reconcile loop tests can swap a fake in.
//
// It is shared by the weft-ha-* operators whose replicas are stateless once
// their secrets are in place (weft-ha-forgejo, weft-ha-irods): no continuous
// leader election, two responsibilities :
//
//  1. Shared-secret store : the bootstrap leader writes its secrets once under
//     the operator's namespace (e.g. `/weft/forgejo/<install>/secrets/...`) ;
//     the other replicas read them on boot. [SealedStore] keeps them
//     encrypted in etcd, and [EtcdSecurity] secures the connection.
//  2. Bootstrap advisory lock : the first replica holds a lease-bound lock
//     and does the one-time bootstrap work ; the others wait for it to
//     release, then observe the secrets already in place.
package dcs

import (
	"context"
	"errors"
)

// Store is the minimum surface the reconcile loop needs from a
// distributed configuration store. Implementations are expected to
// be safe for concurrent use.
type Store interface {
	GetKey(ctx context.Context, path string) (string, error)
	PutKeyIfAbsent(ctx context.Context, path, value string) (bool, error)
	AcquireBootstrapLock(ctx context.Context, owner string) (release func(), err error)
	Close() error
}

// ErrNotImplemented signals that a Store method has not been wired
// for the current build.
var ErrNotImplemented = errors.New("dcs: not implemented in scaffold build")

// MemStore is a process-local stand-in for the etcd-backed store.
// Bootstrap-lock acquisition enforces local exclusion only ; key
// set/get persist for the process's lifetime.
type MemStore struct {
	keys   map[string]string
	locked bool
}

// NewMemStore returns an empty MemStore.
func NewMemStore() *MemStore { return &MemStore{keys: map[string]string{}} }

// GetKey implements Store.
func (m *MemStore) GetKey(_ context.Context, path string) (string, error) {
	return m.keys[path], nil
}

// PutKeyIfAbsent implements Store.
func (m *MemStore) PutKeyIfAbsent(_ context.Context, path, value string) (bool, error) {
	if _, ok := m.keys[path]; ok {
		return false, nil
	}
	m.keys[path] = value
	return true, nil
}

// ReplaceKey implements Rewriter.
func (m *MemStore) ReplaceKey(_ context.Context, path, prev, next string) (bool, error) {
	if cur, ok := m.keys[path]; !ok || cur != prev {
		return false, nil
	}
	m.keys[path] = next
	return true, nil
}

// Compact implements Rewriter ; a map keeps no history.
func (m *MemStore) Compact(context.Context) error { return nil }

// AcquireBootstrapLock implements Store.
func (m *MemStore) AcquireBootstrapLock(_ context.Context, _ string) (func(), error) {
	if m.locked {
		return func() {}, errors.New("memstore bootstrap lock already held in-process")
	}
	m.locked = true
	return func() { m.locked = false }, nil
}

// Close implements Store.
func (m *MemStore) Close() error { return nil }
