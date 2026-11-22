package dcs

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// memEtcd is an in-memory etcd (KV and Lease services over plaintext gRPC),
// enough for EtcdStore: ranges, transactions on value and create revision,
// deletes, compaction, leases. fail makes a method answer with a refusal.
type memEtcd struct {
	etcdserverpb.UnimplementedKVServer
	etcdserverpb.UnimplementedLeaseServer

	mu          sync.Mutex
	rev         int64
	kvs         map[string]*mvccpb.KeyValue
	fail        map[string]bool
	compactedAt int64
	grants      int
	nextLease   int64
	revoked     []int64
}

func newMemEtcd() *memEtcd {
	return &memEtcd{kvs: map[string]*mvccpb.KeyValue{}, fail: map[string]bool{}}
}

func (m *memEtcd) refuse(method string) error {
	if m.fail[method] {
		return status.Error(codes.InvalidArgument, "etcdserver: refused "+method)
	}
	return nil
}

func (m *memEtcd) setFail(method string, on bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fail[method] = on
}

func (m *memEtcd) header() *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{Revision: m.rev}
}

func (m *memEtcd) rangeLocked(r *etcdserverpb.RangeRequest) *etcdserverpb.RangeResponse {
	var kvs []*mvccpb.KeyValue
	for k, kv := range m.kvs {
		in := false
		switch {
		case len(r.RangeEnd) == 0:
			in = k == string(r.Key)
		case bytes.Equal(r.RangeEnd, []byte{0}):
			in = k >= string(r.Key)
		default:
			in = k >= string(r.Key) && k < string(r.RangeEnd)
		}
		if in {
			kvs = append(kvs, kv)
		}
	}
	sort.Slice(kvs, func(i, j int) bool {
		if r.SortTarget == etcdserverpb.RangeRequest_CREATE {
			return kvs[i].CreateRevision < kvs[j].CreateRevision
		}
		return string(kvs[i].Key) < string(kvs[j].Key)
	})
	count := int64(len(kvs))
	if r.Limit > 0 && int64(len(kvs)) > r.Limit {
		kvs = kvs[:r.Limit]
	}
	return &etcdserverpb.RangeResponse{Header: m.header(), Kvs: kvs, Count: count}
}

func (m *memEtcd) Range(_ context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.refuse("Range"); err != nil {
		return nil, err
	}
	return m.rangeLocked(r), nil
}

func (m *memEtcd) putLocked(r *etcdserverpb.PutRequest) {
	m.rev++
	k := string(r.Key)
	kv := &mvccpb.KeyValue{Key: r.Key, Value: r.Value, Lease: r.Lease, ModRevision: m.rev, CreateRevision: m.rev, Version: 1}
	if old, ok := m.kvs[k]; ok {
		kv.CreateRevision = old.CreateRevision
		kv.Version = old.Version + 1
	}
	m.kvs[k] = kv
}

func (m *memEtcd) compareOK(c *etcdserverpb.Compare) bool {
	kv := m.kvs[string(c.Key)]
	var cmp int
	switch c.Target {
	case etcdserverpb.Compare_CREATE:
		var have int64
		if kv != nil {
			have = kv.CreateRevision
		}
		want := c.GetCreateRevision()
		switch {
		case have < want:
			cmp = -1
		case have > want:
			cmp = 1
		}
	case etcdserverpb.Compare_VALUE:
		if kv == nil {
			return false // etcd: a comparison on a missing key's value fails
		}
		cmp = bytes.Compare(kv.Value, c.GetValue())
	default:
		return false
	}
	switch c.Result {
	case etcdserverpb.Compare_EQUAL:
		return cmp == 0
	case etcdserverpb.Compare_GREATER:
		return cmp > 0
	case etcdserverpb.Compare_LESS:
		return cmp < 0
	default:
		return cmp != 0
	}
}

func (m *memEtcd) Txn(_ context.Context, r *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.refuse("Txn"); err != nil {
		return nil, err
	}
	ok := true
	for _, c := range r.Compare {
		if !m.compareOK(c) {
			ok = false
		}
	}
	ops := r.Success
	if !ok {
		ops = r.Failure
	}
	var out []*etcdserverpb.ResponseOp
	for _, op := range ops {
		switch {
		case op.GetRequestPut() != nil:
			m.putLocked(op.GetRequestPut())
			out = append(out, &etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: &etcdserverpb.PutResponse{Header: m.header()}}})
		case op.GetRequestRange() != nil:
			out = append(out, &etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: m.rangeLocked(op.GetRequestRange())}})
		}
	}
	return &etcdserverpb.TxnResponse{Header: m.header(), Succeeded: ok, Responses: out}, nil
}

func (m *memEtcd) DeleteRange(_ context.Context, r *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(r.RangeEnd) != 0 {
		return nil, status.Error(codes.Unimplemented, "prefix delete")
	}
	var n int64
	if _, ok := m.kvs[string(r.Key)]; ok {
		delete(m.kvs, string(r.Key))
		m.rev++
		n = 1
	}
	return &etcdserverpb.DeleteRangeResponse{Header: m.header(), Deleted: n}, nil
}

func (m *memEtcd) Compact(_ context.Context, r *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.refuse("Compact"); err != nil {
		return nil, err
	}
	if r.Revision > m.rev {
		return nil, status.Error(codes.OutOfRange, "etcdserver: mvcc: required revision is a future revision")
	}
	m.compactedAt = r.Revision
	return &etcdserverpb.CompactionResponse{Header: m.header()}, nil
}

func (m *memEtcd) LeaseGrant(_ context.Context, r *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.refuse("LeaseGrant"); err != nil {
		return nil, err
	}
	m.grants++
	m.nextLease++
	return &etcdserverpb.LeaseGrantResponse{Header: m.header(), ID: m.nextLease, TTL: r.TTL}, nil
}

func (m *memEtcd) LeaseRevoke(_ context.Context, r *etcdserverpb.LeaseRevokeRequest) (*etcdserverpb.LeaseRevokeResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.revoked = append(m.revoked, r.ID)
	for k, kv := range m.kvs {
		if kv.Lease == r.ID {
			delete(m.kvs, k)
		}
	}
	m.rev++
	return &etcdserverpb.LeaseRevokeResponse{Header: m.header()}, nil
}

func (m *memEtcd) LeaseKeepAlive(stream grpc.BidiStreamingServer[etcdserverpb.LeaseKeepAliveRequest, etcdserverpb.LeaseKeepAliveResponse]) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return nil
		}
		m.mu.Lock()
		h := m.header()
		m.mu.Unlock()
		if err := stream.Send(&etcdserverpb.LeaseKeepAliveResponse{Header: h, ID: req.ID, TTL: 15}); err != nil {
			return err
		}
	}
}

func (m *memEtcd) value(key string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	kv, ok := m.kvs[key]
	if !ok {
		return "", false
	}
	return string(kv.Value), true
}

// fakeStore serves m and returns an EtcdStore dialled to it, in namespace /ns.
func fakeStore(t *testing.T, m *memEtcd) *EtcdStore {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	etcdserverpb.RegisterKVServer(srv, m)
	etcdserverpb.RegisterLeaseServer(srv, m)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	e, err := NewEtcdStore([]string{"http://" + lis.Addr().String()}, "/ns", 15, EtcdSecurity{AllowPlaintext: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func tctx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestNewEtcdStoreRefusesAPlaintextEndpoint(t *testing.T) {
	_, err := NewEtcdStore([]string{"http://127.0.0.1:2379"}, "/ns", 15, EtcdSecurity{})
	if err == nil || !strings.Contains(err.Error(), "plaintext") {
		t.Fatalf("err = %v", err)
	}
}

func TestEtcdStoreGetAndPutIfAbsent(t *testing.T) {
	m := newMemEtcd()
	e := fakeStore(t, m)
	ctx := tctx(t)
	if v, err := e.GetKey(ctx, "/ns/a"); err != nil || v != "" {
		t.Fatalf("absent: (%q, %v)", v, err)
	}
	if ok, err := e.PutKeyIfAbsent(ctx, "/ns/a", "one"); err != nil || !ok {
		t.Fatalf("first put: (%v, %v)", ok, err)
	}
	if ok, err := e.PutKeyIfAbsent(ctx, "/ns/a", "two"); err != nil || ok {
		t.Fatalf("second put: (%v, %v)", ok, err)
	}
	if v, err := e.GetKey(ctx, "/ns/a"); err != nil || v != "one" {
		t.Fatalf("present: (%q, %v)", v, err)
	}
	if m.grants != 1 {
		t.Fatalf("%d leases granted: the session was not reused", m.grants)
	}
}

func TestEtcdStoreOperationErrorsAreWrappedAndSecretFree(t *testing.T) {
	m := newMemEtcd()
	e := fakeStore(t, m)
	ctx := tctx(t)
	if _, err := e.GetKey(ctx, "/ns/a"); err != nil { // connect once
		t.Fatal(err)
	}
	m.setFail("Range", true)
	if _, err := e.GetKey(ctx, "/ns/a"); err == nil || !strings.Contains(err.Error(), "etcd Get(/ns/a)") {
		t.Fatalf("GetKey: %v", err)
	}
	m.setFail("Range", false)
	m.setFail("Txn", true)
	if ok, err := e.PutKeyIfAbsent(ctx, "/ns/a", "SECRET-V"); err == nil || ok || !strings.Contains(err.Error(), "etcd Txn(/ns/a)") || strings.Contains(err.Error(), "SECRET-V") {
		t.Fatalf("PutKeyIfAbsent: (%v, %v)", ok, err)
	}
	if ok, err := e.ReplaceKey(ctx, "/ns/a", "SECRET-OLD", "SECRET-NEW"); err == nil || ok || !strings.Contains(err.Error(), "etcd Txn(/ns/a)") ||
		strings.Contains(err.Error(), "SECRET-OLD") || strings.Contains(err.Error(), "SECRET-NEW") {
		t.Fatalf("ReplaceKey: (%v, %v)", ok, err)
	}
	if _, err := e.AcquireBootstrapLock(ctx, "me"); err == nil || !strings.Contains(err.Error(), "acquire bootstrap lock") {
		t.Fatalf("AcquireBootstrapLock: %v", err)
	}
}

func TestEtcdStoreReplaceKeyIsACompareAndSwapOnTheValue(t *testing.T) {
	m := newMemEtcd()
	e := fakeStore(t, m)
	ctx := tctx(t)
	if ok, err := e.ReplaceKey(ctx, "/ns/a", "x", "y"); err != nil || ok {
		t.Fatalf("missing key: (%v, %v)", ok, err)
	}
	if _, err := e.PutKeyIfAbsent(ctx, "/ns/a", "x"); err != nil {
		t.Fatal(err)
	}
	if ok, err := e.ReplaceKey(ctx, "/ns/a", "wrong", "y"); err != nil || ok {
		t.Fatalf("stale prev: (%v, %v)", ok, err)
	}
	if v, _ := m.value("/ns/a"); v != "x" {
		t.Fatalf("a refused swap changed the value to %q", v)
	}
	if ok, err := e.ReplaceKey(ctx, "/ns/a", "x", "y"); err != nil || !ok {
		t.Fatalf("good swap: (%v, %v)", ok, err)
	}
	if v, _ := m.value("/ns/a"); v != "y" {
		t.Fatalf("value = %q", v)
	}
}

func TestEtcdStoreCompactsToTheCurrentRevision(t *testing.T) {
	m := newMemEtcd()
	e := fakeStore(t, m)
	ctx := tctx(t)
	for _, k := range []string{"/ns/a", "/ns/b", "/ns/c"} {
		if _, err := e.PutKeyIfAbsent(ctx, k, "v"); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	if m.compactedAt != 3 {
		t.Fatalf("compacted at revision %d, want 3 (the head)", m.compactedAt)
	}

	m.setFail("Range", true)
	if err := e.Compact(ctx); err == nil || !strings.Contains(err.Error(), "etcd revision") {
		t.Fatalf("revision read failure: %v", err)
	}
	m.setFail("Range", false)
	m.setFail("Compact", true)
	if err := e.Compact(ctx); err == nil || !strings.Contains(err.Error(), "etcd Compact") {
		t.Fatalf("compaction failure: %v", err)
	}
}

func TestEtcdStoreBootstrapLockIsHeldThenReleased(t *testing.T) {
	m := newMemEtcd()
	e := fakeStore(t, m)
	ctx := tctx(t)
	release, err := e.AcquireBootstrapLock(ctx, "me")
	if err != nil {
		t.Fatal(err)
	}
	var held []string
	m.mu.Lock()
	for k := range m.kvs {
		held = append(held, k)
	}
	m.mu.Unlock()
	if len(held) != 1 || !strings.HasPrefix(held[0], "/ns/bootstrap-lock/") {
		t.Fatalf("lock keys = %v, want one under /ns/bootstrap-lock/", held)
	}
	release()
	if _, ok := m.value(held[0]); ok {
		t.Fatal("release left the lock key in place")
	}
}

func TestEtcdStoreCloseRevokesTheSessionAndIsIdempotent(t *testing.T) {
	m := newMemEtcd()
	e := fakeStore(t, m)
	if _, err := e.GetKey(tctx(t), "/ns/a"); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if len(m.revoked) != 1 {
		t.Fatalf("revoked leases = %v, want the session's", m.revoked)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if e.client != nil || e.session != nil {
		t.Fatal("Close left the client or session set")
	}
}

func TestEtcdStoreConnectFailureThenRetryKeepsTheClient(t *testing.T) {
	m := newMemEtcd()
	e := fakeStore(t, m)
	ctx := tctx(t)
	m.setFail("LeaseGrant", true)
	if _, err := e.GetKey(ctx, "/ns/a"); err == nil || !strings.Contains(err.Error(), "etcd session") {
		t.Fatalf("session failure: %v", err)
	}
	first := e.client
	if first == nil || e.session != nil {
		t.Fatal("after a failed session the client must stay and the session be unset")
	}
	m.setFail("LeaseGrant", false)
	if _, err := e.GetKey(ctx, "/ns/a"); err != nil {
		t.Fatal(err)
	}
	if e.client != first {
		t.Fatal("the retry dialled a second client")
	}
	// The failure reaches every entry point through connect.
	for name, call := range map[string]func(*EtcdStore) error{
		"PutKeyIfAbsent": func(s *EtcdStore) error { _, err := s.PutKeyIfAbsent(ctx, "/ns/b", "v"); return err },
		"ReplaceKey":     func(s *EtcdStore) error { _, err := s.ReplaceKey(ctx, "/ns/b", "a", "b"); return err },
		"Compact":        func(s *EtcdStore) error { return s.Compact(ctx) },
		"Lock":           func(s *EtcdStore) error { _, err := s.AcquireBootstrapLock(ctx, "me"); return err },
	} {
		m2 := newMemEtcd()
		m2.setFail("LeaseGrant", true)
		s := fakeStore(t, m2)
		if err := call(s); err == nil || !strings.Contains(err.Error(), "etcd session") {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

func TestEtcdStoreDialFailureIsWrapped(t *testing.T) {
	e, err := NewEtcdStore(nil, "/ns", 15, EtcdSecurity{AllowPlaintext: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.GetKey(tctx(t), "/k")
	if err == nil || !strings.Contains(err.Error(), "etcd client") {
		t.Fatalf("err = %v", err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("a dial error must not be a timeout")
	}
}
