// Copyright (c) the go-ruby-nats/nats authors
//
// SPDX-License-Identifier: BSD-3-Clause

package nats

import (
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	server "github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"
)

// These tests validate the production transport ([natsConn] over nats.go) against
// a real, embedded nats-server started in-process with DontListen — so it opens
// no socket, satisfying "core opens no external socket in unit tests" while still
// exercising the genuine NATS protocol end to end. They skip under qemu (where
// NATS_SKIP_SERVER is set) because running a full server under emulation is slow;
// the deterministic broker suite keeps coverage at 100% on those arches, and this
// suite covers the adapter on the native coverage lanes.

func requireServer(t *testing.T) {
	t.Helper()
	if os.Getenv("NATS_SKIP_SERVER") != "" {
		t.Skip("NATS_SKIP_SERVER set; skipping embedded nats-server (emulated arch)")
	}
}

// inProcessNATS starts an embedded nats-server with no network listener and
// returns a [Client] connected to it in-process.
func inProcessNATS(t *testing.T) *Client {
	t.Helper()
	requireServer(t)
	opts := &server.Options{
		Host:       "127.0.0.1",
		Port:       -1,
		DontListen: true, // in-process only; no socket
		NoLog:      true,
		NoSigs:     true,
	}
	srv, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("server not ready")
	}
	t.Cleanup(srv.Shutdown)

	nc, err := Connect(NATSOption(natsgo.InProcessServer(srv)), Name("test"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func TestServerPubSubRequestQueue(t *testing.T) {
	nc := inProcessNATS(t)

	// Register the connection callbacks (covers the adapter's registration path).
	nc.OnError(func(error) {})
	nc.OnReconnect(func() {})
	nc.OnDisconnect(func() {})

	// Plain pub/sub with headers, delivered asynchronously.
	got := make(chan *Msg, 1)
	sub, err := nc.Subscribe("evt.one", func(m *Msg) { got <- m })
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	m := &Msg{Subject: "evt.one", Data: []byte("payload"), Header: Header{}}
	m.Header.Set("X-Trace", "abc")
	if err := nc.PublishMsg(m); err != nil {
		t.Fatalf("publish msg: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	select {
	case d := <-got:
		if string(d.Data) != "payload" || d.Headers().Get("X-Trace") != "abc" {
			t.Fatalf("delivery = %q hdr=%v", d.Data, d.Header)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no delivery")
	}

	// Plain Publish + FlushTimeout.
	if err := nc.Publish("evt.one", []byte("again")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := nc.FlushTimeout(5 * time.Second); err != nil {
		t.Fatalf("flush timeout: %v", err)
	}
	<-got

	// AutoUnsubscribe then Unsubscribe.
	if err := sub.AutoUnsubscribe(100); err != nil {
		t.Fatalf("auto-unsubscribe: %v", err)
	}
	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}

	// Request/reply against a real responder.
	if _, err := nc.Subscribe("svc.echo", func(rm *Msg) {
		_ = rm.Respond(append([]byte("re:"), rm.Data...))
	}); err != nil {
		t.Fatalf("subscribe responder: %v", err)
	}
	rep, err := nc.Request("svc.echo", []byte("hi"), 5*time.Second)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if string(rep.Data) != "re:hi" {
		t.Fatalf("reply = %q", rep.Data)
	}

	// RequestMsg variant.
	rep2, err := nc.RequestMsg(&Msg{Subject: "svc.echo", Data: []byte("yo")}, 5*time.Second)
	if err != nil || string(rep2.Data) != "re:yo" {
		t.Fatalf("request msg: %v / %q", err, rep2.Data)
	}

	// Queue group: two members share the load.
	var mu sync.Mutex
	counts := map[string]int{}
	for _, name := range []string{"a", "b"} {
		n := name
		qs, err := nc.QueueSubscribe("work", "q", func(*Msg) { mu.Lock(); counts[n]++; mu.Unlock() })
		if err != nil {
			t.Fatalf("queue subscribe: %v", err)
		}
		t.Cleanup(func() { _ = qs.Drain() })
	}
	for i := 0; i < 20; i++ {
		if err := nc.Publish("work", nil); err != nil {
			t.Fatalf("publish work: %v", err)
		}
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	total := counts["a"] + counts["b"]
	mu.Unlock()
	if total != 20 {
		t.Fatalf("queue total = %d, want 20 (no broadcast)", total)
	}
}

func TestServerRequestNoRespondersAndTimeout(t *testing.T) {
	nc := inProcessNATS(t)

	// No responder at all: the server answers with a no-responders control msg.
	if _, err := nc.Request("nobody", []byte("x"), 2*time.Second); !errors.Is(err, ErrNoResponders) {
		t.Fatalf("request no-responders err = %v", err)
	}

	// A responder that never answers: the request must time out.
	if _, err := nc.Subscribe("silent", func(*Msg) {}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if _, err := nc.Request("silent", nil, 100*time.Millisecond); !errors.Is(err, ErrTimeout) {
		t.Fatalf("request timeout err = %v", err)
	}
}

func TestServerSubscribeBadSubject(t *testing.T) {
	nc := inProcessNATS(t)
	if _, err := nc.Subscribe("", func(*Msg) {}); !errors.Is(err, ErrBadSubject) {
		t.Fatalf("subscribe empty subject err = %v", err)
	}
}

func TestServerDrainAndClose(t *testing.T) {
	nc := inProcessNATS(t)
	closed := make(chan struct{})
	nc.OnClose(func() { close(closed) })
	if _, err := nc.Subscribe("x", func(*Msg) {}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := nc.Drain(); err != nil {
		t.Fatalf("drain: %v", err)
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("close callback not fired after drain")
	}
	if !nc.IsClosed() {
		t.Fatal("not closed after drain")
	}
}
