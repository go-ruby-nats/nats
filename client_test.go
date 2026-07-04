// Copyright (c) the go-ruby-nats/nats authors
//
// SPDX-License-Identifier: BSD-3-Clause

package nats

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// dialTo returns a Client wired to b via the injectable dialer seam.
func dialTo(t *testing.T, b *broker, opts ...Option) *Client {
	t.Helper()
	c, err := connect(b.dial, opts...)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return c
}

func TestPublishSubscribeRoundTrip(t *testing.T) {
	b := newBroker()
	nc := dialTo(t, b)
	var mu sync.Mutex
	var got []*Msg
	sub, err := nc.Subscribe("greet.world", func(m *Msg) {
		mu.Lock()
		got = append(got, m)
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if sub.Subject != "greet.world" || sub.Queue != "" {
		t.Fatalf("subscription fields: %+v", sub)
	}
	if err := nc.Publish("greet.world", []byte("hi")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || string(got[0].Data) != "hi" {
		t.Fatalf("delivery: %+v", got)
	}
	if got[0].Sub != sub {
		t.Fatal("Msg.Sub back-reference not set")
	}
	if got[0].Subject != "greet.world" {
		t.Fatalf("subject: %q", got[0].Subject)
	}
}

func TestWildcardSubjects(t *testing.T) {
	b := newBroker()
	nc := dialTo(t, b)
	var star, tail int
	var mu sync.Mutex
	nc.Subscribe("greet.*", func(*Msg) { mu.Lock(); star++; mu.Unlock() })
	nc.Subscribe("greet.>", func(*Msg) { mu.Lock(); tail++; mu.Unlock() })
	nc.Subscribe("nope.exact", func(*Msg) { t.Error("should not match") })
	nc.Publish("greet.alice", nil) // matches * and >
	nc.Publish("greet.a.b", nil)   // matches > only
	nc.Publish("other.thing", nil) // matches neither of the greet subs
	mu.Lock()
	defer mu.Unlock()
	if star != 1 {
		t.Fatalf("* matches = %d, want 1", star)
	}
	if tail != 2 {
		t.Fatalf("> matches = %d, want 2", tail)
	}
}

func TestQueueSubscribeLoadBalances(t *testing.T) {
	b := newBroker()
	nc := dialTo(t, b)
	var mu sync.Mutex
	counts := map[string]int{}
	mk := func(name string) func(*Msg) {
		return func(*Msg) { mu.Lock(); counts[name]++; mu.Unlock() }
	}
	if _, err := nc.QueueSubscribe("work", "q", mk("a")); err != nil {
		t.Fatalf("queue subscribe: %v", err)
	}
	if _, err := nc.QueueSubscribe("work", "q", mk("b")); err != nil {
		t.Fatalf("queue subscribe: %v", err)
	}
	const n = 6
	for i := 0; i < n; i++ {
		nc.Publish("work", nil)
	}
	mu.Lock()
	defer mu.Unlock()
	if counts["a"]+counts["b"] != n {
		t.Fatalf("total deliveries %d, want %d (queue must not broadcast)", counts["a"]+counts["b"], n)
	}
	if counts["a"] == 0 || counts["b"] == 0 {
		t.Fatalf("load not balanced: %v", counts)
	}
}

func TestRequestReply(t *testing.T) {
	b := newBroker()
	nc := dialTo(t, b)
	_, err := nc.Subscribe("svc.echo", func(m *Msg) {
		if err := m.Respond(append([]byte("re:"), m.Data...)); err != nil {
			t.Errorf("respond: %v", err)
		}
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	rep, err := nc.Request("svc.echo", []byte("ping"), time.Second)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if string(rep.Data) != "re:ping" {
		t.Fatalf("reply data %q", rep.Data)
	}
}

func TestRequestNoResponders(t *testing.T) {
	b := newBroker()
	nc := dialTo(t, b)
	_, err := nc.Request("nobody.here", []byte("x"), time.Second)
	if !errors.Is(err, ErrNoResponders) {
		t.Fatalf("err = %v, want ErrNoResponders", err)
	}
}

func TestRequestTimeout(t *testing.T) {
	b := newBroker()
	nc := dialTo(t, b)
	// A responder exists but never answers, so the request must time out.
	nc.Subscribe("svc.silent", func(*Msg) {})
	_, err := nc.Request("svc.silent", nil, 20*time.Millisecond)
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("err = %v, want ErrTimeout", err)
	}
}

func TestRequestMsgHeadersRoundTrip(t *testing.T) {
	b := newBroker()
	nc := dialTo(t, b)
	nc.Subscribe("svc.hdr", func(m *Msg) {
		reply := &Msg{Data: []byte("ok"), Header: Header{}}
		reply.Header.Set("X-Echo", m.Header.Get("X-Req"))
		if err := m.RespondMsg(reply); err != nil {
			t.Errorf("respond msg: %v", err)
		}
	})
	req := &Msg{Subject: "svc.hdr", Data: []byte("q"), Header: Header{}}
	req.Header.Set("X-Req", "v42")
	rep, err := nc.RequestMsg(req, time.Second)
	if err != nil {
		t.Fatalf("request msg: %v", err)
	}
	if rep.Headers().Get("X-Echo") != "v42" {
		t.Fatalf("header round-trip: %v", rep.Header)
	}
}

func TestPublishRequestAndPublishMsg(t *testing.T) {
	b := newBroker()
	nc := dialTo(t, b)
	done := make(chan string, 1)
	nc.Subscribe("inbox.a", func(m *Msg) { done <- string(m.Data) })
	// PublishRequest sets the reply subject explicitly.
	replies := make(chan string, 1)
	nc.Subscribe("reply.here", func(m *Msg) { replies <- m.Reply })
	if err := nc.PublishRequest("reply.here", "back.to.me", []byte("d")); err != nil {
		t.Fatalf("publish request: %v", err)
	}
	if r := <-replies; r != "back.to.me" {
		t.Fatalf("reply subject = %q", r)
	}
	// PublishMsg carries an explicit Msg.
	if err := nc.PublishMsg(&Msg{Subject: "inbox.a", Data: []byte("body")}); err != nil {
		t.Fatalf("publish msg: %v", err)
	}
	if d := <-done; d != "body" {
		t.Fatalf("publish msg data = %q", d)
	}
}

func TestUnsubscribeStopsDelivery(t *testing.T) {
	b := newBroker()
	nc := dialTo(t, b)
	var n int
	var mu sync.Mutex
	sub, _ := nc.Subscribe("s", func(*Msg) { mu.Lock(); n++; mu.Unlock() })
	nc.Publish("s", nil)
	if err := nc.Unsubscribe(sub); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}
	nc.Publish("s", nil)
	mu.Lock()
	defer mu.Unlock()
	if n != 1 {
		t.Fatalf("deliveries after unsubscribe = %d, want 1", n)
	}
	// Unsubscribing again reports a bad subscription.
	if err := sub.Unsubscribe(); !errors.Is(err, ErrBadSubscription) {
		t.Fatalf("double unsubscribe err = %v", err)
	}
}

func TestAutoUnsubscribe(t *testing.T) {
	b := newBroker()
	nc := dialTo(t, b)
	var n int
	var mu sync.Mutex
	sub, _ := nc.Subscribe("s", func(*Msg) { mu.Lock(); n++; mu.Unlock() })
	if err := sub.AutoUnsubscribe(2); err != nil {
		t.Fatalf("auto-unsubscribe: %v", err)
	}
	for i := 0; i < 5; i++ {
		nc.Publish("s", nil)
	}
	mu.Lock()
	defer mu.Unlock()
	if n != 2 {
		t.Fatalf("deliveries = %d, want 2 after AutoUnsubscribe(2)", n)
	}
}

func TestAutoUnsubscribeAlreadyReached(t *testing.T) {
	b := newBroker()
	nc := dialTo(t, b)
	sub, _ := nc.Subscribe("s", func(*Msg) {})
	nc.Publish("s", nil)
	nc.Publish("s", nil)
	// Two already delivered; requesting max=1 must drop the sub immediately.
	if err := sub.AutoUnsubscribe(1); err != nil {
		t.Fatalf("auto-unsubscribe: %v", err)
	}
	if err := sub.Unsubscribe(); !errors.Is(err, ErrBadSubscription) {
		t.Fatalf("sub should already be gone, err = %v", err)
	}
}

func TestSubscriptionDrain(t *testing.T) {
	b := newBroker()
	nc := dialTo(t, b)
	sub, _ := nc.Subscribe("s", func(*Msg) {})
	if err := sub.Drain(); err != nil {
		t.Fatalf("sub drain: %v", err)
	}
	nc.Publish("s", nil) // no panic, no delivery
}

func TestFlushAndFlushTimeout(t *testing.T) {
	b := newBroker()
	nc := dialTo(t, b)
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatalf("flush timeout: %v", err)
	}
}

func TestDrainClosesAndFiresCallback(t *testing.T) {
	b := newBroker()
	var closed bool
	nc := dialTo(t, b, ClosedCallback(func() { closed = true }))
	if nc.IsClosed() {
		t.Fatal("closed before drain")
	}
	if err := nc.Drain(); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if !closed {
		t.Fatal("ClosedCallback not fired on drain")
	}
	if !nc.IsClosed() {
		t.Fatal("not closed after drain")
	}
	// Draining removed all subs and closed the connection.
	if err := nc.Drain(); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("drain after close err = %v", err)
	}
}

func TestCloseIdempotentAndCallback(t *testing.T) {
	b := newBroker()
	var n int
	nc := dialTo(t, b)
	nc.OnClose(func() { n++ })
	nc.Close()
	nc.Close() // idempotent, no second callback
	if n != 1 {
		t.Fatalf("close callback fired %d times, want 1", n)
	}
	if !nc.IsClosed() {
		t.Fatal("not closed")
	}
}

func TestConnectWiresAllCallbacks(t *testing.T) {
	b := newBroker()
	nc := dialTo(t, b,
		ErrorCallback(func(error) {}),
		ReconnectCallback(func() {}),
		DisconnectCallback(func() {}),
		Name("app"), UserInfo("u", "p"), Token("tok"),
		Servers("nats://x:4222"), MaxReconnects(3),
		ReconnectWait(time.Second), Timeout(time.Second),
		NATSOption(nil),
	)
	if b.errCB == nil || b.rcnCB == nil || b.dscCB == nil {
		t.Fatal("callbacks not wired through connect")
	}
	// Method-form setters also work.
	nc.OnError(func(error) {})
	nc.OnReconnect(func() {})
	nc.OnDisconnect(func() {})
}

func TestErrorPaths(t *testing.T) {
	b := newBroker()
	nc := dialTo(t, b)

	if _, err := nc.Subscribe("s", nil); !errors.Is(err, ErrInvalidCallback) {
		t.Fatalf("nil callback err = %v", err)
	}
	if _, err := nc.Subscribe("", func(*Msg) {}); !errors.Is(err, ErrBadSubject) {
		t.Fatalf("empty subject subscribe err = %v", err)
	}
	if err := nc.Publish("", nil); !errors.Is(err, ErrBadSubject) {
		t.Fatalf("empty subject publish err = %v", err)
	}

	// Draining state rejects publishes with ErrConnectionDraining.
	b.mu.Lock()
	b.draining = true
	b.mu.Unlock()
	if err := nc.Publish("s", nil); !errors.Is(err, ErrConnectionDraining) {
		t.Fatalf("publish while draining err = %v", err)
	}

	// Now closed: every operation reports ErrConnectionClosed.
	nc.Close()
	if err := nc.Publish("s", nil); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("publish closed err = %v", err)
	}
	if _, err := nc.Subscribe("s", func(*Msg) {}); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("subscribe closed err = %v", err)
	}
	if _, err := nc.Request("s", nil, time.Second); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("request closed err = %v", err)
	}
	if err := nc.Flush(); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("flush closed err = %v", err)
	}
}

func TestRespondErrorPaths(t *testing.T) {
	// No reply subject.
	m := &Msg{client: &Client{}, Reply: ""}
	if err := m.Respond(nil); !errors.Is(err, ErrNoReplySubject) {
		t.Fatalf("respond without reply err = %v", err)
	}
	// No bound client.
	m2 := &Msg{Reply: "r"}
	if err := m2.Respond(nil); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("respond without client err = %v", err)
	}
}

func TestConnectDialError(t *testing.T) {
	boom := errors.New("dial boom")
	_, err := connect(func(*options) (conn, error) { return nil, boom }, Name("x"))
	if !errors.Is(err, boom) {
		t.Fatalf("connect dial err = %v", err)
	}
}

func TestRequestSubscribeErrorOnInbox(t *testing.T) {
	// A closed broker with an existing responder still refuses the inbox
	// subscription, exercising the request() subscribe-error branch. We simulate
	// by pre-registering a responder then closing between the responder check and
	// inbox subscribe is not deterministic; instead cover the branch directly.
	b := newBroker()
	b.subs[999] = &brokerSub{id: 999, subject: "svc", b: b}
	b.closed = true
	if _, err := b.request(&Msg{Subject: "svc"}, time.Second); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("request on closed broker err = %v", err)
	}
}

func TestHeaderMethods(t *testing.T) {
	var nilH Header
	if nilH.Get("x") != "" {
		t.Fatal("nil header Get should be empty")
	}
	h := Header{}
	if h.Get("absent") != "" {
		t.Fatal("absent key Get should be empty")
	}
	h.Set("K", "v1")
	h.Add("K", "v2")
	if h.Get("K") != "v1" {
		t.Fatalf("Get = %q", h.Get("K"))
	}
	if got := h.Values("K"); len(got) != 2 || got[1] != "v2" {
		t.Fatalf("Values = %v", got)
	}
	h.Del("K")
	if h.Values("K") != nil {
		t.Fatal("Del did not remove key")
	}
}
