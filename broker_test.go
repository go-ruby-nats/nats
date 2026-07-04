// Copyright (c) the go-ruby-nats/nats authors
//
// SPDX-License-Identifier: BSD-3-Clause

package nats

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// broker is an in-process NATS broker used as the injectable transport seam in
// the deterministic tests. It implements [conn] and [subHandle] with real
// subject matching (including the `*` token and `>` tail wildcards), queue-group
// load balancing, and request/reply — so the client's pub/sub, request, queue,
// unsubscribe, drain and header behaviour are verified with genuine round-trips
// and no socket. It is not shipped: it lives only in the test binary.
type broker struct {
	mu       sync.Mutex
	subs     map[uint64]*brokerSub
	next     uint64
	closed   bool
	draining bool
	qrr      map[string]uint64 // queue -> round-robin counter

	errCB func(error)
	rcnCB func()
	dscCB func()
	clsCB func()
}

type brokerSub struct {
	id      uint64
	subject string
	queue   string
	cb      func(*Msg)
	max     int // 0 = unlimited
	got     int
	b       *broker
}

func newBroker() *broker {
	return &broker{subs: map[uint64]*brokerSub{}, qrr: map[string]uint64{}}
}

// dial returns a dialFunc that always yields this broker, for injection into
// connect.
func (b *broker) dial(*options) (conn, error) { return b, nil }

// subjectMatch reports whether a published subject matches a subscription
// pattern, honouring `*` (single token) and `>` (one-or-more trailing tokens).
func subjectMatch(pattern, subject string) bool {
	pt := strings.Split(pattern, ".")
	st := strings.Split(subject, ".")
	for i, p := range pt {
		if p == ">" {
			return i < len(st)
		}
		if i >= len(st) {
			return false
		}
		if p == "*" {
			continue
		}
		if p != st[i] {
			return false
		}
	}
	return len(pt) == len(st)
}

func (b *broker) publish(m *Msg) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrConnectionClosed
	}
	if b.draining {
		b.mu.Unlock()
		return ErrConnectionDraining
	}
	if m.Subject == "" {
		b.mu.Unlock()
		return ErrBadSubject
	}
	targets := b.selectSubs(m.Subject)
	b.mu.Unlock()

	for _, s := range targets {
		deliver := &Msg{Subject: m.Subject, Reply: m.Reply, Data: m.Data, Header: m.Header}
		s.cb(deliver)
	}
	return nil
}

// selectSubs picks the subscriptions a message on subject reaches: every plain
// subscriber, and exactly one member of each matching queue group. Caller holds
// the lock. It also enforces auto-unsubscribe (max).
func (b *broker) selectSubs(subject string) []*brokerSub {
	var plain []*brokerSub
	queues := map[string][]*brokerSub{}
	for _, s := range b.subs {
		if !subjectMatch(s.subject, subject) {
			continue
		}
		if s.queue == "" {
			plain = append(plain, s)
		} else {
			queues[s.queue] = append(queues[s.queue], s)
		}
	}
	chosen := plain
	for q, members := range queues {
		sort.Slice(members, func(i, j int) bool { return members[i].id < members[j].id })
		idx := b.qrr[q] % uint64(len(members))
		b.qrr[q]++
		chosen = append(chosen, members[idx])
	}
	// Apply auto-unsubscribe bookkeeping.
	var out []*brokerSub
	for _, s := range chosen {
		s.got++
		out = append(out, s)
		if s.max > 0 && s.got >= s.max {
			delete(b.subs, s.id)
		}
	}
	return out
}

func (b *broker) subscribe(subject, queue string, cb func(*Msg)) (subHandle, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrConnectionClosed
	}
	if subject == "" {
		return nil, ErrBadSubject
	}
	b.next++
	s := &brokerSub{id: b.next, subject: subject, queue: queue, cb: cb, b: b}
	b.subs[s.id] = s
	return s, nil
}

func (b *broker) request(m *Msg, timeout time.Duration) (*Msg, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, ErrConnectionClosed
	}
	hasResponder := false
	for _, s := range b.subs {
		if subjectMatch(s.subject, m.Subject) {
			hasResponder = true
			break
		}
	}
	b.mu.Unlock()
	if !hasResponder {
		return nil, ErrNoResponders
	}

	inbox := "_INBOX." + randToken(&b.next, &b.mu)
	ch := make(chan *Msg, 1)
	h, err := b.subscribe(inbox, "", func(rm *Msg) {
		select {
		case ch <- rm:
		default:
		}
	})
	if err != nil {
		return nil, err
	}
	defer h.unsubscribe(0)

	req := &Msg{Subject: m.Subject, Reply: inbox, Data: m.Data, Header: m.Header}
	if err := b.publish(req); err != nil {
		return nil, err
	}
	select {
	case rm := <-ch:
		return rm, nil
	case <-time.After(timeout):
		return nil, ErrTimeout
	}
}

func (b *broker) flush(time.Duration) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrConnectionClosed
	}
	return nil
}

func (b *broker) drain() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrConnectionClosed
	}
	b.draining = true
	b.subs = map[uint64]*brokerSub{}
	cb := b.clsCB
	b.closed = true
	b.mu.Unlock()
	if cb != nil {
		cb()
	}
	return nil
}

func (b *broker) close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	cb := b.clsCB
	b.mu.Unlock()
	if cb != nil {
		cb()
	}
}

func (b *broker) isClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

func (b *broker) onError(cb func(error)) { b.errCB = cb }
func (b *broker) onReconnect(cb func())  { b.rcnCB = cb }
func (b *broker) onDisconnect(cb func()) { b.dscCB = cb }
func (b *broker) onClose(cb func())      { b.clsCB = cb }

// subHandle implementation.

func (s *brokerSub) unsubscribe(max int) error {
	s.b.mu.Lock()
	defer s.b.mu.Unlock()
	if _, ok := s.b.subs[s.id]; !ok {
		return ErrBadSubscription
	}
	if max > 0 {
		s.max = max
		if s.got >= max {
			delete(s.b.subs, s.id)
		}
		return nil
	}
	delete(s.b.subs, s.id)
	return nil
}

func (s *brokerSub) drain() error { return s.unsubscribe(0) }

// randToken returns a short unique token for reply inboxes.
func randToken(next *uint64, mu *sync.Mutex) string {
	mu.Lock()
	*next++
	n := *next
	mu.Unlock()
	const hex = "0123456789abcdef"
	var buf [8]byte
	for i := range buf {
		buf[i] = hex[n&0xf]
		n >>= 4
	}
	return string(buf[:])
}
