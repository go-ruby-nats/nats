// Copyright (c) the go-ruby-nats/nats authors
//
// SPDX-License-Identifier: BSD-3-Clause

package nats

// subHandle is the transport-level handle to one subscription, part of the host
// seam. The real implementation wraps a *nats.Subscription; tests supply an
// in-process handle.
type subHandle interface {
	// unsubscribe removes the subscription. A max > 0 requests auto-unsubscribe
	// after max messages (nats.go's AutoUnsubscribe); max == 0 removes it now.
	unsubscribe(max int) error
	// drain removes the subscription after processing buffered messages.
	drain() error
}

// Subscription is an interest registration, mirroring NATS::Subscription. It is
// returned by [Client.Subscribe] / [Client.QueueSubscribe] and delivers messages
// to the callback given there.
type Subscription struct {
	// Subject is the subscribed subject (may contain wildcards).
	Subject string
	// Queue is the queue group, or "" for a plain subscription.
	Queue string

	client *Client
	handle subHandle
}

// Unsubscribe removes the subscription, mirroring NATS::Subscription#unsubscribe.
func (s *Subscription) Unsubscribe() error { return s.handle.unsubscribe(0) }

// AutoUnsubscribe removes the subscription after max more messages are delivered,
// mirroring the gem's subscribe(max:) auto-unsubscribe.
func (s *Subscription) AutoUnsubscribe(max int) error { return s.handle.unsubscribe(max) }

// Drain removes the subscription after its buffered messages are processed,
// mirroring NATS::Subscription#drain.
func (s *Subscription) Drain() error { return s.handle.drain() }
