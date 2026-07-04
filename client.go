// Copyright (c) the go-ruby-nats/nats authors
//
// SPDX-License-Identifier: BSD-3-Clause

package nats

import "time"

// dialFunc opens a transport [conn] from resolved options. It is the injectable
// host seam: [Connect] uses [defaultDial] (a real nats.go connection); tests
// pass a dialer backed by an in-process broker or embedded server.
type dialFunc func(*options) (conn, error)

// Client is a NATS connection, mirroring NATS::Client. It publishes and
// subscribes over a transport [conn] and does not own the socket itself — the
// dialer does. Construct one with [Connect].
type Client struct {
	conn conn
}

// Connect opens a connection to a NATS server, mirroring NATS.connect. The
// transport is a real nats.go connection; configure it with [Option] values such
// as [Servers], [Name] and [UserInfo].
func Connect(opts ...Option) (*Client, error) {
	return connect(defaultDial, opts...)
}

// connect is the seam-aware core of [Connect]: it resolves options, dials via the
// supplied dialer, and wires any connect-time callbacks.
func connect(dial dialFunc, opts ...Option) (*Client, error) {
	o := &options{}
	for _, op := range opts {
		op(o)
	}
	c, err := dial(o)
	if err != nil {
		return nil, err
	}
	cl := &Client{conn: c}
	if o.onError != nil {
		cl.OnError(o.onError)
	}
	if o.onReconnect != nil {
		cl.OnReconnect(o.onReconnect)
	}
	if o.onDisconnect != nil {
		cl.OnDisconnect(o.onDisconnect)
	}
	if o.onClose != nil {
		cl.OnClose(o.onClose)
	}
	return cl, nil
}

// Publish sends data to subject, mirroring NATS::Client#publish.
func (c *Client) Publish(subject string, data []byte) error {
	return c.conn.publish(&Msg{Subject: subject, Data: data})
}

// PublishRequest publishes data to subject with reply set as the response
// subject, mirroring NATS::Client#publish(subject, data, reply:).
func (c *Client) PublishRequest(subject, reply string, data []byte) error {
	return c.conn.publish(&Msg{Subject: subject, Reply: reply, Data: data})
}

// PublishMsg publishes a fully-formed [Msg] (carrying Reply and Header),
// mirroring NATS::Client#publish_msg.
func (c *Client) PublishMsg(m *Msg) error { return c.conn.publish(m) }

// Subscribe registers interest in subject and delivers each matching message to
// cb, mirroring NATS::Client#subscribe(subject) { |msg| }.
func (c *Client) Subscribe(subject string, cb func(*Msg)) (*Subscription, error) {
	return c.queueSubscribe(subject, "", cb)
}

// QueueSubscribe registers a queue-group subscription: the server load-balances
// matching messages across members of queue, mirroring
// NATS::Client#subscribe(subject, queue:).
func (c *Client) QueueSubscribe(subject, queue string, cb func(*Msg)) (*Subscription, error) {
	return c.queueSubscribe(subject, queue, cb)
}

func (c *Client) queueSubscribe(subject, queue string, cb func(*Msg)) (*Subscription, error) {
	if cb == nil {
		return nil, ErrInvalidCallback
	}
	sub := &Subscription{client: c, Subject: subject, Queue: queue}
	handle, err := c.conn.subscribe(subject, queue, func(m *Msg) {
		m.client = c
		m.Sub = sub
		cb(m)
	})
	if err != nil {
		return nil, err
	}
	sub.handle = handle
	return sub, nil
}

// Unsubscribe removes sub, mirroring NATS::Client#unsubscribe.
func (c *Client) Unsubscribe(sub *Subscription) error { return sub.Unsubscribe() }

// Request publishes data to subject and waits up to timeout for a single reply,
// mirroring NATS::Client#request. It returns [ErrNoResponders] if nobody is
// listening and [ErrTimeout] if no reply arrives in time.
func (c *Client) Request(subject string, data []byte, timeout time.Duration) (*Msg, error) {
	return c.RequestMsg(&Msg{Subject: subject, Data: data}, timeout)
}

// RequestMsg is [Client.Request] for a pre-built [Msg] (carrying Header),
// mirroring NATS::Client#request with a message.
func (c *Client) RequestMsg(m *Msg, timeout time.Duration) (*Msg, error) {
	r, err := c.conn.request(m, timeout)
	if err != nil {
		return nil, err
	}
	r.client = c
	return r, nil
}

// Flush waits for the server to process all buffered messages, mirroring
// NATS::Client#flush.
func (c *Client) Flush() error { return c.conn.flush(0) }

// FlushTimeout is [Client.Flush] bounded by timeout, returning [ErrTimeout] if it
// does not complete in time.
func (c *Client) FlushTimeout(timeout time.Duration) error { return c.conn.flush(timeout) }

// Drain unsubscribes all subscriptions, flushes pending messages and closes the
// connection, mirroring NATS::Client#drain.
func (c *Client) Drain() error { return c.conn.drain() }

// Close closes the connection immediately, mirroring NATS::Client#close.
func (c *Client) Close() { c.conn.close() }

// IsClosed reports whether the connection is closed, mirroring
// NATS::Client#closed?.
func (c *Client) IsClosed() bool { return c.conn.isClosed() }

// OnError registers a callback for asynchronous errors, mirroring
// NATS::Client#on_error.
func (c *Client) OnError(cb func(error)) { c.conn.onError(cb) }

// OnReconnect registers a callback fired after the client reconnects, mirroring
// NATS::Client#on_reconnect.
func (c *Client) OnReconnect(cb func()) { c.conn.onReconnect(cb) }

// OnDisconnect registers a callback fired when the client disconnects, mirroring
// NATS::Client#on_disconnect.
func (c *Client) OnDisconnect(cb func()) { c.conn.onDisconnect(cb) }

// OnClose registers a callback fired when the connection closes, mirroring
// NATS::Client#on_close.
func (c *Client) OnClose(cb func()) { c.conn.onClose(cb) }
