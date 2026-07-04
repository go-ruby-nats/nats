// Copyright (c) the go-ruby-nats/nats authors
//
// SPDX-License-Identifier: BSD-3-Clause

package nats

import (
	"time"

	natsgo "github.com/nats-io/nats.go"
)

// conn is the transport seam a [Client] drives. The default implementation
// ([natsConn]) wraps the official pure-Go client's *nats.Conn; tests inject an
// in-process implementation so the client runs with no socket. All methods speak
// this package's own [Msg]/[Header] types, keeping nats.go out of the surface a
// host binds.
type conn interface {
	publish(m *Msg) error
	subscribe(subject, queue string, cb func(*Msg)) (subHandle, error)
	request(m *Msg, timeout time.Duration) (*Msg, error)
	flush(timeout time.Duration) error
	drain() error
	close()
	isClosed() bool
	onError(cb func(error))
	onReconnect(cb func())
	onDisconnect(cb func())
	onClose(cb func())
}

// natsConn is the production seam: a thin adapter over the official nats.go
// client. Each method translates this package's types to nats.go's and maps
// returned errors through [mapError].
type natsConn struct {
	nc    *natsgo.Conn
	errCB func(error)
	rcnCB func()
	dscCB func()
	clsCB func()
}

// toNats converts a *Msg into a *nats.Msg for the wire.
func toNats(m *Msg) *natsgo.Msg {
	return &natsgo.Msg{
		Subject: m.Subject,
		Reply:   m.Reply,
		Data:    m.Data,
		Header:  natsgo.Header(m.Header),
	}
}

// fromNats converts a delivered *nats.Msg into a *Msg.
func fromNats(nm *natsgo.Msg) *Msg {
	return &Msg{
		Subject: nm.Subject,
		Reply:   nm.Reply,
		Data:    nm.Data,
		Header:  Header(nm.Header),
	}
}

func (c *natsConn) publish(m *Msg) error { return mapError(c.nc.PublishMsg(toNats(m))) }

func (c *natsConn) subscribe(subject, queue string, cb func(*Msg)) (subHandle, error) {
	handler := func(nm *natsgo.Msg) { cb(fromNats(nm)) }
	var (
		s   *natsgo.Subscription
		err error
	)
	if queue != "" {
		s, err = c.nc.QueueSubscribe(subject, queue, handler)
	} else {
		s, err = c.nc.Subscribe(subject, handler)
	}
	if err != nil {
		return nil, mapError(err)
	}
	return &natsSub{s: s}, nil
}

func (c *natsConn) request(m *Msg, timeout time.Duration) (*Msg, error) {
	r, err := c.nc.RequestMsg(toNats(m), timeout)
	if err != nil {
		return nil, mapError(err)
	}
	return fromNats(r), nil
}

func (c *natsConn) flush(timeout time.Duration) error {
	if timeout > 0 {
		return mapError(c.nc.FlushTimeout(timeout))
	}
	return mapError(c.nc.Flush())
}

func (c *natsConn) drain() error   { return mapError(c.nc.Drain()) }
func (c *natsConn) close()         { c.nc.Close() }
func (c *natsConn) isClosed() bool { return c.nc.IsClosed() }

func (c *natsConn) onError(cb func(error)) {
	c.errCB = cb
	c.nc.SetErrorHandler(c.handleError)
}

func (c *natsConn) onReconnect(cb func()) {
	c.rcnCB = cb
	c.nc.SetReconnectHandler(c.handleReconnect)
}

func (c *natsConn) onDisconnect(cb func()) {
	c.dscCB = cb
	c.nc.SetDisconnectErrHandler(c.handleDisconnect)
}

func (c *natsConn) onClose(cb func()) {
	c.clsCB = cb
	c.nc.SetClosedHandler(c.handleClose)
}

// handleError adapts nats.go's ErrorHandler signature, mapping the error before
// invoking the user callback. It is a named method (not an inline closure) so it
// is directly exercisable without provoking a live async error.
func (c *natsConn) handleError(_ *natsgo.Conn, _ *natsgo.Subscription, err error) {
	if c.errCB != nil {
		c.errCB(mapError(err))
	}
}

func (c *natsConn) handleReconnect(_ *natsgo.Conn) {
	if c.rcnCB != nil {
		c.rcnCB()
	}
}

func (c *natsConn) handleDisconnect(_ *natsgo.Conn, _ error) {
	if c.dscCB != nil {
		c.dscCB()
	}
}

func (c *natsConn) handleClose(_ *natsgo.Conn) {
	if c.clsCB != nil {
		c.clsCB()
	}
}

// natsSub is the production [subHandle], wrapping a *nats.Subscription.
type natsSub struct{ s *natsgo.Subscription }

func (n *natsSub) unsubscribe(max int) error {
	if max > 0 {
		return mapError(n.s.AutoUnsubscribe(max))
	}
	return mapError(n.s.Unsubscribe())
}

func (n *natsSub) drain() error { return mapError(n.s.Drain()) }
