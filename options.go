// Copyright (c) the go-ruby-nats/nats authors
//
// SPDX-License-Identifier: BSD-3-Clause

package nats

import (
	"time"

	natsgo "github.com/nats-io/nats.go"
)

// options holds the connection configuration [Connect] assembles from its
// functional [Option] arguments. The fields mirror the gem's NATS.connect
// keywords that shape the connection.
type options struct {
	servers       []string
	name          string
	user          string
	password      string
	token         string
	maxReconnect  int
	reconnectWait time.Duration
	timeout       time.Duration
	onError       func(error)
	onReconnect   func()
	onDisconnect  func()
	onClose       func()
	natsOpts      []natsgo.Option
}

// Option configures a [Connect] call. The provided options mirror the gem's
// NATS.connect keyword arguments.
type Option func(*options)

// Servers sets the server URLs to connect to (NATS.connect servers:).
func Servers(urls ...string) Option {
	return func(o *options) { o.servers = append(o.servers, urls...) }
}

// Name sets the connection name (NATS.connect name:).
func Name(name string) Option { return func(o *options) { o.name = name } }

// UserInfo sets username/password credentials (NATS.connect user:/password:).
func UserInfo(user, password string) Option {
	return func(o *options) { o.user = user; o.password = password }
}

// Token sets a token credential (NATS.connect auth_token:).
func Token(token string) Option { return func(o *options) { o.token = token } }

// MaxReconnects sets the reconnect attempt limit (NATS.connect max_reconnect_attempts:).
func MaxReconnects(n int) Option { return func(o *options) { o.maxReconnect = n } }

// ReconnectWait sets the delay between reconnect attempts (NATS.connect reconnect_time_wait:).
func ReconnectWait(d time.Duration) Option { return func(o *options) { o.reconnectWait = d } }

// Timeout sets the connect timeout (NATS.connect connect_timeout:).
func Timeout(d time.Duration) Option { return func(o *options) { o.timeout = d } }

// ErrorCallback registers the on_error callback at connect time.
func ErrorCallback(cb func(error)) Option { return func(o *options) { o.onError = cb } }

// ReconnectCallback registers the on_reconnect callback at connect time.
func ReconnectCallback(cb func()) Option { return func(o *options) { o.onReconnect = cb } }

// DisconnectCallback registers the on_disconnect callback at connect time.
func DisconnectCallback(cb func()) Option { return func(o *options) { o.onDisconnect = cb } }

// ClosedCallback registers the on_close callback at connect time.
func ClosedCallback(cb func()) Option { return func(o *options) { o.onClose = cb } }

// NATSOption threads a raw nats.go [github.com/nats-io/nats.go.Option] into the
// underlying connection — an escape hatch for transport features (TLS, JWT,
// in-process server) not surfaced by the gem-shaped options above.
func NATSOption(opt natsgo.Option) Option {
	return func(o *options) { o.natsOpts = append(o.natsOpts, opt) }
}

// buildOptions assembles nats.go connection options from o. It is pure (it never
// dials), so the URL/credential wiring is unit-testable without a server.
func buildOptions(o *options) (natsgo.Options, error) {
	nopts := natsgo.GetDefaultOptions()
	if len(o.servers) > 0 {
		nopts.Servers = o.servers
	}
	nopts.Name = o.name
	nopts.User = o.user
	nopts.Password = o.password
	nopts.Token = o.token
	if o.maxReconnect != 0 {
		nopts.MaxReconnect = o.maxReconnect
	}
	if o.reconnectWait != 0 {
		nopts.ReconnectWait = o.reconnectWait
	}
	if o.timeout != 0 {
		nopts.Timeout = o.timeout
	}
	for _, op := range o.natsOpts {
		if err := op(&nopts); err != nil {
			return nopts, mapError(err)
		}
	}
	return nopts, nil
}

// defaultDial is the production dialer: it builds nats.go options and opens a
// real connection, wrapping it in a [natsConn].
func defaultDial(o *options) (conn, error) {
	nopts, err := buildOptions(o)
	if err != nil {
		return nil, err
	}
	nc, err := nopts.Connect()
	if err != nil {
		return nil, mapError(err)
	}
	return &natsConn{nc: nc}, nil
}
