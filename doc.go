// Copyright (c) the go-ruby-nats/nats authors
//
// SPDX-License-Identifier: BSD-3-Clause

// Package nats is a pure-Go (CGO_ENABLED=0) port of the Ruby
// [nats-pure] / [nats] client's public surface — the messaging client Ruby
// programs use to talk to a NATS server.
//
// Upstream, the Ruby client speaks the NATS protocol over a socket it owns; this
// module keeps the same object model — [Client], [Msg], [Subscription] and the
// [Error] tree — but delegates the wire protocol to the official pure-Go client
// [github.com/nats-io/nats.go]. It does not reimplement the protocol.
//
// # MRI-faithful surface
//
//	nc, _ := nats.Connect(nats.Servers("nats://127.0.0.1:4222"))
//	sub, _ := nc.Subscribe("greet.*", func(m *nats.Msg) {
//		m.Respond([]byte("hello, " + string(m.Data)))
//	})
//	rep, _ := nc.Request("greet.joe", []byte("joe"), time.Second)
//	fmt.Println(string(rep.Data)) // hello, joe
//	sub.Unsubscribe()
//	nc.Drain()
//	nc.Close()
//
// The method names mirror the gem: [Client.Publish], [Client.Subscribe] /
// [Client.QueueSubscribe], [Client.Request], [Client.Flush], [Client.Drain],
// [Client.Close], and the connection callbacks [Client.OnError],
// [Client.OnReconnect] and [Client.OnDisconnect]. [Msg] exposes Subject / Data /
// Reply / Header and [Msg.Respond]; errors map to the gem's classes
// ([ErrTimeout] is NATS::IO::Timeout, [ErrNoResponders] is NoRespondersError).
//
// # Host seam
//
// The transport is injectable. [Connect] dials a real server through nats.go by
// default, but the dialer is a seam: tests drive the whole client against an
// in-process broker (no socket) or an embedded nats-server started with
// [github.com/nats-io/nats.go.InProcessServer]. The core opens no external
// socket in unit tests.
//
// [nats-pure]: https://github.com/nats-io/nats-pure.rb
// [nats]: https://github.com/nats-io/nats.rb
package nats
