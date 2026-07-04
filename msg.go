// Copyright (c) the go-ruby-nats/nats authors
//
// SPDX-License-Identifier: BSD-3-Clause

package nats

// Header carries a message's optional headers, mirroring NATS::Msg#headers — a
// case-preserving multimap of string keys to string values (the same shape as
// nats.go's Header and net/textproto's MIMEHeader). A nil Header is a valid
// empty header for reads.
type Header map[string][]string

// Get returns the first value for key, or "" if absent.
func (h Header) Get(key string) string {
	if h == nil {
		return ""
	}
	v := h[key]
	if len(v) == 0 {
		return ""
	}
	return v[0]
}

// Values returns all values for key (nil if absent).
func (h Header) Values(key string) []string { return h[key] }

// Set replaces any existing values for key with a single value.
func (h Header) Set(key, value string) { h[key] = []string{value} }

// Add appends a value to the values for key.
func (h Header) Add(key, value string) { h[key] = append(h[key], value) }

// Del removes all values for key.
func (h Header) Del(key string) { delete(h, key) }

// Msg is a NATS message, mirroring NATS::Msg. Subscribers receive one; publishers
// may build one for [Client.PublishMsg] to carry a Reply subject or Header.
type Msg struct {
	// Subject the message was published to (or delivered on).
	Subject string
	// Reply is the subject a responder should answer on, if any.
	Reply string
	// Data is the message payload.
	Data []byte
	// Header holds optional headers.
	Header Header
	// Sub is the subscription that delivered this message (nil for messages the
	// caller builds).
	Sub *Subscription

	// client is set on delivered and request-reply messages so Respond can
	// publish the answer over the same connection.
	client *Client
}

// Headers returns the message headers, mirroring NATS::Msg#headers.
func (m *Msg) Headers() Header { return m.Header }

// Respond publishes data to the message's Reply subject, mirroring
// NATS::Msg#respond. It returns [ErrNoReplySubject] if the message has no reply
// subject and [ErrConnectionClosed] if it is not bound to a connection.
func (m *Msg) Respond(data []byte) error {
	return m.RespondMsg(&Msg{Data: data})
}

// RespondMsg publishes resp (with its Header) to the message's Reply subject,
// mirroring NATS::Msg#respond_msg.
func (m *Msg) RespondMsg(resp *Msg) error {
	if m.client == nil {
		return ErrConnectionClosed
	}
	if m.Reply == "" {
		return ErrNoReplySubject
	}
	resp.Subject = m.Reply
	resp.Reply = ""
	return m.client.PublishMsg(resp)
}
