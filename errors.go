// Copyright (c) the go-ruby-nats/nats authors
//
// SPDX-License-Identifier: BSD-3-Clause

package nats

import (
	"errors"

	natsgo "github.com/nats-io/nats.go"
)

// The error tree mirrors the Ruby client's exception classes. Upstream they are
// Ruby classes under NATS::Error / NATS::IO::*; here each is a sentinel value
// callers match with [errors.Is]. [mapError] translates the errors nats.go
// returns into these, so downstream code — and the Ruby host that raises the
// matching class — sees a stable set independent of the nats.go version.
var (
	// ErrConnectionClosed is returned when an operation is attempted on a closed
	// connection (NATS::IO::ConnectionClosedError).
	ErrConnectionClosed = errors.New("nats: connection closed")

	// ErrConnectionDraining is returned when publishing on a draining connection
	// (NATS::IO::ConnectionDrainingError).
	ErrConnectionDraining = errors.New("nats: connection draining")

	// ErrConnectionReconnecting is returned while the connection is reconnecting
	// (NATS::IO::ConnectionReconnectingError).
	ErrConnectionReconnecting = errors.New("nats: connection reconnecting")

	// ErrTimeout is returned when a request or flush does not complete in time
	// (NATS::IO::Timeout).
	ErrTimeout = errors.New("nats: timeout")

	// ErrNoResponders is returned by a request when no subscriber is listening on
	// the subject (NATS::IO::NoRespondersError).
	ErrNoResponders = errors.New("nats: no responders available for request")

	// ErrBadSubscription is returned for an invalid or already-removed
	// subscription (NATS::IO::BadSubscription).
	ErrBadSubscription = errors.New("nats: invalid subscription")

	// ErrBadSubject is returned for an empty or malformed subject
	// (NATS::IO::BadSubject).
	ErrBadSubject = errors.New("nats: invalid subject")

	// ErrNoServers is returned when no server is reachable at connect time
	// (NATS::IO::NoServersError).
	ErrNoServers = errors.New("nats: no servers available for connection")

	// ErrMaxPayload is returned when a message exceeds the server's max payload
	// (NATS::IO::MaxPayloadError).
	ErrMaxPayload = errors.New("nats: maximum payload exceeded")

	// ErrInvalidCallback is returned when a subscription is created without a
	// message-handler block.
	ErrInvalidCallback = errors.New("nats: invalid callback")

	// ErrNoReplySubject is returned by [Msg.Respond] when the message carries no
	// reply subject to answer on.
	ErrNoReplySubject = errors.New("nats: message has no reply subject")
)

// mapError translates a nats.go error into this package's stable error tree.
// Unrecognised errors pass through unchanged so no information is lost.
func mapError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, natsgo.ErrTimeout):
		return ErrTimeout
	case errors.Is(err, natsgo.ErrNoResponders):
		return ErrNoResponders
	case errors.Is(err, natsgo.ErrConnectionClosed):
		return ErrConnectionClosed
	case errors.Is(err, natsgo.ErrConnectionDraining):
		return ErrConnectionDraining
	case errors.Is(err, natsgo.ErrConnectionReconnecting):
		return ErrConnectionReconnecting
	case errors.Is(err, natsgo.ErrBadSubscription):
		return ErrBadSubscription
	case errors.Is(err, natsgo.ErrBadSubject):
		return ErrBadSubject
	case errors.Is(err, natsgo.ErrNoServers):
		return ErrNoServers
	case errors.Is(err, natsgo.ErrMaxPayload):
		return ErrMaxPayload
	default:
		return err
	}
}
