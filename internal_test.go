// Copyright (c) the go-ruby-nats/nats authors
//
// SPDX-License-Identifier: BSD-3-Clause

package nats

import (
	"errors"
	"testing"

	natsgo "github.com/nats-io/nats.go"
)

func TestMapError(t *testing.T) {
	other := errors.New("something else")
	cases := []struct {
		in   error
		want error
	}{
		{nil, nil},
		{natsgo.ErrTimeout, ErrTimeout},
		{natsgo.ErrNoResponders, ErrNoResponders},
		{natsgo.ErrConnectionClosed, ErrConnectionClosed},
		{natsgo.ErrConnectionDraining, ErrConnectionDraining},
		{natsgo.ErrConnectionReconnecting, ErrConnectionReconnecting},
		{natsgo.ErrBadSubscription, ErrBadSubscription},
		{natsgo.ErrBadSubject, ErrBadSubject},
		{natsgo.ErrNoServers, ErrNoServers},
		{natsgo.ErrMaxPayload, ErrMaxPayload},
		{other, other},
	}
	for _, c := range cases {
		if got := mapError(c.in); got != c.want {
			t.Errorf("mapError(%v) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestBuildOptions(t *testing.T) {
	// Default: no servers set leaves nats.go defaults; credentials thread through.
	o := &options{name: "n", user: "u", password: "p", token: "t"}
	nopts, err := buildOptions(o)
	if err != nil {
		t.Fatalf("buildOptions: %v", err)
	}
	if nopts.Name != "n" || nopts.User != "u" || nopts.Password != "p" || nopts.Token != "t" {
		t.Fatalf("credentials not applied: %+v", nopts)
	}

	// Servers + timing knobs are applied.
	o2 := &options{
		servers:       []string{"nats://a:4222", "nats://b:4222"},
		maxReconnect:  7,
		reconnectWait: 5,
		timeout:       9,
	}
	nopts2, err := buildOptions(o2)
	if err != nil {
		t.Fatalf("buildOptions: %v", err)
	}
	if len(nopts2.Servers) != 2 || nopts2.MaxReconnect != 7 {
		t.Fatalf("server/reconnect not applied: %+v", nopts2)
	}

	// A raw nats option that errors is surfaced.
	boom := errors.New("bad nats opt")
	o3 := &options{natsOpts: []natsgo.Option{func(*natsgo.Options) error { return boom }}}
	if _, err := buildOptions(o3); !errors.Is(err, boom) {
		t.Fatalf("buildOptions raw-opt err = %v, want %v", err, boom)
	}
}

func TestDefaultDialBuildError(t *testing.T) {
	// A raw nats option that errors short-circuits defaultDial before any dial,
	// so no socket is opened.
	boom := errors.New("bad opt")
	o := &options{natsOpts: []natsgo.Option{func(*natsgo.Options) error { return boom }}}
	if _, err := defaultDial(o); !errors.Is(err, boom) {
		t.Fatalf("defaultDial err = %v, want %v", err, boom)
	}
}

func TestDefaultDialConnectError(t *testing.T) {
	// A malformed server URL makes nats.go's Connect fail during URL parsing,
	// before any network I/O, exercising defaultDial's connect-error branch.
	o := &options{servers: []string{"nats://:invalid:port:here"}}
	if _, err := defaultDial(o); err == nil {
		t.Fatal("defaultDial with malformed URL should error")
	}
}

// TestNatsConnHandlers exercises the adapter's handler method bodies directly —
// these translate nats.go callback signatures to the user callbacks. Driving
// them here (rather than provoking live async/reconnect events) keeps them
// deterministic and covered. A nil *nats.Conn is fine: the handlers never touch
// it.
func TestNatsConnHandlers(t *testing.T) {
	c := &natsConn{}
	var gotErr error
	var rcn, dsc, cls int
	c.errCB = func(e error) { gotErr = e }
	c.rcnCB = func() { rcn++ }
	c.dscCB = func() { dsc++ }
	c.clsCB = func() { cls++ }

	c.handleError(nil, nil, natsgo.ErrTimeout)
	c.handleReconnect(nil)
	c.handleDisconnect(nil, nil)
	c.handleClose(nil)

	if !errors.Is(gotErr, ErrTimeout) {
		t.Fatalf("handleError mapped err = %v", gotErr)
	}
	if rcn != 1 || dsc != 1 || cls != 1 {
		t.Fatalf("handlers fired rcn=%d dsc=%d cls=%d", rcn, dsc, cls)
	}

	// With no callbacks set, the handlers are safe no-ops (nil-guard branch).
	empty := &natsConn{}
	empty.handleError(nil, nil, errors.New("x"))
	empty.handleReconnect(nil)
	empty.handleDisconnect(nil, nil)
	empty.handleClose(nil)
}
