<p align="center"><img src="https://raw.githubusercontent.com/go-ruby-nats/brand/main/social/go-ruby-nats-nats.png" alt="go-ruby-nats/nats" width="720"></p>

# nats — go-ruby-nats

[![Docs](https://img.shields.io/badge/docs-mkdocs--material-DC2626)](https://go-ruby-nats.github.io/docs/)
[![License](https://img.shields.io/badge/license-BSD--3--Clause-blue)](LICENSE)
[![Go](https://img.shields.io/badge/go-1.26.4%2B-00ADD8)](https://go.dev/dl/)
[![Coverage](https://img.shields.io/badge/coverage-100%25-1a7f37)](#tests--coverage)

**A pure-Go (no cgo) port of the Ruby [`nats-pure`](https://github.com/nats-io/nats-pure.rb) /
[`nats`](https://github.com/nats-io/nats.rb) client's public surface — the
messaging client Ruby programs use to talk to a [NATS](https://nats.io) server.**

The Ruby client speaks the NATS protocol over a socket it owns. This module keeps
the same object model — `NATS::Client`, `NATS::Msg`, `NATS::Subscription` and the
`NATS::Error` tree — but delegates the wire protocol to the **official pure-Go
client** [`nats.go`](https://github.com/nats-io/nats.go). It does **not**
reimplement the protocol, and it links statically with `CGO_ENABLED=0` on every
64-bit target the go-\* ecosystem supports (amd64, arm64, riscv64, loong64,
ppc64le and big-endian s390x).

It is the NATS backend for
[go-embedded-ruby](https://github.com/go-embedded-ruby/ruby), but is a
**standalone, reusable** module — a sibling of
[go-ruby-redis](https://github.com/go-ruby-redis/redis) and
[go-ruby-sqlite3](https://github.com/go-ruby-sqlite3/sqlite3).

## Example

```go
nc, _ := nats.Connect(nats.Servers("nats://127.0.0.1:4222"))
defer nc.Close()

// Subscribe with a responder.
nc.Subscribe("greet.*", func(m *nats.Msg) {
    m.Respond([]byte("hello, " + string(m.Data)))
})

// Publish.
nc.Publish("greet.world", []byte("world"))

// Request/reply.
rep, _ := nc.Request("greet.joe", []byte("joe"), time.Second)
fmt.Println(string(rep.Data)) // hello, joe

nc.Drain()
```

## Surface

Faithful port of the gem's client API:

- **`Client`** — `Connect` (`NATS.connect`), `Publish` / `PublishRequest` /
  `PublishMsg`, `Subscribe` / `QueueSubscribe` (`subscribe(queue:)`),
  `Unsubscribe`, `Request` / `RequestMsg`, `Flush` / `FlushTimeout`, `Drain`,
  `Close`, `IsClosed`, and the connection callbacks `OnError`, `OnReconnect`,
  `OnDisconnect`, `OnClose`.
- **`Msg`** — `Subject`, `Data`, `Reply`, `Header` / `Headers`, `Respond` /
  `RespondMsg`.
- **`Subscription`** — `Unsubscribe`, `AutoUnsubscribe`, `Drain`.
- **`Header`** — `Get` / `Values` / `Set` / `Add` / `Del`.
- **`Error` tree** — `ErrTimeout` (`NATS::IO::Timeout`), `ErrNoResponders`
  (`NoRespondersError`), `ErrConnectionClosed`, `ErrConnectionDraining`,
  `ErrBadSubscription`, `ErrBadSubject`, `ErrNoServers`, `ErrMaxPayload`, … —
  each a sentinel matched with `errors.Is`.

Connection options mirror the gem's `NATS.connect` keywords: `Servers`, `Name`,
`UserInfo`, `Token`, `MaxReconnects`, `ReconnectWait`, `Timeout`, and the
`*Callback` registrations. `NATSOption` threads a raw `nats.go` option through for
transport features (TLS, JWT, in-process server) not surfaced by the gem-shaped
options.

## Host seam

The transport is **injectable**. `Connect` dials a real server through `nats.go`
by default, but the dialer is a seam, so the whole client can run against an
in-process broker or an embedded `nats-server` started with
`nats.InProcessServer` — **no external socket in unit tests**.

## Tests & coverage

`go test ./...` runs two suites, both verifying **real round-trips** (delivery,
queue groups, request/reply, timeout, unsubscribe, drain, headers) rather than
asserting on internals:

- a deterministic **in-process broker** that drives the client with no socket and
  runs on every arch (including under qemu), and
- an **embedded `nats-server`** (in-process, `DontListen`) that validates the real
  `nats.go` adapter end to end on the native lanes.

Coverage is **100% of statements**, enforced in CI with `-race` across three OSes
and all six 64-bit arches (`CGO_ENABLED=0`, s390x big-endian under qemu).

## License

BSD-3-Clause — see [LICENSE](LICENSE). Copyright (c) 2026, the go-ruby-nats/nats
authors.

## WebAssembly

Being pure Go (CGO=0), this library also compiles to **WebAssembly** — both
`GOOS=js GOARCH=wasm` (browser / Node.js) and `GOOS=wasip1 GOARCH=wasm` (WASI).
CI builds both targets on every push, alongside the six 64-bit native/qemu arches.

```sh
GOOS=js     GOARCH=wasm go build ./...   # browser / Node
GOOS=wasip1 GOARCH=wasm go build ./...   # WASI (wasmtime, wasmer, wasmedge, …)
```
