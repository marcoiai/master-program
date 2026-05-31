# Mesh Protocol v0

`master-program` is gaining a local-first mesh protocol layer. The goal is not to
replace the app or the existing local stream flow. The goal is to move the
current HTTP-only peer behavior behind a reusable protocol architecture that can
support more transports later.

## Current Stack

- Rust 2024 binary crate.
- Axum HTTP API on port `17321` by default.
- Server-sent events on `/v1/events`.
- JSON is the v0 wire format.

## Core Concepts

### Node

A node is a running `master-program` instance. It has:

- `nodeId`
- optional `displayName`
- capabilities
- known transports
- optional `transmissionProfile`

`transmissionProfile` is an identity and budget hint. For example,
`pirate_broadcast` intentionally accepts scanlines, color crawl, lower FPS, and
lower bitrate. This is aesthetic identity, not protocol corruption.

### Peer

A peer is a known node. The legacy payload still works:

```json
{ "id": "m1", "url": "http://192.168.100.7:17321" }
```

New registrations can describe transports directly:

```json
{
  "id": "m1",
  "displayName": "MacBook M1",
  "transport": "bluetooth"
}
```

Unsupported transports are registered as unavailable rather than pretending they
failed over HTTP.

### Envelope

All protocol messages use a `ProtocolEnvelope`:

- `protocolVersion`
- `messageId`
- optional `correlationId`
- `sourceNodeId`
- optional `targetNodeId`
- `destination`
- `timestamp`
- `ttl`
- `hopCount`
- `messageType`
- `payloadType`
- `payload`
- optional `checksum`
- optional `metadata`

The serialized JSON envelope hard limit is `64 KiB`. Control messages should
target `2 KiB` or less.

### Transport

The transport interface supports:

- `start`
- `stop`
- `send`
- `broadcast`
- `status`
- `capacity`
- `capabilities`

Implemented in v0:

- `local_http`: wraps the existing local HTTP ping path.
- `loopback`: in-memory transport for tests and simulation.

Stubbed in v0:

- `webrtc`
- `bluetooth`
- `serial`
- `store_forward`

These stubs do not add dependencies and report explicit unavailable status.

## Routing and Error Control

The mesh engine:

- registers transports
- stores peers
- sends direct messages
- receives envelopes
- deduplicates by `messageId`
- enforces `ttl` / `hopCount`
- tracks peer telemetry

Error control in v0 is intentionally practical:

- malformed envelopes are rejected
- duplicate message IDs are dropped
- oversized payloads are rejected
- streams ACK/NACK chunks
- stream retransmits are bounded
- sessions can timeout, cancel, or error

Checksum is integrity detection only. It is not encryption or security.

## Capacity, Lag, and Rate Limits

Defaults:

- per-peer control lane: `20 msg/s`, burst `40`
- per-peer stream lane: `200 KiB/s`, burst `400 KiB`
- global node cap: `2 MiB/s` inbound and outbound
- stream chunk target: `4 KiB`
- healthy adaptive ceiling: `8 KiB`
- max in-flight chunks: `8`

Telemetry:

- RTT
- ACK delay
- queue depth
- dropped messages
- retransmits
- rate-limited count

Escalation states:

- `normal`
- `warn`
- `throttle`
- `degrade`
- `fail_safe`

Control messages have priority over stream chunks. If the node is under load,
stream data is throttled first.

## Stream Protocol

Stream messages:

- `stream.open`
- `stream.chunk`
- `stream.ack`
- `stream.nack`
- `stream.end`
- `stream.cancel`
- `stream.error`

The v0 stream session implementation is text-oriented and validates ordering,
missing chunks, duplicate chunks, cancellation, timeout, and reassembly.

Large binary ROM/video transfer is not forced through this layer yet. Existing
local stream behavior should keep working while the protocol grows around it.

## Dynamic Scene Layers

Scene layers are a general control surface for UI/runtime composition. A TV is
only one possible layer.

Layer message families:

- `scene.layer.upsert`
- `scene.layer.patch`
- `scene.layer.show`
- `scene.layer.hide`
- `scene.layer.move`
- `scene.layer.remove`
- `scene.layer.setSource`
- `scene.layer.setProfile`

Example:

```json
{
  "layerId": "center-tv",
  "type": "mediaEmbed",
  "source": {
    "kind": "youtube",
    "url": "https://www.youtube.com/embed/abc123"
  },
  "presentation": {
    "visible": true,
    "slot": "center-wall",
    "motion": "drop_from_top",
    "profile": "pirate_broadcast",
    "zIndex": 40
  }
}
```

The protocol controls the layer. The browser/Tauri frontend renders the media.
YouTube bytes are not relayed through the mesh.

## Current API Surface

Existing routes remain:

- `GET /v1/health`
- `GET /v1/capabilities`
- `POST /v1/ping`
- `GET /v1/mesh/node`
- `GET /v1/mesh/peers`
- `POST /v1/mesh/connect`
- `POST /v1/mesh/peers/register`
- `POST /v1/mesh/peers/{id}/ping`
- `GET /v1/scene-state`
- `POST /v1/scene-state`
- `GET /v1/events`

New additive routes:

- `POST /v1/mesh/envelope`
- `POST /v1/scene-layers`
- `DELETE /v1/scene-layers/{id}`

### Manual Node Connect

Any node can receive connections when it is listening on a reachable interface:

```bash
MASTER_PROGRAM_HOST=0.0.0.0 cargo run
```

Another node can connect to it with:

```bash
curl -X POST http://127.0.0.1:17321/v1/mesh/connect \
  -H 'Content-Type: application/json' \
  -d '{ "url": "http://192.168.100.7:17321" }'
```

The local node will:

1. fetch the remote `/v1/mesh/node`,
2. register the remote node as a peer,
3. ping the remote node to prove reachability,
4. return the stored peer plus `reachable`.

This is the current v0 flow for LAN and hotspot mode. It is manual by design:
there is no central server requirement and no automatic radio discovery yet.

## Limitations

- No automatic offline nearby discovery yet.
- No native Bluetooth, Wi-Fi Direct, LoRa, or USB code yet.
- HTTP is still the only real peer transport.
- Peers are in memory only.
- The protocol is not secure, encrypted, or production-hardened.

## QA

Run:

```bash
cargo fmt
cargo test
cargo check
```

Manual checks:

- Existing peer registration with `{ "id", "url" }` still works.
- `POST /v1/mesh/connect` registers a reachable remote node by URL.
- `POST /v1/mesh/peers/{id}/ping` still uses HTTP for HTTP peers.
- Registering a non-HTTP peer returns clear unavailable transport behavior.
- `GET /v1/scene-state` includes `layers` and keeps old ambience/object fields.
- `POST /v1/scene-layers` can upsert a media layer.
- `DELETE /v1/scene-layers/{id}` removes a layer.
