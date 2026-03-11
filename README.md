# rustmq

A task queue built from scratch in async Rust. No external queue dependencies — everything from the persistence layer to the TCP broker is implemented here.

## What it does

Clients submit tasks over TCP. Workers pull from a priority queue, execute them concurrently, and report back results. Failed tasks are retried with exponential backoff. Tasks that exhaust their retries move to a dead letter queue. System metrics are exposed via a Prometheus-compatible HTTP endpoint.

## Features

- **Write-Ahead Log** — every state change is written to disk before being applied in memory. Crash the process mid-run and re-start; tasks are recovered automatically.
- **Priority queue** — tasks have a priority field (0–255). Higher priority tasks are processed first. Equal-priority tasks are FIFO.
- **Retry with exponential backoff** — failed tasks are re-queued with an increasing delay (`base * 2^attempt + jitter`). Once `max_retries` is exceeded the task is dead-lettered.
- **Dead letter queue** — tasks that exhaust retries are parked here for inspection rather than silently discarded.
- **Backpressure** — the queue has a configurable capacity. When full, new submissions get a `Reject` response instead of silently dropping.
- **Worker heartbeats** — each worker pings a liveness tracker while executing. A background monitor declares workers dead if they miss their heartbeat window and re-queues any in-flight tasks.
- **Binary TCP protocol** — a custom framing layer (`Magic + Type + Length + Payload`) instead of HTTP. Compact headers, easy to parse, no text overhead.
- **Prometheus metrics** — a `/metrics` HTTP endpoint that exposes pending count, dead letter count, submit/complete/fail/dead-letter totals, and uptime.
- **Graceful shutdown** — a `CancellationToken` propagates through all workers and server loops. Everything stops cleanly on shutdown.
- **WAL checkpointing** — after N writes the WAL snapshots the full task state and truncates the log file, keeping recovery fast regardless of uptime.

## Project structure

```
src/
├── main.rs          # entry point + demo
├── task.rs          # Task struct and TaskStatus enum
├── error.rs         # unified AppError type
├── wal/
│   ├── mod.rs       # write-ahead log (append, recover, checkpoint)
│   └── checkpoint.rs
├── queue/
│   └── mod.rs       # in-memory priority queue wrapping the WAL
├── worker/
│   ├── mod.rs       # Pool supervisor and worker loop
│   ├── heartbeat.rs # per-worker liveness tracking
│   └── retry.rs     # exponential backoff calculation
├── broker/
│   ├── mod.rs       # TCP server + accept loop
│   └── connection.rs
├── protocol/
│   ├── mod.rs
│   ├── frame.rs     # wire format constants and FrameError
│   ├── codec.rs     # tokio-util Encoder/Decoder
│   └── message.rs   # typed message structs
└── metrics/
    └── mod.rs       # Prometheus HTTP endpoint + atomic counters
```

## Running

```bash
cargo run
```

The demo submits 8 tasks with different priorities and failure rates, scrapes the metrics endpoint at two points during processing, then shuts down cleanly.

```bash
# In a separate terminal while the demo is running:
curl http://127.0.0.1:8888/metrics
```

## Wire protocol

Each frame on the TCP stream has a 7-byte header followed by a JSON payload:

```
[Magic: 2B] [Type: 1B] [Length: 4B] [Payload: N bytes]
```

`Magic = 0xDEAD` — if the decoder ever sees a different value it closes the connection immediately rather than trying to interpret garbage data.

| Type byte | Message   | Direction       |
| --------- | --------- | --------------- |
| `0x01`    | Submit    | client → broker |
| `0x02`    | Ack       | broker → client |
| `0x03`    | Reject    | broker → client |
| `0x04`    | Heartbeat | worker → broker |
| `0x05`    | Complete  | worker → broker |
| `0x06`    | Fail      | worker → broker |

## WAL record format

Each entry on disk is:

```
[CRC32: 4B] [Length: 4B] [bincode-encoded WalRecord]
```

On startup the WAL is replayed from top to bottom. A CRC mismatch means the write was interrupted mid-crash — that record and everything after it is discarded. This means at most one task might be duplicated on recovery (the client would retry if it never got an Ack), but tasks are never silently lost.

## Stack

| Crate               | Purpose                  |
| ------------------- | ------------------------ |
| `tokio`             | async runtime            |
| `tokio-util`        | `Framed` codec helpers   |
| `serde` + `bincode` | WAL record serialization |
| `serde_json`        | TCP message payloads     |
| `crc32fast`         | WAL record checksums     |
| `thiserror`         | error types              |
| `uuid`              | task IDs                 |
| `chrono`            | timestamps               |
| `rand`              | backoff jitter           |
| `tracing`           | structured logging       |

## Tests

```bash
cargo test
```

The codec module has unit tests covering round-trip encoding for all message types, partial-read handling, bad magic, and oversized payloads.
