# Transaction events for `sql.subscribe()`

One event per database transaction, carrying all of its changes (mixed
insert/update/delete) as an async iterable — instead of upstream's per-row-only events.
Implemented via pgoutput `proto_version '2'` with `streaming 'on'` (PostgreSQL 14+), so
large transactions are delivered in chunks while they are still in progress rather than
being buffered server-side until commit.

## API

```js
const sql = postgres({ publications: 'alltables', subscribe_high_water_mark: 1024 })

sql.subscribe('transaction', async (changes, info) => {
  // info: { xid: number, streaming: boolean, lsn: string|null, date: Date|null }
  try {
    for await (const c of changes) {
      // c: { command: 'insert'|'update'|'delete', row, old, relation, xid }
      //  | { command: 'truncate', relations, cascade, restartIdentity, xid }
      //  | { command: 'abort', xid }   ← subtransaction rollback marker
    }
    // iterator ended = COMMIT; info.lsn ('X/XXXXXXXX') and info.date are now set
  } catch (err) {
    // whole-transaction abort OR connection loss — discard/rollback local work
  }
}, onsubscribe, onerror)
```

## Semantics

1. **Delivery** — async iterator per transaction. Small (non-streamed) transactions are
   decoded at commit and arrive complete: the iterator is effectively a ready list.
   Streamed transactions (decoded size > server `logical_decoding_work_mem`, default
   64MB) yield changes before their commit is known.
2. **Lazy fire** — the callback fires on the first actual change, never on bare
   Begin/Stream Start: empty transactions and empty stream segments produce no event.
   The subscriber set is snapshotted at first change; late subscribers join at the next
   transaction. The one exception is `subscribe_tables` (§16): with a filter configured a
   transaction that reached commit with nothing left fires at commit instead, empty.
3. **Per-row events are disabled** — `subscribe()` accepts only `'transaction'`; any
   other event (`'*'`, `insert`, `update:users`, …) throws
   `Only the transaction event is supported in this fork`. Rationale: per-row events
   would silently skip rows inside streamed transactions (buffer-and-replay was
   deliberately dropped), so allowing them invites silent data loss — failing loudly is
   safer. The upstream fan-out machinery is kept intact (unreachable) to keep the diff
   against upstream minimal; re-enabling is a one-function revert of `parseEvent`
   (see Future work).
4. **Subtransaction aborts** (ROLLBACK TO SAVEPOINT inside a streamed transaction) — the
   iterator yields `{ command: 'abort', xid: subxid }`. Every change carries its
   (sub)transaction xid, so a consumer applying changes inside its own DB transaction can
   `SAVEPOINT` whenever `change.xid` switches and roll back to that savepoint on a
   marker. Descendant subtransactions abort first, so nesting composes. Consumers that
   cannot compensate should treat a marker as fatal for the whole transaction.
5. **Top-level abort** (subxid == xid) — the iterator rejects.
6. **Backpressure** — a shared counter of queued-but-unconsumed changes across all live
   iterators. Above `subscribe_high_water_mark` (default 1024) the replication stream is
   paused (propagates to the socket → the server stops sending); below HWM/4 it resumes.
   While paused, a 15s unref'd interval keeps writing standby-status updates so
   `wal_sender_timeout` (default 60s) never kills the connection. The pause threshold is
   advisory (a few KiB of in-flight data still arrives after pausing).
7. **Concurrency** — Postgres interleaves stream segments of concurrent large
   transactions, so multiple iterators can be live at once, each ending at its own
   commit/abort. No cross-transaction serialization; order by `info.lsn` if needed.
   One stalled consumer pauses the single replication stream for all subscribers
   (head-of-line blocking) — keep consumers moving or unsubscribe them.
8. **Reconnects — at-most-once (temporary slot)** — on stream close every live iterator
   rejects; the TEMPORARY replication slot is recreated at the current WAL position on
   reconnect, so events resume for new transactions only and anything in between is lost.
   This is the same guarantee upstream's per-row subscribe has. With a durable slot
   (§15) the guarantee becomes at-least-once instead.
9. **TRUNCATE** — a truncate arrives as a single change
   `{ command: 'truncate', relations: [relation, ...], cascade: boolean, restartIdentity: boolean, xid }`
   (one message may cover several tables: explicit multi-table truncate or CASCADE via
   foreign keys). `relations` entries have the same shape as `change.relation`. Works in
   buffered, streamed, and proto v1 fallback paths; counts as a "first change" for lazy
   fire. Requires the publication to publish truncate (default for
   `CREATE PUBLICATION ... FOR ALL TABLES`).
10. **Version gate** — server ≥ 14 → `proto_version '2', streaming 'on'`; otherwise
   `proto_version '1'` with the streaming option omitted entirely (PG ≤ 13 rejects any
   `streaming` option). The fallback assembles Begin..Commit in memory: same API,
   `info.streaming === false` always.
11. **LSN format** — `'X/XXXXXXXX'` uppercase unpadded (Postgres `%X/%X`), e.g.
    `16/B374D848`. `info.lsn`/`info.date` are null until commit.
12. **Ack discipline** — with a temporary slot, unchanged from upstream (keepalive walEnd /
    Begin final_lsn is acked before delivery). Safe only because the slot is TEMPORARY —
    there is never a replay. With a durable slot the flushed/applied positions are
    consumer-driven instead (§15).
13. **Callback safety** — subscriber callbacks are invoked guarded, so a throwing
    consumer cannot kill the replication connection.
14. **Filters** — `'transaction'` accepts no path/key filter; `parseEvent` throws on
    `transaction:<anything>` and on any non-transaction event (see §3).
15. **Durable slots** — `postgres({ slot })`, or `subscribe(event, fn, onsubscribe,
    onerror, { slot })`. The slot is created without `TEMPORARY` (an existing one is
    reused: `42710` on create is the resume case, not an error), `START_REPLICATION`
    passes `0/0` so the server resumes from `confirmed_flush_lsn`, and reconnects keep the
    name instead of randomising it. Ack discipline:
    - `state.lsn` (received) is reported as *written*; a separate `acked` position is
      reported as *flushed* and *applied*, and only that moves `confirmed_flush_lsn`.
      *written* is `max(received, acked)` so it never trails the confirmation.
    - `acked` is seeded from the slot's `confirmed_flush_lsn` at startup — never `0/0`,
      which the server would take literally and write back into the slot.
    - A transaction is acked when every handler's returned promise has resolved, or on an
      explicit `info.ack()`. Handlers that return a non-promise cannot be waited for and
      ack at once. Confirmation is by commit-order prefix: a pending transaction holds
      back every later one.
    - The confirmed position is the commit message's **end_lsn** (`C` offset 10, `c`
      offset 14), not its commit_lsn — the same position `pg_recvlogical` reports, so a
      handled transaction is not redelivered.
    - A rejecting or throwing handler deliberately does *not* ack: the slot stalls (loudly,
      on the console and in `pg_replication_slots`) rather than dropping the transaction.
    - Keepalives advance `acked` to the server's walEnd only when nothing is outstanding
      (no queued commit, no open Begin, no live streamed transaction); otherwise the slot
      would pin WAL for every unpublished write.
    - Acks are coalesced onto a 100ms unref'd timer, and a keepalive with reply-requested
      still answers immediately.
    - Lifecycle: `sql.end()` leaves the slot in place; `subscription.drop()` ends the
      stream, `DROP_REPLICATION_SLOT <name> WAIT` (retried on `55006` while the walsender
      releases it), then ends the connection. An abandoned slot retains WAL indefinitely —
      see `max_slot_wal_keep_size`.
    - Slot names are validated against `[a-z0-9_]{1,63}` (they are interpolated into the
      replication commands, which take no parameters).
    - `onsubscribe({ slot, resumed })` on the initial connect and every reconnect.
      `resumed` is true when the connect resumed an existing durable slot, false when it
      created one at the current position (temporary slots: always false). On a
      reconnect, `resumed: false` means the retained history is gone.
    - An invalidated slot (`wal_status = 'lost'`: it outgrew `max_slot_wal_keep_size` while
      disconnected) can never stream again - `START_REPLICATION` fails with `55000` on
      every attempt. The connect drops it and creates it afresh, logs that, and reports
      `resumed: false`. Servers before PG 13 have no `wal_status` and never invalidate.

16. **`subscribe_tables`** — an instance option restricting which relations are decoded:
    an array of `schema.table` names, or a `(schema, table) => boolean` predicate. The
    decision is taken once per relation, when the `R` (Relation) message arrives: a relation
    that fails the test is remembered as `null` and its column list is not even read, so
    `I`/`D`/`U` drop their message on the relation lookup, before `tuples()`. `T` (truncate)
    keeps the relations that passed — one message can cover several tables — and is dropped
    when none did. An oid that was never announced stays `undefined` and still throws, so a
    protocol violation is not silently swallowed.
    - Array entries must be schema qualified; an unqualified name throws at `postgres()`
      time rather than silently matching nothing.
    - A transaction whose changes were **all** filtered out still fires the handler — empty
      change iterator, then commit — so a consumer tracking position still sees its commit
      lsn, and a durable slot still gets its ack. This also means a genuinely empty
      transaction (PG < 15 sends Begin/Commit for those) fires while a filter is configured.
      Streamed transactions get the same treatment at Stream Commit.
    - Why not a publication: a publication is server state shared by every consumer of the
      slot, cannot differ per connection, and changing it at runtime would disturb every
      other consumer. The motivating consumer is a relay holding one slot for many apps,
      each watching a subset that changes as its live queries come and go.
17. **`subscribe_raw`** — an instance option delivering every value as the text form
    pgoutput sent it: `null` for SQL NULL, `undefined` for an unchanged TOASTed column, the
    row still keyed by column name. Implemented as "no parser anywhere" — the parser map
    handed to `parse()` is empty, so every column takes the text branch in `tuples()`, and
    `transform.value.from` is dropped for the stream. `transform.column`/`transform.row`
    still apply: they shape the row, they do not decode it. `relation.columns[i].type` (the
    type oid) and `.atttypmod` already travel with the relation, so a consumer can parse
    later, elsewhere — with *its* parsers and `transform`, not the relay's. Distinct from
    `transform.raw`, which changes the row's shape (an array) and still parses each value;
    with both set the row is an array of unparsed text.

## Protocol notes (pgoutput v2)

- New messages: `S` Stream Start (xid, first-segment flag), `E` Stream Stop, `c` Stream
  Commit (xid, flags, commit_lsn, end_lsn, ts), `A` Stream Abort (xid, subxid).
- Inside stream segments, `R`/`I`/`U`/`D` (and `Y`/`T`/`M`) carry an extra Int32 xid
  right after the type byte — all offsets shift by 4. That per-message xid is the
  **subtransaction's** xid; segment routing must key off Stream Start's (top-level) xid.
- `B` Begin carries xid at offset 17; `C` Commit carries flags(1), commit_lsn(2-9),
  end_lsn(10-17), ts(18-25). Non-streamed B..C blocks never interleave.

## Future work (saved follow-ups — do not lose)

- **Re-enable per-row events + buffer-and-replay for streamed transactions** — required
  before offering this feature upstream as a PR: revert the `parseEvent` guard (the
  upstream fan-out machinery is still in place) and add buffer-and-replay so per-row
  subscribers get committed-only semantics for huge transactions. The fork intentionally
  disables per-row events entirely (see Semantics §3).
- `subscribe_high_water_mark` is the only knob in v1; LWM is fixed at HWM/4.
