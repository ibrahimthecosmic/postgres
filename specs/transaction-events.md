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
   transaction.
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
8. **Reconnects — at-most-once** — on stream close every live iterator rejects; the
   TEMPORARY replication slot is recreated at the current WAL position on reconnect, so
   events resume for new transactions only and anything in between is lost. This is the
   same guarantee upstream's per-row subscribe has.
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
12. **Ack discipline** — unchanged from upstream (keepalive walEnd / Begin final_lsn is
    acked before delivery). Safe only because the slot is TEMPORARY — there is never a
    replay. Must be revisited if durable slots are added.
13. **Callback safety** — subscriber callbacks are invoked guarded, so a throwing
    consumer cannot kill the replication connection.
14. **Filters** — `'transaction'` accepts no path/key filter; `parseEvent` throws on
    `transaction:<anything>` and on any non-transaction event (see §3).

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
- **Durable named slots + commit-LSN acking** — at-least-once delivery across
  reconnects; requires ack discipline changes (see Semantics §11).
- `subscribe_high_water_mark` is the only knob in v1; LWM is fixed at HWM/4.
