# Detached fork of porsager/postgres

This repository is a detached fork of [porsager/postgres](https://github.com/porsager/postgres)
carrying the **transaction events** feature (`sql.subscribe('transaction', ...)` — see
[specs/transaction-events.md](specs/transaction-events.md)).

## Branches

| Branch        | Purpose                                                                  |
|---------------|--------------------------------------------------------------------------|
| `main`        | Our code. Default branch; all fork releases are tagged here.             |
| `postgres-js` | Pristine mirror of upstream at the exact release tag we're currently based on. Never tracks upstream master. |
| `upgrade/v*`  | Temporary branches used to merge an upstream release into `main`.         |

No upstream tags are kept in this repository (`remote.upstream.tagOpt --no-tags` is set).

## Versioning

Only the **major** version aligns with upstream. We bump minor/patch independently for our
own releases (upstream 3.4.9 → fork releases 3.5.0, 3.5.1, …). The major version changes
only when upstream releases a new major.

## Upgrading to a new upstream release

```sh
# 1. Point postgres-js at the upstream tag (fetches into FETCH_HEAD, creates no tags)
scripts/sync-upstream.sh v3.4.10

# 2. Merge into an upgrade branch off main
git checkout -b upgrade/v3.4.10 main
git merge postgres-js
# resolve conflicts — expected hotspots: src/subscribe.js, types/index.d.ts,
# tests/index.js, README.md, and the generated cjs/ deno/ cf/ directories
# (for generated dirs, take either side and regenerate)

# 3. Rebuild generated targets and run the full suite
pnpm run build
pnpm test

# 4. Land it
git checkout main
git merge --no-ff upgrade/v3.4.10
git branch -d upgrade/v3.4.10
```

There is also a Claude Code skill encoding this runbook: `.claude/skills/upgrade-upstream/`.

## Installing the fork in applications (pnpm)

The package name stays `postgres`, so imports don't change. Generated `cjs/`, `deno/` and
`cf/` outputs are committed, and `prepare` rebuilds them, so git installs work directly:

```sh
pnpm add 'postgres@github:<owner>/postgres#v3.8.5'
```

Upgrading an app = bump the tag in `package.json`, `pnpm install`. For a private repo, CI
needs an SSH key or token that can read it.

Alternative for many projects / cleaner CI: publish as a scoped package and alias it, so
imports still resolve to `postgres`:

```sh
pnpm add postgres@npm:@<owner>/postgres@3.8.5
```

## Fork-specific behavior differences from upstream

- `sql.subscribe('transaction', fn)` — one async-iterable event per database transaction
  (pgoutput proto_version 2 + streaming on PG 14+; buffered fallback on PG < 14).
- Per-row subscribe events (`'*'`, `'insert:users'`, …) are **disabled** — `subscribe()`
  throws on any event other than `'transaction'`. They could not see rows inside streamed
  transactions (decoded size > server `logical_decoding_work_mem`, default 64MB), so they
  were turned off rather than left silently lossy. The upstream fan-out code is kept
  intact; re-enabling is a one-function revert of `parseEvent` in `src/subscribe.js`.
- TRUNCATE is delivered to transaction iterators as
  `{ command: 'truncate', relations, cascade, restartIdentity, xid }` (upstream ignores it).
- New option `subscribe_high_water_mark` (default 1024).
- `TransactionInfo.end` (3.8.2): the commit record's end LSN — exactly what a durable
  slot confirms for the transaction, so a consumer that records resume points records
  the position the slot itself will resume from. `info.lsn` stays the commit LSN.
- `SubscriptionHandle.position` (3.8.2): the position the stream confirms on its next
  status update (the durable slot's resume point, keepalive advances included), so a
  consumer that hands out resume points can hand out exactly what the slot will honor.
- A **failed retry reports its own error** (3.8.3). A prepared statement whose cached plan
  the server invalidated is re-prepared and run again; upstream answers a second failure
  with the error that *triggered* the retry, so a `0A000` "cached plan must not change
  result type" masked whatever the re-run actually hit — a `22001` the caller's own data
  caused, for instance. The retry's error is now what surfaces, with the trigger kept as
  `error.cause`. Only the failing-retry path changes; a retry that succeeds is unaffected.
- New options `subscribe_tables` and `subscribe_raw` for `sql.subscribe('transaction')`.
  `subscribe_tables` (an array of `schema.table` names, or a `(schema, table) => boolean`)
  drops changes to every other relation as soon as the relation id is read — before any tuple
  is decoded — and narrows a `truncate` to the relations that passed; a transaction whose
  changes were all filtered still fires with an empty iterator so its commit lsn is seen.
  An array is resolved once per relation and remembered; a predicate is asked per change, so
  a consumer can widen or narrow what it watches at runtime and the next change obeys.
  `subscribe_raw` delivers every value as the text form pgoutput sent (`null` for NULL,
  `undefined` for an unchanged TOASTed column), keyed by column name, with the type oid and
  `atttypmod` on the relation — parsing is left to whoever consumes the change. Both exist
  for consumers that forward or ignore most of a publication (a relay fanning one slot out to
  many apps): the publication is shared server state and cannot be narrowed per connection.
- New option `slot` — a durable (named, non-TEMPORARY) replication slot. Streaming resumes
  from the slot's `confirmed_flush_lsn` on reconnect, so delivery becomes at-least-once
  instead of upstream's at-most-once. The slot only advances as the consumer acks
  (the transaction handler's promise resolving, or `info.ack()`), survives `sql.end()`, and
  is removed by the subscription's new `drop()`.
- Array decoding maps an unquoted `NULL` element to `null` (upstream hands the token to the
  element parser, yielding `NaN` / `'NULL'`); the quoted literal string `"NULL"` stays a string.
- `subscribe()` takes a fifth options argument (`{ slot }`), and the subscription handle
  gained `drop()` and `slot`. `onsubscribe` receives `{ slot, resumed }` — `resumed: false`
  on a reconnect means the durable slot had to be recreated (dropped or invalidated while
  away) and its retained history is gone; an invalidated slot is recreated automatically.
- `sql.end()` settles when a connection that was still connecting dies with nothing left to
  do (upstream parks it on a promise only `terminate()` resolves, so `end()` after a refused
  connection hangs forever on Node when a second connection was mid-connect).
- `query.cancel()` returns the CancelRequest's promise (upstream drops it on a comma
  operator and returns `null`). That promise rejects when the second connection the
  CancelRequest needs cannot be opened; dropped, it is an unhandledRejection — fatal on
  Node by default. Callers may ignore the return value as before.
- A query cancelled before it reached the wire no longer strands the connection it was
  given. Upstream's `ReadyForQuery` returns right after `execute(initial)`, which writes
  nothing for a cancelled query (and is skipped entirely for a `reserve`), so the
  connection stays in the `connecting` queue with no further `ReadyForQuery` coming: the
  pool loses a slot per occurrence (every query hangs after `max` of them) and an `end()`
  awaited meanwhile is never settled. Also fixes `sql.reserve()` hanging forever with
  `fetch_types: false`, which took the same early return.
- `cancel()` on a query that was never dispatched leaves it rejected but no longer
  dispatchable, so a later `then()`/`catch()` doesn't hand it to the pool. Upstream opens
  (or takes) a connection to run nothing and parks it in the `full` queue — stuck for good
  on an idle connection, and inside `sql.begin`/`sql.reserve` the rest of the scope's
  queries stall behind it.
- A query cancelled while parked in a `sql.begin`/`sql.reserve` scope's own queue is skipped
  when that queue is drained. Upstream spends the scope's next turn on it, and since nothing
  was written for a cancelled query no further `ReadyForQuery` arrives: the scope stalls
  forever with its remaining statements queued and its server-side transaction open. Any
  cursor inside the scope is enough to park queries there.
- **A connection whose backend dies fails what it owed, then forgets it** (3.8.4). A
  terminated backend (`pg_terminate_backend`, a restart, a failover) sends a FATAL such as
  `57P01` and closes. The in-flight query now fails with that error, where upstream gave it
  a generic `CONNECTION_CLOSED`, or `ECONNRESET` when the reset came first. Upstream also
  kept the dead socket's query, error and row counter: the reopened connection handed the
  stale `57P01` to the next query, which never ran, and a `sql.end()` waiting on the dead
  query never settled. Work sent between the socket's `error` and `close` events is failed
  too, where upstream left it waiting.
- The row counter restarts with every query (3.8.4). Upstream reset it only on
  `CommandComplete`. A query that failed partway through its rows left the next query's
  rows offset by that many, so its result started with holes: ``const [row] = await sql`…` ``
  read `undefined`. This needs no connection fault, any error mid-stream is enough (a
  division by zero in row three will do). Upstream issue #1181.
- **Cursors surface a connection that dies between batches** (3.8.4). An async-iterated
  `cursor()` whose connection closes while the caller holds a batch throws the error from
  the next `next()`. Upstream ends the loop as if every row had been read. A `cursor(n, fn)`
  whose callback outlives its connection no longer writes to the dead socket once the
  callback returns, nor to the reopened one that now serves another query. Upstream
  crashes the process there, with a TypeError outside any promise.
- **`sql.begin` and `sql.reserve` send nothing once their connection closed** (3.8.4). The
  pool takes the connection back and may reopen it for someone else. A transaction or
  reserved handle whose connection died now rejects what it had queued and everything it
  sends afterwards, commit and rollback included. `release()` of such a handle is a no-op.
  Upstream queued the transaction's rollback onto the closed connection and crashed the
  process (an uncaught TypeError in `nextWrite`). Once the pool had reopened the
  connection, it ran the handle's statements, and even its commit or rollback, in a
  different session. Releasing a dead reserved connection broke the pool for good.
- **Subscriptions no longer print on stream loss** (3.8.4). Each replication stream, the
  first and every reconnect's, reports its loss once to every subscriber's `onerror`, and
  nothing goes to `console.error`. Upstream attached `onerror` to the first stream only,
  so after a reconnect the application no longer heard of outages, while the library kept
  logging "Unexpected error during logical streaming". A temporary-slot transaction handler
  that rejects is reported to its own subscriber's `onerror`, where it used to get that
  same log line.
- **A FATAL that arrives while a connection is idle is its close's reason** (3.8.5). The
  server ends an idle session with a FATAL of its own: `25P03` for
  `idle_in_transaction_session_timeout`, `57P05` for `idle_session_timeout`, `57P01` for a
  terminate. Nothing was in flight to receive it, so upstream dropped it, and the
  transaction or reserved handle whose connection it was failed with a bare
  `CONNECTION_CLOSED`. The error is now kept: it is what the close reports to a
  `sql.begin` / `sql.reserve` scope, and what the connection's next query is rejected with.
  It is forgotten when the connection reopens.
- **An invalidated durable slot is reported to `onerror`, not printed** (3.8.5). Recreating
  a slot the server invalidated (`wal_status = 'lost'`) wrote a line to `console.error`.
  Each subscriber's `onerror` now receives an error with code `SLOT_INVALIDATED` before the
  slot is recreated, and `onsubscribe`'s `resumed: false` follows as before.
- **Arrays with a non-default lower bound decode** (3.8.5). Postgres prefixes such a
  value with its dimensions (`[0:1]={7,8}`), which upstream parsed as one more level of
  nesting (`[[7,8]]`). The prefix is dropped with its bounds, as `to_jsonb` drops them, and
  the value decodes as `[7,8]`. Multi-dimensional values keep decoding as nested arrays.
- **`query.startedAt` and `sql.stats()`** (3.8.7). Each query records `startedAt`
  (`performance.now()`) when it becomes the query its connection's backend works on: written
  to an idle connection, or, pipelined behind another statement, once that statement
  completes. `0` until then. The time from dispatch to `startedAt` is the wait for a
  connection or behind another statement; from `startedAt` to settle is the statement's own.
  `sql.stats()` returns `{ max, open, busy, idle, waiting }` from the pool's queues: its
  size, connections open or opening, those serving a query, transaction or `reserve()`,
  those idle, and the queries (and `reserve()` calls) dispatched with no connection yet.
  Upstream has neither, so a client timing a query from dispatch counts a saturated pool's
  queue as query time.
- **A released `reserve()` and an ended `begin` scope refuse every later query** (3.8.8).
  Upstream kept executing through them on the connection they had held, after it went
  back to the pool: a second `rollback` sent through a released reservation ran inside
  whichever transaction held the connection next, and rolled it back; a scope that escaped
  its `sql.begin` callback did the same. A query through a released reservation now rejects
  with code `RESERVATION_RELEASED` (queries it had parked are rejected at `release()`), one
  through a scope whose transaction ended with `TRANSACTION_ENDED`, and neither touches the
  connection. A second `release()` is a no-op.
- **An idle pool does not keep the process alive** (3.8.8). A connection's socket is
  unref'd while it sits idle in the pool and ref'd again the moment work is queued on it,
  and the idle and lifetime timers are unref'd, so a script that never calls `sql.end()`
  exits when its work is done. Upstream's idle sockets held the event loop forever
  (`idle_timeout` defaults to none). A LISTEN connection stays ref'd, as does every
  connection once `end()` has begun, since `end()` settles on the sockets' close. The Deno
  build's socket polyfill has no ref/unref and is unchanged.
- **`end()` settles when a reservation is released after it began** (3.8.8). A `reserve()`d
  connection has its `end()` deferred until it is released; upstream's `release()` then
  put it back in the pool as an idle connection, and nothing ever settled that `end()`.
  It now closes, like any other connection of an ending pool.
- **A cancelled statement on a connection that stopped answering loses the connection**
  (3.8.9). A CancelRequest only reaches a backend that is still there; on a half-open
  connection (a partition, a NAT dropping state, a failover that moved the address) the
  statement never settled, and every statement the pool kept routing to that connection
  hung with it until TCP gave up, a quarter of an hour on Linux defaults. A statement still
  unsettled `cancel_timeout` seconds (default 2) after its CancelRequest now destroys its
  connection's socket: what the connection held fails with `CONNECTION_CLOSED`, and the pool
  replaces it. `cancel_timeout: 0` turns it off. Connections also gained `lose(error)`, a
  `socket.destroy()` that works where `terminate()`'s polite close waits forever.
- **The replication stream notices a link that stopped delivering** (3.8.9). Upstream's
  stream only answered the server's keepalives, so a half-open replication connection left a
  subscription silent - no changes, no error - until TCP gave up. Every third of
  `subscribe_timeout` (default 30 s) the stream now sends a status update with *reply
  requested*, which a walsender answers at once; nothing received for the whole timeout
  loses the stream's connection, reported to `onerror` with code `SUBSCRIPTION_TIMEOUT`, and
  the usual re-establishment follows. A paused stream (backpressure) is not watched. The
  status update's reply byte is also no longer set by accident: `fill()` of the LSN ran to
  the end of the buffer.
- **An unprepared statement is described once per text, not on every run** (3.8.9).
  Upstream described every unprepared parameterized statement before binding it -
  Parse/Describe, wait, Bind/Execute: two round trips - to learn the parameter types the
  values did not pin. With `prepare: false` (the transaction-pooler setting) that was every
  query. Described types are now kept per pool (1,024 texts, oldest first), and a statement
  whose types are all known - pinned by its values, or described before - goes out as one
  pipeline. A reused description the server rejects (the schema changed under the text) is
  dropped; outside a transaction the statement, which never ran, is described afresh and run
  again, as a stale prepared statement already is.
- **`max` from the URL or `PGMAX` is a number** (3.8.10). Options read from the URL's
  query or the environment arrive as strings, and the parser coerces a fixed list of
  integer options; `max` was missing from it, so `?max=20` gave `Array("20")` - a pool of
  **one** connection - while `options.max` read `"20"`. Nothing was reported. `max` is now
  coerced like the others, and the pool opens that many connections.
- **A statement cancelled before it was written does not fail its transaction** (3.8.11).
  Inside `begin()`, a statement that fails fails the transaction even when the callback
  catches it, because the server aborted the transaction. One cancelled while it still sat
  in the transaction's own queue (behind a statement that parked the connection as full: a
  describe-first run, a cursor) was rejected with `57014` locally and never reached the
  server, yet it counted too: the callback could catch it, carry on and return, and
  `begin()` rejected anyway with that `57014`. It is the caller's rejection alone now, and
  the transaction commits. A statement cancelled once written still fails it.
- **The first call of a statement no longer pins its arguments** (3.8.12). A tagged query's
  origin (the stack an error reports) is captured once per strings array and cached for the
  array's life. V8 formats a stack lazily, and until then it holds its frames' functions,
  and through their closures whatever the caller held: the first call's arguments among
  them. A library building a 100,000-row batch through a memoized strings array kept
  ~370 MB alive for the life of the process. The stack is formatted at capture now, and
  both caches are keyed weakly. Upstream has the same code.
- **...on Deno too** (3.8.13). Deno keeps an Error's call sites after its stack is formatted,
  so 3.8.12 still pinned the first call there. The cache holds the stack's text alone now,
  formatted at capture on every runtime.
- **Real time under a test runner's fake timers** (3.8.14). The timers and clocks the
  driver schedules with are taken once when it loads (`src/timers.js`), so
  `vi.useFakeTimers()`, Jest's, `@sinonjs/fake-timers` or Deno's `FakeTime` installed
  later cannot reach them: faking `setImmediate` alone hung every query (the write
  batching), faking `setTimeout` every new connection. Durations (the subscribe
  watchdog's silence, the reconnect delay) read the monotonic clock, so a host clock step
  cannot stretch or shrink them.
- **`SubscriptionHandle.quietMs`** (3.8.14): milliseconds since the stream last delivered
  anything, keepalives included — an idle stream's keepalives keep it low, a stalled
  one's grows (through a reconnect too, until the new stream delivers).
- **A statement answered with `57P05` runs again on a fresh connection** (3.8.15).
  Postgres ends an idle session with `57P05` (`idle_session_timeout`) only while nothing
  is in flight on it, so a statement answered with it was written after the session was
  already gone and never ran: a process frozen past the timeout (a laptop's sleep, a
  serverless thaw) writes its first statement to the reaped pooled connection before it
  reads the server's goodbye. Outside a reserved scope (`begin`, `reserve`: the session and
  its state went with it) each such statement goes back to the pool once, for a fresh
  connection. A cursor, and a statement already sent again, fail with the error as before.
- **A cancel never reaches the next statement on its connection** (3.8.16). A
  CancelRequest is a second connection to the postmaster, and the signal it makes the
  server send lands whenever it gets there: the statement it was for has often finished
  by then, and the connection was already running the next borrower's statement, which
  the signal cancelled. A connection with a CancelRequest in flight now sends nothing
  more until that request's socket has closed (the backend is signalled by then, and an
  idle backend discards a late signal); one still open after `cancel_timeout` loses the
  connection. Nothing is pipelined behind a `cancellable()` statement either: written
  before its cancel existed, a statement queued behind it was what a late signal hit.
- **A transaction that cannot end cleanly closes its connection** (3.8.16). A COMMIT or
  ROLLBACK that fails at its start (cancelled, say) leaves the session in its
  transaction, aborted. `begin` kept such a connection reserved for good (a lost pool
  slot, and an `end()` that never returned); a `reserve()`d one released in a
  transaction went back to the pool, where every later statement failed `25P02`. Both
  close the connection now, and the server rolls the transaction back.
- **A `reserve()` waiting when a connection closes gets the reopened one** (3.8.16): the
  reconnect took it off the queue it is resolved from, and it never resolved.
- **The stream's reconnect backoff is bounded** (3.8.16): `50 << attempt` overflowed
  past attempt 25 (negative, then zero delays, with a `TimeoutNegativeWarning`).
- **A `subscribe()` that fails leaves nothing behind** (3.8.17). Its first stream start
  failing (every replication slot in use, `53400`, say) left the rejection cached as the
  stream, so every later `subscribe()` got the same failure without trying, and the
  subscriber registered on a stream that never came: nothing retried, since the
  replication connection had not closed. Now the subscriber is removed and the next
  `subscribe()` starts the stream again; one that arrives while a lost replication
  connection is being re-established waits for that attempt instead of starting a second.
