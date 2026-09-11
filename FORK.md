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
pnpm add 'postgres@github:<owner>/postgres#v3.8.0'
```

Upgrading an app = bump the tag in `package.json`, `pnpm install`. For a private repo, CI
needs an SSH key or token that can read it.

Alternative for many projects / cleaner CI: publish as a scoped package and alias it, so
imports still resolve to `postgres`:

```sh
pnpm add postgres@npm:@<owner>/postgres@3.8.0
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
- New options `subscribe_tables` and `subscribe_raw` for `sql.subscribe('transaction')`.
  `subscribe_tables` (an array of `schema.table` names, or a `(schema, table) => boolean`)
  drops changes to every other relation as soon as the relation id is read — before any tuple
  is decoded — and narrows a `truncate` to the relations that passed; a transaction whose
  changes were all filtered still fires with an empty iterator so its commit lsn is seen.
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
