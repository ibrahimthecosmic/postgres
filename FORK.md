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
pnpm add 'postgres@github:<owner>/postgres#v3.6.0'
```

Upgrading an app = bump the tag in `package.json`, `pnpm install`. For a private repo, CI
needs an SSH key or token that can read it.

Alternative for many projects / cleaner CI: publish as a scoped package and alias it, so
imports still resolve to `postgres`:

```sh
pnpm add postgres@npm:@<owner>/postgres@3.6.0
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
