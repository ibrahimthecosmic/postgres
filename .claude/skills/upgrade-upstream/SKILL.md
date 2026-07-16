---
name: upgrade-upstream
description: Upgrade this detached postgres.js fork to a new upstream release tag — sync the postgres-js branch, merge into main via an upgrade branch, rebuild, test. Use when the user asks to upgrade/sync/merge upstream or mentions a new porsager/postgres release.
---

# Upgrade the fork to a new upstream release

This repo is a detached fork of porsager/postgres (see FORK.md). `main` holds our code;
`postgres-js` mirrors upstream at the exact release tag we're based on. Never merge
upstream master — only release tags. No upstream tags may be created in this repo.

## Inputs

The upstream release tag to upgrade to (e.g. `v3.4.10`). If not given, ask the user or
check upstream releases: `git ls-remote --tags upstream | grep -v '\^{}' | tail -5`.

## Steps

1. Require a clean working tree (`git status --porcelain` empty).
2. `scripts/sync-upstream.sh <tag>` — fetches the tag into FETCH_HEAD (no tags created),
   points `postgres-js` at it, pushes to origin if configured.
3. `git checkout -b upgrade/<tag> main`
4. `git merge postgres-js`
5. Resolve conflicts. Expected hotspots:
   - `src/subscribe.js` — our transaction-events feature lives here; preserve it while
     adopting upstream changes. Consult specs/transaction-events.md for invariants.
   - `types/index.d.ts` (TransactionChange/TransactionInfo + subscribe overload),
     `tests/index.js` (our 4 transaction tests), `README.md` (Transaction events section).
   - `cjs/`, `deno/`, `cf/` are GENERATED — take either side, they get regenerated next.
6. `pnpm run build` (regenerates cjs/deno/cf) and commit the result.
7. Run the full test suite against a logical-replication-enabled Postgres (see FORK.md
   and .github/workflows/test.yml for the required server config: wal_level=logical,
   max_prepared_transactions=100). All tests must pass, including the transaction tests.
8. Update `package.json` version per policy: keep our own minor/patch line; only adopt a
   new MAJOR if the upstream tag has one.
9. `git checkout main && git merge --no-ff upgrade/<tag> && git branch -d upgrade/<tag>`
10. Tag the new fork release on main (e.g. `v3.5.2`) and push `main`, `postgres-js`, tags.

## Invariants (do not violate)

- `postgres-js` must always equal an exact upstream release-tag commit.
- No upstream tags in this repo (`remote.upstream.tagOpt = --no-tags` must stay set).
- Fork versioning: major follows upstream, minor/patch are ours.
- Per-row events intentionally skip streamed transactions (documented fork limitation) —
  don't "fix" this during conflict resolution; see specs/transaction-events.md.
