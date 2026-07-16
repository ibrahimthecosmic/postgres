#!/usr/bin/env bash
#
# Sync the `postgres-js` branch to a specific upstream release tag.
#
# Usage: scripts/sync-upstream.sh v3.4.10
#
# Fetches the tag from the `upstream` remote into FETCH_HEAD only — no upstream
# tags are ever created in this repository — and hard-points `postgres-js` at
# that commit. Follow up with an upgrade branch off `main`:
#
#   git checkout -b upgrade/v3.4.10 main
#   git merge postgres-js
#   # resolve conflicts (expect: src/subscribe.js, types/index.d.ts,
#   # tests/index.js, README.md, cjs/ deno/ cf/), then:
#   pnpm run build && pnpm test
#   git checkout main && git merge --no-ff upgrade/v3.4.10
#   git branch -d upgrade/v3.4.10
#
set -euo pipefail

tag="${1:-}"

if [[ -z "$tag" ]]; then
  echo "Usage: $0 <upstream-tag>  (e.g. $0 v3.4.10)" >&2
  exit 1
fi

if [[ -n "$(git status --porcelain)" ]]; then
  echo "Working tree not clean — commit or stash first." >&2
  exit 1
fi

echo "Fetching upstream tag $tag (no local tags created)..."
git fetch --no-tags upstream "refs/tags/$tag"

commit="$(git rev-parse 'FETCH_HEAD^{commit}')"
echo "Tag $tag resolves to commit $commit"

git update-ref refs/heads/postgres-js "$commit"
echo "postgres-js is now at $tag ($commit)"

if git remote get-url origin >/dev/null 2>&1; then
  echo "Pushing postgres-js to origin..."
  git push --force-with-lease origin postgres-js
else
  echo "No 'origin' remote configured — skipping push."
fi
