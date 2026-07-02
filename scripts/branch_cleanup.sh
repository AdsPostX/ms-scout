#!/usr/bin/env bash
# Prunes branches that are provably safe to delete and flags the rest for
# human review. Run this periodically (e.g. after merging a PR) instead of
# letting worktree-agent-*/claude/* branches accumulate silently.
#
# Classification (in order of certainty, cheapest check first):
#   1. ANCESTOR_OF_MAIN — tip commit already reachable from origin/main
#   2. MERGED_PR        — gh pr list shows a MERGED PR for this branch
#   3. EMPTY_DIFF       — no PR, but diff against origin/main is empty (content
#                          already landed under a different commit hash)
#   4. NEEDS_REVIEW      — everything else: open PR, closed/no PR with a real
#                          diff, or no merge-base at all (orphan history).
#                          Never auto-deleted.
#
# Scope:
#   Local branches are always scanned. Remote-only branches on origin are
#   scanned too when --remote (or --all) is passed. Without that flag this
#   script only ever prunes your local checkout — the count of unscanned
#   remote-only branches is printed every run so that gap is never silent.
#
# Usage:
#   scripts/branch_cleanup.sh                    # dry run, local only
#   scripts/branch_cleanup.sh --remote            # dry run, local + remote
#   scripts/branch_cleanup.sh --apply             # delete local tiers 1-3
#   scripts/branch_cleanup.sh --apply --remote    # delete local + remote tiers 1-3

set -euo pipefail

# --- config -------------------------------------------------------------
# Branches that must never be auto-deleted regardless of classification.
PROTECTED_PATTERNS=(
  "^main$"
  "^backup/"
)

APPLY=false
SCAN_REMOTE=false
for arg in "$@"; do
  case "$arg" in
    --apply) APPLY=true ;;
    --remote|--all) SCAN_REMOTE=true ;;
  esac
done

cd "$(git rev-parse --show-toplevel)"
git fetch origin --quiet --prune

is_protected() {
  local branch="$1"
  local pat
  for pat in "${PROTECTED_PATTERNS[@]}"; do
    [[ "$branch" =~ $pat ]] && return 0
  done
  return 1
}

# Classifies one branch against $BASE_REF. Prints exactly one line:
#   TIER<TAB>reason
# $1 = git ref to run merge-base/diff against (e.g. "foo" or "origin/foo")
# $2 = bare branch name to look up in PR_JSON (e.g. "foo")
classify_branch() {
  local gitref="$1" prname="$2" pr_state

  pr_state=$(python3 -c "
import json, sys
prs = json.load(sys.stdin)
match = [p for p in prs if p['headRefName'] == sys.argv[1]]
print(match[0]['state'] if match else 'NONE')
" "$prname" <<< "$PR_JSON")

  if [[ "$pr_state" == "OPEN" ]]; then
    printf 'REVIEW\t%s\n' "OPEN PR — never auto-delete"
    return
  fi

  if git merge-base --is-ancestor "$gitref" "$BASE_REF" 2>/dev/null; then
    printf 'DELETE\t%s\n' "ANCESTOR_OF_MAIN"
    return
  fi

  if [[ "$pr_state" == "MERGED" ]]; then
    printf 'DELETE\t%s\n' "MERGED_PR"
    return
  fi

  if ! git merge-base "$BASE_REF" "$gitref" >/dev/null 2>&1; then
    printf 'REVIEW\t%s\n' "NO_MERGE_BASE — orphan history, needs manual look"
    return
  fi

  local diff_stat
  diff_stat=$(git diff "$BASE_REF...$gitref" --stat 2>/dev/null || true)
  if [[ -z "$diff_stat" ]]; then
    printf 'DELETE\t%s\n' "EMPTY_DIFF_VS_MAIN"
  else
    local reason="no PR"
    [[ "$pr_state" == "CLOSED" ]] && reason="CLOSED PR"
    printf 'REVIEW\t%s\n' "HAS_UNIQUE_DIFF, $reason — needs manual look"
  fi
}

BASE_REF="origin/main"
CURRENT_BRANCH=$(git branch --show-current)
PR_JSON=$(gh pr list --state all --limit 1000 --json number,state,headRefName)

# --- local scope ----------------------------------------------------------
to_delete_local=()
to_review_local=()

while IFS= read -r branch; do
  [[ -z "$branch" ]] && continue
  [[ "$branch" == "$CURRENT_BRANCH" ]] && continue
  if is_protected "$branch"; then
    to_review_local+=("$branch  [PROTECTED — never auto-delete]")
    continue
  fi

  IFS=$'\t' read -r tier reason <<< "$(classify_branch "$branch" "$branch")"
  case "$tier" in
    DELETE) to_delete_local+=("$branch  [$reason]") ;;
    *)      to_review_local+=("$branch  [$reason]") ;;
  esac
done < <(git branch --format='%(refname:short)')

echo "=== Local: safe to delete (${#to_delete_local[@]}) ==="
printf '%s\n' "${to_delete_local[@]:-}"
echo
echo "=== Local: needs human review — not touched (${#to_review_local[@]}) ==="
printf '%s\n' "${to_review_local[@]:-}"

if [[ "$APPLY" == true && ${#to_delete_local[@]} -gt 0 ]]; then
  echo
  echo "Deleting ${#to_delete_local[@]} local branches..."
  for entry in "${to_delete_local[@]}"; do
    git branch -D "${entry%%  *}"
  done
elif [[ ${#to_delete_local[@]} -gt 0 ]]; then
  echo
  echo "Dry run — re-run with --apply to delete the local branches above."
fi

# --- remote scope -----------------------------------------------------------
if [[ "$SCAN_REMOTE" != true ]]; then
  untouched=$(git branch -r --format='%(refname:short)' | grep -v '^origin$' | sed 's#^origin/##' | grep -vc '^main$')
  echo
  echo "Note: $untouched remote-only branches on origin were not scanned (local-only run). Re-run with --remote to include them."
  exit 0
fi

to_delete_remote=()
to_review_remote=()

while IFS= read -r name; do
  [[ -z "$name" ]] && continue
  [[ "$name" == "main" ]] && continue
  [[ "$name" == "$CURRENT_BRANCH" ]] && continue
  if is_protected "$name"; then
    to_review_remote+=("$name  [PROTECTED — never auto-delete]")
    continue
  fi

  IFS=$'\t' read -r tier reason <<< "$(classify_branch "origin/$name" "$name")"
  case "$tier" in
    DELETE) to_delete_remote+=("$name  [$reason]") ;;
    *)      to_review_remote+=("$name  [$reason]") ;;
  esac
done < <(git branch -r --format='%(refname:short)' | grep -v '^origin$' | sed 's#^origin/##')

echo
echo "=== Remote (origin): safe to delete (${#to_delete_remote[@]}) ==="
printf '%s\n' "${to_delete_remote[@]:-}"
echo
echo "=== Remote (origin): needs human review — not touched (${#to_review_remote[@]}) ==="
printf '%s\n' "${to_review_remote[@]:-}"

if [[ "$APPLY" == true && ${#to_delete_remote[@]} -gt 0 ]]; then
  echo
  echo "Deleting ${#to_delete_remote[@]} remote branches..."
  for entry in "${to_delete_remote[@]}"; do
    git push origin --delete "${entry%%  *}"
  done
elif [[ ${#to_delete_remote[@]} -gt 0 ]]; then
  echo
  echo "Dry run — re-run with --apply --remote to delete the remote branches above."
fi
