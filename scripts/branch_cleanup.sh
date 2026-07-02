#!/usr/bin/env bash
# Prunes local branches that are provably safe to delete and flags the rest
# for human review. Run this periodically (e.g. after merging a PR) instead
# of letting worktree-agent-*/claude/* branches accumulate silently.
#
# Classification (in order of certainty, cheapest check first):
#   1. ANCESTOR_OF_MAIN — tip commit already reachable from main (merge-base)
#   2. MERGED_PR        — gh pr list shows a MERGED PR for this branch
#   3. EMPTY_DIFF       — no PR, but diff against main is empty (content
#                          already landed under a different commit hash)
#   4. NEEDS_REVIEW      — everything else: open PR, closed/no PR with a real
#                          diff, or no merge-base at all (orphan history).
#                          Never auto-deleted.
#
# Usage:
#   scripts/branch_cleanup.sh            # dry run, prints the plan
#   scripts/branch_cleanup.sh --apply    # actually deletes tiers 1-3

set -euo pipefail

# --- config -------------------------------------------------------------
# Branches that must never be auto-deleted regardless of classification.
PROTECTED_PATTERNS=(
  "^main$"
  "^backup/"
)
APPLY=false
[[ "${1:-}" == "--apply" ]] && APPLY=true

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

CURRENT_BRANCH=$(git branch --show-current)
PR_JSON=$(gh pr list --state all --limit 1000 --json number,state,headRefName)

to_delete=()
to_review=()

while IFS= read -r branch; do
  [[ -z "$branch" ]] && continue
  [[ "$branch" == "$CURRENT_BRANCH" ]] && continue
  if is_protected "$branch"; then
    to_review+=("$branch  [PROTECTED — never auto-delete]")
    continue
  fi

  pr_state=$(python3 -c "
import json, sys
prs = json.load(sys.stdin)
match = [p for p in prs if p['headRefName'] == sys.argv[1]]
print(match[0]['state'] if match else 'NONE')
" "$branch" <<< "$PR_JSON")

  if [[ "$pr_state" == "OPEN" ]]; then
    to_review+=("$branch  [OPEN PR — never auto-delete]")
    continue
  fi

  if git merge-base --is-ancestor "$branch" main 2>/dev/null; then
    to_delete+=("$branch  [ANCESTOR_OF_MAIN]")
    continue
  fi

  if [[ "$pr_state" == "MERGED" ]]; then
    to_delete+=("$branch  [MERGED_PR]")
    continue
  fi

  if ! git merge-base main "$branch" >/dev/null 2>&1; then
    to_review+=("$branch  [NO_MERGE_BASE — orphan history, needs manual look]")
    continue
  fi

  diff_stat=$(git diff "main...$branch" --stat 2>/dev/null || true)
  if [[ -z "$diff_stat" ]]; then
    to_delete+=("$branch  [EMPTY_DIFF_VS_MAIN]")
  else
    reason="no PR"
    [[ "$pr_state" == "CLOSED" ]] && reason="CLOSED PR"
    to_review+=("$branch  [HAS_UNIQUE_DIFF, $reason — needs manual look]")
  fi
done < <(git branch --format='%(refname:short)')

echo "=== Safe to delete (${#to_delete[@]}) ==="
printf '%s\n' "${to_delete[@]:-}"
echo
echo "=== Needs human review — not touched (${#to_review[@]}) ==="
printf '%s\n' "${to_review[@]:-}"

if [[ "$APPLY" == true && ${#to_delete[@]} -gt 0 ]]; then
  echo
  echo "Deleting ${#to_delete[@]} branches..."
  for entry in "${to_delete[@]}"; do
    git branch -D "${entry%%  *}"
  done
elif [[ ${#to_delete[@]} -gt 0 ]]; then
  echo
  echo "Dry run — re-run with --apply to delete the branches above."
fi
