# Scout — Current Objectives

1. **Demand feed → live.** 5 `MS_PLATFORM_TODO`s in `demand_feed_main.py`. Gated on the MS Platform team providing `CAMPAIGN_CREATE_WEBHOOK_URL`. See `KNOWN_DEBT.md` for the flip-live checklist.

2. **App Home adoption signal.** PR 2 (drill modals, scheduled warm-refresh, Redis swap) ships only after Jon/Todd/Roj show real open-rate on the current Home. Don't build what no one opens.

3. **Projection autocheck unattended.** `fires_log` persistence and daemon unification deferred in PR #159 — revisit after autocheck runs 3+ days without intervention.

4. **CH timeout rate.** `_retry_after_timeout` fires are the proxy metric. No baseline measured yet — measure before optimizing.

5. **Docstring coverage ≥70%.** CR threshold set in `.coderabbit.yaml`. Maintain — docstrings only where the WHY is non-obvious.
