---
globs: smoke_test.py
---

# Rules for smoke_test.py

- Changes here affect what ops sees on every Render redeploy — treat this file with the same care as a production health check.
- Verify fallback `text=` is always set alongside `blocks=` in every new test — Slack requires non-empty `text` for mobile push previews.
- Every new button action ID added to `_BLOCK_ACTION_DISPATCH` needs a corresponding smoke test here.
- Every new MIME type added to `scout_attachments._EXTRACTORS` needs a row in `test_dispatch_table_routes_each_known_format`.
