# Add a New Attachment Format to Scout

**Triggers:** "add a new attachment format", "new file type", `/scout-new-attachment`

## Workflow

1. **Implement an extractor function** in `scout_attachments.py`. Signature: `(file_bytes: bytes, mime_type: str) -> str`. Returns extracted text (max 30K chars).

2. **Add a row to `_EXTRACTORS`** dispatch table in `scout_attachments.py`:
   ```python
   "mime/type": _extract_your_format,
   ```

3. **Add the new MIME type** to `smoke_test.test_dispatch_table_routes_each_known_format` — this is the regression guard.

4. **Run `python3 smoke_test.py`** — must pass.

## Security constraints (already enforced by the framework)
- SSRF protection: Sheets fetch uses an allowlist + private IP block — do not bypass.
- `pdftotext` runs via `subprocess.run` with timeout, never `shell=True`.
- Slack `url_private` downloads gated on `https://files.slack.com/` prefix only.

## Limits
- 10MB per file/sheet
- 30K char extracted text
- 5MB raw image bytes before base64

## Notes
- Unsupported types degrade gracefully — Scout answers the text question and prepends a one-line note. You don't need to handle every edge case.
- `ask()` is not modified for attachment handling — `ask_with_attachment()` is the attachment path. Keep this separation.
