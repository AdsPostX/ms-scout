# Scout — Engineering Standards

Five checks new Scout code should pass before it's considered done. These are
the durable core of an earlier audit effort (`docs/archive/VAMSEE_AUDIT.md`) —
kept as a standing reference, not a progress tracker. There is no "percent
complete" here; every new tool, handler, or query function is expected to
satisfy all five at the time it's written.

## 1. No invisible accumulators

Don't build a list or dict silently inside a loop via closure mutation and
implicitly return it. If a function accumulates values, the accumulation
should be an explicit named variable that is itself returned (or passed in as
a collector), not a side effect a reader has to trace through the loop body
to discover.

## 2. No no-op side-channel handlers

If a function returns immediately while the real work happens somewhere else
(a background thread, a fire-and-forget call, a queued job), that contract
must be documented at the function — a docstring or comment saying what
happens and where — not left for the next reader to infer from behavior.

## 3. No repeated inline patterns

If the same filter, transform, or normalization expression appears two or
more times in a file, extract it to a named pure function. This applies to
things like `(field or "").lower().strip()`-style normalization, repeated
dict-building-from-loop shapes, repeated count/filter logic, and repeated
percentage or ratio calculations.

## 4. Config objects, not scattered env reads

Feature flags and configuration values belong in a config object (dataclass
or similar) built once at module init, not read ad hoc via `os.getenv()` at
scattered call sites throughout a file. `scout_thresholds.py`'s
`ThresholdManager` is the reference pattern for this in Scout — new config
surfaces should follow its shape rather than reinvent one.

## 5. Validate at construction

Objects and dataclasses that carry required fields or required config should
validate them at `__init__`/construction time, not at first use. If a feature
is flag-enabled but its required config is missing, that should fail loudly
at startup, not silently at the first call that needs it.

---

These checks apply uniformly — they are not domain-specific. Any new
`queries_*.py` function, `scout_agent.py` tool, or `scout_handlers.py` handler
should be checked against all five before it's considered finished.
