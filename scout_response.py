from __future__ import annotations
from dataclasses import dataclass, field

_VALID_STATUSES = frozenset({"ok", "warn", "critical", "empty"})
_VALID_SUBJECT_TYPES = frozenset({"publisher", "advertiser", "monitor", "system"})
_MAX_METRICS = 4
_MAX_SUGGESTIONS = 2


@dataclass
class Metric:
    label: str
    value: str
    delta: str | None = None


@dataclass
class Item:
    label: str
    value: str
    rank: int | None = None


@dataclass
class ScoutResponse:
    status: str           # "ok" | "warn" | "critical" | "empty"
    subject_type: str     # "publisher" | "advertiser" | "monitor" | "system"
    subject_id: str | None
    headline: str
    body: str | None = None
    metrics: list[Metric] = field(default_factory=list)
    items: list[Item] = field(default_factory=list)
    assumptions: list[str] = field(default_factory=list)
    suggestions: list[str] = field(default_factory=list)
    action: str | None = None
    action_command: str | None = None
    alert_id: str | None = None
    projection_n: int | None = None
    data_freshness: str | None = None
    confidence: str = field(init=False)

    def __post_init__(self) -> None:
        if self.status not in _VALID_STATUSES:
            raise ValueError(
                f"Invalid status {self.status!r}. Must be one of {sorted(_VALID_STATUSES)}"
            )
        if self.subject_type not in _VALID_SUBJECT_TYPES:
            raise ValueError(
                f"Invalid subject_type {self.subject_type!r}. "
                f"Must be one of {sorted(_VALID_SUBJECT_TYPES)}"
            )
        if len(self.metrics) > _MAX_METRICS:
            raise ValueError(
                f"metrics must be ≤{_MAX_METRICS}, got {len(self.metrics)}"
            )
        if len(self.suggestions) > _MAX_SUGGESTIONS:
            raise ValueError(
                f"suggestions must be ≤{_MAX_SUGGESTIONS}, got {len(self.suggestions)}"
            )
        if self.projection_n is None or self.projection_n < 3:
            self.confidence = "low"
        elif self.projection_n < 6:
            self.confidence = "medium"
        else:
            self.confidence = "high"
