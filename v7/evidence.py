"""Traceable evidence values. Anonymous scalars are not stored in the knowledge layer."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Literal

Confidence = Literal["curated", "document", "experimental", "predicted", "unknown"]

PROVIDER_PRIORITY = (
    "manual",
    "hspip",
    "doss",
    "gsd",
    "sigma_sds",
    "cameo",
    "pubchem",
    "other",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass(frozen=True)
class EvidenceValue:
    value: Any
    source: str
    provider: str
    confidence: Confidence
    retrieval_date: str
    citation: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def evidence(
    value: Any,
    *,
    source: str,
    provider: str,
    confidence: Confidence,
    citation: str,
    retrieval_date: str | None = None,
) -> EvidenceValue:
    return EvidenceValue(
        value=value,
        source=source,
        provider=provider,
        confidence=confidence,
        retrieval_date=retrieval_date or utc_now_iso(),
        citation=citation,
    )


def provider_rank(provider: str) -> int:
    key = (provider or "other").strip().lower()
    try:
        return PROVIDER_PRIORITY.index(key)
    except ValueError:
        return PROVIDER_PRIORITY.index("other")


def prefer(existing: EvidenceValue | None, incoming: EvidenceValue) -> EvidenceValue:
    """Keep the higher-priority (lower rank) value. Never overwrite curated with live."""
    if existing is None:
        return incoming
    if provider_rank(incoming.provider) < provider_rank(existing.provider):
        return incoming
    return existing
