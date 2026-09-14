"""Persistent SDS PDF cache. Fingerprint by SHA256; reuse parsed JSON."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from v7.providers.base import SDSDownload, SDSHit


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _safe_segment(value: str | None, fallback: str = "unknown") -> str:
    raw = (value or fallback).strip() or fallback
    out = []
    for ch in raw:
        if ch.isalnum() or ch in ("-", "_", "."):
            out.append(ch)
        else:
            out.append("_")
    return "".join(out)[:80] or fallback


@dataclass
class CachedSDS:
    cas: str
    manufacturer: str
    revision: str
    sha256: str
    pdf_path: Path
    parsed_path: Path
    metadata_path: Path
    reused_pdf: bool
    parsed: dict[str, Any] | None


class SDSCache:
    """
    Layout::

        cache/SDS/{CAS}/{manufacturer}/{revision}/{sha25612}/
            original.pdf
            parsed.json
            metadata.json
    """

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)

    def locate_identical(self, digest: str) -> Path | None:
        if not self.root.is_dir():
            return None
        for pdf in self.root.rglob("original.pdf"):
            try:
                if sha256_bytes(pdf.read_bytes()) == digest:
                    return pdf
            except OSError:
                continue
        return None

    def locate_by_product(self, manufacturer: str, product_id: str) -> CachedSDS | None:
        """Reuse a cached PDF for the same manufacturer + product number (any revision)."""
        if not self.root.is_dir() or not product_id:
            return None
        mfr_want = manufacturer.strip().lower()
        prod_want = product_id.strip().lower()
        for meta_path in self.root.rglob("metadata.json"):
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if str(meta.get("manufacturer") or "").strip().lower() != mfr_want:
                continue
            if str(meta.get("product_id") or "").strip().lower() != prod_want:
                continue
            pdf_path = meta_path.parent / "original.pdf"
            if not pdf_path.is_file():
                continue
            try:
                digest = str(meta.get("sha256") or "") or sha256_bytes(pdf_path.read_bytes())
            except OSError:
                continue
            parsed_path = meta_path.parent / "parsed.json"
            parsed = None
            if parsed_path.is_file():
                try:
                    parsed = json.loads(parsed_path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    parsed = None
            return CachedSDS(
                cas=str(meta.get("cas") or ""),
                manufacturer=str(meta.get("manufacturer") or manufacturer),
                revision=str(meta.get("revision") or "unknown-rev"),
                sha256=digest,
                pdf_path=pdf_path,
                parsed_path=parsed_path,
                metadata_path=meta_path,
                reused_pdf=True,
                parsed=parsed,
            )
        return None

    def slot(self, hit: SDSHit, digest: str) -> Path:
        cas = _safe_segment(hit.cas.replace("/", "-"), "no-cas")
        mfr = _safe_segment(hit.manufacturer, "unknown")
        rev = _safe_segment(hit.revision, "unknown-rev")
        return self.root / cas / mfr / rev / digest[:12]

    def store(self, download: SDSDownload, parsed: dict[str, Any] | None = None) -> CachedSDS:
        digest = download.sha256 or sha256_bytes(download.pdf_bytes)
        existing = self.locate_identical(digest)
        reused = existing is not None
        folder = existing.parent if existing is not None else self.slot(download.hit, digest)
        folder.mkdir(parents=True, exist_ok=True)
        pdf_path = folder / "original.pdf"
        if not pdf_path.is_file():
            pdf_path.write_bytes(download.pdf_bytes)
            reused = False
        parsed_path = folder / "parsed.json"
        metadata_path = folder / "metadata.json"
        if parsed is not None:
            parsed_path.write_text(json.dumps(parsed, indent=2, default=str), encoding="utf-8")
        meta = {
            "cas": download.hit.cas,
            "provider": download.hit.provider,
            "manufacturer": download.hit.manufacturer,
            "product_id": download.hit.product_id,
            "revision": download.hit.revision,
            "sds_url": download.hit.sds_url,
            "sha256": digest,
            "stored_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "hit": asdict(download.hit),
        }
        metadata_path.write_text(json.dumps(meta, indent=2, default=str), encoding="utf-8")
        loaded = None
        if parsed_path.is_file():
            loaded = json.loads(parsed_path.read_text(encoding="utf-8"))
        return CachedSDS(
            cas=download.hit.cas,
            manufacturer=download.hit.manufacturer,
            revision=str(download.hit.revision or "unknown-rev"),
            sha256=digest,
            pdf_path=pdf_path,
            parsed_path=parsed_path,
            metadata_path=metadata_path,
            reused_pdf=reused,
            parsed=loaded if parsed is None else parsed,
        )

    def load_parsed_if_present(self, pdf_path: Path) -> dict[str, Any] | None:
        parsed_path = pdf_path.parent / "parsed.json"
        if not parsed_path.is_file():
            return None
        return json.loads(parsed_path.read_text(encoding="utf-8"))
