"""SDS provider interface. Parsers must not import concrete providers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class SDSHit:
    cas: str
    provider: str
    manufacturer: str
    product_id: str | None
    title: str | None
    sds_url: str | None
    revision: str | None
    language: str = "en-US"
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SDSDownload:
    hit: SDSHit
    pdf_bytes: bytes
    sha256: str
    cached_path: Path | None = None
    reused: bool = False


@dataclass(frozen=True)
class SDSDocument:
    """Vendor-agnostic downloaded SDS payload consumed by parsers and the cache.

    Concrete providers (TCI, Sigma, Fisher, …) produce this shape after download.
    Parsers must depend only on this / ``SDSDownload``, never on a vendor module.
    """

    manufacturer: str
    product_number: str | None
    revision: str | None
    pdf_bytes: bytes
    source_url: str | None
    cas: str = ""
    sha256: str = ""
    cached_path: Path | None = None
    reused: bool = False
    provider: str = ""
    title: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_download(cls, download: SDSDownload) -> SDSDocument:
        hit = download.hit
        return cls(
            manufacturer=hit.manufacturer,
            product_number=hit.product_id,
            revision=hit.revision,
            pdf_bytes=download.pdf_bytes,
            source_url=hit.sds_url,
            cas=hit.cas,
            sha256=download.sha256,
            cached_path=download.cached_path,
            reused=download.reused,
            provider=hit.provider,
            title=hit.title,
            extra=dict(hit.extra),
        )

    def to_download(self) -> SDSDownload:
        hit = SDSHit(
            cas=self.cas,
            provider=self.provider or "unknown",
            manufacturer=self.manufacturer,
            product_id=self.product_number,
            title=self.title,
            sds_url=self.source_url,
            revision=self.revision,
            extra=dict(self.extra),
        )
        return SDSDownload(
            hit=hit,
            pdf_bytes=self.pdf_bytes,
            sha256=self.sha256,
            cached_path=self.cached_path,
            reused=self.reused,
        )


class SDSProvider(ABC):
    """CAS → candidate SDS metadata → download. Isolated from parsing."""

    name: str = "base"

    @abstractmethod
    def find(self, cas: str) -> list[SDSHit]:
        """Return zero or more SDS candidates for a CAS (do not download)."""

    @abstractmethod
    def download(self, hit: SDSHit) -> SDSDownload:
        """Fetch PDF bytes for a hit. Callers should pass results through the SDS cache."""
