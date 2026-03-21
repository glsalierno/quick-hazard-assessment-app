"""Thin alias for memory-wrapped extractor (see ``utils.cas_extractor``)."""

from utils.cas_extractor import MemoryOptimizedPureCASExtractor, PureCASExtractor

__all__ = ["MemoryOptimizedPureCASExtractor", "PureCASExtractor"]
