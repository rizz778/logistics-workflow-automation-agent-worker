import hashlib
import json
from typing import List, Dict, Any

class HashService:
    """Service for computing deterministic SHA-256 fingerprints for files, rulesets, and cache keys."""

    @staticmethod
    def compute_bytes_hash(data: bytes) -> str:
        """Returns standard hex SHA-256 hash of raw byte payload."""
        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def compute_ruleset_hash(rules: Dict[str, Any]) -> str:
        """Computes a deterministic hash of the compliance ruleset.
        Sorts JSON keys canonicalizing the structure so formatting differences don't alter the hash.
        """
        canonical_json = json.dumps(rules, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()

    @staticmethod
    def build_shipment_cache_key(file_hashes: List[str], rules_hash: str = "default_rules") -> str:
        """Constructs a composite cache key for a multi-document shipment.
        Sorts file hashes alphabetically so upload order does not affect the cache key.
        """
        valid_hashes = sorted([h.strip().lower() for h in file_hashes if h and h.strip()])
        combined = ":".join(valid_hashes)
        combined_hash = hashlib.sha256(combined.encode("utf-8")).hexdigest()
        return f"cache:shipment:{combined_hash}:{rules_hash}"

    @staticmethod
    def build_document_cache_key(file_hash: str) -> str:
        """Constructs a cache key for an individual document's extracted fields."""
        clean_hash = file_hash.strip().lower()
        return f"cache:doc:{clean_hash}"

hash_service = HashService()

