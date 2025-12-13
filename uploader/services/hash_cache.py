"""
HashCache - Local cache for blake3 file hashes.

Stores hashes locally to avoid recalculating for unchanged files.
Validates based on file modification time (mtime).
"""
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

# Default cache location
DEFAULT_CACHE_DIR = Path.home() / ".cache" / "uploader"
DEFAULT_CACHE_FILE = "hashes.json"


class HashCache:
    """
    Local cache for blake3 file hashes.
    
    Stores hashes based on file path and modification time.
    If mtime matches, the cached hash is valid.
    If mtime changed, hash needs to be recalculated.
    """

    def __init__(self, cache_dir: Optional[Path] = None, cache_file: str = DEFAULT_CACHE_FILE):
        """
        Initialize hash cache.
        
        Args:
            cache_dir: Directory to store cache file (default: ~/.cache/uploader)
            cache_file: Name of cache file (default: hashes.json)
        """
        self._cache_dir = cache_dir or DEFAULT_CACHE_DIR
        self._cache_file = self._cache_dir / cache_file
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._dirty = False  # Track if cache needs to be saved
        
    async def load(self) -> None:
        """Load cache from disk."""
        try:
            if self._cache_file.exists():
                with open(self._cache_file, "r", encoding="utf-8") as f:
                    self._cache = json.load(f)
                logger.info("HashCache: Loaded %d entries from %s", len(self._cache), self._cache_file)
            else:
                logger.debug("HashCache: No cache file found at %s, starting fresh", self._cache_file)
                self._cache = {}
        except json.JSONDecodeError as e:
            logger.warning("HashCache: Failed to parse cache file: %s - starting fresh", e)
            self._cache = {}
        except Exception as e:
            logger.warning("HashCache: Failed to load cache: %s - starting fresh", e)
            self._cache = {}
    
    async def save(self) -> None:
        """Save cache to disk if dirty."""
        if not self._dirty:
            return
            
        try:
            # Ensure directory exists
            self._cache_dir.mkdir(parents=True, exist_ok=True)
            
            with open(self._cache_file, "w", encoding="utf-8") as f:
                json.dump(self._cache, f, indent=2)
            
            self._dirty = False
            logger.info("HashCache: Saved %d entries to %s", len(self._cache), self._cache_file)
        except Exception as e:
            logger.error("HashCache: Failed to save cache: %s", e)
    
    def _get_file_key(self, file_path: Path) -> str:
        """Get cache key for a file (absolute path as string)."""
        return str(file_path.resolve())
    
    def _get_file_mtime(self, file_path: Path) -> str:
        """Get file modification time as ISO string."""
        try:
            mtime = file_path.stat().st_mtime
            return datetime.fromtimestamp(mtime).isoformat()
        except Exception:
            return ""
    
    def _get_file_size(self, file_path: Path) -> int:
        """Get file size in bytes."""
        try:
            return file_path.stat().st_size
        except Exception:
            return 0
    
    async def get(self, file_path: Path) -> Optional[str]:
        """
        Get cached hash for file if valid.
        
        Returns None if:
        - File not in cache
        - File was modified (mtime changed)
        - File size changed
        
        Args:
            file_path: Path to file
            
        Returns:
            blake3 hash string if cache hit, None otherwise
        """
        key = self._get_file_key(file_path)
        
        if key not in self._cache:
            logger.debug("HashCache: MISS (not cached) - %s", file_path.name)
            return None
        
        cached = self._cache[key]
        current_mtime = self._get_file_mtime(file_path)
        current_size = self._get_file_size(file_path)
        
        # Validate cache entry
        if cached.get("mtime") != current_mtime:
            logger.debug(
                "HashCache: MISS (mtime changed) - %s (cached: %s, current: %s)",
                file_path.name, cached.get("mtime"), current_mtime
            )
            return None
        
        if cached.get("file_size") != current_size:
            logger.debug(
                "HashCache: MISS (size changed) - %s (cached: %d, current: %d)",
                file_path.name, cached.get("file_size", 0), current_size
            )
            return None
        
        blake3_hash = cached.get("blake3_hash")
        if blake3_hash:
            logger.debug("HashCache: HIT - %s -> %s...", file_path.name, blake3_hash[:16])
            return blake3_hash
        
        return None
    
    async def set(self, file_path: Path, blake3_hash: str) -> None:
        """
        Store hash in cache.
        
        Args:
            file_path: Path to file
            blake3_hash: Calculated blake3 hash
        """
        key = self._get_file_key(file_path)
        
        self._cache[key] = {
            "blake3_hash": blake3_hash,
            "file_size": self._get_file_size(file_path),
            "mtime": self._get_file_mtime(file_path),
            "cached_at": datetime.now().isoformat()
        }
        
        self._dirty = True
        logger.debug("HashCache: Cached - %s -> %s...", file_path.name, blake3_hash[:16])
    
    async def invalidate(self, file_path: Path) -> None:
        """
        Remove file from cache.
        
        Args:
            file_path: Path to file to invalidate
        """
        key = self._get_file_key(file_path)
        
        if key in self._cache:
            del self._cache[key]
            self._dirty = True
            logger.debug("HashCache: Invalidated - %s", file_path.name)
    
    async def clear(self) -> None:
        """Clear all cache entries."""
        self._cache = {}
        self._dirty = True
        logger.info("HashCache: Cleared all entries")
    
    def stats(self) -> Dict[str, int]:
        """Get cache statistics."""
        return {
            "entries": len(self._cache),
            "dirty": self._dirty
        }
    
    async def cleanup_stale(self, max_age_days: int = 30) -> int:
        """
        Remove cache entries for files that no longer exist or are too old.
        
        Args:
            max_age_days: Maximum age in days for cache entries
            
        Returns:
            Number of entries removed
        """
        removed = 0
        cutoff = datetime.now().timestamp() - (max_age_days * 24 * 60 * 60)
        
        keys_to_remove = []
        for key, entry in self._cache.items():
            file_path = Path(key)
            
            # Remove if file doesn't exist
            if not file_path.exists():
                keys_to_remove.append(key)
                continue
            
            # Remove if too old
            cached_at = entry.get("cached_at")
            if cached_at:
                try:
                    cached_time = datetime.fromisoformat(cached_at).timestamp()
                    if cached_time < cutoff:
                        keys_to_remove.append(key)
                except (ValueError, TypeError):
                    pass
        
        for key in keys_to_remove:
            del self._cache[key]
            removed += 1
        
        if removed > 0:
            self._dirty = True
            logger.info("HashCache: Cleaned up %d stale entries", removed)
        
        return removed


# Global instance for convenience
_global_cache: Optional[HashCache] = None


async def get_hash_cache() -> HashCache:
    """Get global hash cache instance (creates and loads if needed)."""
    global _global_cache
    
    if _global_cache is None:
        _global_cache = HashCache()
        await _global_cache.load()
    
    return _global_cache
