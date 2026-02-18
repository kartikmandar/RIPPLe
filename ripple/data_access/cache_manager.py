"""
Enhanced Cache Manager for RIPPLe Data Access

This module provides intelligent caching for LSST data access operations
including LRU caching, memory management, and cache performance optimization.
"""

import logging
import threading
import time
from typing import Any, Dict, Optional, Tuple
from functools import lru_cache
from collections import OrderedDict
import hashlib
import pickle
import os


class MemoryCache:
    """
    Thread-safe in-memory cache with LRU eviction policy.
    """

    def __init__(self, max_size: int = 1000):
        """
        Initialize memory cache.

        Args:
            max_size (int): Maximum number of items to cache
        """
        self.max_size = max_size
        self.cache: OrderedDict[Hashable, Tuple[Any, float]] = OrderedDict()
        self.lock = threading.RLock()
        self.hits = 0
        self.misses = 0

    def get(self, key) -> Optional[Any]:
        """Get item from cache."""
        with self.lock:
            if key in self.cache:
                # Move to end (mark as recently used)
                value = self.cache.pop(key)
                self.cache[key] = value
                self.hits += 1
                return value[0]  # Return cached value
            else:
                self.misses += 1
                return None

    def put(self, key, value: Any, ttl: float = None) -> None:
        """
        Put item in cache with optional TTL.

        Args:
            key (Hashable): Cache key
            value (Any): Value to cache
            ttl (float, optional): Time to live in seconds
        """
        with self.lock:
            current_time = time.time()
            cache_value = (value, current_time + ttl if ttl else float('inf'))

            if len(self.cache) >= self.max_size:
                # Remove least recently used item
                self.cache.popitem(last=False)

            self.cache[key] = cache_value

    def clear(self) -> None:
        """Clear all items from cache."""
        with self.lock:
            self.cache.clear()
            self.hits = 0
            self.misses = 0

    def get_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics."""
        total_requests = self.hits + self.misses
        hit_rate = self.hits / total_requests if total_requests > 0 else 0

        return {
            "hits": self.hits,
            "misses": self.misses,
            "total_requests": total_requests,
            "hit_rate": hit_rate,
            "size": len(self.cache),
            "max_size": self.max_size
        }


class EnhancedCacheManager:
    """
    Enhanced cache manager with multiple caching strategies and persistent storage.
    """

    def __init__(self, cache_dir: str = "./cache", max_memory_items: int = 1000):
        """
        Initialize enhanced cache manager.

        Args:
            cache_dir (str): Directory for persistent cache
            max_memory_items (int): Maximum items in memory cache
        """
        self.cache_dir = os.path.expanduser(cache_dir)
        self.max_memory_items = max_memory_items
        self.memory_cache = MemoryCache(max_memory_items)
        self.disk_cache_enabled = True
        self.logger = logging.getLogger(__name__)

        # Create cache directory if it doesn't exist
        os.makedirs(self.cache_dir, exist_ok=True)

        # Cache statistics
        self.stats = {
            "memory_hits": 0,
            "disk_hits": 0,
            "misses": 0,
            "evictions": 0
        }

    def _get_cache_key(self, *args, **kwargs) -> str:
        """Generate a cache key from arguments."""
        key_data = str(args) + str(sorted(kwargs.items()))
        return hashlib.md5(key_data.encode()).hexdigest()

    def _get_disk_cache_path(self, key: str) -> str:
        """Get file path for disk cache item."""
        return os.path.join(self.cache_dir, f"{key}.cache")

    def _load_from_disk(self, key: str) -> Optional[Any]:
        """Load cached item from disk."""
        if not self.disk_cache_enabled:
            return None

        cache_path = self._get_disk_cache_path(key)

        try:
            if os.path.exists(cache_path):
                with open(cache_path, 'rb') as f:
                    cached_data = pickle.load(f)

                    # Check if cache item has expired
                    if "ttl" in cached_data:
                        if time.time() > cached_data["ttl"]:
                            os.remove(cache_path)
                            return None

                    self.stats["disk_hits"] += 1
                    self.logger.debug(f"Disk cache hit for key: {key}")
                    return cached_data["data"]

        except Exception as e:
            self.logger.warning(f"Failed to load from disk cache: {e}")

        return None

    def _save_to_disk(self, key: str, data: Any, ttl: float = None) -> None:
        """Save cached item to disk."""
        if not self.disk_cache_enabled:
            return

        cache_path = self._get_disk_cache_path(key)

        try:
            cache_data = {
                "data": data,
                "cached_at": time.time(),
                "ttl": ttl if ttl else time.time() + 86400  # 24 hours default
            }

            with open(cache_path, 'wb') as f:
                pickle.dump(cache_data, f)

        except Exception as e:
            self.logger.error(f"Failed to save to disk cache: {e}")

    def get(self, *args, **kwargs) -> Optional[Any]:
        """
        Get cached item using multi-tier strategy.

        Returns:
            Optional[Any]: Cached item or None if not found
        """
        key = self._get_cache_key(*args, **kwargs)

        # Try memory cache first
        result = self.memory_cache.get(key)
        if result is not None:
            self.stats["memory_hits"] += 1
            return result

        # Fallback to disk cache
        result = self._load_from_disk(key)
        if result is not None:
            # Promote to memory cache for faster future access
            self.memory_cache.put(key, result)
            return result

        self.stats["misses"] += 1
        return None

    def put(self, data: Any, ttl: float = None, persist_to_disk: bool = True,
             *args, **kwargs) -> None:
        """
        Put item in cache with intelligent storage strategy.

        Args:
            data (Any): Data to cache
            ttl (float, optional): Time to live in seconds
            persist_to_disk (bool): Whether to persist to disk
            *args, **kwargs: Arguments to generate cache key
        """
        key = self._get_cache_key(*args, **kwargs)

        # Always store in memory cache
        memory_ttl = ttl if ttl and ttl < 3600 else 3600  # Max 1 hour in memory
        self.memory_cache.put(key, data, memory_ttl)

        # Optionally persist to disk for longer-term storage
        if persist_to_disk and ttl:
            self._save_to_disk(key, data, ttl)

    def clear_memory(self) -> None:
        """Clear memory cache."""
        self.memory_cache.clear()

    def clear_all(self) -> None:
        """Clear all caches (memory and disk)."""
        self.memory_cache.clear()

        # Clear disk cache
        try:
            import glob
            cache_files = glob.glob(os.path.join(self.cache_dir, "*.cache"))
            for cache_file in cache_files:
                os.remove(cache_file)

            self.logger.info(f"Cleared {len(cache_files)} disk cache files")

        except Exception as e:
            self.logger.error(f"Failed to clear disk cache: {e}")

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics."""
        memory_stats = self.memory_cache.get_stats()

        total_hits = self.stats["memory_hits"] + self.stats["disk_hits"]
        total_requests = total_hits + self.stats["misses"]
        overall_hit_rate = total_hits / total_requests if total_requests > 0 else 0

        return {
            "memory_cache": memory_stats,
            "disk_hits": self.stats["disk_hits"],
            "memory_hits": self.stats["memory_hits"],
            "misses": self.stats["misses"],
            "total_requests": total_requests,
            "overall_hit_rate": overall_hit_rate,
            "cache_dir": self.cache_dir,
            "disk_cache_enabled": self.disk_cache_enabled
        }

    def optimize_cache(self) -> None:
        """Optimize cache by removing expired items and managing size."""
        # This is called periodically to clean up expired items
        try:
            import glob

            cache_files = glob.glob(os.path.join(self.cache_dir, "*.cache"))
            current_time = time.time()
            removed_count = 0

            for cache_file in cache_files:
                try:
                    with open(cache_file, 'rb') as f:
                        cached_data = pickle.load(f)

                    # Check TTL and remove if expired
                    if current_time > cached_data.get("ttl", float('inf')):
                        os.remove(cache_file)
                        removed_count += 1

                except Exception:
                    # Remove corrupted cache files
                    try:
                        os.remove(cache_file)
                        removed_count += 1
                    except:
                        pass

            if removed_count > 0:
                self.logger.info(f"Cache optimization: removed {removed_count} expired items")

        except Exception as e:
            self.logger.error(f"Cache optimization failed: {e}")


# Factory function for backward compatibility
def CacheManager(config=None):
    """
    Factory function to create appropriate cache manager.
    """
    return EnhancedCacheManager(
        cache_dir=config.get('cache_dir', './cache') if config else './cache',
        max_memory_items=config.get('max_memory_items', 1000) if config else 1000
    )