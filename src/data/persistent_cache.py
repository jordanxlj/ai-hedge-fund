"""
PersistentCache - 持久化文件缓存模块

提供基于文件系统的缓存功能，支持TTL过期机制和数据持久化存储。
"""
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import hashlib
from .data_config import get_cache_ttl
import logging

logger = logging.getLogger(__name__)

class PersistentCache:
    """File-based persistent cache with TTL support for API responses."""

    def __init__(self, cache_dir: str = ".cache", default_ttl: int = 3600):
        """
        Initialize persistent cache.
        
        Args:
            cache_dir: Directory to store cache files
            default_ttl: Default time-to-live in seconds (1 hour by default)
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.default_ttl = default_ttl
        
        # Cache metadata for TTL tracking
        self._cache_metadata: Dict[str, Dict[str, Any]] = {}
        
        # Load metadata from disk
        self._load_metadata()

    def _get_cache_key(self, prefix: str, **kwargs) -> str:
        """Generate a unique cache key based on parameters."""
        # Sort kwargs for consistent key generation
        sorted_kwargs = sorted(kwargs.items())
        key_string = f"{prefix}_{sorted_kwargs}"
        # Use hash for shorter, consistent keys
        return hashlib.md5(key_string.encode()).hexdigest()

    def _get_cache_file_path(self, cache_key: str) -> Path:
        """Get the file path for a cache key."""
        return self.cache_dir / f"{cache_key}.json"

    def _load_metadata(self):
        """Load cache metadata from disk."""
        metadata_file = self.cache_dir / "metadata.json"
        if metadata_file.exists():
            try:
                with open(metadata_file, 'r') as f:
                    self._cache_metadata = json.load(f)
            except (json.JSONDecodeError, IOError):
                self._cache_metadata = {}
        else:
            self._cache_metadata = {}

    def _save_metadata(self):
        """Save cache metadata to disk."""
        metadata_file = self.cache_dir / "metadata.json"
        try:
            with open(metadata_file, 'w') as f:
                json.dump(self._cache_metadata, f, indent=2)
        except IOError as e:
            print(f"Warning: Could not save cache metadata: {e}")

    def _is_expired(self, cache_key: str) -> bool:
        """Check if cache entry is expired."""
        if cache_key not in self._cache_metadata:
            return True
        
        metadata = self._cache_metadata[cache_key]
        if 'expires_at' not in metadata:
            return True
        
        return time.time() > metadata['expires_at']

    def _load_from_disk(self, cache_key: str) -> Optional[List[Dict[str, Any]]]:
        """Load data from disk cache."""
        cache_file = self._get_cache_file_path(cache_key)
        if not cache_file.exists():
            return None
        
        try:
            with open(cache_file, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            # Remove corrupted cache file
            try:
                cache_file.unlink()
            except OSError:
                pass
            return None

    def _save_to_disk(self, cache_key: str, data: List[Dict[str, Any]], ttl: int = None):
        """Save data to disk cache."""
        cache_file = self._get_cache_file_path(cache_key)
        
        try:
            with open(cache_file, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            
            # Update metadata
            ttl = ttl or self.default_ttl
            self._cache_metadata[cache_key] = {
                'created_at': time.time(),
                'expires_at': time.time() + ttl,
                'ttl': ttl,
                'size': len(data)
            }
            self._save_metadata()
            
        except IOError as e:
            print(f"Warning: Could not save cache to disk: {e}")

    def _merge_data(self, existing: List[Dict] | None, new_data: List[Dict], key_field: str) -> List[Dict]:
        """Merge existing and new data, avoiding duplicates based on a key field."""
        if not existing:
            return new_data

        # Create a set of existing keys for O(1) lookup
        existing_keys = {item[key_field] for item in existing}

        # Only add items that don't exist yet
        merged = existing.copy()
        merged.extend([item for item in new_data if item[key_field] not in existing_keys])
        return merged

    def get(self, cache_type: str, **kwargs) -> Optional[List[Dict[str, Any]]]:
        """
        Generic get method for any cache type.
        
        Args:
            cache_type: Type of cache (e.g., 'prices', 'financial_metrics')
            **kwargs: Parameters to generate cache key
        """
        cache_key = self._get_cache_key(cache_type, **kwargs)
        
        # Check if expired
        if self._is_expired(cache_key):
            return None
        
        # Load from disk
        return self._load_from_disk(cache_key)

    def set(self, cache_type: str, data: List[Dict[str, Any]], ttl: int = None, merge_key: str = None, **kwargs):
        """
        Generic set method for any cache type.
        
        Args:
            cache_type: Type of cache (e.g., 'prices', 'financial_metrics')
            data: Data to cache
            ttl: Time-to-live in seconds
            merge_key: Key field for merging with existing data
            **kwargs: Parameters to generate cache key
        """
        cache_key = self._get_cache_key(cache_type, **kwargs)
        
        # If merge_key is provided, merge with existing data
        if merge_key:
            existing_data = self.get(cache_type, **kwargs)
            data = self._merge_data(existing_data, data, merge_key)
        
        # Save to disk cache
        self._save_to_disk(cache_key, data, ttl)

    # Specific methods for different data types
    def get_prices(self, ticker: str, start_date: str, end_date: str) -> Optional[List[Dict[str, Any]]]:
        """Get cached price data if available."""
        return self.get('prices', ticker=ticker, start_date=start_date, end_date=end_date)

    def set_prices(self, ticker: str, start_date: str, end_date: str, data: List[Dict[str, Any]]):
        """Set price data to cache."""
        # Use TTL from configuration
        ttl = get_cache_ttl('prices')
        self.set('prices', data, ttl=ttl, merge_key='time', 
                ticker=ticker, start_date=start_date, end_date=end_date)

    def get_financial_metrics(self, ticker: str, period: str, end_date: str, limit: int) -> Optional[List[Dict[str, Any]]]:
        """Get cached financial metrics if available."""
        return self.get('financial_metrics', ticker=ticker, period=period, 
                      end_date=end_date, limit=limit)

    def set_financial_metrics(self, ticker: str, period: str, end_date: str, limit: int, data: List[Dict[str, Any]]):
        """Set financial metrics to cache."""
        # Use TTL from configuration
        ttl = get_cache_ttl('financial_metrics')
        self.set('financial_metrics', data, ttl=ttl, merge_key='report_period',
                ticker=ticker, period=period, end_date=end_date, limit=limit)

    def get_line_items(self, ticker: str, line_items: List[str], period: str, end_date: str, limit: int) -> Optional[List[Dict[str, Any]]]:
        """Get cached line items if available."""
        line_items_str = "_".join(sorted(line_items))  # Sort for consistent cache key
        result = self.get('line_items', ticker=ticker, line_items=line_items_str,
                      period=period, end_date=end_date, limit=limit)
        logger.debug(f"get_line_items result: {result}")
        return result

    def set_line_items(self, ticker: str, line_items: List[str], period: str, end_date: str, limit: int, data: List[Dict[str, Any]]):
        """Set line items to cache."""
        line_items_str = "_".join(sorted(line_items))  # Sort for consistent cache key
        # Use TTL from configuration
        ttl = get_cache_ttl('line_items')
        logger.debug(f"set_line_items: {line_items_str}")

        self.set('line_items', data, ttl=ttl, merge_key='report_period',
                ticker=ticker, line_items=line_items_str, period=period, end_date=end_date, limit=limit)

    def get_insider_trades(self, ticker: str, start_date: str, end_date: str, limit: int) -> Optional[List[Dict[str, Any]]]:
        """Get cached insider trades if available."""
        return self.get('insider_trades', ticker=ticker, start_date=start_date,
                      end_date=end_date, limit=limit)

    def set_insider_trades(self, ticker: str, start_date: str, end_date: str, limit: int, data: List[Dict[str, Any]]):
        """Set insider trades to cache."""
        # Use TTL from configuration
        ttl = get_cache_ttl('insider_trades')
        self.set('insider_trades', data, ttl=ttl, merge_key='filing_date',
                ticker=ticker, start_date=start_date, end_date=end_date, limit=limit)

    def get_company_news(self, ticker: str, start_date: str, end_date: str, limit: int) -> Optional[List[Dict[str, Any]]]:
        """Get cached company news if available."""
        return self.get('company_news', ticker=ticker, start_date=start_date,
                      end_date=end_date, limit=limit)

    def set_company_news(self, ticker: str, start_date: str, end_date: str, limit: int, data: List[Dict[str, Any]]):
        """Set company news to cache."""
        # Use TTL from configuration
        ttl = get_cache_ttl('company_news')
        self.set('company_news', data, ttl=ttl, merge_key='date',
                ticker=ticker, start_date=start_date, end_date=end_date, limit=limit)

    def clear_expired(self):
        """Remove all expired cache entries."""
        expired_keys = []
        for cache_key in self._cache_metadata:
            if self._is_expired(cache_key):
                expired_keys.append(cache_key)
        
        for cache_key in expired_keys:
            # Remove from disk
            cache_file = self._get_cache_file_path(cache_key)
            try:
                if cache_file.exists():
                    cache_file.unlink()
            except OSError:
                pass
            
            # Remove from metadata
            self._cache_metadata.pop(cache_key, None)
        
        if expired_keys:
            self._save_metadata()
        
        return len(expired_keys)

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_entries = len(self._cache_metadata)
        expired_entries = sum(1 for key in self._cache_metadata if self._is_expired(key))
        
        total_size = 0
        total_files_size = 0
        for cache_key, metadata in self._cache_metadata.items():
            total_size += metadata.get('size', 0)
            # Get actual file size
            cache_file = self._get_cache_file_path(cache_key)
            if cache_file.exists():
                total_files_size += cache_file.stat().st_size
        
        return {
            'total_entries': total_entries,
            'expired_entries': expired_entries,
            'active_entries': total_entries - expired_entries,
            'total_cached_items': total_size,
            'total_file_size_bytes': total_files_size,
            'total_file_size_mb': round(total_files_size / 1024 / 1024, 2),
            'cache_dir': str(self.cache_dir)
        }

    def force_refresh_ticker(self, ticker: str):
        """Force refresh all cache entries for a specific ticker."""
        removed_keys = []
        
        # Check all existing cache keys by loading and examining the data content
        for cache_key in list(self._cache_metadata.keys()):
            cache_file = self._get_cache_file_path(cache_key)
            try:
                if cache_file.exists():
                    # Load the data to check if it contains the ticker
                    with open(cache_file, 'r') as f:
                        data = json.load(f)
                        # Check if any item in the data contains the ticker
                        ticker_found = False
                        if isinstance(data, list):
                            for item in data:
                                if isinstance(item, dict) and item.get('ticker') == ticker:
                                    ticker_found = True
                                    break
                        
                        if ticker_found:
                            removed_keys.append(cache_key)
            except (OSError, json.JSONDecodeError):
                pass
        
        # Remove cache files and metadata
        for cache_key in removed_keys:
            # Remove the cache file
            cache_file = self._get_cache_file_path(cache_key)
            try:
                if cache_file.exists():
                    cache_file.unlink()
            except OSError:
                pass
            
            # Remove from metadata
            self._cache_metadata.pop(cache_key, None)
        
        if removed_keys:
            self._save_metadata()
        
        return len(removed_keys)

    def clear_all(self):
        """Clear all cache data."""
        # Remove all cache files
        for cache_file in self.cache_dir.glob("*.json"):
            if cache_file.name != "metadata.json":
                try:
                    cache_file.unlink()
                except OSError:
                    pass
        
        # Clear metadata
        self._cache_metadata = {}
        self._save_metadata()


# Global persistent cache instance
_persistent_cache = PersistentCache()


def get_persistent_cache() -> PersistentCache:
    """Get the global persistent cache instance."""
    return _persistent_cache


def clear_persistent_cache():
    """Clear all expired persistent cache entries."""
    return _persistent_cache.clear_expired()


def get_persistent_cache_stats():
    """Get persistent cache statistics."""
    return _persistent_cache.get_cache_stats() 