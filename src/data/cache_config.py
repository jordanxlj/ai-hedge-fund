"""Cache configuration for TTL policies."""

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    import json
    YAML_AVAILABLE = False
    print("Warning: PyYAML not available, falling back to JSON format")

from pathlib import Path
from datetime import datetime
from typing import Dict, Any


class CacheConfig:
    """Manages cache TTL configurations."""
    
    def __init__(self, config_file: str = None):
        """
        Initialize cache configuration.
        
        Args:
            config_file: Path to the configuration file
        """
        if config_file is None:
            # Use YAML if available, otherwise JSON
            if YAML_AVAILABLE:
                config_file = "conf/cache_config.yaml"
            else:
                config_file = "conf/cache_config.json"
        
        self.config_file = Path(config_file)
        self.config_file.parent.mkdir(exist_ok=True)
        
        # Default TTL configurations (in seconds)
        self.default_config = {
            "prices": {
                "market_hours": 3600,      # 1 hour during market hours
                "after_hours": 86400       # 24 hours after market hours
            },
            "financial_metrics": {
                "default": 86400           # 24 hours
            },
            "line_items": {
                "default": 86400           # 24 hours
            },
            "insider_trades": {
                "default": 21600           # 6 hours
            },
            "company_news": {
                "default": 3600            # 1 hour
            },
            "llm_responses": {
                "default": 86400           # 24 hours
            }
        }
        
        # Load configuration from file
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file."""
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    if YAML_AVAILABLE and self.config_file.suffix == '.yaml':
                        loaded_config = yaml.safe_load(f)
                    else:
                        loaded_config = json.load(f)
                # Merge with defaults (in case new types are added)
                config = self.default_config.copy()
                config.update(loaded_config)
                return config
            except (yaml.YAMLError, json.JSONDecodeError, IOError) as e:
                print(f"Warning: Could not load cache config from {self.config_file}: {e}, using defaults")
        
        # Save default config to file
        self._save_config(self.default_config)
        return self.default_config.copy()
    
    def _save_config(self, config: Dict[str, Any]):
        """Save configuration to file."""
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                if YAML_AVAILABLE and self.config_file.suffix == '.yaml':
                    yaml.dump(config, f, default_flow_style=False, indent=2, allow_unicode=True)
                else:
                    json.dump(config, f, indent=2, ensure_ascii=False)
        except IOError as e:
            print(f"Warning: Could not save cache config: {e}")
    
    def get_ttl(self, cache_type: str, **kwargs) -> int:
        """
        Get TTL for a specific cache type.
        
        Args:
            cache_type: Type of cache (e.g., 'prices', 'financial_metrics')
            **kwargs: Additional parameters that might affect TTL
        
        Returns:
            TTL in seconds
        """
        if cache_type not in self.config:
            print(f"Warning: Unknown cache type '{cache_type}', using default TTL")
            return 3600  # 1 hour default
        
        type_config = self.config[cache_type]
        
        # Special handling for prices based on market hours
        if cache_type == "prices":
            current_hour = datetime.now().hour
            is_market_hours = 9 <= current_hour <= 16  # Rough market hours
            return type_config["market_hours"] if is_market_hours else type_config["after_hours"]
        
        # Default handling
        return type_config.get("default", 3600)
    
    def set_ttl(self, cache_type: str, ttl_config: Dict[str, int]):
        """
        Set TTL configuration for a cache type.
        
        Args:
            cache_type: Type of cache
            ttl_config: TTL configuration dictionary
        """
        self.config[cache_type] = ttl_config
        self._save_config(self.config)
    
    def get_all_config(self) -> Dict[str, Any]:
        """Get all TTL configurations."""
        return self.config.copy()
    
    def reset_to_defaults(self):
        """Reset configuration to defaults."""
        self.config = self.default_config.copy()
        self._save_config(self.config)


# Global cache config instance
_cache_config = CacheConfig()


def get_cache_config() -> CacheConfig:
    """Get the global cache configuration instance."""
    return _cache_config


def get_cache_ttl(cache_type: str, **kwargs) -> int:
    """Get TTL for a cache type."""
    return _cache_config.get_ttl(cache_type, **kwargs)


def set_cache_ttl(cache_type: str, ttl_config: Dict[str, int]):
    """Set TTL configuration for a cache type."""
    _cache_config.set_ttl(cache_type, ttl_config) 