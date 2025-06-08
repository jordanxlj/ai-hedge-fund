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


class DataConfig:
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
                config_file = "conf/data_config.yaml"
            else:
                config_file = "conf/data_config.json"
        
        self.config_file = Path(config_file)
        self.config_file.parent.mkdir(exist_ok=True)
        
        # Default configuration with TTL in interface properties
        self.default_config = {
            # Interface configurations with embedded TTL
            "interfaces": {
                # Financial Data APIs
                "get_prices": {
                    "cache_type": "prices",
                    "description": "Stock price data from financialdatasets.ai",
                    "cache_layers": ["memory", "persistent"],
                    "merge_key": "time",
                    "ttl": {
                        "market_hours": 3600,      # 1 hour during market hours
                        "after_hours": 86400       # 24 hours after market hours
                    }
                },
                "get_financial_metrics": {
                    "cache_type": "financial_metrics", 
                    "description": "Financial metrics data from financialdatasets.ai",
                    "cache_layers": ["memory", "persistent"],
                    "merge_key": "report_period",
                    "ttl": {
                        "default": 86400           # 24 hours
                    }
                },
                "search_line_items": {
                    "cache_type": "line_items",
                    "description": "Financial statement line items from financialdatasets.ai", 
                    "cache_layers": ["memory", "persistent"],
                    "merge_key": "report_period",
                    "ttl": {
                        "default": 86400           # 24 hours
                    }
                },
                "get_insider_trades": {
                    "cache_type": "insider_trades",
                    "description": "Insider trading data from financialdatasets.ai",
                    "cache_layers": ["memory", "persistent"], 
                    "merge_key": "filing_date",
                    "ttl": {
                        "default": 21600           # 6 hours
                    }
                },
                "get_company_news": {
                    "cache_type": "company_news",
                    "description": "Company news from financialdatasets.ai",
                    "cache_layers": ["memory", "persistent"],
                    "merge_key": "date",
                    "ttl": {
                        "default": 3600            # 1 hour
                    }
                },
                
                # LLM APIs
                "call_llm_deepseek": {
                    "cache_type": "llm_responses",
                    "description": "DeepSeek LLM responses (deepseek-reasoner, deepseek-chat)",
                    "cache_layers": ["persistent"],
                    "providers": ["DeepSeek"],
                    "models": ["deepseek-reasoner", "deepseek-chat", "deepseek-*"],
                    "cache_key_components": ["prompt", "model_name", "pydantic_model", "agent_name"],
                    "ttl": {
                        "default": 86400           # 24 hours
                    }
                },
                "call_llm_other": {
                    "cache_type": "none", 
                    "description": "Other LLM providers (no caching)",
                    "cache_layers": [],
                    "providers": ["OpenAI", "Anthropic", "Gemini", "Groq", "Ollama"],
                    "ttl": {
                        "default": 0               # No caching
                    }
                }
            },
            
            # Agent Model Configuration
            "agent_models": {
                "portfolio_manager": {
                    "default_model": "gpt-4o",
                    "default_provider": "OpenAI"
                },
                "bill_ackman": {
                    "default_model": "gpt-4o", 
                    "default_provider": "OpenAI"
                },
                "michael_burry": {
                    "default_model": "gpt-4o",
                    "default_provider": "OpenAI"
                },
                "rakesh_jhunjhunwala": {
                    "default_model": "gpt-4o",
                    "default_provider": "OpenAI"
                }
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
        Get TTL for a specific cache type by finding the corresponding interface.
        
        Args:
            cache_type: Type of cache (e.g., 'prices', 'financial_metrics')
            **kwargs: Additional parameters that might affect TTL
        
        Returns:
            TTL in seconds
        """
        # Find interface with this cache_type
        interfaces = self.config.get("interfaces", {})
        interface_config = None
        
        for interface_name, config in interfaces.items():
            if config.get("cache_type") == cache_type:
                interface_config = config
                break
        
        if not interface_config:
            print(f"Warning: Unknown cache type '{cache_type}', using default TTL")
            return 3600  # 1 hour default
        
        ttl_config = interface_config.get("ttl", {})
        
        # Special handling for prices based on market hours
        if cache_type == "prices":
            current_hour = datetime.now().hour
            is_market_hours = 9 <= current_hour <= 16  # Rough market hours
            return ttl_config.get("market_hours", 3600) if is_market_hours else ttl_config.get("after_hours", 86400)
        
        # Default handling
        return ttl_config.get("default", 3600)
    
    def set_ttl(self, cache_type: str, ttl_config: Dict[str, int]):
        """
        Set TTL configuration for a cache type by finding the corresponding interface.
        
        Args:
            cache_type: Type of cache
            ttl_config: TTL configuration dictionary
        """
        interfaces = self.config.get("interfaces", {})
        
        # Find interface with this cache_type
        for interface_name, config in interfaces.items():
            if config.get("cache_type") == cache_type:
                config["ttl"] = ttl_config
                self._save_config(self.config)
                return
        
        print(f"Warning: Cache type '{cache_type}' not found in any interface")
    
    def set_interface_ttl(self, interface_name: str, ttl_config: Dict[str, int]):
        """
        Set TTL configuration for a specific interface.
        
        Args:
            interface_name: Name of the interface
            ttl_config: TTL configuration dictionary
        """
        if "interfaces" not in self.config:
            self.config["interfaces"] = {}
        
        if interface_name not in self.config["interfaces"]:
            print(f"Warning: Interface '{interface_name}' not found")
            return
        
        self.config["interfaces"][interface_name]["ttl"] = ttl_config
        self._save_config(self.config)
    
    def get_all_config(self) -> Dict[str, Any]:
        """Get all TTL configurations."""
        return self.config.copy()
    
    def reset_to_defaults(self):
        """Reset configuration to defaults."""
        self.config = self.default_config.copy()
        self._save_config(self.config)
    
    def get_interfaces(self) -> Dict[str, Any]:
        """Get all interface configurations."""
        return self.config.get("interfaces", {})
    
    def get_interface_config(self, interface_name: str) -> Dict[str, Any]:
        """Get full configuration for a specific interface."""
        interfaces = self.get_interfaces()
        return interfaces.get(interface_name, {})
    
    def get_interface_cache_type(self, interface_name: str) -> str:
        """Get cache type for a specific interface."""
        interface_config = self.get_interface_config(interface_name)
        return interface_config.get("cache_type", "none")
    
    def get_interface_description(self, interface_name: str) -> str:
        """Get description for a specific interface."""
        interface_config = self.get_interface_config(interface_name)
        return interface_config.get("description", "No description available")
    
    def get_cache_layers(self, interface_name: str) -> list:
        """Get cache layers used by a specific interface."""
        interface_config = self.get_interface_config(interface_name)
        return interface_config.get("cache_layers", [])
    
    def get_interface_ttl(self, interface_name: str) -> Dict[str, int]:
        """Get TTL configuration for a specific interface."""
        interface_config = self.get_interface_config(interface_name)
        return interface_config.get("ttl", {"default": 3600})
    
    def get_agent_default_model(self, agent_name: str) -> tuple:
        """Get default model and provider for an agent."""
        agent_models = self.config.get("agent_models", {})
        agent_config = agent_models.get(agent_name, {})
        model = agent_config.get("default_model", "gpt-4o")
        provider = agent_config.get("default_provider", "OpenAI")
        return model, provider
    
    def list_cached_interfaces(self) -> list:
        """List all interfaces that use caching."""
        interfaces = self.get_interfaces()
        cached_interfaces = []
        for interface, config in interfaces.items():
            if config.get("cache_type", "none") != "none":
                cached_interfaces.append(interface)
        return cached_interfaces
    
    def get_data_provider_config(self) -> Dict[str, Any]:
        """获取数据提供商配置"""
        try:
            config = self._load_config()
            return config.get('data_providers', {
                'default': 'financial_datasets',
                'available': {
                    'financial_datasets': {
                        'name': 'FinancialDatasets.ai',
                        'description': '美股财务数据API服务'
                    },
                    'tushare': {
                        'name': 'Tushare Pro',
                        'description': '中国A股财务数据API服务'
                    }
                }
            })
        except Exception as e:
            print(f"Warning: 无法获取数据提供商配置 - {e}")
            return {'default': 'financial_datasets', 'available': {}}
    
    def get_default_data_provider(self) -> str:
        """获取默认数据提供商"""
        try:
            config = self.get_data_provider_config()
            return config.get('default', 'financial_datasets')
        except Exception as e:
            print(f"Warning: 无法获取默认数据提供商 - {e}")
            return 'financial_datasets'
    
    def set_default_data_provider(self, provider_name: str):
        """设置默认数据提供商"""
        try:
            config = self._load_config()
            if 'data_providers' not in config:
                config['data_providers'] = {'available': {}}
            
            config['data_providers']['default'] = provider_name
            self._save_config(config)
            print(f"默认数据提供商已设置为: {provider_name}")
        except Exception as e:
            print(f"Error: 无法设置默认数据提供商 - {e}")
    
    def get_available_data_providers(self) -> Dict[str, Any]:
        """获取可用的数据提供商列表"""
        try:
            config = self.get_data_provider_config()
            return config.get('available', {})
        except Exception as e:
            print(f"Warning: 无法获取可用数据提供商列表 - {e}")
            return {}
    
    def get_timeout_config(self, interface_name: str) -> Dict[str, Any]:
        """获取接口的超时配置"""
        interface_config = self.get_interface_config(interface_name)
        return interface_config.get("timeout", {
            "timeout_seconds": 30,  # 默认超时时间
            "max_retries": 3,       # 默认重试次数
            "retry_delay_factor": 0.1  # 默认重试延迟系数
        })
    
    def get_timeout_seconds(self, interface_name: str) -> int:
        """获取接口的超时时间（秒）"""
        timeout_config = self.get_timeout_config(interface_name)
        return timeout_config.get("timeout_seconds", 30)
    
    def get_max_retries(self, interface_name: str) -> int:
        """获取接口的最大重试次数"""
        timeout_config = self.get_timeout_config(interface_name)
        return timeout_config.get("max_retries", 3)
    
    def get_retry_delay_factor(self, interface_name: str) -> float:
        """获取接口的重试延迟系数"""
        timeout_config = self.get_timeout_config(interface_name)
        return timeout_config.get("retry_delay_factor", 0.1)
    
    def get_retry_delay(self, interface_name: str) -> float:
        """计算实际的重试延迟时间（秒）"""
        timeout_seconds = self.get_timeout_seconds(interface_name)
        delay_factor = self.get_retry_delay_factor(interface_name)
        return timeout_seconds * delay_factor


# Global cache config instance
_data_config = DataConfig()


def get_data_config() -> DataConfig:
    """Get the global data configuration instance."""
    return _data_config


# Backward compatibility alias
def get_cache_config() -> DataConfig:
    """Deprecated: Use get_data_config() instead."""
    return get_data_config()


def get_cache_ttl(cache_type: str, **kwargs) -> int:
    """Get TTL for a cache type."""
    return _data_config.get_ttl(cache_type, **kwargs)


def set_cache_ttl(cache_type: str, ttl_config: Dict[str, int]):
    """Set TTL configuration for a cache type."""
    _data_config.set_ttl(cache_type, ttl_config)


def get_timeout_config(interface_name: str) -> Dict[str, Any]:
    """Get timeout configuration for an interface."""
    return _data_config.get_timeout_config(interface_name)


def get_timeout_seconds(interface_name: str) -> int:
    """Get timeout seconds for an interface."""
    return _data_config.get_timeout_seconds(interface_name)


def get_max_retries(interface_name: str) -> int:
    """Get max retries for an interface."""
    return _data_config.get_max_retries(interface_name)


def get_retry_delay(interface_name: str) -> float:
    """Get retry delay for an interface."""
    return _data_config.get_retry_delay(interface_name) 