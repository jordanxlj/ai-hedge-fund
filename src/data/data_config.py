"""Cache configuration for TTL policies."""

from pathlib import Path
from datetime import datetime
from typing import Dict, Any
from src.utils.config_utils import load_yaml_config, save_yaml_config


class DataConfig:
    """Manages cache TTL configurations."""
    
    def __init__(self, config_file: str = None):
        """
        Initialize cache configuration.
        
        Args:
            config_file: Path to the configuration file
        """
        if config_file is None:
            config_file = "conf/data_config.yaml"
        
        self.config_file = Path(config_file)
        
        # 检查配置文件是否存在，如果不存在则创建默认配置
        if not self.config_file.exists():
            self._create_default_config()
        
        # Load configuration from file
        try:
            self.config = load_yaml_config(self.config_file)
        except Exception as e:
            raise RuntimeError(f"加载配置文件失败: {self.config_file}, 错误: {e}")
    
    def _create_default_config(self):
        """Create a default configuration file."""
        default_config = {
            "agent_models": {
                "bill_ackman": {
                    "default_model": "gpt-4o",
                    "default_provider": "OpenAI"
                },
                "michael_burry": {
                    "default_model": "gpt-4o",
                    "default_provider": "OpenAI"
                },
                "portfolio_manager": {
                    "default_model": "gpt-4o",
                    "default_provider": "OpenAI"
                },
                "rakesh_jhunjhunwala": {
                    "default_model": "gpt-4o",
                    "default_provider": "OpenAI"
                }
            },
            "data_providers": {
                "available": {
                    "financial_datasets": {
                        "api_key_env": "FINANCIAL_DATASETS_API_KEY",
                        "name": "FinancialDatasets.ai"
                    },
                    "tushare": {
                        "api_key_env": "TUSHARE_API_KEY",
                        "name": "Tushare Pro"
                    }
                },
                "default": "tushare"
            },
            "interfaces": {
                "call_llm_deepseek": {
                    "cache_key_components": ["prompt", "model_name", "pydantic_model", "agent_name"],
                    "cache_layers": ["persistent"],
                    "cache_type": "llm_responses",
                    "models": ["deepseek-reasoner", "deepseek-chat", "deepseek-*"],
                    "providers": ["DeepSeek"],
                    "timeout": {
                        "timeout_seconds": 30,
                        "max_retries": 3,
                        "retry_delay_factor": 0.1
                    },
                    "ttl": {
                        "default": 86400
                    }
                },
                "call_llm_other": {
                    "cache_layers": [],
                    "cache_type": "none",
                    "providers": ["OpenAI", "Anthropic", "Gemini", "Groq", "Ollama"],
                    "timeout": {
                        "timeout_seconds": 30,
                        "max_retries": 3,
                        "retry_delay_factor": 0.1
                    },
                    "ttl": {
                        "default": 0
                    }
                },
                "get_company_news": {
                    "cache_layers": ["memory", "persistent"],
                    "cache_type": "company_news",
                    "merge_key": "date",
                    "timeout": {
                        "timeout_seconds": 5,
                        "max_retries": 3,
                        "retry_delay_factor": 0.5
                    },
                    "ttl": {
                        "default": 86400
                    }
                },
                "get_financial_metrics": {
                    "cache_layers": ["memory", "persistent"],
                    "cache_type": "financial_metrics",
                    "merge_key": "report_period",
                    "timeout": {
                        "timeout_seconds": 5,
                        "max_retries": 3,
                        "retry_delay_factor": 0.5
                    },
                    "ttl": {
                        "default": 86400
                    }
                },
                "get_insider_trades": {
                    "cache_layers": ["memory", "persistent"],
                    "cache_type": "insider_trades",
                    "merge_key": "filing_date",
                    "timeout": {
                        "timeout_seconds": 5,
                        "max_retries": 3,
                        "retry_delay_factor": 0.5
                    },
                    "ttl": {
                        "default": 86400
                    }
                },
                "get_prices": {
                    "cache_layers": ["memory", "persistent"],
                    "cache_type": "prices",
                    "merge_key": "time",
                    "timeout": {
                        "timeout_seconds": 5,
                        "max_retries": 3,
                        "retry_delay_factor": 0.5
                    },
                    "ttl": {
                        "after_hours": 86400,
                        "market_hours": 3600
                    }
                },
                "search_line_items": {
                    "cache_layers": ["memory", "persistent"],
                    "cache_type": "line_items",
                    "merge_key": "report_period",
                    "timeout": {
                        "timeout_seconds": 5,
                        "max_retries": 3,
                        "retry_delay_factor": 0.5
                    },
                    "ttl": {
                        "default": 86400
                    }
                }
            }
        }
        
        # Create directory if it doesn't exist
        self.config_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Save the default configuration
        if not save_yaml_config(self.config_file, default_config):
            raise RuntimeError(f"无法创建默认配置文件: {self.config_file}")
    
    def _save_config(self, config: Dict[str, Any]):
        """Save configuration to file."""
        if not save_yaml_config(self.config_file, config):
            raise RuntimeError(f"保存配置文件失败: {self.config_file}")
    
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
        """Reset configuration to defaults. 此方法已不再支持，因为不再有默认配置。"""
        raise NotImplementedError("不再支持重置到默认配置，请确保配置文件存在且正确")
    
    def reload_config(self):
        """重新加载配置文件"""
        try:
            self.config = load_yaml_config(self.config_file)
        except Exception as e:
            raise RuntimeError(f"重新加载配置文件失败: {self.config_file}, 错误: {e}")
    
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
        data_providers = self.config.get('data_providers')
        if not data_providers:
            raise ValueError("配置文件中缺少 data_providers 配置")
        return data_providers
    
    def get_default_data_provider(self) -> str:
        """获取默认数据提供商"""
        config = self.get_data_provider_config()
        default_provider = config.get('default')
        if not default_provider:
            raise ValueError("配置文件中缺少默认数据提供商配置")
        return default_provider
    
    def set_default_data_provider(self, provider_name: str):
        """设置默认数据提供商"""
        if 'data_providers' not in self.config:
            raise ValueError("配置文件中缺少 data_providers 配置")
        
        self.config['data_providers']['default'] = provider_name
        self._save_config(self.config)
        print(f"默认数据提供商已设置为: {provider_name}")
    
    def get_available_data_providers(self) -> Dict[str, Any]:
        """获取可用的数据提供商列表"""
        config = self.get_data_provider_config()
        available = config.get('available')
        if available is None:
            raise ValueError("配置文件中缺少可用数据提供商配置")
        return available
    
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