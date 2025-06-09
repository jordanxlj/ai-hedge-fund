"""通用配置工具模块，提供YAML和JSON配置文件的加载和保存功能。"""

import os
import json
from pathlib import Path
from typing import Dict, Any, Optional, Union
import logging

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    yaml = None
    YAML_AVAILABLE = False

logger = logging.getLogger(__name__)


class ConfigLoader:
    """通用配置加载器，支持YAML和JSON格式"""
    
    def __init__(self, config_file: Union[str, Path], default_config: Optional[Dict[str, Any]] = None):
        """
        初始化配置加载器
        
        Args:
            config_file: 配置文件路径
            default_config: 默认配置字典
        """
        self.config_file = Path(config_file)
        self.default_config = default_config or {}
        
        # 确保配置文件目录存在
        self.config_file.parent.mkdir(parents=True, exist_ok=True)
    
    def load_config(self) -> Dict[str, Any]:
        """
        从文件加载配置
        
        Returns:
            Dict[str, Any]: 配置字典
        """
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    if YAML_AVAILABLE and self.config_file.suffix == '.yaml':
                        loaded_config = yaml.safe_load(f)
                    else:
                        loaded_config = json.load(f)
                
                if loaded_config is None:
                    loaded_config = {}
                
                # 与默认配置合并（新类型可能会被添加）
                config = self.default_config.copy()
                config.update(loaded_config)
                return config
                
            except (yaml.YAMLError if YAML_AVAILABLE else Exception, json.JSONDecodeError, IOError) as e:
                logger.warning(f"无法从 {self.config_file} 加载配置: {e}，使用默认配置")
        
        # 如果文件不存在或加载失败，保存默认配置到文件
        if self.default_config:
            self.save_config(self.default_config)
        
        return self.default_config.copy()
    
    def save_config(self, config: Dict[str, Any]) -> bool:
        """
        保存配置到文件
        
        Args:
            config: 要保存的配置字典
            
        Returns:
            bool: 保存是否成功
        """
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                if YAML_AVAILABLE and self.config_file.suffix == '.yaml':
                    yaml.dump(config, f, default_flow_style=False, indent=2, allow_unicode=True)
                else:
                    json.dump(config, f, indent=2, ensure_ascii=False)
            
            logger.debug(f"配置已保存到: {self.config_file}")
            return True
            
        except IOError as e:
            logger.error(f"无法保存配置到 {self.config_file}: {e}")
            return False
    
    def reload_config(self) -> Dict[str, Any]:
        """
        重新加载配置文件
        
        Returns:
            Dict[str, Any]: 重新加载的配置字典
        """
        logger.info(f"重新加载配置文件: {self.config_file}")
        return self.load_config()
    
    def get_config_path(self) -> Path:
        """获取配置文件路径"""
        return self.config_file
    
    def config_exists(self) -> bool:
        """检查配置文件是否存在"""
        return self.config_file.exists()


def load_yaml_config(config_file: Union[str, Path], default_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    快速加载YAML配置文件的便捷函数
    
    Args:
        config_file: 配置文件路径
        default_config: 默认配置字典
        
    Returns:
        Dict[str, Any]: 配置字典
    """
    loader = ConfigLoader(config_file, default_config)
    return loader.load_config()


def save_yaml_config(config_file: Union[str, Path], config: Dict[str, Any]) -> bool:
    """
    快速保存YAML配置文件的便捷函数
    
    Args:
        config_file: 配置文件路径
        config: 要保存的配置字典
        
    Returns:
        bool: 保存是否成功
    """
    loader = ConfigLoader(config_file)
    return loader.save_config(config)


def load_json_config(config_file: Union[str, Path], default_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    快速加载JSON配置文件的便捷函数
    
    Args:
        config_file: 配置文件路径
        default_config: 默认配置字典
        
    Returns:
        Dict[str, Any]: 配置字典
    """
    # 确保使用.json扩展名
    config_path = Path(config_file)
    if config_path.suffix != '.json':
        config_path = config_path.with_suffix('.json')
    
    loader = ConfigLoader(config_path, default_config)
    return loader.load_config()


def save_json_config(config_file: Union[str, Path], config: Dict[str, Any]) -> bool:
    """
    快速保存JSON配置文件的便捷函数
    
    Args:
        config_file: 配置文件路径
        config: 要保存的配置字典
        
    Returns:
        bool: 保存是否成功
    """
    # 确保使用.json扩展名
    config_path = Path(config_file)
    if config_path.suffix != '.json':
        config_path = config_path.with_suffix('.json')
    
    loader = ConfigLoader(config_path)
    return loader.save_config(config)


def get_config_file_path(base_path: Union[str, Path], config_name: str, prefer_yaml: bool = True) -> Path:
    """
    智能选择配置文件路径，根据YAML可用性和用户偏好选择格式
    
    Args:
        base_path: 配置文件基础路径（目录）
        config_name: 配置文件名（不含扩展名）
        prefer_yaml: 是否偏好YAML格式
        
    Returns:
        Path: 配置文件路径
    """
    base_path = Path(base_path)
    
    if YAML_AVAILABLE and prefer_yaml:
        return base_path / f"{config_name}.yaml"
    else:
        return base_path / f"{config_name}.json"


def merge_configs(base_config: Dict[str, Any], override_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    递归合并两个配置字典
    
    Args:
        base_config: 基础配置
        override_config: 覆盖配置
        
    Returns:
        Dict[str, Any]: 合并后的配置
    """
    merged = base_config.copy()
    
    for key, value in override_config.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = merge_configs(merged[key], value)
        else:
            merged[key] = value
    
    return merged


def validate_config_structure(config: Dict[str, Any], required_keys: list, config_name: str = "配置") -> bool:
    """
    验证配置结构是否包含必需的键
    
    Args:
        config: 要验证的配置
        required_keys: 必需的键列表
        config_name: 配置名称（用于日志）
        
    Returns:
        bool: 配置是否有效
    """
    missing_keys = []
    for key in required_keys:
        if key not in config:
            missing_keys.append(key)
    
    if missing_keys:
        logger.error(f"{config_name}缺少必需的配置项: {missing_keys}")
        return False
    
    return True


# 保持向后兼容性的便捷函数
def load_config_file(config_file: Union[str, Path], default_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    自动检测文件格式并加载配置文件（向后兼容）
    
    Args:
        config_file: 配置文件路径
        default_config: 默认配置字典
        
    Returns:
        Dict[str, Any]: 配置字典
    """
    return load_yaml_config(config_file, default_config)


def save_config_file(config_file: Union[str, Path], config: Dict[str, Any]) -> bool:
    """
    自动检测文件格式并保存配置文件（向后兼容）
    
    Args:
        config_file: 配置文件路径
        config: 要保存的配置字典
        
    Returns:
        bool: 保存是否成功
    """
    return save_yaml_config(config_file, config) 