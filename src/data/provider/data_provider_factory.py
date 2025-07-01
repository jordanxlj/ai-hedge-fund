import os
import logging
from typing import Optional, Dict, Type
from enum import Enum

from src.data.provider.abstract_data_provider import AbstractDataProvider
from src.data.provider.financial_datasets_provider import FinancialDatasetsProvider
from src.data.provider.tushare_provider import TushareProvider
from src.data.provider.futu_provider import FutuDataProvider
from src.data.provider.yfinance_provider import YFinanceProvider

logger = logging.getLogger(__name__)


class DataProviderType(Enum):
    """数据提供商类型枚举"""
    FINANCIAL_DATASETS = "financial_datasets"
    TUSHARE = "tushare"
    FUTU = "futu"
    YFINANCE = "yfinance"


class DataProviderFactory:
    """数据提供商工厂类"""
    
    _providers: Dict[DataProviderType, Type[AbstractDataProvider]] = {
        DataProviderType.FINANCIAL_DATASETS: FinancialDatasetsProvider,
        DataProviderType.TUSHARE: TushareProvider,
        DataProviderType.FUTU: FutuDataProvider,
        DataProviderType.YFINANCE: YFinanceProvider,
    }
    
    _instances: Dict[DataProviderType, AbstractDataProvider] = {}
    
    @classmethod
    def create_provider(
        self,
        provider_type: DataProviderType,
        api_key: Optional[str] = None
    ) -> AbstractDataProvider:
        """创建数据提供商实例"""
        
        if provider_type not in self._providers:
            raise ValueError(f"不支持的数据提供商类型: {provider_type}")
        
        # 使用单例模式，避免重复创建
        if provider_type not in self._instances:
            provider_class = self._providers[provider_type]
            self._instances[provider_type] = provider_class(api_key=api_key)
            logger.info(f"创建数据提供商实例: {provider_type.value}")
        
        return self._instances[provider_type]
    
    @classmethod
    def get_provider_by_name(
        self,
        provider_name: str,
        api_key: Optional[str] = None
    ) -> AbstractDataProvider:
        """根据名称获取数据提供商"""
        try:
            provider_type = DataProviderType(provider_name.lower())
            return self.create_provider(provider_type, api_key)
        except ValueError:
            raise ValueError(f"不支持的数据提供商名称: {provider_name}")
    
    @classmethod
    def get_available_providers(self) -> Dict[str, bool]:
        """获取所有可用的数据提供商状态"""
        status = {}
        for provider_type in DataProviderType:
            try:
                provider = self.create_provider(provider_type)
                status[provider_type.value] = provider.is_available()
            except Exception as e:
                logger.error(f"检查数据提供商 {provider_type.value} 可用性失败: {e}")
                status[provider_type.value] = False
        
        return status
    
    @classmethod
    def get_default_provider(self) -> AbstractDataProvider:
        """获取默认数据提供商"""
        # 首先尝试从环境变量获取配置
        default_provider_name = os.environ.get("DEFAULT_DATA_PROVIDER", "financial_datasets")
        
        try:
            return self.get_provider_by_name(default_provider_name)
        except ValueError:
            logger.warning(f"默认数据提供商 {default_provider_name} 不可用，使用 financial_datasets")
            return self.create_provider(DataProviderType.FINANCIAL_DATASETS)
    
    @classmethod
    def clear_instances(self):
        """清除所有实例（主要用于测试）"""
        self._instances.clear()
        logger.info("已清除所有数据提供商实例")

    @classmethod
    def _check_futu_availability(cls) -> dict:
        """检查 Futu 数据提供商可用性"""
        try:
            from src.data.provider.futu_provider import FutuDataProvider
            provider = FutuDataProvider()
            is_available = provider.is_available()
            return {
                "available": is_available,
                "name": "Futu OpenAPI",
                "description": "富途证券开放平台 API",
                "requires_api_key": False,
                "requires_opend": True,
                "status": "connected" if is_available else "disconnected"
            }
        except Exception as e:
            return {
                "available": False,
                "name": "Futu OpenAPI", 
                "description": "富途证券开放平台 API",
                "requires_api_key": False,
                "requires_opend": True,
                "status": "error",
                "error": str(e)
            }

SUPPORTED_PROVIDERS = {
    "futu": FutuDataProvider,
    "tushare": TushareProvider,
    "yfinance": YFinanceProvider,
    "financial_datasets": FinancialDatasetsProvider,
} 