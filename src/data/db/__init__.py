"""
数据库API工厂

此模块提供了一个工厂函数，用于根据配置获取适当的数据库API实现。
"""

from .base import DatabaseAPI
from .duckdb_impl import DuckDBAPI
from typing import Dict, Any

def get_database_api(db_type: str = "duckdb", **kwargs: Any) -> DatabaseAPI:
    """
    数据库API工厂函数。

    Args:
        db_type: 数据库类型 (例如, "duckdb")。
        **kwargs: 传递给数据库API构造函数的参数。

    Returns:
        一个 DatabaseAPI 的实例。
        
    Raises:
        ValueError: 如果指定的 db_type 不受支持。
    """
    if db_type.lower() == "duckdb":
        # 确保 'db_path' 参数被提供
        if "db_path" not in kwargs:
            raise ValueError("DuckDBAPI requires a 'db_path' argument.")
        return DuckDBAPI(db_path=kwargs["db_path"])
    
    # 在此添加对其他数据库类型的支持
    # elif db_type.lower() == "postgresql":
    #     return PostgreSQLAPI(**kwargs)
    
    else:
        raise ValueError(f"Unsupported database type: {db_type}") 