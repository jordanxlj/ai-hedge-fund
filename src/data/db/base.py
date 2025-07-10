"""
定义数据库接口的抽象基类。
"""

from abc import ABC, abstractmethod
from typing import Type, List, Dict, Any, Optional
from pydantic import BaseModel
import pandas as pd


class DatabaseAPI(ABC):
    """
    数据库操作的抽象基类 (API)。
    所有具体的数据库实现都应继承此类并实现其抽象方法。
    """

    @abstractmethod
    def connect(self, read_only: bool = True, **kwargs: Any) -> Any:
        """建立数据库连接并返回连接对象。"""
        pass

    @abstractmethod
    def close(self) -> None:
        """关闭数据库连接。"""
        pass

    @abstractmethod
    def create_table_from_model(
        self,
        table_name: str,
        model: Type[BaseModel],
        primary_keys: Optional[List[str]] = None
    ) -> None:
        """
        根据 Pydantic 模型创建表。

        Args:
            table_name: 表名。
            model: Pydantic 模型类。
            primary_keys: 主键字段列表。
        """
        pass

    @abstractmethod
    def upsert_data_from_models(
        self,
        table_name: str,
        data: List[BaseModel],
        primary_keys: List[str]
    ) -> None:
        """
        将 Pydantic 模型列表插入或更新到表中。

        Args:
            table_name: 表名。
            data: Pydantic 模型实例列表。
            primary_keys: 主键字段列表。
        """
        pass

    @abstractmethod
    def upsert_data_from_dicts(
        self,
        table_name: str,
        data: List[Dict[str, Any]],
        primary_keys: List[str]
    ) -> None:
        """
        将字典列表插入或更新到表中。

        Args:
            table_name: 表名。
            data: 字典数据列表。
            primary_keys: 主键字段列表。
        """
        pass

    @abstractmethod
    def insert_dataframe(
        self,
        table_name: str,
        df: pd.DataFrame,
        if_exists: str = "append"
    ) -> None:
        """
        将 DataFrame 插入到表中。

        Args:
            table_name: 表名。
            df: Pandas DataFrame。
            if_exists: 如果表已存在该如何操作 ('fail', 'replace', 'append')。
        """
        pass

    @abstractmethod
    def query_to_models(
        self,
        query: str,
        model: Type[BaseModel],
        params: Optional[List[Any]] = None
    ) -> List[BaseModel]:
        """
        执行查询并将结果转换为 Pydantic 模型列表。

        Args:
            query: SQL 查询语句。
            model: 目标 Pydantic 模型类。
            params: 查询参数。

        Returns:
            Pydantic 模型实例列表。
        """
        pass

    @abstractmethod
    def query_to_dataframe(
        self,
        query: str,
        params: Optional[List[Any]] = None
    ) -> pd.DataFrame:
        """
        执行查询并将结果返回为 DataFrame。

        Args:
            query: SQL 查询语句。
            params: 查询参数。
            
        Returns:
            一个 Pandas DataFrame。
        """
        pass

    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        """
        检查表是否存在。

        Args:
            table_name: 表名。

        Returns:
            如果表存在则返回 True，否则返回 False。
        """
        pass

    @abstractmethod
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        获取表的元数据信息。

        Args:
            table_name: 表名。

        Returns:
            一个包含表信息的字典。
        """
        pass

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close() 