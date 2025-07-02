"""
DuckDB 数据库接口的具体实现。
"""

import duckdb
import pandas as pd
import time
import logging
import os
from pydantic import BaseModel
from typing import Type, List, Dict, Any, Optional, get_type_hints, get_origin, get_args
from datetime import datetime

from .base import DatabaseAPI

logger = logging.getLogger(__name__)


def _get_pydantic_sql_type(field_type) -> str:
    """将 Pydantic/Python 类型映射到 DuckDB SQL 类型。"""
    origin = get_origin(field_type)
    if origin is None:  # Simple type
        if field_type is str: return "VARCHAR"
        if field_type is float: return "DOUBLE"
        if field_type is int: return "BIGINT"
        if field_type is bool: return "BOOLEAN"
        if field_type is datetime: return "TIMESTAMP"
    
    args = get_args(field_type)
    if len(args) == 2 and args[1] is type(None): # Optional[T]
        return _get_pydantic_sql_type(args[0])

    return "VARCHAR"  # Default


class DuckDBAPI(DatabaseAPI):
    """使用 DuckDB 实现数据库操作的接口。"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

    def connect(self, **kwargs: Any) -> duckdb.DuckDBPyConnection:
        """建立并返回一个 DuckDB 连接。"""
        if self.conn is None:
            self.conn = duckdb.connect(database=self.db_path, read_only=False, **kwargs)
        return self.conn

    def close(self) -> None:
        """关闭 DuckDB 连接。"""
        if self.conn:
            self.conn.close()
            self.conn = None

    def _ensure_connection(self):
        """确保连接是活动的。"""
        if self.conn is None:
            raise ConnectionError("Database connection is not established. Call connect() first.")

    def create_table_from_model(
        self,
        table_name: str,
        model: Type[BaseModel],
        primary_keys: Optional[List[str]] = None
    ) -> None:
        self._ensure_connection()
        fields = get_type_hints(model)
        
        columns_sql = []
        for name, field_type in fields.items():
            if name == 'model_config': continue
            
            # Special handling for price table to use INTEGER for storage optimization
            if table_name == 'hk_stock_minute_price' and name in ['open', 'close', 'high', 'low']:
                sql_type = 'INTEGER'
            else:
                sql_type = _get_pydantic_sql_type(field_type)
            
            columns_sql.append(f'"{name}" {sql_type}')
        
        if primary_keys:
            pk_sql = "PRIMARY KEY (" + ", ".join(f'"{pk}"' for pk in primary_keys) + ")"
            columns_sql.append(pk_sql)
        
        create_table_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(columns_sql)})'
        
        self.conn.execute(create_table_sql)
        logger.info(f"Table '{table_name}' is ready in DuckDB.")

    def upsert_data_from_models(
        self,
        table_name: str,
        data: List[BaseModel],
        primary_keys: List[str]
    ) -> None:
        self._ensure_connection()
        if not data:
            logger.info("No data provided to upsert.")
            return

        model = data[0].__class__
        
        # Special handling for price data to convert floats to integers
        if table_name == 'hk_stock_minute_price' and model.__name__ == 'Price':
            data_dicts = []
            for m in data:
                d = m.model_dump(exclude_none=True)
                for key in ['open', 'close', 'high', 'low']:
                    if key in d and isinstance(d[key], float):
                        d[key] = int(d[key] * 100)
                data_dicts.append(d)
        else:
            data_dicts = [m.model_dump(exclude_none=True) for m in data]

        model_fields = list(model.model_fields.keys())
        df = pd.DataFrame(data_dicts)
        
        for col in model_fields:
            if col not in df.columns:
                df[col] = None
        
        df = df[model_fields]
        
        self._upsert_dataframe(table_name, df, primary_keys)

    def upsert_data_from_dicts(
        self,
        table_name: str,
        data: List[Dict[str, Any]],
        primary_keys: List[str]
    ) -> None:
        self._ensure_connection()
        if not data:
            logger.info("No data provided to upsert.")
            return
        
        df = pd.DataFrame(data)
        self._upsert_dataframe(table_name, df, primary_keys)

    def _upsert_dataframe(self, table_name: str, df: pd.DataFrame, primary_keys: List[str]):
        """Helper to upsert a DataFrame."""
        self._ensure_connection()
        temp_table_name = f"temp_upsert_{table_name}_{int(time.time())}"
        self.conn.register(temp_table_name, df)
        
        try:
            columns = list(df.columns)
            quoted_cols = [f'"{col}"' for col in columns]
            quoted_pk = [f'"{pk}"' for pk in primary_keys]
            
            update_set_sql = ", ".join([f'{col} = excluded.{col}' for col in quoted_cols if col not in quoted_pk])
            on_conflict_sql = f"DO UPDATE SET {update_set_sql}" if update_set_sql else "DO NOTHING"

            upsert_sql = f"""
            INSERT INTO "{table_name}" ({', '.join(quoted_cols)})
            SELECT {', '.join(quoted_cols)} FROM {temp_table_name}
            ON CONFLICT ({', '.join(quoted_pk)}) {on_conflict_sql};
            """
            self.conn.execute(upsert_sql)
            logger.info(f"Successfully upserted {len(df)} records into '{table_name}'.")
        finally:
            self.conn.unregister(temp_table_name)
    
    def insert_dataframe(
        self,
        table_name: str,
        df: pd.DataFrame,
        if_exists: str = "append"
    ) -> None:
        self._ensure_connection()
        if df.empty:
            logger.info("Empty DataFrame provided, nothing to insert.")
            return
        
        if if_exists == "replace":
            self.conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        
        self.conn.register(f'df_to_insert_{table_name}', df)
        self.conn.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" AS SELECT * FROM df_to_insert_{table_name} WHERE 1=0') # Create table if not exists
        self.conn.execute(f'INSERT INTO "{table_name}" SELECT * FROM df_to_insert_{table_name}')
        self.conn.unregister(f'df_to_insert_{table_name}')
        logger.info(f"Successfully inserted {len(df)} records into '{table_name}' with mode='{if_exists}'.")

    def query_to_models(
        self,
        query: str,
        model: Type[BaseModel],
        params: Optional[List[Any]] = None
    ) -> List[BaseModel]:
        self._ensure_connection()
        df = self.query_to_dataframe(query, params)
        if df.empty:
            return []
        
        # Special handling for price data to convert integers back to floats
        if model.__name__ == 'Price':
            for key in ['open', 'close', 'high', 'low']:
                if key in df.columns:
                    df[key] = df[key] / 100.0

        records = df.to_dict('records')
        return [model(**record) for record in records]

    def query_to_dataframe(
        self,
        query: str,
        params: Optional[List[Any]] = None
    ) -> pd.DataFrame:
        self._ensure_connection()
        if params:
            return self.conn.execute(query, params).fetchdf()
        else:
            return self.conn.execute(query).fetchdf()

    def table_exists(self, table_name: str) -> bool:
        self._ensure_connection()
        try:
            result = self.conn.execute(f"SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()
            return result is not None
        except Exception:
            return False

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        self._ensure_connection()
        try:
            columns_df = self.conn.execute(f"DESCRIBE {table_name}").fetchdf()
            row_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            return {
                "table_name": table_name,
                "columns": columns_df.to_dict('records'),
                "row_count": row_count
            }
        except Exception as e:
            logger.error(f"Error getting table info for '{table_name}': {e}")
            return {} 