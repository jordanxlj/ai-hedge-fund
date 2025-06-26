import pytest
import os
import duckdb
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List

from src.data.db.duckdb_impl import DuckDBAPI, _get_pydantic_sql_type

# Fixtures and test classes will be added here

class _TestModel(BaseModel):
    id: int
    name: str
    value: Optional[float] = None
    timestamp: datetime = Field(default_factory=datetime.now)

@pytest.fixture
def db_api():
    """Fixture to set up and tear down a test DuckDB database."""
    db_path = "test_duckdb_impl.db"
    api = DuckDBAPI(db_path)
    api.connect()
    yield api
    api.close()
    if os.path.exists(db_path):
        os.remove(db_path)

class TestDuckDBAPI:
    def test_placeholder(self):
        assert True 

    def test_create_and_table_exists(self, db_api: DuckDBAPI):
        """Test creating a table from a model and checking its existence."""
        table_name = "test_table"
        db_api.create_table_from_model(table_name, _TestModel, primary_keys=["id"])
        
        assert db_api.table_exists(table_name)
        assert not db_api.table_exists("non_existent_table")

        # Verify schema
        table_info = db_api.get_table_info(table_name)
        assert table_info["row_count"] == 0
        columns = {col['column_name']: col['column_type'] for col in table_info['columns']}
        assert columns['id'] == 'BIGINT'
        assert columns['name'] == 'VARCHAR'
        assert columns['value'] == 'DOUBLE'
        assert columns['timestamp'] == 'TIMESTAMP' 

    def test_upsert_from_models(self, db_api: DuckDBAPI):
        """Test upserting data from a list of Pydantic models."""
        table_name = "upsert_models_table"
        db_api.create_table_from_model(table_name, _TestModel, primary_keys=["id"])

        # Initial insert
        data1 = [_TestModel(id=1, name="test1", value=1.1), _TestModel(id=2, name="test2", value=2.2)]
        db_api.upsert_data_from_models(table_name, data1, primary_keys=["id"])
        
        result_df = db_api.query_to_dataframe(f"SELECT * FROM {table_name} ORDER BY id")
        assert len(result_df) == 2
        assert result_df['name'][0] == 'test1'

        # Upsert (update 1, insert 3)
        data2 = [_TestModel(id=1, name="updated_test1", value=11.1), _TestModel(id=3, name="test3", value=3.3)]
        db_api.upsert_data_from_models(table_name, data2, primary_keys=["id"])
        
        result_df_after_upsert = db_api.query_to_dataframe(f"SELECT * FROM {table_name} ORDER BY id")
        assert len(result_df_after_upsert) == 3
        
        updated_row = result_df_after_upsert[result_df_after_upsert['id'] == 1]
        assert updated_row['name'].iloc[0] == 'updated_test1'
        assert updated_row['value'].iloc[0] == 11.1
        
        original_row = result_df_after_upsert[result_df_after_upsert['id'] == 2]
        assert original_row['name'].iloc[0] == 'test2'
        
        new_row = result_df_after_upsert[result_df_after_upsert['id'] == 3]
        assert new_row['name'].iloc[0] == 'test3' 

    def test_query_to_models(self, db_api: DuckDBAPI):
        """Test querying data and converting it back to Pydantic models."""
        table_name = "query_to_models_table"
        db_api.create_table_from_model(table_name, _TestModel, primary_keys=["id"])

        data = [
            _TestModel(id=1, name="query_test1", value=10.1),
            _TestModel(id=2, name="query_test2", value=20.2),
        ]
        db_api.upsert_data_from_models(table_name, data, primary_keys=["id"])

        # Query all
        models = db_api.query_to_models(f"SELECT * FROM {table_name} WHERE id = 1", _TestModel)
        assert len(models) == 1
        assert isinstance(models[0], _TestModel)
        assert models[0].id == 1
        assert models[0].name == "query_test1"

        # Query with params
        models_with_params = db_api.query_to_models(
            f"SELECT * FROM {table_name} WHERE name = ?", _TestModel, params=["query_test2"]
        )
        assert len(models_with_params) == 1
        assert models_with_params[0].id == 2
        
        # Query for no results
        no_results = db_api.query_to_models(f"SELECT * FROM {table_name} WHERE id = 99", _TestModel)
        assert len(no_results) == 0 

@pytest.mark.parametrize("input_type, expected_sql", [
    (str, "VARCHAR"),
    (int, "BIGINT"),
    (float, "DOUBLE"),
    (bool, "BOOLEAN"),
    (datetime, "TIMESTAMP"),
    (Optional[str], "VARCHAR"),
    (Optional[int], "BIGINT"),
    (List[str], "VARCHAR"), # Default fallback
    (dict, "VARCHAR"), # Default fallback
])
def test_get_pydantic_sql_type(input_type, expected_sql):
    """Test the mapping of Pydantic/Python types to DuckDB SQL types."""
    assert _get_pydantic_sql_type(input_type) == expected_sql 