
import pandas as pd
from src.data.db import get_database_api, DatabaseAPI

class DataLoader:
    def __init__(self, db_api: DatabaseAPI):
        self.db_api = db_api

    def load_daily_prices(self, tickers: list = None, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        query = "SELECT * FROM hk_stock_daily_price"
        params = {}
        if tickers:
            query += " WHERE ticker IN :tickers"
            params['tickers'] = tuple(tickers)
        if start_date:
            query += " AND time >= :start_date"
            params['start_date'] = start_date
        if end_date:
            query += " AND time <= :end_date"
            params['end_date'] = end_date
        return self.db_api.query_to_dataframe(query, params)

    def load_minute_prices(self, tickers: list = None, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        query = "SELECT * FROM hk_stock_minute_price"
        params = {}
        if tickers:
            query += " WHERE ticker IN :tickers"
            params['tickers'] = tuple(tickers)
        if start_date:
            query += " AND time >= :start_date"
            params['start_date'] = start_date
        if end_date:
            query += " AND time <= :end_date"
            params['end_date'] = end_date
        return self.db_api.query_to_dataframe(query, params)

    def load_financial_profiles(self, tickers: list = None, period: str = None) -> pd.DataFrame:
        query = "SELECT * FROM financial_profile"
        params = {}
        if tickers:
            query += " WHERE ticker IN :tickers"
            params['tickers'] = tuple(tickers)
        if period:
            query += " AND period = :period"
            params['period'] = period
        return self.db_api.query_to_dataframe(query, params)

