
import pandas as pd
from src.data.db import get_database_api, DatabaseAPI

class DataLoader:
    def __init__(self, db_api: DatabaseAPI):
        self.db_api = db_api

    def load_daily_prices(self, tickers: list = None, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        query = "SELECT * FROM hk_stock_daily_price"
        conditions = []
        params = []

        if tickers:
            conditions.append(f"ticker IN ({','.join(['?'] * len(tickers))})")
            params.extend(tickers)
        if start_date:
            conditions.append("time >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("time <= ?")
            params.append(end_date)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        df = self.db_api.query_to_dataframe(query, params)
        if not df.empty:
            df = df.sort_values(by='time').reset_index(drop=True)
        return df

    def load_minute_prices(self, tickers: list = None, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        query = "SELECT * FROM hk_stock_minute_price"
        conditions = []
        params = []

        if tickers:
            conditions.append(f"ticker IN ({','.join(['?'] * len(tickers))})")
            params.extend(tickers)
        if start_date:
            conditions.append("time >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("time <= ?")
            params.append(end_date)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        df = self.db_api.query_to_dataframe(query, params)
        if not df.empty:
            df = df.sort_values(by='time').reset_index(drop=True)
        return df

    def load_financial_profiles(self, tickers: list = None, period: str = None) -> pd.DataFrame:
        query = "SELECT * FROM financial_profile"
        conditions = []
        params = []

        if tickers:
            conditions.append(f"ticker IN ({','.join(['?'] * len(tickers))})")
            params.extend(tickers)
        if period:
            conditions.append("period = ?")
            params.append(period)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        df = self.db_api.query_to_dataframe(query, params)
        if not df.empty:
            df = df.sort_values(by='time').reset_index(drop=True)
        return df

    def get_plate_summary(self) -> pd.DataFrame:
        query = "SELECT * FROM plate_daily_summary"
        return self.db_api.query_to_dataframe(query)
