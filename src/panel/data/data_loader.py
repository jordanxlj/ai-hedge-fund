
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

    def get_plate_summary(self, days_back: int = 2) -> pd.DataFrame:
        query = f"""
            WITH
                plate_sizes AS (
                    SELECT plate_code, COUNT(ticker) AS num_stocks
                    FROM stock_plate_mappings
                    GROUP BY plate_code
                ),
                ranked_plates AS (
                    SELECT
                        sm.ticker,
                        sm.plate_name,
                        ROW_NUMBER() OVER(PARTITION BY sm.ticker ORDER BY ps.num_stocks ASC, sm.plate_name ASC) as rnk
                    FROM stock_plate_mappings sm
                    JOIN plate_sizes ps ON sm.plate_code = ps.plate_code
                ),
                smallest_plates AS (
                    SELECT ticker, plate_name
                    FROM ranked_plates
                    WHERE rnk = 1
                )
            SELECT 
                sp.plate_name, 
                p.ticker, 
                p.time, 
                p.close, 
                p.volume,
                f.market_cap
            FROM smallest_plates sp
            JOIN hk_stock_daily_price p ON sp.ticker = p.ticker
            JOIN financial_profile f ON p.ticker = f.ticker AND f.report_period = (SELECT MAX(report_period) FROM financial_profile WHERE ticker = p.ticker)
            WHERE CAST(p.time AS TIMESTAMP) >= CAST((SELECT MAX(time) FROM hk_stock_daily_price) AS TIMESTAMP) - INTERVAL '{days_back} DAY'
        """
        return self.db_api.query_to_dataframe(query)

    def get_plate_details(self, plate_name: str, days_back: int = 1) -> pd.DataFrame:
        # The query needs to be an f-string to interpolate 'days_back'.
        query = f"""
            WITH 
                period_data AS (
                    SELECT
                        ticker,
                        time,
                        close,
                        volume
                    FROM hk_stock_daily_price
                    WHERE ticker IN (SELECT ticker FROM stock_plate_mappings WHERE plate_name = ?)
                      AND CAST(time AS TIMESTAMP) >= CAST((SELECT MAX(time) FROM hk_stock_daily_price) AS TIMESTAMP) - INTERVAL '{days_back} DAY'
                ),
                ranked_prices AS (
                    SELECT
                        ticker,
                        close,
                        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY time ASC) as rn_asc,
                        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY time DESC) as rn_desc
                    FROM period_data
                ),
                start_prices AS (
                    SELECT ticker, close as start_price FROM ranked_prices WHERE rn_asc = 1
                ),
                end_prices AS (
                    SELECT ticker, close as end_price FROM ranked_prices WHERE rn_desc = 1
                ),
                turnover AS (
                    SELECT ticker, SUM(volume * close) as total_turnover
                    FROM period_data
                    GROUP BY ticker
                )
            SELECT 
                m.ticker AS "代码",
                m.stock_name AS "名称",
                ep.end_price AS "现价",
                (ep.end_price - sp.start_price) / sp.start_price AS "涨跌幅",
                ep.end_price - sp.start_price AS "涨跌",
                t.total_turnover AS "成交额",
                f.price_to_earnings_ratio AS "市盈率(TTM)",
                f.price_to_book_ratio AS "市净率(MRQ)"
            FROM stock_plate_mappings m
            JOIN start_prices sp ON m.ticker = sp.ticker
            JOIN end_prices ep ON m.ticker = ep.ticker
            JOIN turnover t ON m.ticker = t.ticker
            LEFT JOIN financial_profile f ON m.ticker = f.ticker AND f.report_period = (SELECT MAX(report_period) FROM financial_profile WHERE ticker = m.ticker)
            WHERE m.plate_name = ?
        """
        return self.db_api.query_to_dataframe(query, [plate_name, plate_name])
