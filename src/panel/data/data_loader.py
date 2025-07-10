import pandas as pd
from src.data.db import get_database_api, DatabaseAPI

class DataLoader:
    def __init__(self, db_api: DatabaseAPI):
        self.db_api = db_api

    def load_daily_prices(self, tickers: list = None, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        query = "SELECT ticker, time, open / 100.0 AS open, close / 100.0 AS close, high / 100.0 AS high, low / 100.0 AS low, volume FROM hk_stock_daily_price"
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
                ),
                ranked_prices AS (
                    SELECT
                        p.ticker,
                        p.time,
                        p.close,
                        p.volume,
                        ROW_NUMBER() OVER (PARTITION BY p.ticker ORDER BY p.time DESC) as rn
                    FROM hk_stock_daily_price p
                    WHERE p.ticker IN (SELECT ticker FROM smallest_plates)
                ),
                last_n_prices AS (
                    SELECT *
                    FROM ranked_prices
                    WHERE rn <= {days_back + 1}
                )
            SELECT 
                sp.plate_name, 
                p.ticker, 
                p.time, 
                p.close / 100.0 AS close, 
                p.volume,
                f.market_cap
            FROM smallest_plates sp
            JOIN last_n_prices p ON sp.ticker = p.ticker
            JOIN financial_profile f ON p.ticker = f.ticker AND f.report_period = (SELECT MAX(report_period) FROM financial_profile WHERE ticker = p.ticker)
        """
        return self.db_api.query_to_dataframe(query)

    def get_stock_summary(self, days_back: int = 2) -> pd.DataFrame:
        query = f"""
            WITH
                ranked_prices AS (
                    SELECT
                        p.ticker,
                        p.time,
                        p.close,
                        p.volume,
                        ROW_NUMBER() OVER (PARTITION BY p.ticker ORDER BY p.time DESC) as rn
                    FROM hk_stock_daily_price p
                ),
                last_n_prices AS (
                    SELECT *
                    FROM ranked_prices
                    WHERE rn <= {days_back + 1}
                )
            SELECT 
                p.ticker, 
                p.time, 
                p.close / 100.0 AS close, 
                p.volume,
                f.market_cap,
                f.name as stock_name
            FROM last_n_prices p
            JOIN financial_profile f ON p.ticker = f.ticker AND f.report_period = (SELECT MAX(report_period) FROM financial_profile WHERE ticker = p.ticker)
        """
        return self.db_api.query_to_dataframe(query)

    def get_plate_details(self, plate_name: str, days_back: int = 1) -> pd.DataFrame:
        # The query needs to be an f-string to interpolate 'days_back'.
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
                ),
                all_financial_data AS (
                    SELECT ticker, report_period, revenue, net_income FROM financial_profile
                ),
                revenue_cagr AS (
                    SELECT
                        ticker,
                        (POWER(
                            (SELECT revenue FROM all_financial_data WHERE ticker = fd.ticker ORDER BY report_period DESC LIMIT 1) / 
                            NULLIF(ABS((SELECT revenue FROM all_financial_data WHERE ticker = fd.ticker ORDER BY report_period ASC LIMIT 1)), 0),
                            1.0/3
                        ) - 1) AS revenue_cagr_3y
                    FROM (SELECT DISTINCT ticker FROM all_financial_data) fd
                ),
                net_income_cagr AS (
                    SELECT
                        ticker,
                        (POWER(
                            (SELECT net_income FROM all_financial_data WHERE ticker = fd.ticker ORDER BY report_period DESC LIMIT 1) / 
                            NULLIF(ABS((SELECT net_income FROM all_financial_data WHERE ticker = fd.ticker ORDER BY report_period ASC LIMIT 1)), 0),
                            1.0/3
                        ) - 1) AS net_income_cagr_3y
                    FROM (SELECT DISTINCT ticker FROM all_financial_data) fd
                ),
                ranked_prices AS (
                    SELECT
                        ticker,
                        time,
                        close,
                        volume,
                        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY time DESC) as rn
                    FROM hk_stock_daily_price
                    WHERE ticker IN (SELECT ticker FROM stock_plate_mappings WHERE plate_name = ?)
                ),
                period_data AS (
                    SELECT * FROM ranked_prices WHERE rn <= {days_back + 1}
                ),
                start_prices AS (
                    SELECT ticker, close as start_price FROM period_data WHERE rn = {days_back + 1}
                ),
                end_prices AS (
                    SELECT ticker, close as end_price FROM period_data WHERE rn = 1
                ),
                turnover AS (
                    SELECT ticker, SUM(volume * close / 100.0) as total_turnover
                    FROM period_data
                    WHERE rn <= {days_back}
                    GROUP BY ticker
                )
            SELECT 
                m.ticker AS "ticker",
                m.stock_name AS "name",
                ep.end_price / 100.0 AS "price",
                (ep.end_price - sp.start_price) / sp.start_price AS "price_change_pct",
                (ep.end_price - sp.start_price) / 100.0 AS "price_change",
                t.total_turnover / 100000000 AS "turnover",
                f.price_to_earnings_ratio AS "pe_ttm",
                f.price_to_book_ratio AS "pb_mrq",
                f.return_on_equity AS "roe",
                f.return_on_invested_capital AS "roic",
                f.market_cap / 100000000 AS "market_cap",
                f.gross_margin AS "gross_margin",
                f.net_margin AS "net_margin",
                rc.revenue_cagr_3y AS "revenue_cagr_3y",
                nic.net_income_cagr_3y AS "net_income_cagr_3y",
                CASE WHEN sp_check.ticker IS NOT NULL THEN '是' ELSE '否' END AS "is_smallest_plate"
            FROM stock_plate_mappings m
            JOIN start_prices sp ON m.ticker = sp.ticker
            JOIN end_prices ep ON m.ticker = ep.ticker
            JOIN turnover t ON m.ticker = t.ticker
            LEFT JOIN financial_profile f ON m.ticker = f.ticker AND f.report_period = (SELECT MAX(report_period) FROM financial_profile WHERE ticker = m.ticker)
            LEFT JOIN smallest_plates sp_check ON m.ticker = sp_check.ticker AND m.plate_name = sp_check.plate_name
            LEFT JOIN revenue_cagr rc ON m.ticker = rc.ticker
            LEFT JOIN net_income_cagr nic ON m.ticker = nic.ticker
            WHERE m.plate_name = ?
        """
        return self.db_api.query_to_dataframe(query, [plate_name, plate_name])