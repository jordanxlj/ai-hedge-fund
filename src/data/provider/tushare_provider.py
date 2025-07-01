import os
import pandas as pd
from pathlib import Path
from typing import List, Optional, Dict
import logging
from datetime import datetime, timedelta
import tushare as ts

from src.data.provider.abstract_data_provider import AbstractDataProvider
from src.utils.config_utils import load_yaml_config
from src.data.models import (
    Price,
    FinancialMetrics,
    LineItem,
    InsiderTrade,
    CompanyNews,
    TransactionType,
)
from src.utils.timeout_retry import with_timeout_retry
from src.data.provider.tushare_mapping import get_tushare_fields, apply_field_mapping

logger = logging.getLogger(__name__)


class TushareProvider(AbstractDataProvider):
    """Tushare数据提供商实现"""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__("Tushare", api_key or os.environ.get("TUSHARE_API_KEY"))
        self.pro = None
        self.h2a_mapping = self._load_h2a_mapping('conf/H2A_mapping.yaml')
        
        if self.api_key:
            try:
                ts.set_token(self.api_key)
                self.pro = ts.pro_api()
            except Exception as e:
                logger.error(f"Tushare初始化失败: {e}")
                self.pro = None
    
    def _convert_ticker(self, ticker: str) -> str:
        if '.' in ticker:
            return ticker
        if len(ticker) == 6 and ticker.isdigit():
            return f"{ticker}.SH" if ticker.startswith(('60', '68')) else f"{ticker}.SZ"
        if len(ticker) <= 5 and ticker.isdigit():
            return f"{ticker.zfill(5)}.HK"
        return ticker
    
    def _is_hk_stock(self, ticker: str) -> bool:
        return ticker.endswith('.HK')
    
    def _load_h2a_mapping(self, config_file) -> dict:
        try:
            config_path = Path(config_file)
            if not config_path.exists():
                logger.info(f"H2A_mapping.yaml not found: {config_path}")
                return {}
            config = load_yaml_config(config_path, {})
            hk_to_a_mapping = config.get('h2a_mapping', {})
            logger.info(f"Loaded H->A mapping: {len(hk_to_a_mapping)} entries.")
            return hk_to_a_mapping
        except Exception as e:
            logger.warning(f"Failed to load H->A mapping: {e}")
            return {}

    def _get_corresponding_a_stock(self, hk_ticker: str) -> str:
        if not self._is_hk_stock(hk_ticker):
            return hk_ticker
        a_stock_code = self.h2a_mapping.get(hk_ticker)
        if a_stock_code:
            logger.info(f"H-share {hk_ticker} found corresponding A-share: {a_stock_code}")
            return a_stock_code
        return hk_ticker
    
    def _convert_date_format(self, date_str: str) -> str:
        return date_str.replace('-', '')

    @with_timeout_retry("get_prices")
    def get_prices(
        self,
        ticker: str,
        start_date: str,
        end_date: str,
        freq: str = '1d'
    ) -> List[Price]:
        """获取股价数据，支持A股和港股，并支持不同频率 (1d, 1m)"""
        try:
            if not self.is_available():
                logger.error("Tushare API not initialized")
                return []
                
            ts_code = self._convert_ticker(ticker)
            start_date_ts = self._convert_date_format(start_date)
            end_date_ts = self._convert_date_format(end_date)
            
            df = None
            is_hk = self._is_hk_stock(ts_code)

            if freq == '1m':
                if not is_hk:
                    logger.warning(f"Tushare minute data is only supported for HK stocks. Skipping {ticker}.")
                    return []
                df = self.pro.hk_mins(
                    ts_code=ts_code,
                    freq='1min',
                    start_date=start_date_ts + ' 09:00:00',
                    end_date=end_date_ts + ' 16:00:00'
                )
            elif freq == '1d':
                if is_hk:
                    df = self.pro.hk_daily(ts_code=ts_code, start_date=start_date_ts, end_date=end_date_ts)
                else:
                    df = self.pro.daily(ts_code=ts_code, start_date=start_date_ts, end_date=end_date_ts)
            else:
                logger.error(f"Unsupported frequency '{freq}' for TushareProvider.")
                return []

            if df is None or df.empty:
                return []
            
            prices = []
            time_col = 'trade_time' if freq == '1m' else 'trade_date'
            vol_col = 'vol'
            
            for _, row in df.iterrows():
                time_str = str(row[time_col])
                if freq == '1d' and len(time_str) == 8:
                    time_str = f"{time_str[:4]}-{time_str[4:6]}-{time_str[6:8]}"

                prices.append(Price(
                    time=time_str,
                    open=float(row['open']),
                    high=float(row['high']),
                    low=float(row['low']),
                    close=float(row['close']),
                    volume=int(row[vol_col] * 100) if not is_hk and freq == '1d' else int(row[vol_col]),
                    ticker=ticker
                ))
            
            prices.sort(key=lambda x: x.time)
            return prices
            
        except Exception as e:
            logger.error(f"Failed to get Tushare price data for {ticker}: {e}", exc_info=True)
            return []

    @with_timeout_retry("get_financial_metrics")
    def get_financial_metrics(
        self,
        ticker: str,
        end_date: str,
        period: str = "ttm",
        limit: int = 10,
    ) -> List[FinancialMetrics]:
        """
        获取财务指标，支持A股和港股
        对于H股，如果有对应的A股代码，则使用A股接口查询基本面信息
        """
        if not self.is_available():
            return []
        
        try:
            effective_ticker = self._get_effective_ticker(ticker)
            if effective_ticker is None:
                return []
            
            end_date_ts = self._convert_date_format(end_date)
            currency = "HKD" if self._is_hk_stock(ticker) else "CNY"
            
            # 获取财务指标数据
            financial_metrics_df = self._fetch_financial_metrics_data(effective_ticker, end_date_ts)
            
            # 获取估值指标数据
            valuation_metrics_df = self._fetch_valuation_metrics_data(effective_ticker, end_date_ts)
            
            if financial_metrics_df is None or financial_metrics_df.empty:
                logger.warning(f"财务指标数据为空 {ticker}")
                return []
            
            logger.debug(f"获取财务指标 {ticker}: {len(financial_metrics_df)} 条记录")
            
            # 处理财务指标数据
            financial_data = self._process_financial_metrics_dataframe(financial_metrics_df)
            
            # 处理估值指标数据
            valuation_data = {}
            if valuation_metrics_df is not None and not valuation_metrics_df.empty:
                valuation_data = self._process_valuation_metrics_dataframe(valuation_metrics_df)
            
            # 合并数据并创建FinancialMetrics对象
            metrics = self._create_financial_metrics(financial_data, valuation_data, ticker, currency)
            
            # 放在上层接口做limit过滤
            return metrics
            
        except Exception as e:
            logger.error(f"获取财务指标失败 {ticker}: {e}")
            return []
    
    def _safe_get_float(self, row, field: str) -> Optional[float]:
        """安全获取浮点数字段"""
        try:
            if field in row and pd.notna(row[field]):
                return float(row[field])
        except (KeyError, ValueError, TypeError):
            pass
        return None
    
    def _fetch_financial_metrics_data(self, ts_code: str, end_date: str) -> Optional[pd.DataFrame]:
        """获取财务指标数据"""
        try:
            # 使用映射表获取所有财务指标字段
            fields = get_tushare_fields('financial_metrics')
            df = self.pro.fina_indicator_vip(
                ts_code=ts_code,
                end_date=end_date,
                fields=fields
            )
            return df
        except Exception as e:
            logger.error(f"获取财务指标数据失败 {ts_code}: {e}")
            return None
    
    def _fetch_valuation_metrics_data(self, ts_code: str, end_date: str) -> Optional[pd.DataFrame]:
        """获取估值指标数据"""
        try:
            from datetime import datetime, timedelta
            
            # 使用映射表获取估值指标字段
            fields = get_tushare_fields('valuation_metrics')
            
            # 尝试获取最近交易日的估值数据
            trade_date = end_date
            df = self.pro.daily_basic(
                ts_code=ts_code,
                trade_date=trade_date,
                fields=fields
            )
            
            if df.empty:
                # 如果当日没有数据，尝试获取最近一个月的数据
                end_dt = datetime.strptime(end_date, '%Y%m%d')
                start_dt = end_dt - timedelta(days=30)
                start_date_str = start_dt.strftime('%Y%m%d')
                
                df = self.pro.daily_basic(
                    ts_code=ts_code,
                    start_date=start_date_str,
                    end_date=trade_date,
                    fields=fields
                )
                if not df.empty:
                    df = df.sort_values('trade_date', ascending=False)
            
            return df
        except Exception as e:
            logger.error(f"获取估值指标数据失败 {ts_code}: {e}")
            return None
    
    def _process_financial_metrics_dataframe(self, df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
        """处理财务指标DataFrame，返回按期间组织的数据"""
        data_by_period = {}
        
        for _, row in df.iterrows():
            # 转换日期格式
            end_date = row['end_date']
            report_period = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
            
            if report_period not in data_by_period:
                data_by_period[report_period] = {}
            
            # 应用字段映射
            row_dict = row.to_dict()
            mapped_fields = apply_field_mapping(row_dict, 'financial_metrics')
            
            # 转换数据并存储
            for standard_field, value in mapped_fields.items():
                converted_value = self._convert_financial_metric_value(standard_field, value)
                data_by_period[report_period][standard_field] = converted_value
            
            logger.debug(f"财务指标数据处理完成: {report_period}")
        
        return data_by_period
    
    def _process_valuation_metrics_dataframe(self, df: pd.DataFrame) -> Dict[str, float]:
        """处理估值指标DataFrame，返回最新的估值数据"""
        if df.empty:
            return {}
        
        # 使用最新的数据
        row = df.iloc[0]
        row_dict = row.to_dict()
        mapped_fields = apply_field_mapping(row_dict, 'valuation_metrics')
        
        # 转换数据
        result = {}
        for standard_field, value in mapped_fields.items():
            converted_value = self._convert_financial_metric_value(standard_field, value)
            result[standard_field] = converted_value
        
        logger.debug(f"估值指标数据处理完成")
        return result
    
    def _convert_financial_metric_value(self, field_name: str, value) -> Optional[float]:
        """转换财务指标数值，处理百分比等特殊格式"""
        if pd.isna(value):
            return None
        
        try:
            float_value = float(value)
            
            # 百分比字段需要转换为小数（除以100）
            percentage_fields = {
                'return_on_equity', 'return_on_assets', 'debt_to_assets', 'gross_margin',
                'operating_margin', 'net_margin', 'revenue_growth', 'earnings_growth',
                'book_value_growth', 'return_on_invested_capital', 'gross_profit_margin',
                'net_profit_margin', 'operating_profit_to_revenue', 'current_assets_to_total_assets',
                'non_current_assets_to_total_assets', 'current_debt_to_total_debt', 
                'long_term_debt_to_total_debt', 'cash_flow_to_sales', 'cash_flow_to_net_income',
                'cash_flow_to_liabilities', 'cash_flow_margin', 'profit_to_gross_revenue',
                'sales_expense_to_revenue', 'admin_expense_to_revenue', 'finance_expense_to_revenue',
                'impairment_to_revenue_ttm', 'goods_cost_ratio', 'ebitda_margin',
                'basic_eps_growth', 'diluted_eps_growth', 'cash_flow_per_share_growth',
                'operating_profit_growth', 'ebt_growth', 'net_profit_growth',
                'diluted_net_profit_growth', 'operating_cash_flow_growth', 'roe_growth',
                'book_value_per_share_growth', 'total_assets_growth', 'equity_growth',
                'total_revenue_growth', 'revenue_growth', 'quarterly_revenue_growth',
                'quarterly_revenue_growth_qoq', 'quarterly_sales_growth', 'quarterly_sales_growth_qoq',
                'quarterly_operating_profit_growth', 'quarterly_operating_profit_growth_qoq',
                'quarterly_profit_growth', 'quarterly_profit_growth_qoq', 'quarterly_net_profit_growth',
                'quarterly_net_profit_growth_qoq', 'equity_growth_alt', 'turnover_rate',
                'turnover_rate_float', 'dividend_yield', 'dividend_yield_ttm'
            }
            
            if field_name in percentage_fields:
                return float_value / 100.0
            else:
                return float_value
                
        except (ValueError, TypeError):
            return None
    
    def _create_financial_metrics(self, financial_data: Dict[str, Dict[str, float]], 
                                valuation_data: Dict[str, float], 
                                ticker: str, currency: str) -> List[FinancialMetrics]:
        """创建FinancialMetrics对象列表"""
        metrics = []
        
        # 按时间顺序排序
        sorted_periods = sorted(financial_data.keys())
        
        for report_period in sorted_periods:
            period_data = financial_data[report_period]
            
            # 确定期间类型
            actual_period = "annual" if report_period.endswith("-12-31") else "quarter"
            
            # 合并财务指标和估值指标数据
            combined_data = {
                'ticker': ticker,
                'report_period': report_period,
                'period': actual_period,
                'currency': currency,
                **period_data,
                **valuation_data
            }
            
            # 创建FinancialMetrics对象
            try:
                metric = FinancialMetrics(**combined_data)
                metrics.append(metric)
                logger.debug(f"创建FinancialMetrics: {ticker} - {report_period}")
            except Exception as e:
                logger.warning(f"创建FinancialMetrics失败 {ticker} - {report_period}: {e}")
        
        return metrics
    
    @with_timeout_retry("search_line_items")
    def search_line_items(
        self,
        ticker: str,
        line_items: List[str],
        end_date: str,
        period: str = "ttm",
        limit: int = 10,
    ) -> List[LineItem]:
        """搜索财务报表项目，支持A股和港股
        对于H股，如果有对应的A股代码，则使用A股接口查询财务报表数据
        
        Args:
            ticker: The stock ticker.
            line_items: List of financial line items to retrieve.
            end_date: The end date for the data.
            period: The period type, defaults to 'ttm'.
            limit: Maximum number of results to return, defaults to 10.
        
        Returns:
            A list of LineItem objects containing the financial data.
        """
        try:
            effective_ticker = self._get_effective_ticker(ticker)
            if effective_ticker is None:
                return []
            
            end_date_ts = self._convert_date_format(end_date)
            currency = "HKD" if self._is_hk_stock(ticker) else "CNY"
            
            # Define fields for each statement
            income_fields = [
                'revenue', 'operating_profit', 'total_profit', 'net_income',
                'basic_eps', 'total_cost_of_goods_sold', 'selling_expenses',
                'administrative_expenses', 'finance_expenses', 'investment_income',
                'interest_expense', 'operating_expense', 'ebit', 'ebitda'
            ]
            balance_fields = [
                'total_assets', 'total_liabilities', 'total_equity',
                'current_assets', 'current_liabilities', 'accounts_receivable',
                'inventories', 'accounts_payable', 'fixed_assets', 'long_term_borrowings',
                'research_and_development', 'goodwill', 'intangible_assets', 'short_term_borrowings'
            ]
            cashflow_fields = [
                'operating_cash_flow', 'investing_cash_flow', 'financing_cash_flow',
                'free_cash_flow', 'capital_expenditure', 'cash_from_sales',
                'cash_paid_for_goods', 'cash_paid_to_employees', 'cash_paid_for_taxes',
                'net_cash_increase', 'cash_from_investments', 'capital_expenditure',
                'dividends_and_other_cash_distributions'
            ]
            
            # Fetch data
            income_df = self._fetch_data('income_vip', effective_ticker, end_date_ts, get_tushare_fields('income', income_fields))
            balance_df = self._fetch_data('balancesheet_vip', effective_ticker, end_date_ts, get_tushare_fields('balance', balance_fields))
            cashflow_df = self._fetch_data('cashflow_vip', effective_ticker, end_date_ts, get_tushare_fields('cashflow', cashflow_fields))
            
            # Process data
            income_data = self._process_dataframe(income_df, income_fields, 'income') if income_df is not None else {}
            balance_data = self._process_dataframe(balance_df, balance_fields, 'balance') if balance_df is not None else {}
            cashflow_data = self._process_dataframe(cashflow_df, cashflow_fields, 'cashflow') if cashflow_df is not None else {}
            
            # Aggregate data
            aggregated_data = self._aggregate_data(income_data, balance_data, cashflow_data, ticker, period, currency)
            logger.debug(f"aggregated_data: {aggregated_data}")

            # Create LineItem objects
            line_items_result = self._create_line_items(aggregated_data)

            #for tushare query, without limit
            return line_items_result
        
        except Exception as e:
            logger.error(f"搜索财务报表项目失败 {ticker}: {e}")
            return []

    def _get_effective_ticker(self, ticker: str) -> Optional[str]:
        """Get the effective ticker to use for data fetching.
        
        For H-shares, attempt to map to the corresponding A-share if available.
        
        Args:
            ticker: The stock ticker.
        
        Returns:
            The effective ticker code or None if not supported.
        """
        ts_code = self._convert_ticker(ticker)
        if self._is_hk_stock(ticker):
            corresponding_a_stock = self._get_corresponding_a_stock(ts_code)
            if corresponding_a_stock != ts_code:
                logger.info(f"H股 {ticker} 使用对应A股代码 {corresponding_a_stock} 查询财务报表数据")
                return corresponding_a_stock
            else:
                logger.info(f"H股 {ticker} 未找到对应A股代码，Tushare暂不支持纯港股财务报表数据")
                return None
        return ts_code

    def _fetch_data(self, api_method: str, ts_code: str, end_date: str, fields: str) -> Optional[pd.DataFrame]:
        """Fetch data from Tushare API.
        
        Args:
            api_method: The Tushare API method to call ('income', 'balancesheet', 'cashflow').
            ts_code: The ticker code.
            end_date: The end date in 'YYYYMMDD' format.
            fields: Comma-separated string of fields to fetch.
        
        Returns:
            DataFrame containing the fetched data, or None if fetching fails.
        """
        try:
            df = getattr(self.pro, api_method)(
                ts_code=ts_code,
                end_date=end_date,
                fields=fields
            )
            if df is None or df.empty:
                logger.debug(f"No data found for {api_method} of {ts_code}")
                return None
            logger.debug(f"Fetched {api_method} data: {len(df)} rows, fields: {list(df.columns)}")
            return df
        except Exception as e:
            logger.error(f"Failed to fetch {api_method} data for {ts_code}: {e}")
            return None

    def _process_dataframe(self, df: pd.DataFrame, target_fields: List[str], statement_type: str) -> Dict[str, Dict[str, float]]:
        """Process a dataframe to extract and map fields for each report period.
        
        Args:
            df: The dataframe to process.
            target_fields: List of target fields to extract.
            statement_type: The type of statement ('income', 'balance', 'cashflow').
        
        Returns:
            A dictionary mapping report periods to their corresponding data.
        """
        data_by_period = {}
        logger.debug(f"Processing {statement_type} dataframe with {len(df)} rows")
        
        for _, row in df.iterrows():
            report_period = f"{row['end_date'][:4]}-{row['end_date'][4:6]}-{row['end_date'][6:8]}"
            if report_period not in data_by_period:
                data_by_period[report_period] = {}
            
            logger.debug(f"Processing {statement_type} data for {report_period}: {row.to_dict()}")
            
            row_dict = row.to_dict()
            mapped_fields = apply_field_mapping(row_dict, statement_type)
            
            for field in target_fields:
                data_by_period[report_period][field] = self.safe_convert(field, mapped_fields)
            
            logger.debug(f"{statement_type}字段映射结果: {[f'{k}:{v}' for k,v in data_by_period[report_period].items() if k in target_fields]}")
        
        return data_by_period

    def _aggregate_data(self, income_data: Dict[str, Dict[str, float]], 
                        balance_data: Dict[str, Dict[str, float]], 
                        cashflow_data: Dict[str, Dict[str, float]], 
                        ticker: str, period: str, currency: str) -> Dict[str, Dict[str, float]]:
        """Aggregate data from income, balance, and cashflow statements.
        
        Args:
            income_data: Processed income statement data.
            balance_data: Processed balance sheet data.
            cashflow_data: Processed cash flow statement data.
            ticker: The stock ticker.
            period: The period type (e.g., 'ttm').
            currency: The currency of the data.
        
        Returns:
            A dictionary mapping report periods to their aggregated data.
        """
        all_periods = set(income_data.keys()) | set(balance_data.keys()) | set(cashflow_data.keys())
        # 按时间顺序排序（从小到大）
        sorted_periods = sorted(all_periods)
        aggregated_data = {}
        
        logger.debug(f"Aggregating data for periods: {sorted_periods}")
        
        for report_period in sorted_periods:
            aggregated_data[report_period] = {
                'ticker': ticker,
                'report_period': report_period,
                'period': self._get_period_type(report_period),
                'currency': currency,
                **income_data.get(report_period, {}),
                **balance_data.get(report_period, {}),
                **cashflow_data.get(report_period, {})
            }
            logger.debug(f"Aggregated data for {report_period}: {aggregated_data[report_period]}")

        return aggregated_data

    def _create_line_items(self, aggregated_data: Dict[str, Dict[str, float]]) -> List[LineItem]:
        """Create LineItem objects from aggregated data.
        
        Args:
            aggregated_data: The aggregated data for each report period.
        
        Returns:
            A list of LineItem objects.
        """
        line_items = []
        for period_data in aggregated_data.values():
            logger.debug(f"创建LineItem: {period_data['ticker']} - {period_data['report_period']}")
            line_item = LineItem(**period_data)
            line_items.append(line_item)
        return line_items

    def safe_convert(self, item, data):
        #如果存在，且不为NAN，则转换为float，否则转换为None
        if item in data and pd.notna(data[item]):
            return float(data[item])
        return None

    def _get_period_type(self, report_period):
        # 根据报告期间判断期间类型：12-31为年报，其他为季报
        return "annual" if report_period.endswith("-12-31") else "quarter"

    @with_timeout_retry("get_insider_trades")
    def get_insider_trades(
        self,
        ticker: str,
        end_date: str,
        start_date: Optional[str] = None,
        limit: int = 1000,
    ) -> List[InsiderTrade]:
        """获取股东增减持数据
        
        使用Tushare stk_holdertrade接口获取股东增减持数据
        参考文档: https://tushare.pro/document/2?doc_id=175
        """
        try:
            if self.pro is None:
                logger.error("Tushare API未初始化")
                return []
                
            # 构建查询参数
            params = {
                'fields': 'ts_code,ann_date,holder_name,holder_type,in_de,change_vol,change_ratio,after_share,after_ratio,avg_price,total_share,begin_date,close_date'
            }
            
            # 只有当ticker不为空且不为None时才添加ts_code参数
            if ticker and str(ticker).strip():
                ts_code = self._convert_ticker(ticker)
                params['ts_code'] = ts_code
            
            # 设置日期参数
            if start_date:
                params['start_date'] = self._convert_date_format(start_date)
            if end_date:
                params['end_date'] = self._convert_date_format(end_date)
            else:
                params['ann_date'] = self._convert_date_format(end_date)
            
            # 获取股东增减持数据
            df = self.pro.stk_holdertrade(**params)
            
            if df is None or df.empty:
                logger.debug(f"未找到股东增减持数据: {ticker}")
                return []
            
            trades = []
            for _, row in df.iterrows():
                # 转换日期格式
                filing_date = row['ann_date']
                if pd.notna(filing_date):
                    filing_date = filing_date[:4] + '-' + filing_date[4:6] + '-' + filing_date[6:8]
                else:
                    filing_date = end_date
                
                # 确定交易类型 (IN=增持, DE=减持)
                transaction_type = TransactionType.BUY if row['in_de'] == 'IN' else TransactionType.SELL
                
                # 股份数量 (change_vol 单位为股)
                shares = float(row['change_vol']) if pd.notna(row['change_vol']) else 0
                
                # 平均价格
                price = float(row['avg_price']) if pd.notna(row['avg_price']) else 0.0
                
                # 交易金额 = 股份数量 * 平均价格
                value = shares * price if shares and price else 0.0
                
                # 股东信息映射到现有字段
                holder_name = row['holder_name'] if pd.notna(row['holder_name']) else "未知"
                holder_type_map = {'G': '高管', 'P': '个人', 'C': '公司'}
                holder_type = holder_type_map.get(row['holder_type'], '未知') if pd.notna(row['holder_type']) else '未知'
                
                # 获取其他相关字段
                change_ratio = float(row['change_ratio']) if pd.notna(row['change_ratio']) else None
                after_share = float(row['after_share']) if pd.notna(row['after_share']) else None
                after_ratio = float(row['after_ratio']) if pd.notna(row['after_ratio']) else None
                
                trade = InsiderTrade(
                    ticker=ticker,
                    filing_date=filing_date,
                    name=holder_name,  # 使用现有的name字段存储股东姓名
                    title=holder_type,  # 使用现有的title字段存储股东类型
                    transaction_type=transaction_type,  # 只存储增持/减持
                    transaction_shares=shares,  # 使用现有的transaction_shares字段
                    transaction_price_per_share=price,  # 使用现有的transaction_price_per_share字段
                    transaction_value=value,  # 使用现有的transaction_value字段
                    shares_owned_after_transaction=after_share,  # 使用现有字段存储变动后持股
                    change_ratio=change_ratio,  # 占流通比例
                    after_ratio=after_ratio  # 变动后占流通比例
                )
                trades.append(trade)
            
            # 按公告日期排序，最新的在前
            trades.sort(key=lambda x: x.filing_date, reverse=True)
            
            logger.info(f"获取股东增减持数据成功 {ticker}: {len(trades)} 条记录")
            return trades[:limit]
            
        except Exception as e:
            logger.error(f"获取股东增减持数据失败 {ticker}: {e}")
            return []
    
    @with_timeout_retry("get_company_news")
    def get_company_news(
        self,
        ticker: str,
        end_date: str,
        start_date: Optional[str] = None,
        limit: int = 1000,
    ) -> List[CompanyNews]:
        """获取公司新闻 (Tushare可能没有新闻数据，返回空列表)"""
        try:
            # Tushare主要提供财务数据，新闻数据可能需要其他数据源
            logger.warning(f"Tushare暂不支持公司新闻数据: {ticker}")
            return []
            
        except Exception as e:
            logger.error(f"获取公司新闻失败 {ticker}: {e}")
            return []
    
    def get_market_cap(self, ticker: str, end_date: str) -> Optional[float]:
        """获取市值数据"""
        if not self.is_available():
            return None
            
        try:
            tushare_ticker = self._convert_ticker(ticker)
            
            # 首先尝试获取最近的交易日
            trade_date = end_date.replace('-', '')
            
            # 港股暂不支持市值数据
            if self._is_hk_stock(ticker):
                print(f"Tushare暂不支持港股市值数据: {ticker}")
                return None
            
            # A股使用daily_basic获取市值数据
            df = self.pro.daily_basic(ts_code=tushare_ticker, 
                                     trade_date=trade_date,
                                     fields='ts_code,trade_date,total_mv')
            
            if df.empty:
                # 如果指定日期没有数据，尝试获取最近一个月的数据
                from datetime import datetime, timedelta
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                start_dt = end_dt - timedelta(days=30)
                start_date_str = start_dt.strftime('%Y%m%d')
                
                df = self.pro.daily_basic(ts_code=tushare_ticker, 
                                         start_date=start_date_str,
                                         end_date=trade_date,
                                         fields='ts_code,trade_date,total_mv')
                
                if df.empty:
                    print(f"市值数据为空 {ticker} 在日期 {end_date}")
                    return None
                
                # 取最近的有数据的日期
                df = df.sort_values('trade_date', ascending=False)
            
            # A股total_mv字段，单位是万元
            if pd.notna(df.iloc[0]['total_mv']):
                market_cap = float(df.iloc[0]['total_mv']) * 10000
                print(f"获取市值成功 {ticker}: {market_cap:,.0f} 人民币 (日期: {df.iloc[0]['trade_date']})")
                return market_cap
            else:
                print(f"市值字段为空 {ticker}")
                return None
            
        except Exception as e:
            print(f"获取市值失败 {ticker}: {e}")
            return None
    
    def is_available(self) -> bool:
        """检查数据提供商是否可用"""
        try:
            if not self.api_key or self.pro is None:
                return False
            
            # 简单测试，获取交易日历
            cal_df = self.pro.trade_cal(
                exchange='SSE',
                start_date='20240101',
                end_date='20240102',
                fields='cal_date,is_open'
            )
            return cal_df is not None and not cal_df.empty
            
        except Exception as e:
            logger.error(f"Tushare可用性检查失败: {e}")
            return False

    def convert_period(self, period: str) -> str:
        return "annual" if period == 'ttm' else period
