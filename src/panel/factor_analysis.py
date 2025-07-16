import yfinance as yf
import pandas as pd
import numpy as np
import alphalens as al
from alphalens.tears import create_full_tear_sheet
import pyfolio as pf
from sklearn.preprocessing import StandardScaler
import cvxpy as cp
from scipy.stats import norm
import matplotlib.pyplot as plt
import seaborn as sns
import logging
from src.utils.log_util import logger_setup as _init_logging

_init_logging()
logger = logging.getLogger(__name__)

# Data Fetcher
class DataFetcher:
    def __init__(self, tickers, start_date, end_date):
        self.tickers = tickers
        self.start_date = start_date
        self.end_date = end_date
        self.prices = self.fetch_prices()
        logger.info(f"Fetched prices for {len(self.tickers)} tickers from {start_date} to {end_date}. Shape: {self.prices.shape}")

    def fetch_prices(self):
        data = yf.download(self.tickers, start=self.start_date, end=self.end_date)
        prices = data['Close']
        prices.index = prices.index.tz_localize(None)
        # Filter out tickers with insufficient data
        valid_tickers = [ticker for ticker in prices.columns if prices[ticker].dropna().shape[0] >= 252]
        prices = prices[valid_tickers]
        logger.info(f"Filtered to {len(valid_tickers)} valid tickers with at least 252 data points. Valid tickers: {valid_tickers}")
        logger.info(f"Sample prices: {prices.head()}")
        return prices

# Factor Engine
class FactorEngine:
    def __init__(self, prices):
        self.prices = prices
        logger.info(f"FactorEngine initialized with prices shape: {prices.shape}")

    def calculate_factors(self):
        factors = {}
        # Momentum Factor
        factors['momentum'] = self.prices.pct_change(252).shift(1)
        # Value Factor (simplified: inverse of price to MA)
        #factors['value'] = 1 / (self.prices / self.prices.rolling(252).mean())
        logger.info("Calculated factors: momentum and value")
        return factors

    def normalize_factors(self, factor):
        factor_long = factor.stack().reset_index()
        factor_long.columns = ['date', 'asset', 'factor']
        factor_long.set_index(['date', 'asset'], inplace=True)
        def zscore(group):
            scaler = StandardScaler()
            return scaler.fit_transform(group.values.reshape(-1, 1)).flatten()
        normalized = factor_long.groupby(level=0)['factor'].transform(zscore)
        normalized.name = 'factor'
        logger.info(f"Normalized factor sample: {normalized.head()}")
        return normalized

# Strategy Generator
class StrategyGenerator:
    def __init__(self, factors, prices):
        self.factors = factors
        self.prices = prices
        logger.info(f"StrategyGenerator initialized with {len(factors)} factors")

    def generate_strategies(self):
        strategies = {}
        for name, factor in self.factors.items():
            normalized = FactorEngine(self.prices).normalize_factors(factor)
            factor_data = al.utils.get_clean_factor_and_forward_returns(
                normalized, self.prices, quantiles=5, periods=(1, 5, 10)
            )
            strategies[name] = factor_data
            logger.info(f"Generated strategy for {name}. Factor data shape: {factor_data.shape}")
            # Generate and save visualization
            sns.set_style("whitegrid")
            sns.set_palette("deep")
            plt.rcParams.update({'figure.figsize': (24, 16), 'figure.subplot.hspace': 0.6, 'figure.subplot.wspace': 0.4, 'axes.titlesize': 10, 'axes.labelsize': 8, 'xtick.labelsize': 8, 'ytick.labelsize': 8})
            create_full_tear_sheet(factor_data, long_short=True, group_neutral=False, by_group=False)
            plt.tight_layout()
            plt.savefig(f'result/{name}_factor_tear_sheet.png')
            logger.info(f"Saved tear sheet visualization for {name} factor to result/{name}_factor_tear_sheet.png")
        return strategies

# Portfolio Manager
class PortfolioManager:
    def __init__(self, expected_returns, cov_matrix, risk_aversion):
        self.expected_returns = expected_returns
        self.cov_matrix = cov_matrix
        self.risk_aversion = risk_aversion
        logger.info(f"PortfolioManager initialized with {len(expected_returns)} assets")

    def optimize(self, risk_preference='aggressive'):
        n = len(self.expected_returns)
        w = cp.Variable(n)
        ret = self.expected_returns.T @ w
        risk = cp.quad_form(w, self.cov_matrix)
        gamma = self.risk_aversion / 2 if risk_preference == 'aggressive' else self.risk_aversion * 2
        obj = cp.Maximize(ret - gamma * risk)
        constraints = [cp.sum(w) == 1, w >= 0]
        prob = cp.Problem(obj, constraints)
        prob.solve()
        if prob.status == 'optimal':
            logger.info(f"Optimization successful. Weights: {w.value}")
            return w.value
        else:
            logger.info("Optimization failed, using equal weights")
            return np.ones(n) / n  # Fallback to equal weights

# Risk Manager
class RiskManager:
    def __init__(self, portfolio_returns, confidence_level=0.95):
        self.portfolio_returns = portfolio_returns
        self.confidence_level = confidence_level
        logger.info(f"RiskManager initialized. Portfolio returns shape: {portfolio_returns.shape}")

    def calculate_var(self):
        var = norm.ppf(1 - self.confidence_level) * np.std(self.portfolio_returns)
        logger.info(f"Calculated VaR: {var}")
        return var

    def calculate_cvar(self):
        var = self.calculate_var()
        cvar = - (self.portfolio_returns[self.portfolio_returns <= -var].mean())
        logger.info(f"Calculated CVaR: {cvar}")
        return cvar

    def adjust_portfolio(self, weights, threshold=0.05):
        var = abs(self.calculate_var())
        if var > threshold:
            adjusted = weights * (threshold / var)
            logger.info(f"Adjusted weights due to high VaR: {adjusted}")
            return adjusted  # Scale down if risk exceeds threshold
        logger.info("No adjustment needed")
        return weights

# PNL Module (Main Closed-Loop System)
class PNLModule:
    def __init__(self, tickers, start_date, end_date, risk_aversion=0.5, initial_capital=100000):
        self.tickers = tickers
        self.start_date = start_date
        self.end_date = end_date
        self.risk_aversion = risk_aversion
        self.initial_capital = initial_capital
        self.data_fetcher = DataFetcher(tickers, start_date, end_date)
        self.prices = self.data_fetcher.prices
        self.returns = self.prices.pct_change().dropna(how='all')
        logger.info(f"Computed returns shape: {self.returns.shape}. Sample returns: {self.returns.head()}")
        logger.info(f"PNLModule initialized. Returns shape: {self.returns.shape}")

    def get_data_slice(self, end_date):
        return self.prices.loc[:end_date], self.returns.loc[:end_date]

    def backtest(self):
        overall_portfolio_value = pd.Series(dtype=float)
        current_cash = self.initial_capital
        current_positions = pd.Series(0, index=self.tickers)

        # Initial period: up to 2024-06-30 (calculation only, no simulation)
        initial_end = pd.to_datetime('2024-06-30')
        prices_initial, returns_initial = self.get_data_slice(initial_end)
        factor_engine = FactorEngine(prices_initial)
        factors = factor_engine.calculate_factors()
        strategy_gen = StrategyGenerator(factors, prices_initial)
        strategies = strategy_gen.generate_strategies()
        factor_data = self.select_best_strategy(strategies)
        selected_stocks = factor_data.index.get_level_values(1).unique()[:10]
        selected_returns = returns_initial[selected_stocks].dropna(how='all', axis=1)
        selected_stocks = selected_returns.columns.tolist()
        if not selected_stocks:
            logger.warning("No valid stocks in initial period. Aborting backtest.")
            return
        expected_returns = selected_returns.mean().values
        cov_matrix = selected_returns.cov(min_periods=1).values
        cov_matrix = np.nan_to_num(cov_matrix, nan=0.0)
        cov_matrix = (cov_matrix + cov_matrix.T) / 2
        pm = PortfolioManager(expected_returns, cov_matrix, self.risk_aversion)
        weights = pm.optimize('aggressive')
        # Set initial positions at end of period
        initial_end_date = prices_initial.index[prices_initial.index <= initial_end].max()
        initial_prices = prices_initial.loc[initial_end_date]
        initial_values = self.initial_capital * weights
        initial_shares = (initial_values / initial_prices[selected_stocks]).fillna(0).astype(int)
        initial_cash = self.initial_capital - (initial_shares * initial_prices[selected_stocks]).sum()
        initial_pv = pd.Series(self.initial_capital, index=[initial_end])
        overall_portfolio_value = initial_pv
        current_positions[selected_stocks] = initial_shares
        current_cash = initial_cash
        logger.info(f"Initial positions set at {initial_end}: Shares {initial_shares}, Cash {initial_cash}, Value {self.initial_capital}")

        # Monthly iterations from 2024-07-01 to 2025-07-11
        dates = pd.date_range(start='2024-07-01', end='2025-07-11', freq='M')
        for month_end in dates:
            month_start = month_end - pd.offsets.MonthBegin(1)
            prices_month, returns_month = self.get_data_slice(month_end)
            factor_engine = FactorEngine(prices_month)
            factors = factor_engine.calculate_factors()
            strategy_gen = StrategyGenerator(factors, prices_month)
            strategies = strategy_gen.generate_strategies()
            factor_data = self.select_best_strategy(strategies)
            selected_stocks = factor_data.index.get_level_values(1).unique()[:10]
            selected_returns = returns_month[selected_stocks].dropna(how='all', axis=1)
            selected_stocks = selected_returns.columns.tolist()
            if not selected_stocks:
                logger.warning(f"No valid stocks for {month_end}. Skipping month.")
                continue
            expected_returns = selected_returns.mean().values
            cov_matrix = selected_returns.cov(min_periods=1).values
            cov_matrix = np.nan_to_num(cov_matrix, nan=0.0)
            cov_matrix = (cov_matrix + cov_matrix.T) / 2
            pm = PortfolioManager(expected_returns, cov_matrix, self.risk_aversion)
            new_weights = pm.optimize('aggressive')
            # Trend-following: adjust weights based on momentum
            last_date = factors['momentum'].index[factors['momentum'].index <= month_end].max()
            momentum = factors['momentum'].loc[last_date, selected_stocks]
            trend_adjust = (momentum > 0).astype(int) * 1.2 + (momentum <= 0).astype(int) * 0.8
            new_weights *= trend_adjust.values
            new_weights /= new_weights.sum()  # Renormalize
            month_pv, month_pos, month_cash = self.simulate_positions(new_weights, selected_stocks, f'month_{month_end.strftime("%Y-%m")}', initial_capital=current_cash, start_date=month_start, end_date=month_end, initial_positions=current_positions[selected_stocks])
            overall_portfolio_value = pd.concat([overall_portfolio_value, month_pv])
            current_positions[selected_stocks] = month_pos
            current_cash = month_cash

        # Plot equity curve
        plt.figure(figsize=(12, 6))
        overall_portfolio_value.plot()
        plt.title('Equity Return Curve')
        plt.xlabel('Date')
        plt.ylabel('Portfolio Value')
        plt.savefig('result/equity_curve.png')
        logger.info("Saved equity return curve to result/equity_curve.png")

    def select_best_strategy(self, strategies):
        best_key = max(strategies, key=lambda k: strategies[k].filter(like='forward_returns').mean().mean())
        logger.info(f"Selected best strategy: {best_key}")
        return strategies[best_key]

    def compute_pnl(self, weights, selected_stocks, label):
        portfolio_returns = self.returns[selected_stocks] @ weights
        portfolio_returns.name = label
        pf.create_full_tear_sheet(portfolio_returns, display_plots=False)
        pnl = portfolio_returns.cumsum()
        final_pnl = pnl.iloc[-1]
        logger.info(f"Computed PNL for {label}: {final_pnl}")
        return final_pnl

    def simulate_positions(self, weights, stocks, strategy_name, initial_capital=100000, rebalance_freq='M', start_date=None, end_date=None, initial_positions=None):
        logger.info(f"Simulating positions for {strategy_name} strategy from {start_date} to {end_date}")
        prices = self.data_fetcher.prices[stocks]
        if start_date:
            prices = prices.loc[start_date:]
        if end_date:
            prices = prices.loc[:end_date]
        dates = prices.index
        positions = pd.DataFrame(0, index=dates, columns=stocks)  # Shares held
        if initial_positions is not None:
            positions.iloc[0] = initial_positions
        cash = initial_capital
        portfolio_value = pd.Series(index=dates, dtype=float)
        actions = []

        for i, date in enumerate(dates):
            current_prices = prices.loc[date]
            current_value = (positions.loc[date] * current_prices).sum() + cash
            portfolio_value[date] = current_value

            if i == 0 or (date.month != dates[i - 1].month):  # Monthly rebalance
                target_values = current_value * weights
                target_shares = (target_values / current_prices).fillna(0).astype(int)
                share_diff = target_shares - positions.loc[date]
                logger.info(f"At {date}, target_shares shape: {target_shares.shape}, slice shape: {positions.loc[date:].shape}")

                for stock, diff in share_diff.items():
                    if diff > 0:
                        action = f'Buy {diff} shares of {stock}'
                        cost = diff * current_prices[stock]
                        cash -= cost
                    elif diff < 0:
                        action = f'Sell {-diff} shares of {stock}'
                        revenue = -diff * current_prices[stock]
                        cash += revenue
                    else:
                        action = f'Hold {stock}'
                    actions.append({'Date': date, 'Stock': stock, 'Action': action})

                num_rows = len(positions.loc[date:])
                positions.loc[date:] = np.tile(target_shares.values, (num_rows, 1))

        actions_df = pd.DataFrame(actions)
        actions_df.to_csv(f'result/{strategy_name}_position_actions.csv', index=False)
        logger.info(f"{strategy_name} Position Actions:\n{actions_df}")
        portfolio_value.to_csv(f'result/{strategy_name}_portfolio_value.csv')
        logger.info(f"Final portfolio value for {strategy_name}: {portfolio_value.iloc[-1]}")
        return portfolio_value, positions.iloc[-1], cash

# Example Usage
if __name__ == '__main__':
    tickers = ['0002.HK', '0003.HK', '0139.HK', '1810.HK', '0001.HK', '0690.HK', '2020.HK', '0012.HK', '1044.HK', '1093.HK', '1109.HK', '0267.HK', '0388.HK', '101.HK', '027.HK', '2319.HK', '2331.HK', '9988.HK', '0669.HK', '9633.HK']
    pnl_module = PNLModule(tickers, '2020-01-01', '2025-07-11')
    pnl_module.backtest()
