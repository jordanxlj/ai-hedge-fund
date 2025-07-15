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

# Data Fetcher
class DataFetcher:
    def __init__(self, tickers, start_date, end_date):
        self.tickers = tickers
        self.start_date = start_date
        self.end_date = end_date
        self.prices = self.fetch_prices()
        print(f"Fetched prices for {len(self.tickers)} tickers from {start_date} to {end_date}. Shape: {self.prices.shape}")

    def fetch_prices(self):
        data = yf.download(self.tickers, start=self.start_date, end=self.end_date)
        prices = data['Close']
        prices.index = prices.index.tz_localize(None)
        print("Sample prices:\n", prices.head())
        return prices

# Factor Engine
class FactorEngine:
    def __init__(self, prices):
        self.prices = prices
        print(f"FactorEngine initialized with prices shape: {prices.shape}")

    def calculate_factors(self):
        factors = {}
        # Momentum Factor
        factors['momentum'] = self.prices.pct_change(252).shift(1)
        # Value Factor (simplified: inverse of price to MA)
        factors['value'] = 1 / (self.prices / self.prices.rolling(252).mean())
        print("Calculated factors: momentum and value")
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
        print("Normalized factor sample:\n", normalized.head())
        return normalized

# Strategy Generator
class StrategyGenerator:
    def __init__(self, factors, prices):
        self.factors = factors
        self.prices = prices
        print(f"StrategyGenerator initialized with {len(factors)} factors")

    def generate_strategies(self):
        strategies = {}
        for name, factor in self.factors.items():
            normalized = FactorEngine(self.prices).normalize_factors(factor)
            factor_data = al.utils.get_clean_factor_and_forward_returns(
                normalized, self.prices, quantiles=5, periods=(1, 5, 10)
            )
            strategies[name] = factor_data
            print(f"Generated strategy for {name}. Factor data shape: {factor_data.shape}")
            # Generate and save visualization
            sns.set_style("whitegrid")
            sns.set_palette("deep")
            plt.rcParams.update({'figure.figsize': (24, 16), 'figure.subplot.hspace': 0.6, 'figure.subplot.wspace': 0.4, 'axes.titlesize': 10, 'axes.labelsize': 8, 'xtick.labelsize': 8, 'ytick.labelsize': 8})
            create_full_tear_sheet(factor_data, long_short=True, group_neutral=False, by_group=False)
            plt.tight_layout()
            plt.savefig(f'result/{name}_factor_tear_sheet.png')
            print(f"Saved tear sheet visualization for {name} factor to result/{name}_factor_tear_sheet.png")
        return strategies

# Portfolio Manager
class PortfolioManager:
    def __init__(self, expected_returns, cov_matrix, risk_aversion):
        self.expected_returns = expected_returns
        self.cov_matrix = cov_matrix
        self.risk_aversion = risk_aversion
        print(f"PortfolioManager initialized with {len(expected_returns)} assets")

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
            print("Optimization successful. Weights:", w.value)
            return w.value
        else:
            print("Optimization failed, using equal weights")
            return np.ones(n) / n  # Fallback to equal weights

# Risk Manager
class RiskManager:
    def __init__(self, portfolio_returns, confidence_level=0.95):
        self.portfolio_returns = portfolio_returns
        self.confidence_level = confidence_level
        print(f"RiskManager initialized. Portfolio returns shape: {portfolio_returns.shape}")

    def calculate_var(self):
        var = norm.ppf(1 - self.confidence_level) * np.std(self.portfolio_returns)
        print(f"Calculated VaR: {var}")
        return var

    def calculate_cvar(self):
        var = self.calculate_var()
        cvar = - (self.portfolio_returns[self.portfolio_returns <= -var].mean())
        print(f"Calculated CVaR: {cvar}")
        return cvar

    def adjust_portfolio(self, weights, threshold=0.05):
        var = abs(self.calculate_var())
        if var > threshold:
            adjusted = weights * (threshold / var)
            print(f"Adjusted weights due to high VaR: {adjusted}")
            return adjusted  # Scale down if risk exceeds threshold
        print("No adjustment needed")
        return weights

# PNL Module (Main Closed-Loop System)
class PNLModule:
    def __init__(self, tickers, start_date, end_date, risk_aversion=0.5, pnl_threshold=0.1, max_iterations=5):
        self.tickers = tickers
        self.start_date = start_date
        self.end_date = end_date
        self.risk_aversion = risk_aversion
        self.pnl_threshold = pnl_threshold
        self.max_iterations = max_iterations
        self.iteration = 0
        self.data_fetcher = DataFetcher(tickers, start_date, end_date)
        self.factor_engine = FactorEngine(self.data_fetcher.prices)
        self.factors = self.factor_engine.calculate_factors()
        self.strategy_gen = StrategyGenerator(self.factors, self.data_fetcher.prices)
        self.strategies = self.strategy_gen.generate_strategies()
        self.returns = self.data_fetcher.prices.pct_change().dropna()
        print(f"PNLModule initialized. Returns shape: {self.returns.shape}")

    def select_best_strategy(self):
        best_key = max(self.strategies, key=lambda k: self.strategies[k].filter(like='forward_returns').mean().mean())
        print(f"Selected best strategy: {best_key}")
        return self.strategies[best_key]

    def compute_pnl(self, weights, selected_stocks, label):
        portfolio_returns = self.returns[selected_stocks] @ weights
        portfolio_returns.name = label
        pf.create_full_tear_sheet(portfolio_returns)
        pnl = portfolio_returns.cumsum()
        final_pnl = pnl.iloc[-1]
        print(f"Computed PNL for {label}: {final_pnl}")
        return final_pnl

    def run(self):
        self.iteration += 1
        if self.iteration > self.max_iterations:
            print("Max iterations reached. Stopping.")
            return

        factor_data = self.select_best_strategy()
        selected_stocks = factor_data.index.get_level_values(1).unique()[:10]  # Top 10 stocks
        expected_returns = self.returns[selected_stocks].mean().values
        cov_matrix = self.returns[selected_stocks].cov().values

        pm = PortfolioManager(expected_returns, cov_matrix, self.risk_aversion)
        aggressive_weights = pm.optimize('aggressive')
        conservative_weights = pm.optimize('conservative')

        rm_aggr = RiskManager(self.returns[selected_stocks] @ aggressive_weights)
        aggressive_weights = rm_aggr.adjust_portfolio(aggressive_weights)
        rm_cons = RiskManager(self.returns[selected_stocks] @ conservative_weights)
        conservative_weights = rm_cons.adjust_portfolio(conservative_weights)

        # Compute PNL
        pnl_aggr = self.compute_pnl(aggressive_weights, selected_stocks, 'Aggressive')
        pnl_cons = self.compute_pnl(conservative_weights, selected_stocks, 'Conservative')

        # Closed loop: Re-optimize if PNL below threshold
        if min(pnl_aggr, pnl_cons) < self.pnl_threshold:
            self.risk_aversion *= 1.2 if pnl_aggr > pnl_cons else 0.8  # Adjust based on which is lower
            print(f"Iteration {self.iteration}: PNL below threshold. Re-optimizing with risk_aversion={self.risk_aversion}")
            self.run()
        else:
            print(f"Optimal PNL achieved: Aggressive={pnl_aggr:.2f}, Conservative={pnl_cons:.2f}")

# Example Usage
if __name__ == '__main__':
    tickers = ['0002.HK', '0003.HK', '0139.HK', '1810.HK', '0001.HK', '0690.HK', '2020.HK', '0012.HK', '1044.HK', '1093.HK', '1109.HK', '0267.HK', '0388.HK', '101.HK', '027.HK', '2319.HK', '2331.HK', '9988.HK', '0669.HK', '9633.HK']  # Reduced for demo
    pnl_module = PNLModule(tickers, '2020-01-01', '2025-07-14')
    pnl_module.run()
