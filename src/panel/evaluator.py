import pandas as pd
import numpy as np
import statsmodels.api as sm

class StrategyEvaluator:
    """
    A class to evaluate trading strategies and generate performance metrics.
    """
    def __init__(self, strategies: dict, data_loader):
        self.strategies = strategies
        self.data_loader = data_loader

    def run(self, tickers: list, start_date: str, end_date: str, initial_capital: float = 100000.0, risk_free_rate: float = 0.03, visualize: bool = False):
        """
        Run the evaluation for the specified strategies and tickers.

        :param tickers: A list of ticker symbols to test.
        :param start_date: The start date for data loading (YYYY-MM-DD).
        :param end_date: The end date for data loading (YYYY-MM-DD).
        :param initial_capital: The initial capital for the backtest.
        :param risk_free_rate: The risk-free rate for calculating Sharpe and Sortino ratios.
        :param visualize: Whether to display the strategy visualization.
        :return: A dictionary with the evaluation results.
        """
        results = {}
        for strategy_name, strategy in self.strategies.items():
            for ticker in tickers:
                df = self.data_loader.load_daily_prices(tickers=[ticker], start_date=start_date, end_date=end_date)
                if not df.empty:
                    df = df.set_index('time')
                    backtest_results = strategy.backtest(df, initial_capital)
                    metrics = self.calculate_metrics(backtest_results, df, risk_free_rate)
                    results[f'{strategy_name}_{ticker}'] = metrics
                    if visualize:
                        strategy.visualize(df)
        return results

    def calculate_metrics(self, backtest_results: dict, data: pd.DataFrame, risk_free_rate: float) -> dict:
        """
        Calculate performance metrics for a backtest.

        :param backtest_results: A dictionary with the backtest results.
        :param data: The historical data used for the backtest.
        :param risk_free_rate: The risk-free rate.
        :return: A dictionary with the calculated metrics.
        """
        equity_curve = backtest_results['equity_curve']
        signals = backtest_results.get('signals', pd.DataFrame())
        returns = equity_curve.pct_change().dropna()
        total_return = (equity_curve.iloc[-1] - equity_curve.iloc[0]) / equity_curve.iloc[0]
        annualized_return = (1 + total_return) ** (252 / len(returns)) - 1 if len(returns) > 0 else 0
        volatility = returns.std() * np.sqrt(252) if len(returns) > 0 else 0
        max_drawdown = (equity_curve / equity_curve.cummax() - 1).min() if not equity_curve.empty else 0
        sharpe_ratio = (annualized_return - risk_free_rate) / volatility if volatility > 0 else 0
        downside_vol = returns[returns < 0].std() * np.sqrt(252) if len(returns[returns < 0]) > 0 else 0
        sortino_ratio = (annualized_return - risk_free_rate) / downside_vol if downside_vol > 0 else 0
        
        benchmark_returns = data['close'].pct_change().dropna()
        benchmark_total_return = (data['close'].iloc[-1] - data['close'].iloc[0]) / data['close'].iloc[0] if not data.empty else 0
        benchmark_annualized_return = (1 + benchmark_total_return) ** (252 / len(benchmark_returns)) - 1 if len(benchmark_returns) > 0 else 0
        
        tracking_error = (returns - benchmark_returns).std() if len(returns) == len(benchmark_returns) else 0
        information_ratio = (annualized_return - benchmark_annualized_return) / tracking_error if tracking_error > 0 else 0
        win_rate = (returns > 0).mean() if not returns.empty else 0
        
        calmar_ratio = annualized_return / abs(max_drawdown) if max_drawdown != 0 else 0
        
        if len(returns) == len(benchmark_returns) and len(returns) > 1:
            X = sm.add_constant(benchmark_returns)
            model = sm.OLS(returns, X).fit()
            alpha = model.params[0]
            beta = model.params[1]
        else:
            alpha = 0
            beta = 0
        
        if not signals.empty and 'position' in signals.columns:
            trades = signals['position'].diff().abs() > 0
            num_trades = trades.sum()
            avg_holding_period = len(signals) / num_trades if num_trades > 0 else 0
            strategy_returns = signals.get('strategy_returns', returns)
            profit_loss_ratio = strategy_returns[strategy_returns > 0].mean() / abs(strategy_returns[strategy_returns < 0].mean()) if len(strategy_returns[strategy_returns < 0]) > 0 else 0
        else:
            num_trades = 0
            avg_holding_period = 0
            profit_loss_ratio = 0
        
        consecutive_wins = (returns > 0).astype(int).groupby((returns > 0).ne((returns > 0).shift()).cumsum()).cumsum().max()
        consecutive_losses = (returns < 0).astype(int).groupby((returns < 0).ne((returns < 0).shift()).cumsum()).cumsum().max()
        
        monte_carlo_sims = 100
        mc_returns = []
        for _ in range(monte_carlo_sims):
            shuffled_returns = np.random.permutation(returns)
            cum_ret = np.prod(1 + shuffled_returns) - 1
            ann_ret = (1 + cum_ret) ** (252 / len(shuffled_returns)) - 1 if len(shuffled_returns) > 0 else 0
            mc_returns.append(ann_ret)
        mc_mean = np.mean(mc_returns) if mc_returns else 0
        mc_std = np.std(mc_returns) if mc_returns else 0
        
        return {
            'total_return': total_return,
            'annualized_return': annualized_return,
            'benchmark_return': benchmark_annualized_return,
            'volatility': volatility,
            'max_drawdown': max_drawdown,
            'sharpe_ratio': sharpe_ratio,
            'sortino_ratio': sortino_ratio,
            'information_ratio': information_ratio,
            'win_rate': win_rate,
            'calmar_ratio': calmar_ratio,
            'alpha': alpha,
            'beta': beta,
            'num_trades': num_trades,
            'avg_holding_period': avg_holding_period,
            'profit_loss_ratio': profit_loss_ratio,
            'consecutive_wins': consecutive_wins,
            'consecutive_losses': consecutive_losses,
            'mc_mean_return': mc_mean,
            'mc_std_return': mc_std
        }

if __name__ == '__main__':
    import argparse
    from src.panel.data.data_loader import DataLoader
    from src.data.db import get_database_api
    from src.panel.strategy.bollinger_bands_breakout import BollingerBandsBreakoutStrategy
    from src.panel.strategy.ground_truth_strategy import GroundTruthStrategy

    parser = argparse.ArgumentParser(description="Strategy Evaluator")
    parser.add_argument("--db_path", type=str, required=True, help="Path to the DuckDB database file.")
    parser.add_argument("--tickers", type=str, required=True, help="Comma-separated list of ticker symbols to test.")
    parser.add_argument("--strategies", type=str, required=True, help="Comma-separated list of strategy names to test, or 'all'.")
    parser.add_argument("--start_date", type=str, help="Start date for data loading (YYYY-MM-DD).")
    parser.add_argument("--end_date", type=str, help="End date for data loading (YYYY-MM-DD).")
    parser.add_argument("--visualize", action='store_true', help="Display strategy visualizations.")

    args = parser.parse_args()

    db_api = get_database_api("duckdb", db_path=args.db_path)
    with db_api:
        data_loader = DataLoader(db_api)
        
        strategies = {
            'bollinger_bands': BollingerBandsBreakoutStrategy(),
            'ground_truth': GroundTruthStrategy()
        }

        if args.strategies == 'all':
            selected_strategies = strategies
        else:
            selected_strategies = {name: strategy for name, strategy in strategies.items() if name in args.strategies.split(',')}
        
        tickers = args.tickers.split(',')

        evaluator = StrategyEvaluator(selected_strategies, data_loader)
        results = evaluator.run(tickers, args.start_date, args.end_date, visualize=args.visualize)
        
        results_df = pd.DataFrame(results).T
        print(results_df)
