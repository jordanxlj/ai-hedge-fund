"""
测试 AggregatedFinancialInfo 类和 merge_financial_data 函数
"""
import pytest
import pandas as pd
from datetime import datetime
from src.data.models import FinancialMetrics, LineItem, FinancialProfile
from src.tools.api import merge_financial_data
from src.utils.financial_utils import reconstruct_financial_metrics


def create_sample_financial_metrics(ticker="AAPL", periods=3):
    """创建样本财务指标数据"""
    metrics = []
    for i in range(periods):
        period = f"2024-Q{i+1}"
        metrics.append(FinancialMetrics(
            ticker=ticker,
            name=f"{ticker} Inc.",
            report_period=period,
            period="quarterly",
            currency="USD",
            market_cap=3000000000.0,
            price_to_earnings_ratio=25.5,
            revenue_growth=0.15,
            net_margin=0.25,
            return_on_equity=0.18,
            debt_to_equity=0.5,
            earnings_per_share=6.5 + i,
        ))
    return metrics


def create_sample_line_items(ticker="AAPL", periods=3):
    """创建样本财务报表项目数据"""
    line_items = []
    items_to_create = {
        "revenue": 100000000.0,
        "net_income": 25000000.0,
        "free_cash_flow": 20000000.0,
        "total_assets": 500000000.0,
        "working_capital": 50000000.0,
    }
    for i in range(periods):
        period = f"2024-Q{i+1}"
        for name, base_value in items_to_create.items():
            line_items.append(LineItem(
                ticker=ticker,
                report_period=period,
                period="quarterly",
                currency="USD",
                name=name,
                value=base_value + i * 1000000, # Add some variation
            ))
    return line_items


@pytest.fixture
def sample_financial_metrics():
    return create_sample_financial_metrics()


@pytest.fixture
def sample_line_items():
    return create_sample_line_items()


class TestFinancialProfile:
    """测试 FinancialProfile 类"""

    def test_creation_with_all_fields(self):
        """测试使用所有字段创建 FinancialProfile 对象"""
        data = {
            "ticker": "AAPL",
            "name": "Apple Inc.",
            "report_period": "2023-12-31",
            "period": "annual",
            "net_income": 55000,
            "revenue_growth": 0.1
        }
        aggregated = FinancialProfile(**data)
        assert aggregated.ticker == "AAPL"
        assert aggregated.net_income == 55000
        assert aggregated.revenue_growth == 0.1

    def test_creation_with_minimal_fields(self):
        """测试仅使用最少字段创建 FinancialProfile 对象"""
        minimal_data = {
            "ticker": "MSFT",
            "name": "Microsoft",
            "report_period": "2023-12-31",
            "period": "annual"
        }
        aggregated = FinancialProfile(**minimal_data)
        assert aggregated.ticker == "MSFT"
        assert aggregated.net_income is None

    def test_dynamic_field_creation(self):
        """测试动态添加字段"""
        data = {
            "ticker": "GOOGL",
            "name": "Alphabet",
            "report_period": "2023-12-31",
            "period": "annual",
            "new_custom_metric": 123.45
        }
        aggregated = FinancialProfile(**data)
        assert aggregated.new_custom_metric == 123.45


class TestMergeFinancialData:
    """测试 merge_financial_data 函数"""

    def test_merge_with_matching_periods(self, sample_financial_metrics, sample_line_items):
        """测试匹配期间的合并"""
        result = merge_financial_data(sample_financial_metrics, sample_line_items)
        assert len(result) == 3
        assert all(isinstance(item, FinancialProfile) for item in result)
        
        q1_data = next((item for item in result if item.report_period == "2024-Q1"), None)
        assert q1_data is not None
        assert q1_data.price_to_earnings_ratio == 25.5
        assert q1_data.revenue == 100000000.0

    def test_merge_with_no_line_items(self, sample_financial_metrics):
        """测试没有财务报表项目的情况"""
        result = merge_financial_data(sample_financial_metrics, [])
        assert len(result) == 3
        assert all(isinstance(item, FinancialProfile) for item in result)
        q1_data = next((item for item in result if item.report_period == "2024-Q1"), None)
        assert q1_data.revenue is None

    def test_merge_with_no_financial_metrics(self, sample_line_items):
        """测试没有财务指标的情况"""
        result = merge_financial_data([], sample_line_items)
        assert result == []


if __name__ == "__main__":
    # 运行测试
    import sys
    import os
    
    # 添加项目根目录到路径
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    
    pytest.main([__file__, "-v"])