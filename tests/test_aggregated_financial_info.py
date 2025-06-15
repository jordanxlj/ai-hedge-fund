"""
测试 AggregatedFinancialInfo 类和 merge_financial_data 函数
"""
import pytest
from datetime import datetime
from src.data.models import FinancialMetrics, LineItem, AggregatedFinancialInfo
from src.tools.api import merge_financial_data


def create_sample_financial_metrics(ticker="AAPL", periods=3):
    """创建样本财务指标数据"""
    metrics = []
    for i in range(periods):
        period = f"2024-Q{i+1}"
        metrics.append(FinancialMetrics(
            ticker=ticker,
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
    for i in range(periods):
        period = f"2024-Q{i+1}"
        line_items.append(LineItem(
            ticker=ticker,
            report_period=period,
            period="quarterly",
            currency="USD",
            revenue=100000000.0 + i * 10000000,
            net_income=25000000.0 + i * 2500000,
            free_cash_flow=20000000.0 + i * 2000000,
            total_assets=500000000.0,
            working_capital=50000000.0,
        ))
    return line_items


class TestAggregatedFinancialInfo:
    """测试 AggregatedFinancialInfo 类"""
    
    def test_inheritance_from_financial_metrics(self):
        """测试继承自 FinancialMetrics"""
        data = {
            "ticker": "AAPL",
            "report_period": "2024-Q1",
            "period": "quarterly",
            "price_to_earnings_ratio": 25.5,
            "revenue": 100000000.0,
            "net_income": 25000000.0,
        }
        
        aggregated = AggregatedFinancialInfo(**data)
        
        # 验证继承的字段
        assert aggregated.ticker == "AAPL"
        assert aggregated.price_to_earnings_ratio == 25.5
        
        # 验证新增的字段
        assert aggregated.revenue == 100000000.0
        assert aggregated.net_income == 25000000.0
    
    def test_optional_fields(self):
        """测试可选字段"""
        # 只提供必需字段
        minimal_data = {
            "ticker": "AAPL",
            "report_period": "2024-Q1",
            "period": "quarterly",
        }
        
        aggregated = AggregatedFinancialInfo(**minimal_data)
        
        assert aggregated.ticker == "AAPL"
        assert aggregated.revenue is None
        assert aggregated.net_income is None
        assert aggregated.free_cash_flow is None
    
    def test_extra_fields_allowed(self):
        """测试动态字段支持"""
        data = {
            "ticker": "AAPL",
            "report_period": "2024-Q1", 
            "period": "quarterly",
            "custom_field": "custom_value",
            "another_metric": 123.45,
        }
        
        aggregated = AggregatedFinancialInfo(**data)
        
        assert aggregated.ticker == "AAPL"
        # 动态字段应该可以通过 model_dump 访问
        dumped = aggregated.model_dump()
        assert dumped["custom_field"] == "custom_value"
        assert dumped["another_metric"] == 123.45


class TestMergeFinancialData:
    """测试 merge_financial_data 函数"""
    
    def test_merge_with_both_metrics_and_line_items(self):
        """测试同时有财务指标和财务报表项目的合并"""
        metrics = create_sample_financial_metrics(periods=2)
        line_items = create_sample_line_items(periods=2)
        
        result = merge_financial_data(metrics, line_items)
        
        assert len(result) == 2
        assert all(isinstance(item, AggregatedFinancialInfo) for item in result)
        
        # 验证数据合并正确
        first_item = result[0]  # 应该是最新的期间
        assert first_item.ticker == "AAPL"
        assert first_item.price_to_earnings_ratio == 25.5  # 来自财务指标
        assert first_item.revenue == 110000000.0  # 来自财务报表项目
        assert first_item.net_income == 27500000.0  # 来自财务报表项目
    
    def test_merge_only_metrics(self):
        """测试只有财务指标数据的情况"""
        metrics = create_sample_financial_metrics(periods=2)
        line_items = []
        
        result = merge_financial_data(metrics, line_items)
        
        assert len(result) == 2
        assert all(isinstance(item, AggregatedFinancialInfo) for item in result)
        
        # 验证只有财务指标数据
        first_item = result[0]
        assert first_item.price_to_earnings_ratio == 25.5
        assert first_item.revenue is None  # 没有财务报表项目数据
    
    def test_merge_metrics_priority(self):
        """测试财务指标数据优先级"""
        # 创建有重叠字段的数据
        metrics = [FinancialMetrics(
            ticker="AAPL",
            report_period="2024-Q1",
            period="quarterly",
            earnings_per_share=10.0,  # 财务指标中的EPS
        )]
        
        line_items = [LineItem(
            ticker="AAPL",
            report_period="2024-Q1",
            period="quarterly",
            earnings_per_share=8.0,  # 财务报表项目中的EPS (应该被忽略)
            revenue=100000000.0,
        )]
        
        result = merge_financial_data(metrics, line_items)
        
        assert len(result) == 1
        first_item = result[0]
        
        # 财务指标数据应该优先
        assert first_item.earnings_per_share == 10.0
        # 但非重叠字段应该被补充
        assert first_item.revenue == 100000000.0
    
    def test_line_items_without_metrics(self):
        """测试没有对应财务指标的财务报表项目被跳过"""
        metrics = create_sample_financial_metrics(periods=1)  # 只有一个期间
        line_items = create_sample_line_items(periods=2)  # 有两个期间
        
        result = merge_financial_data(metrics, line_items)
        
        # 只应该返回有财务指标的期间
        assert len(result) == 1
        assert result[0].report_period == "2024-Q1"
    
    def test_empty_inputs(self):
        """测试空输入"""
        result = merge_financial_data([], [])
        assert result == []
        
        # 只有财务报表项目，没有财务指标
        line_items = create_sample_line_items(periods=1)
        result = merge_financial_data([], line_items)
        assert result == []
    
    def test_sorting_by_period(self):
        """测试按期间排序（最新的在前）"""
        # 创建不同期间的数据
        metrics = [
            FinancialMetrics(
                ticker="AAPL",
                report_period="2024-Q1",
                period="quarterly",
                earnings_per_share=5.0,
            ),
            FinancialMetrics(
                ticker="AAPL", 
                report_period="2024-Q3",
                period="quarterly",
                earnings_per_share=7.0,
            ),
            FinancialMetrics(
                ticker="AAPL",
                report_period="2024-Q2", 
                period="quarterly",
                earnings_per_share=6.0,
            ),
        ]
        
        result = merge_financial_data(metrics, [])
        
        # 应该按期间降序排列
        assert len(result) == 3
        assert result[0].report_period == "2024-Q3"
        assert result[1].report_period == "2024-Q2"  
        assert result[2].report_period == "2024-Q1"


if __name__ == "__main__":
    # 运行测试
    import sys
    import os
    
    # 添加项目根目录到路径
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    
    pytest.main([__file__, "-v"])