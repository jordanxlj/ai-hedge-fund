"""
LineItem辅助函数
用于从LineItem列表中提取特定line_item的值
"""

from typing import List, Optional
from src.data.models import LineItem

def get_line_item_value(line_items: List[LineItem], line_item_name: str, report_period: Optional[str] = None) -> Optional[float]:
    """
    从LineItem列表中获取指定line_item的值
    
    Args:
        line_items: LineItem对象列表
        line_item_name: 要查找的line_item名称
        report_period: 可选的报告期过滤条件
        
    Returns:
        找到的值，如果没找到返回None
    """
    for item in line_items:
        if item.line_item == line_item_name:
            # 如果指定了report_period，需要匹配
            if report_period is None or item.report_period == report_period:
                return item.value
    return None

def create_line_item_accessor(line_items: List[LineItem]):
    """
    创建一个动态访问器对象，可以通过属性名访问line_item值
    
    Args:
        line_items: LineItem对象列表
        
    Returns:
        具有动态属性的访问器对象
    """
    class LineItemAccessor:
        def __init__(self, items: List[LineItem]):
            self._items = items
            # 创建一个字典映射line_item名称到值
            self._values = {}
            for item in items:
                # 标准化字段名（转换为Python属性名格式）
                attr_name = item.line_item.replace(' ', '_').replace('-', '_').lower()
                self._values[attr_name] = item.value
                
                # 同时保存原始名称
                self._values[item.line_item] = item.value
        
        def __getattr__(self, name: str) -> Optional[float]:
            # 首先尝试直接匹配
            if name in self._values:
                return self._values[name]
            
            # 尝试常见的别名映射
            aliases = {
                'working_capital': ['working_capital', 'working capital'],
                'net_income': ['net_income', 'net income'],
                'depreciation_and_amortization': ['depreciation_and_amortization', 'depreciation and amortization'],
                'capital_expenditure': ['capital_expenditure', 'capital expenditure', 'capex'],
                'free_cash_flow': ['free_cash_flow', 'free cash flow'],
            }
            
            if name in aliases:
                for alias in aliases[name]:
                    if alias in self._values:
                        return self._values[alias]
            
            # 如果找不到，返回None或抛出AttributeError
            return None
        
        def get(self, name: str, default: Optional[float] = None) -> Optional[float]:
            """安全获取值，如果不存在返回默认值"""
            try:
                value = self.__getattr__(name)
                return value if value is not None else default
            except:
                return default
    
    return LineItemAccessor(line_items)

def extract_line_item_values(line_items: List[LineItem]) -> dict:
    """
    提取所有line_item值到字典中
    
    Args:
        line_items: LineItem对象列表
        
    Returns:
        包含所有line_item值的字典
    """
    result = {}
    for item in line_items:
        result[item.line_item] = item.value
        # 同时添加标准化的字段名
        attr_name = item.line_item.replace(' ', '_').replace('-', '_').lower()
        result[attr_name] = item.value
    
    return result 