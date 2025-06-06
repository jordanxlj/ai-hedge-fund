"""
单元测试 - PersistentCache模块

测试PersistentCache类的所有功能，包括：
- 基本的get/set操作
- TTL过期机制
- 数据持久化
- 缓存管理功能
- 不同数据类型的专用方法
"""

import json
import os
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from src.data.persistent_cache import PersistentCache
from src.data.cache_config import CacheConfig, get_cache_ttl


class TestPersistentCache:
    """PersistentCache类的测试用例"""

    @pytest.fixture
    def temp_cache_dir(self):
        """创建临时缓存目录"""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir

    @pytest.fixture
    def cache(self, temp_cache_dir):
        """创建PersistentCache实例"""
        return PersistentCache(cache_dir=temp_cache_dir, default_ttl=3600)

    def test_init(self, temp_cache_dir):
        """测试初始化"""
        cache = PersistentCache(cache_dir=temp_cache_dir, default_ttl=1800)
        
        assert cache.cache_dir == Path(temp_cache_dir)
        assert cache.default_ttl == 1800
        assert cache.cache_dir.exists()
        assert isinstance(cache._cache_metadata, dict)

    def test_cache_key_generation(self, cache):
        """测试缓存键生成"""
        key1 = cache._get_cache_key("test", ticker="AAPL", date="2023-01-01")
        key2 = cache._get_cache_key("test", date="2023-01-01", ticker="AAPL")  # 参数顺序不同
        key3 = cache._get_cache_key("test", ticker="MSFT", date="2023-01-01")
        
        # 相同参数应该生成相同的键
        assert key1 == key2
        # 不同参数应该生成不同的键
        assert key1 != key3
        # 键应该是32字符的MD5哈希
        assert len(key1) == 32
        assert all(c in '0123456789abcdef' for c in key1)

    def test_basic_get_set(self, cache):
        """测试基本的get/set操作"""
        # 测试空缓存
        result = cache.get("test_type", ticker="AAPL")
        assert result is None
        
        # 设置数据
        test_data = [{"ticker": "AAPL", "price": 150.0, "time": "2023-01-01"}]
        cache.set("test_type", test_data, ticker="AAPL")
        
        # 获取数据
        result = cache.get("test_type", ticker="AAPL")
        assert result == test_data

    def test_data_persistence(self, temp_cache_dir):
        """测试数据持久化"""
        # 第一个缓存实例
        cache1 = PersistentCache(cache_dir=temp_cache_dir)
        test_data = [{"ticker": "AAPL", "price": 150.0}]
        cache1.set("prices", test_data, ticker="AAPL")
        
        # 创建新的缓存实例（模拟重启）
        cache2 = PersistentCache(cache_dir=temp_cache_dir)
        result = cache2.get("prices", ticker="AAPL")
        
        assert result == test_data

    def test_ttl_expiration(self, cache):
        """测试TTL过期机制"""
        # 设置短TTL的数据
        test_data = [{"ticker": "AAPL", "price": 150.0}]
        cache.set("test_type", test_data, ttl=1, ticker="AAPL")  # 1秒TTL
        
        # 立即获取应该成功
        result = cache.get("test_type", ticker="AAPL")
        assert result == test_data
        
        # 等待过期
        time.sleep(1.1)
        
        # 过期后应该返回None
        result = cache.get("test_type", ticker="AAPL")
        assert result is None

    def test_data_merging(self, cache):
        """测试数据合并功能"""
        # 第一批数据
        data1 = [
            {"time": "2023-01-01", "price": 150.0},
            {"time": "2023-01-02", "price": 155.0}
        ]
        cache.set("prices", data1, merge_key="time", ticker="AAPL")
        
        # 第二批数据（有重复和新数据）
        data2 = [
            {"time": "2023-01-02", "price": 155.0},  # 重复
            {"time": "2023-01-03", "price": 160.0}   # 新数据
        ]
        cache.set("prices", data2, merge_key="time", ticker="AAPL")
        
        # 检查合并结果
        result = cache.get("prices", ticker="AAPL")
        assert len(result) == 3  # 应该有3条记录（去重后）
        
        # 检查数据内容
        times = [item["time"] for item in result]
        assert "2023-01-01" in times
        assert "2023-01-02" in times
        assert "2023-01-03" in times

    def test_file_operations(self, cache):
        """测试文件操作"""
        test_data = [{"ticker": "AAPL", "price": 150.0}]
        cache_key = cache._get_cache_key("test", ticker="AAPL")
        
        # 保存到磁盘
        cache._save_to_disk(cache_key, test_data, ttl=3600)
        
        # 检查文件是否创建
        cache_file = cache._get_cache_file_path(cache_key)
        assert cache_file.exists()
        
        # 从磁盘加载
        loaded_data = cache._load_from_disk(cache_key)
        assert loaded_data == test_data
        
        # 检查元数据
        assert cache_key in cache._cache_metadata
        metadata = cache._cache_metadata[cache_key]
        assert metadata["ttl"] == 3600
        assert metadata["size"] == 1

    def test_corrupted_file_handling(self, cache):
        """测试损坏文件处理"""
        cache_key = cache._get_cache_key("test", ticker="AAPL")
        cache_file = cache._get_cache_file_path(cache_key)
        
        # 创建损坏的JSON文件
        cache_file.write_text("invalid json content")
        
        # 尝试加载应该返回None并删除损坏文件
        result = cache._load_from_disk(cache_key)
        assert result is None
        assert not cache_file.exists()

    def test_prices_methods(self, cache):
        """测试价格数据专用方法"""
        test_data = [
            {"ticker": "AAPL", "time": "2023-01-01", "price": 150.0},
            {"ticker": "AAPL", "time": "2023-01-02", "price": 155.0}
        ]
        
        # 设置价格数据
        cache.set_prices("AAPL", "2023-01-01", "2023-01-02", test_data)
        
        # 获取价格数据
        result = cache.get_prices("AAPL", "2023-01-01", "2023-01-02")
        assert result == test_data

    @patch('src.data.cache_config.datetime')
    def test_prices_ttl_market_hours(self, mock_datetime, cache):
        """测试价格数据在市场时间的TTL"""
        # 模拟市场时间（上午10点）
        mock_datetime.now.return_value.hour = 10
        
        test_data = [{"ticker": "AAPL", "time": "2023-01-01", "price": 150.0}]
        cache.set_prices("AAPL", "2023-01-01", "2023-01-01", test_data)
        
        # 检查TTL是否为市场时间的配置值
        expected_ttl = get_cache_ttl('prices')  # 从配置获取期望的TTL
        cache_key = cache._get_cache_key("prices", ticker="AAPL", start_date="2023-01-01", end_date="2023-01-01")
        metadata = cache._cache_metadata[cache_key]
        assert metadata["ttl"] == expected_ttl

    @patch('src.data.cache_config.datetime')
    def test_prices_ttl_after_market(self, mock_datetime, cache):
        """测试价格数据在非市场时间的TTL"""
        # 模拟非市场时间（晚上8点）
        mock_datetime.now.return_value.hour = 20
        
        test_data = [{"ticker": "AAPL", "time": "2023-01-01", "price": 150.0}]
        cache.set_prices("AAPL", "2023-01-01", "2023-01-01", test_data)
        
        # 检查TTL是否为非市场时间的配置值
        expected_ttl = get_cache_ttl('prices')  # 从配置获取期望的TTL
        cache_key = cache._get_cache_key("prices", ticker="AAPL", start_date="2023-01-01", end_date="2023-01-01")
        metadata = cache._cache_metadata[cache_key]
        assert metadata["ttl"] == expected_ttl

    def test_financial_metrics_methods(self, cache):
        """测试财务指标专用方法"""
        test_data = [
            {"ticker": "AAPL", "report_period": "2023-Q1", "revenue": 1000000},
            {"ticker": "AAPL", "report_period": "2023-Q2", "revenue": 1100000}
        ]
        
        cache.set_financial_metrics("AAPL", "ttm", "2023-06-30", 10, test_data)
        result = cache.get_financial_metrics("AAPL", "ttm", "2023-06-30", 10)
        
        assert result == test_data

    def test_line_items_methods(self, cache):
        """测试财务条目专用方法"""
        line_items = ["revenue", "net_income"]
        test_data = [
            {"ticker": "AAPL", "report_period": "2023-Q1", "line_item": "revenue", "value": 1000000},
            {"ticker": "AAPL", "report_period": "2023-Q1", "line_item": "net_income", "value": 200000}
        ]
        
        cache.set_line_items("AAPL", line_items, "ttm", "2023-06-30", 10, test_data)
        result = cache.get_line_items("AAPL", line_items, "ttm", "2023-06-30", 10)
        
        assert result == test_data

    def test_line_items_consistent_cache_key(self, cache):
        """测试财务条目缓存键的一致性"""
        line_items1 = ["revenue", "net_income"]
        line_items2 = ["net_income", "revenue"]  # 不同顺序
        
        test_data = [{"ticker": "AAPL", "report_period": "2023-Q1", "value": 1000000}]
        
        # 使用第一个顺序设置数据
        cache.set_line_items("AAPL", line_items1, "ttm", "2023-06-30", 10, test_data)
        
        # 使用第二个顺序获取数据（应该能获取到）
        result = cache.get_line_items("AAPL", line_items2, "ttm", "2023-06-30", 10)
        assert result == test_data

    def test_insider_trades_methods(self, cache):
        """测试内部人交易专用方法"""
        test_data = [
            {"ticker": "AAPL", "filing_date": "2023-01-01", "transaction_shares": 1000},
            {"ticker": "AAPL", "filing_date": "2023-01-02", "transaction_shares": -500}
        ]
        
        cache.set_insider_trades("AAPL", "2023-01-01", "2023-01-02", 100, test_data)
        result = cache.get_insider_trades("AAPL", "2023-01-01", "2023-01-02", 100)
        
        assert result == test_data

    def test_company_news_methods(self, cache):
        """测试公司新闻专用方法"""
        test_data = [
            {"ticker": "AAPL", "date": "2023-01-01", "title": "Apple announces new product"},
            {"ticker": "AAPL", "date": "2023-01-02", "title": "Apple reports earnings"}
        ]
        
        cache.set_company_news("AAPL", "2023-01-01", "2023-01-02", 100, test_data)
        result = cache.get_company_news("AAPL", "2023-01-01", "2023-01-02", 100)
        
        assert result == test_data

    def test_clear_expired(self, cache):
        """测试清除过期缓存"""
        # 添加一些数据，其中一些已过期
        current_time = time.time()
        
        # 未过期的数据
        cache.set("valid", [{"data": "valid"}], ttl=3600, ticker="AAPL")
        
        # 手动创建过期的数据
        cache_key = cache._get_cache_key("expired", ticker="MSFT")
        cache._cache_metadata[cache_key] = {
            'created_at': current_time - 7200,  # 2小时前创建
            'expires_at': current_time - 3600,  # 1小时前过期
            'ttl': 3600,
            'size': 1
        }
        cache_file = cache._get_cache_file_path(cache_key)
        cache_file.write_text(json.dumps([{"data": "expired"}]))
        
        # 清除过期缓存
        expired_count = cache.clear_expired()
        
        assert expired_count == 1
        assert not cache_file.exists()
        assert cache_key not in cache._cache_metadata
        
        # 未过期的数据应该仍然存在
        result = cache.get("valid", ticker="AAPL")
        assert result == [{"data": "valid"}]

    def test_get_cache_stats(self, cache):
        """测试缓存统计"""
        # 添加一些数据
        cache.set_prices("AAPL", "2023-01-01", "2023-01-01", [{"price": 150}])
        cache.set_financial_metrics("AAPL", "ttm", "2023-06-30", 10, [{"revenue": 1000}])
        
        stats = cache.get_cache_stats()
        
        assert "total_entries" in stats
        assert "expired_entries" in stats
        assert "active_entries" in stats
        assert "total_cached_items" in stats
        assert "total_file_size_bytes" in stats
        assert "total_file_size_mb" in stats
        assert "cache_dir" in stats
        
        assert stats["total_entries"] == 2
        assert stats["expired_entries"] == 0
        assert stats["active_entries"] == 2
        assert stats["total_cached_items"] == 2

    def test_force_refresh_ticker(self, cache):
        """测试强制刷新特定股票缓存"""
        # 添加多个股票的数据
        aapl_data = [{"ticker": "AAPL", "price": 150}]
        msft_data = [{"ticker": "MSFT", "price": 300}]
        
        cache.set_prices("AAPL", "2023-01-01", "2023-01-01", aapl_data)
        cache.set_prices("MSFT", "2023-01-01", "2023-01-01", msft_data)
        
        # 强制刷新AAPL的缓存
        removed_count = cache.force_refresh_ticker("AAPL")
        
        assert removed_count == 1
        
        # AAPL的数据应该被清除
        result = cache.get_prices("AAPL", "2023-01-01", "2023-01-01")
        assert result is None
        
        # MSFT的数据应该仍然存在
        result = cache.get_prices("MSFT", "2023-01-01", "2023-01-01")
        assert result == msft_data

    def test_clear_all(self, cache):
        """测试清除所有缓存"""
        # 添加一些数据
        cache.set_prices("AAPL", "2023-01-01", "2023-01-01", [{"price": 150}])
        cache.set_financial_metrics("AAPL", "ttm", "2023-06-30", 10, [{"revenue": 1000}])
        
        # 确认数据存在
        assert cache.get_prices("AAPL", "2023-01-01", "2023-01-01") is not None
        assert len(cache._cache_metadata) > 0
        
        # 清除所有缓存
        cache.clear_all()
        
        # 确认所有数据被清除
        assert cache.get_prices("AAPL", "2023-01-01", "2023-01-01") is None
        assert len(cache._cache_metadata) == 0

    def test_metadata_persistence(self, temp_cache_dir):
        """测试元数据持久化"""
        cache1 = PersistentCache(cache_dir=temp_cache_dir)
        
        # 添加数据
        test_data = [{"ticker": "AAPL", "price": 150}]
        cache1.set("prices", test_data, ttl=7200, ticker="AAPL")
        
        # 创建新实例
        cache2 = PersistentCache(cache_dir=temp_cache_dir)
        
        # 检查元数据是否正确加载
        cache_key = cache1._get_cache_key("prices", ticker="AAPL")
        assert cache_key in cache2._cache_metadata
        assert cache2._cache_metadata[cache_key]["ttl"] == 7200

    def test_error_handling_metadata_load(self, temp_cache_dir):
        """测试元数据加载错误处理"""
        # 创建损坏的元数据文件
        metadata_file = Path(temp_cache_dir) / "metadata.json"
        metadata_file.write_text("invalid json")
        
        # 应该能正常创建缓存实例
        cache = PersistentCache(cache_dir=temp_cache_dir)
        assert isinstance(cache._cache_metadata, dict)
        assert len(cache._cache_metadata) == 0

    def test_error_handling_metadata_save(self, cache, monkeypatch):
        """测试元数据保存错误处理"""
        # 模拟文件写入错误
        def mock_open(*args, **kwargs):
            raise IOError("Permission denied")
        
        monkeypatch.setattr("builtins.open", mock_open)
        
        # 应该不会抛出异常，只是打印警告
        cache._save_metadata()  # 应该正常完成

    def test_various_ttl_values(self, cache):
        """测试各种TTL值"""
        # 测试默认TTL
        cache.set("test1", [{"data": 1}], ticker="TEST1")
        cache_key = cache._get_cache_key("test1", ticker="TEST1")
        assert cache._cache_metadata[cache_key]["ttl"] == cache.default_ttl
        
        # 测试自定义TTL
        cache.set("test2", [{"data": 2}], ttl=1800, ticker="TEST2")
        cache_key = cache._get_cache_key("test2", ticker="TEST2")
        assert cache._cache_metadata[cache_key]["ttl"] == 1800

    def test_empty_data_handling(self, cache):
        """测试空数据处理"""
        # 设置空数据
        cache.set("empty", [], ticker="TEST")
        
        # 应该能正常获取空列表
        result = cache.get("empty", ticker="TEST")
        assert result == []

    def test_large_data_handling(self, cache):
        """测试大数据处理"""
        # 创建大数据集
        large_data = [{"id": i, "value": f"data_{i}"} for i in range(1000)]
        
        cache.set("large", large_data, ticker="TEST")
        result = cache.get("large", ticker="TEST")
        
        assert len(result) == 1000
        assert result[0]["id"] == 0
        assert result[-1]["id"] == 999


class TestCacheConfig:
    """测试YAML配置功能"""
    
    @pytest.fixture
    def temp_config_dir(self):
        """创建临时配置目录"""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir
    
    def test_yaml_config_creation(self, temp_config_dir):
        """测试YAML配置文件创建"""
        config_file = Path(temp_config_dir) / "cache_config.yaml"
        config = CacheConfig(config_file=str(config_file))
        
        # 检查配置文件是否创建
        assert config_file.exists()
        
        # 检查是否为有效的YAML内容
        import yaml
        with open(config_file, 'r', encoding='utf-8') as f:
            loaded_config = yaml.safe_load(f)
        
        assert isinstance(loaded_config, dict)
        # 现在配置结构是 interfaces > interface_name > properties
        assert 'interfaces' in loaded_config
        assert 'agent_models' in loaded_config
        
        # 检查接口配置
        interfaces = loaded_config['interfaces']
        assert 'get_prices' in interfaces
        assert 'call_llm_deepseek' in interfaces
        
        # 检查接口有TTL配置
        assert 'ttl' in interfaces['get_prices']
        assert 'ttl' in interfaces['call_llm_deepseek']
    
    def test_ttl_retrieval(self):
        """测试TTL值获取"""
        # 测试各种缓存类型的TTL
        ttl_prices = get_cache_ttl('prices')
        ttl_financial = get_cache_ttl('financial_metrics')
        ttl_llm = get_cache_ttl('llm_responses')
        
        assert isinstance(ttl_prices, int)
        assert isinstance(ttl_financial, int)
        assert isinstance(ttl_llm, int)
        assert ttl_prices > 0
        assert ttl_financial > 0
        assert ttl_llm > 0
    
    def test_unknown_cache_type(self):
        """测试未知缓存类型的处理"""
        ttl = get_cache_ttl('unknown_type')
        assert ttl == 3600  # 默认1小时


class TestGlobalFunctions:
    """测试全局函数"""

    def test_global_functions(self):
        """测试全局缓存函数"""
        from src.data.persistent_cache import get_persistent_cache, clear_persistent_cache, get_persistent_cache_stats
        
        # 测试获取全局缓存实例
        cache = get_persistent_cache()
        assert isinstance(cache, PersistentCache)
        
        # 测试统计函数
        stats = get_persistent_cache_stats()
        assert isinstance(stats, dict)
        
        # 测试清除过期缓存函数
        result = clear_persistent_cache()
        assert isinstance(result, int)


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 