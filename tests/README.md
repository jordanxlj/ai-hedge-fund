# 测试说明

本目录包含了项目的单元测试，主要使用 pytest 框架。

## 文件结构

```
tests/
├── __init__.py              # 测试包初始化文件
├── conftest.py              # pytest配置文件
├── README.md                # 本说明文件
├── run_tests.py             # 测试运行器脚本
├── test_example.py          # 示例测试（验证框架）
└── test_persistent_cache.py # PersistentCache模块测试
```

## 运行测试

### 方法1：使用pytest直接运行

```bash
# 运行所有测试
python -m pytest tests/

# 运行特定测试文件
python -m pytest tests/test_persistent_cache.py

# 详细输出
python -m pytest tests/ -v

# 运行特定测试方法
python -m pytest tests/test_persistent_cache.py::TestPersistentCache::test_basic_get_set
```

### 方法2：使用测试运行器脚本

```bash
# 运行所有测试
python tests/run_tests.py

# 运行persistent相关测试
python tests/run_tests.py test_persistent

# 详细输出
python tests/run_tests.py -v

# 运行示例测试
python tests/run_tests.py test_example
```

## 测试内容

### PersistentCache 测试 (`test_persistent_cache.py`)

测试覆盖以下功能：

#### 基本功能
- ✅ 缓存初始化
- ✅ 缓存键生成
- ✅ 基本的get/set操作
- ✅ 数据持久化（重启后数据保留）
- ✅ TTL过期机制

#### 数据操作
- ✅ 数据合并功能
- ✅ 文件读写操作
- ✅ 损坏文件处理
- ✅ 空数据和大数据处理

#### 专用方法测试
- ✅ `get_prices` / `set_prices` - 股价数据
- ✅ `get_financial_metrics` / `set_financial_metrics` - 财务指标
- ✅ `get_line_items` / `set_line_items` - 财务条目
- ✅ `get_insider_trades` / `set_insider_trades` - 内部人交易
- ✅ `get_company_news` / `set_company_news` - 公司新闻

#### TTL策略测试
- ✅ 股价数据在市场时间（1小时TTL）
- ✅ 股价数据在非市场时间（24小时TTL）
- ✅ 其他数据类型的TTL设置

#### 缓存管理
- ✅ 清除过期缓存
- ✅ 缓存统计信息
- ✅ 强制刷新特定股票
- ✅ 清除所有缓存

#### 错误处理
- ✅ 元数据文件损坏处理
- ✅ 缓存文件损坏处理
- ✅ 文件权限错误处理

#### 全局函数测试
- ✅ `get_persistent_cache()` - 获取全局实例
- ✅ `clear_persistent_cache()` - 清除过期缓存
- ✅ `get_persistent_cache_stats()` - 获取统计信息

### 示例测试 (`test_example.py`)

验证测试框架是否正常工作的基本测试：
- ✅ 基本断言
- ✅ 数据结构操作
- ✅ 类方法测试
- ✅ Fixture使用
- ✅ 参数化测试

## 测试配置

### pytest 配置 (`conftest.py`)
- 自动添加src目录到Python路径
- 确保能正确导入项目模块

### 测试运行器 (`run_tests.py`)
- 简化测试运行命令
- 支持模式匹配
- 预设常用选项

## 覆盖率报告

如需生成测试覆盖率报告，请安装 pytest-cov：

```bash
pip install pytest-cov

# 生成覆盖率报告
python -m pytest tests/ --cov=src/data/persistent_cache --cov-report=html

# 查看覆盖率
open htmlcov/index.html
```

## 注意事项

1. **临时文件**: 测试使用临时目录，测试结束后自动清理
2. **时间敏感**: 某些TTL测试包含 `time.sleep()`，可能影响运行速度
3. **Mock使用**: 时间相关的测试使用Mock来避免实际时间依赖
4. **错误处理**: 测试包含各种错误场景以确保健壮性

## 持续集成

建议在CI/CD流程中运行以下命令：

```bash
# 安装测试依赖
pip install pytest pytest-cov

# 运行测试并生成覆盖率报告
python -m pytest tests/ --cov=src --cov-report=xml --cov-report=term-missing

# 检查测试是否通过
echo "测试完成，退出码: $?"
```