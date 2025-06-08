# 数据配置说明

## 数据提供商 (Data Providers)

### FinancialDatasets.ai
- **描述**: 美股财务数据API服务
- **支持市场**: 美股 (US), 全球 (Global)
- **支持功能**:
  - 股价数据 (prices)
  - 财务指标 (financial_metrics)  
  - 财务报表项目 (line_items)
  - 内部交易数据 (insider_trades)
  - 公司新闻 (company_news)
  - 市值数据 (market_cap)

### Tushare Pro
- **描述**: 中国A股和港股数据API服务
- **支持市场**: 
  - 中国A股 (CN) - 全功能支持
  - 香港股市 (HK) - 仅行情数据
- **支持功能**:
  - **中国A股 (CN)**: 
    - 股价数据 (prices)
    - 财务指标 (financial_metrics)
    - 财务报表项目 (line_items)
    - 内部交易数据 (insider_trades)
    - 市值数据 (market_cap)
  - **香港股市 (HK)**:
    - 股价数据 (prices)
- **限制说明**:
  - 不支持公司新闻数据
  - 股票代码格式需要转换
  - 港股仅支持行情数据，不支持财务数据
- **股票代码格式**:
  - 中国A股: `6位数字.SH/SZ` (如: 000001.SZ, 600000.SH)
  - 香港股市: `5位数字.HK` (如: 00700.HK)

## 接口说明 (Interfaces)

### LLM 调用接口

#### call_llm_deepseek
- **描述**: DeepSeek LLM responses (deepseek-reasoner, deepseek-chat)
- **支持模型**: deepseek-reasoner, deepseek-chat, deepseek-*
- **提供商**: DeepSeek
- **缓存策略**: 持久化缓存，TTL 24小时

#### call_llm_other  
- **描述**: Other LLM providers (no caching)
- **提供商**: OpenAI, Anthropic, Gemini, Groq, Ollama
- **缓存策略**: 不缓存

### 数据获取接口

#### get_prices
- **描述**: 从 financialdatasets.ai 获取股价数据
- **合并字段**: time
- **缓存策略**: 
  - 交易时间内: 1小时
  - 交易时间外: 24小时

#### get_financial_metrics
- **描述**: 从 financialdatasets.ai 获取财务指标数据
- **合并字段**: report_period
- **缓存策略**: 24小时

#### search_line_items
- **描述**: 从 financialdatasets.ai 获取财务报表项目数据
- **合并字段**: report_period
- **缓存策略**: 24小时

#### get_insider_trades
- **描述**: 从 financialdatasets.ai 获取内部交易数据
- **合并字段**: filing_date
- **缓存策略**: 24小时

#### get_company_news
- **描述**: 从 financialdatasets.ai 获取公司新闻数据
- **合并字段**: date
- **缓存策略**: 24小时

## 超时和重试配置

所有接口都配置了统一的超时和重试机制：
- **超时时间**: 
  - LLM调用: 30秒
  - 数据获取: 5秒
- **最大重试次数**: 3次
- **重试延迟系数**: 
  - LLM调用: 0.1
  - 数据获取: 0.5

## 缓存策略

系统支持多层缓存：
- **memory**: 内存缓存，最快访问
- **persistent**: 持久化缓存，跨会话保持
- **none**: 不缓存

缓存键组件和TTL时间根据不同接口进行优化配置。 