# DolphinDB 数据获取器

这是一个用于从DolphinDB数据库获取股票数据的Python工具。

## 项目结构

```
DolphinDB_data_collector/
├── main.py                    # 主程序入口和批量处理逻辑
├── normalize.py               # Wind -> qlib 数据标准化处理器
├── logger.py                  # 专业日志记录系统
├── CLI.py                     # 命令行接口（开发中）
├── index_fetch.py             # 指数数据获取（待开发）
├── calender.py                # 交易日历相关功能
├── requirements.txt           # 项目依赖包
├── .env                       # 环境变量配置文件
├── code/
│   └── csi300.txt            # CSI300股票代码文件（制表符分隔）
├── data/                      # 数据输出目录
├── log/                       # 日志文件目录
└── calendar/
    └── day.txt               # 交易日历文件
```

## 快速开始

### 1. 环境准备

建议使用虚拟环境：

```bash
# 创建虚拟环境
python -m venv venv

# 激活虚拟环境
# macOS/Linux:
source venv/bin/activate
# Windows:
# venv\Scripts\activate

# 安装依赖
pip install -r requirements.txt
```

### 2. 配置数据库连接

在项目根目录创建 `.env` 文件：

```env
DOLPHINDB_HOST=localhost
DOLPHINDB_PORT=8848
DOLPHINDB_USERNAME=admin
DOLPHINDB_PASSWORD=123456
```

### 3. 准备股票代码文件

在 `code/csi300.txt` 中添加要获取的股票代码，格式为制表符分隔的三列：

```
SZ000001	2005-04-08	2005-06-30
SZ000002	2005-04-08	2005-06-30
SH600000	2005-04-08	2005-06-30
```

**注意**: 程序当前使用固定的数据时间范围（2008-01-01 到 2025-08-01），文件中的日期会被自动覆盖。

### 4. 运行程序

```bash
python main.py
```

## 数据标准化功能

`normalize.py` 模块提供了完整的 Wind 数据到 qlib 格式的转换功能：

### 字段映射
- `TRADE_DT` → `date`
- `S_DQ_OPEN` → `open`
- `S_DQ_HIGH` → `high`
- `S_DQ_LOW` → `low`
- `S_DQ_CLOSE` → `close`
- `S_DQ_VOLUME` → `volume`
- `S_DQ_AMOUNT` → `amount`
- `S_DQ_ADJCLOSE` → `adjclose`


## 输出文件组织

数据按以下结构保存到 `data/` 目录：

```
data/
├── SZ000001.csv
├── SZ000002.csv
├── SH600000.csv
└── ...
```

每个 CSV 文件包含标准化后的股票数据，列名符合 qlib 格式要求。

## 日志系统
自动生成以下日志文件：

```
log/
├── data_collector_YYYYMMDD_HHMMSS.log    # 主日志文件
├── errors_YYYYMMDD_HHMMSS.log            # 错误日志
└── failed_codes_YYYYMMDD_HHMMSS.txt      # 失败的股票代码列表
```

## API 使用示例

### 单只股票数据获取

```python
from main import fetch_and_save_data
import os
from dotenv import load_dotenv

load_dotenv()
host = os.getenv("DOLPHINDB_HOST")
port = int(os.getenv("DOLPHINDB_PORT"))
user = os.getenv("DOLPHINDB_USERNAME")
password = os.getenv("DOLPHINDB_PASSWORD")

# 获取单只股票数据
data = fetch_and_save_data(
    host, port, user, password,
    'dfs://WIND_AShareEODPrices',  # 数据库路径
    'AShareEODPrices',             # 表名
    '000001.SZ',                   # 股票代码
    '2020-01-01',                  # 开始日期
    '2023-12-31',                  # 结束日期
    'SZ000001',                    # 原始代码
    normalize_data=True            # 启用标准化
)
```

### 自定义标准化处理

```python
from normalize import WindNormalize1d
import pandas as pd

# 创建标准化处理器
normalizer = WindNormalize1d()

# 标准化数据
raw_data = pd.read_csv("raw_data.csv")
normalized_data = normalizer(raw_data)
```

## 依赖包

项目主要依赖：

- `dolphindb>=1.30.22` - DolphinDB Python API
- `python-dotenv>=1.0.0` - 环境变量管理
- `pandas>=2.0.0` - 数据处理
- `numpy>=1.24.0` - 数值计算

### TODO 功能
1. 完成日历，index指数的提取
2. 完成CLI，支持命令行参数

## 故障排除

### 常见问题

1. **连接失败**
   - 确认 DolphinDB 服务已启动
   - 检查 `.env` 文件中的连接配置
   - 使用 `telnet host port` 测试网络连通性

2. **数据为空**
   - 检查股票代码格式是否正确
   - 确认数据库中存在指定时间范围的数据
   - 查看日志文件获取详细错误信息

3. **标准化失败**
   - 检查原始数据的列名是否符合预期
   - 确认数据格式（特别是日期格式）
   - 可以设置 `normalize_data=False` 保存原始数据

### 日志分析

查看 `log/` 目录下的日志文件：
- `data_collector_*.log` - 主要处理流程
- `errors_*.log` - 错误详情
- `failed_codes_*.txt` - 处理失败的股票代码

