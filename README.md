# DolphinDB 数据获取器

这是一个用于从DolphinDB数据库获取股票数据的Python工具。

## 功能特性

- 从`.env`文件读取DolphinDB连接配置
- 支持批量获取股票数据
- 可配置的数据表和数据库
- 自动保存数据到CSV文件
- 完整的日志记录
- 支持自定义查询

## 项目结构

```
DolphinDB_data_collector/
├── main.py              # 主程序文件
├── config.py            # 配置文件
├── examples.py          # 使用示例
├── demo_structure.py    # 文件结构演示
├── .env                 # 环境变量配置
├── code/
│   └── csi300.txt      # 股票代码文件（制表符分隔）
├── data/               # 数据输出目录（按表名分类）
│   ├── stock_daily/    # 日线数据
│   ├── stock_minute/   # 分钟线数据
│   ├── stock_fundamental/ # 基本面数据
│   └── stock_financial/   # 财务数据
└── README.md           # 说明文档
```

## 安装依赖

```bash
pip install dolphindb python-dotenv pandas numpy
```

## 配置说明

### 1. 环境变量配置（.env文件）

```env
# DolphinDB 连接配置
DOLPHINDB_HOST=localhost
DOLPHINDB_PORT=8848
DOLPHINDB_USERNAME=admin
DOLPHINDB_PASSWORD=123456
```

### 2. 股票代码文件格式（code/csi300.txt）

文件采用制表符（\t）分隔，包含三列：
- 第1列：股票代码（symbol）
- 第2列：开始日期（start_date）
- 第3列：结束日期（end_date）

示例：
```
SZ000001	2005-04-08	2005-06-30
SZ000002	2005-04-08	2005-06-30
```

## 使用方法

### 基本使用

```python
from main import DolphinDBDataCollector

# 创建数据获取器实例
collector = DolphinDBDataCollector()

# 连接到DolphinDB
collector.connect()

# 加载股票代码
stock_codes = collector.load_stock_codes("code/csi300.txt")

# 批量获取数据
results = collector.get_batch_stock_data(
    stock_codes.head(10),  # 获取前10只股票
    table_name="stock_daily",
    database_name="dfs://stock"
)

# 断开连接
collector.disconnect()
```

### 获取单只股票数据

```python
data = collector.get_stock_data(
    symbol="SZ000001",
    start_date="2024.01.01", 
    end_date="2024.01.31",
    table_name="stock_daily",
    database_name="dfs://stock"
)

# 断开连接
collector.disconnect()
```

### 获取单只股票数据

```python
data = collector.get_stock_data(
    symbol="SZ000001",
    start_date="2024.01.01", 
    end_date="2024.01.31",
    table_name="stock_daily",
    database_name="dfs://stock"
)
```

### 使用配置文件

```python
from config import TABLE_CONFIGS

# 方法1: 使用预定义的日线数据配置
daily_config = TABLE_CONFIGS["daily"]

results = collector.get_batch_stock_data(
    stock_codes,
    table_name=daily_config["table_name"],
    database_name=daily_config["database_name"]
)
# 数据自动保存到 data/stock_daily/ 目录

# 方法2: 使用便捷方法
results = collector.get_data_by_table_type(stock_codes, "daily")
# 自动使用配置文件中的设置
```

### 文件保存结构

数据会按照表名自动分类保存：

```
data/
├── stock_daily/          # 日线数据
│   ├── SZ000001_2005_04_08_2005_06_30.csv
│   ├── SZ000002_2005_04_08_2005_06_30.csv
│   └── ...
├── stock_minute/         # 分钟线数据
│   ├── SZ000001_2024_01_01_2024_01_31.csv
│   └── ...
├── stock_fundamental/    # 基本面数据
│   └── ...
└── stock_financial/      # 财务数据
    └── ...
```

### 自定义查询

```python
# 执行自定义DolphinDB查询
custom_query = """
select symbol, date, close, volume 
from loadTable("dfs://stock", "stock_daily")
where symbol in ("SZ000001", "SZ000002")
and date >= 2024.01.01
"""

result = collector.session.run(custom_query)
```

## 数据表配置

在`config.py`中预定义了常用的数据表配置：

- `daily`: 日线数据表
- `minute`: 分钟线数据表
- `fundamental`: 基本面数据表
- `financial`: 财务数据表

可以根据实际的DolphinDB数据库结构修改这些配置。

## 运行示例

```bash
# 运行主程序
python main.py

# 运行示例程序
python examples.py

# 查看文件结构演示
python demo_structure.py
```

## 输出文件组织

### 自动分类保存

获取的数据会按照表名自动分类保存到`data`目录下的对应子文件夹中：

- **日线数据**: `data/stock_daily/`
- **分钟线数据**: `data/stock_minute/`
- **基本面数据**: `data/stock_fundamental/`
- **财务数据**: `data/stock_financial/`

### 文件命名规则

文件名格式：`{股票代码}_{开始日期}_{结束日期}.csv`

例如：`SZ000001_2024_01_01_2024_01_31.csv`

### 自定义保存位置

可以通过`output_dir`参数自定义根目录：

```python
results = collector.get_batch_stock_data(
    stock_codes,
    table_name="stock_daily",
    output_dir="my_data"  # 数据将保存到 my_data/stock_daily/
)
```

## 注意事项

1. **确保DolphinDB服务器正在运行**
2. **修改.env文件中的连接信息**
3. **根据实际情况修改表名和数据库名**
4. **确保有足够的磁盘空间存储输出文件**
5. **大批量数据获取时注意网络稳定性**

## 日志

程序会生成日志文件`dolphindb_collector.log`，记录：
- 连接状态
- 数据获取进度
- 错误信息
- 操作统计

## 常见问题

### 1. 连接失败
- 检查DolphinDB服务器是否启动
- 验证.env文件中的连接信息
- 确认网络连通性

### 2. 表不存在
- 检查数据库名和表名是否正确
- 使用`list_tables()`方法查看可用的表

### 3. 数据为空
- 验证股票代码格式
- 检查日期范围是否有数据
- 确认数据表中是否包含相应的股票数据

## 扩展功能

可以根据需要扩展以下功能：
- 支持更多数据类型（期货、债券等）
- 添加数据验证和清洗功能
- 支持并行数据获取
- 添加数据可视化功能
- 集成到定时任务中
