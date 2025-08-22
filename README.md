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
```markdown
# DolphinDB 数据获取器

这是一个用于从 DolphinDB 获取股票数据并保存为 CSV 的 Python 工具，包含对 Wind 风格数据到 qlib 风格的基础标准化功能（例如日期解析、列映射、停牌处理和复权字段对齐）。

## 主要功能

- 从 `.env` 读取 DolphinDB 连接配置
- 批量/单只股票数据拉取并保存为 CSV
- 支持自定义查询和表配置
- 可选的 Wind -> qlib 简易标准化（见 `normalize.py`）
- 将结果按表/类型分类保存到 `data/` 目录

## 项目结构（简要）

```
DolphinDB_data_collector/
├── main.py              # 主程序（示例化连接、批量流程）
├── normalize.py         # Wind -> qlib 简易标准化逻辑
├── code/                # 输入的股票代码文件（制表符分隔）
│   └── csi300.txt
├── data/                # 输出目录（CSV 文件）
└── README.md
```

## 快速安装

建议在虚拟环境中安装依赖：

```bash
pip install dolphindb python-dotenv pandas numpy
```

（如果你的环境已经有这些包，可跳过安装步骤。）

## 配置（.env）

在仓库根目录创建 `.env`，示例：

```env
DOLPHINDB_HOST=localhost
DOLPHINDB_PORT=8848
DOLPHINDB_USERNAME=admin
DOLPHINDB_PASSWORD=123456
```

## 股票代码文件格式

将要批量拉取的股票列表放在 `code/csi300.txt`，文件为制表符分隔（TSV），每行三列：

- symbol（例如 `SZ000001`）
- start_date（格式 YYYY-MM-DD 或 YYYY.MM.DD）
- end_date（格式 YYYY-MM-DD 或 YYYY.MM.DD）

示例：

```
SZ000001	2005-04-08	2005-06-30
SZ000002	2005-04-08	2005-06-30
```

说明：脚本会把 Wind 风格代码（`SZ000001`）按需要转换为 `000001.SZ` 用于 DolphinDB 查询。

## 使用示例（Python）

下面示例假设 `main.py` 提供了一个简单的流程函数或类（项目里可能使用不同命名，若不一致请按实际代码调整）。

```python
from main import DolphinDBDataCollector

collector = DolphinDBDataCollector()
collector.connect()

# 从文件加载代码并获取前 10 支股票的数据
codes = collector.load_stock_codes('code/csi300.txt')
results = collector.get_batch_stock_data(codes.head(10), table_name='AShareEODPrices')

# 断开连接
collector.disconnect()
```

如果你的 `main.py` 采用的是函数式接口，也可以直接使用类似 `fetch_and_save_data(host, port, symbol, start, end)` 的函数（以项目中的实际函数签名为准）。

## 输出组织

默认把结果保存到 `data/` 下，按表或数据类型分子目录，例如：

- `data/stock_daily/`（日线）
- `data/stock_minute/`（分钟线）

文件命名示例：`SZ0000010.csv`

## 常见问题与提示

- 连接失败：确认 DolphinDB 服务已启动并且 `.env` 中配置正确的 host/port。可以用 ping 或 telnet 测试端口连通性。
- 日期比较：DolphinDB 的时间分区字段通常需要与日期类型比较（例如使用 `date(2005-04-08)`），请注意查询里日期的格式和类型。
- 空数据：确认表名、字段名与数据库中实际一致；Wind 风格字段名与表结构可能有所差异，请按实际表结构调整查询字段。

## 开发与扩展建议

- 如果要做大规模并发拉取，建议加上重试与并发控制（例如 multiprocessing / asyncio + 限速）。
- 标准化：当前 `normalize.py` 提供基础功能，可按需扩展复权、对齐交易日历或生成训练所需特征。

```

