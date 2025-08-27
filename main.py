from dotenv import load_dotenv
from dolphindb import session
import os
import csv
import pandas as pd
from typing import List, Dict
from normalize import WindNormalize1d


def read_codes(file_path: str, n: int = 10) -> List[Dict[str, str]]:
    """读取以制表符分隔的代码表，返回前 n 条记录。

    每行预期至少有3列：symbol, start_date, end_date。忽略空行和列数不足的行。
    返回的每条记录为 dict: {"symbol":..., "start_date":..., "end_date":...}
    """
    results: List[Dict[str, str]] = []
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"文件不存在: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter='\t')
        for row in reader:
            # 跳过空行或注释行
            if not row:
                continue
            # 有些行可能包含额外的空白或注释，至少需要3列
            if len(row) < 3:
                continue
            symbol = row[0].strip()
            start_date = row[1].strip()
            end_date = row[2].strip()
            # 跳过表头（如果存在）
            if symbol.lower() in ("symbol", "代码", "ticker"):
                continue
            # 重构 symbol: e.g. 'SZ000001' -> '000001.SZ', 'SH600000' -> '600000.SH'
            if "." in symbol:
                symbol_fmt = symbol
            else:
                prefix = symbol[:2].upper() if len(symbol) >= 2 else ""
                rest = symbol[2:] if len(symbol) > 2 else symbol
                if prefix in ("SZ", "SH") and rest:
                    symbol_fmt = f"{rest}.{prefix}"
                else:
                    symbol_fmt = symbol

            results.append({"symbol": symbol_fmt, "start_date": start_date, "end_date": end_date, "original_symbol": symbol})
            if len(results) >= n:
                break
    return results


def fetch_and_save_data(host, port, user, password, db_path, db_table, symbol, start_date, end_date, original_symbol, data_dir="data", normalize_data=True):
    """从 DolphinDB 获取指定股票的 AShareEODPrices 数据，进行标准化并保存到文件
    
    Args:
        host, port, user, password: DolphinDB 连接参数
        db_path, db_table: 数据库路径和表名 (AShareEODPrices)
        symbol: 重构后的股票代码 (如 000001.SZ)
        start_date, end_date: 数据时间范围 (格式: YYYY-MM-DD)
        original_symbol: 原始股票代码 (如 SZ000001)，用作文件名
        data_dir: 保存数据的目录
        normalize_data: 是否对数据进行标准化处理
        
    AShareEODPrices 表字段:
        S_INFO_WINDCODE (Wind代码), TRADE_DT (交易日期),
        S_DQ_OPEN (开盘价), S_DQ_HIGH (最高价), 
        S_DQ_LOW (最低价), S_DQ_CLOSE (收盘价),
        S_DQ_PCTCHANGE (涨跌幅), S_DQ_VOLUME (成交量), S_DQ_AMOUNT (成交金额),
        S_DQ_ADJOPEN (复权开盘价), S_DQ_ADJHIGH (复权最高价),
        S_DQ_ADJLOW (复权最低价), S_DQ_ADJCLOSE (复权收盘价), S_DQ_ADJFACTOR (复权因子),
        S_DQ_AVGPRICE (均价VWAP)
    """
    s = session()
    try:
        s.connect(host, port, user, password)
        
        # 将日期格式从 YYYY-MM-DD 转换为 DolphinDB DATE 格式
        # DolphinDB 中日期需要使用 date() 函数或特定格式
        start_date_fmt = start_date.replace('-', '.')
        end_date_fmt = end_date.replace('-', '.')
        
        # 构造查询脚本 - 根据 AShareEODPrices 表结构
        script = f'''
        db = loadTable("{db_path}", "{db_table}")
        SELECT S_INFO_WINDCODE, TRADE_DT, S_DQ_OPEN, S_DQ_HIGH, S_DQ_LOW, S_DQ_CLOSE,
               S_DQ_VOLUME, S_DQ_AMOUNT, S_DQ_ADJCLOSE
        FROM db
        WHERE S_INFO_WINDCODE = "{symbol}"
          AND TRADE_DT >= date({start_date_fmt})
          AND TRADE_DT <= date({end_date_fmt})
        ORDER BY TRADE_DT
        '''
        
        print(f"正在获取 {symbol} ({original_symbol}) 从 {start_date} 到 {end_date} 的数据...")
        print(f"查询脚本: {script}")
        
        raw_data = s.run(script)
        
        if raw_data is not None and len(raw_data) > 0:
            print(f"从DolphinDB获取到 {len(raw_data)} 条原始记录")
            print(f"原始数据列: {list(raw_data.columns)}")
            
            # 数据标准化处理
            if normalize_data:
                print("开始Wind数据标准化...")
                normalizer = WindNormalize1d()
                
                try:
                    # 使用WindNormalize1d进行标准化
                    normalized_data = normalizer.normalize(raw_data)
                    
                    if not normalized_data.empty:
                        print(f"标准化后数据: {len(normalized_data)} 条记录")
                        print(f"标准化后列: {list(normalized_data.columns)}")
                        
                        # 将symbol列修改为original_symbol
                        if 'symbol' in normalized_data.columns:
                            normalized_data['symbol'] = original_symbol
                            print(f"已将symbol列更新为: {original_symbol}")
                        
                        data_to_save = normalized_data
                    else:
                        print("警告: 标准化后数据为空，保存原始数据")
                        data_to_save = raw_data
                        
                except Exception as norm_error:
                    print(f"数据标准化出错: {norm_error}")
                    print("保存原始数据")
                    data_to_save = raw_data
            else:
                print("跳过数据标准化，保存原始数据")
                data_to_save = raw_data
            
            # 确保 data 目录存在
            os.makedirs(data_dir, exist_ok=True)
            
            # 保存数据文件
            filename = f"{original_symbol}.csv" if normalize_data else f"{original_symbol}_raw.csv"
            filepath = os.path.join(data_dir, filename)
            
            data_to_save.to_csv(filepath, index=False)
            print(f"数据已保存到: {filepath}")
            
            return data_to_save
            
        else:
            print(f"未找到 {symbol} 的数据")
            return None
            
    except Exception as e:
        print(f"获取 {symbol} 数据时出错: {e}")
        return None
    finally:
        s.close()


if __name__ == "__main__":
    load_dotenv(".env")
    host = os.getenv("DOLPHINDB_HOST", "localhost")
    port = int(os.getenv("DOLPHINDB_PORT", "8848"))
    user = os.getenv("DOLPHINDB_USERNAME", "admin")
    password = os.getenv("DOLPHINDB_PASSWORD", "123456")

    print("连接到 DolphinDB 数据库", (host, port, user, password))
    try:
        s = session()
        # 尝试连接DolphinDB
        s.connect(host, port, user, password)
        print("DolphinDB 连接成功")
    except Exception as e:
        print("连接 DolphinDB 失败:", e)

    # 读取 code/csi300.txt 所有记录
    file_path = os.path.join(os.path.dirname(__file__), "code", "csi300.txt")
    print(file_path)
    try:
        codes = read_codes(file_path, n=999999)  # 读取所有股票，设置一个足够大的数字
        print(f"共加载 {len(codes)} 支股票:")
        
        # 为所有记录获取数据并保存
        print(f"\n开始获取并保存 {len(codes)} 支股票的数据...")
        db_path = 'dfs://WIND_AShareEODPrices'
        db_table = 'AShareEODPrices'
        
        success_count = 0
        failed_count = 0
        
        for i, item in enumerate(codes, 1):
            print(f"\n[{i}/{len(codes)}] 处理 {item['symbol']} ({item['original_symbol']})")
            
            result = fetch_and_save_data(
                host, port, user, password,
                db_path, db_table,
                item['symbol'],        # 重构后的 symbol (000001.SZ)
                item['start_date'],    # 开始日期
                item['end_date'],      # 结束日期
                item['original_symbol'], # 原始 symbol (SZ000001)
                normalize_data=True    # 启用数据标准化
            )
            
            if result is not None:
                success_count += 1
                print(f"✅ {item['symbol']} 数据获取和标准化完成 ({len(result)} 条记录)")
            else:
                failed_count += 1
                print(f"❌ {item['symbol']} 数据处理失败")
            
            print("-" * 50)
        
        # 总结
        print(f"\n处理完成!")
        print(f"成功: {success_count} 支股票")
        print(f"失败: {failed_count} 支股票")
        print(f"总计: {len(codes)} 支股票")
            
    except Exception as e:
        print("处理过程中出错:", e)