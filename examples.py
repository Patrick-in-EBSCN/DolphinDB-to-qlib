#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DolphinDB 数据获取器使用示例
演示如何使用数据获取器获取不同类型的股票数据
"""

from main import DolphinDBDataCollector
from config import TABLE_CONFIGS
import pandas as pd


def example_get_daily_data():
    """获取日线数据示例"""
    print("=" * 50)
    print("示例1: 获取日线数据")
    print("=" * 50)
    
    collector = DolphinDBDataCollector()
    
    try:
        # 连接数据库
        if not collector.connect():
            print("连接失败")
            return
            
        # 加载股票代码
        stock_codes = collector.load_stock_codes("code/csi300.txt")
        
        # 获取前3只股票的日线数据
        sample_stocks = stock_codes.head(3)
        
        # 使用配置文件中的日线数据配置
        daily_config = TABLE_CONFIGS["daily"]
        
        results = collector.get_batch_stock_data(
            sample_stocks,
            table_name=daily_config["table_name"],
            database_name=daily_config["database_name"]
        )
        # 数据将自动保存到 data/stock_daily/ 目录下
        
        # 或者直接使用便捷方法：
        # results = collector.get_data_by_table_type(sample_stocks, "daily")
        
        print(f"成功获取 {len(results)} 只股票的日线数据")
        print(f"数据已保存到 data/{daily_config['table_name']}/ 目录下")
        
    finally:
        collector.disconnect()


def example_get_minute_data():
    """获取分钟线数据示例"""
    print("=" * 50)
    print("示例2: 获取分钟线数据")
    print("=" * 50)
    
    collector = DolphinDBDataCollector()
    
    try:
        if not collector.connect():
            print("连接失败")
            return
            
        # 获取单只股票的分钟线数据
        minute_config = TABLE_CONFIGS["minute"]
        
        data = collector.get_stock_data(
            symbol="SZ000001",
            start_date="2024.01.01", 
            end_date="2024.01.31",
            table_name=minute_config["table_name"],
            database_name=minute_config["database_name"]
        )
        
        if data is not None and not data.empty:
            print(f"获取到 {len(data)} 条分钟线数据")
            print(data.head())
        else:
            print("未获取到数据")
            
    finally:
        collector.disconnect()


def example_custom_query():
    """自定义查询示例"""
    print("=" * 50)
    print("示例3: 自定义查询")
    print("=" * 50)
    
    collector = DolphinDBDataCollector()
    
    try:
        if not collector.connect():
            print("连接失败")
            return
            
        # 自定义DolphinDB查询
        custom_query = """
        select symbol, date, close, volume 
        from loadTable("dfs://stock", "stock_daily")
        where symbol in ("SZ000001", "SZ000002", "SZ000009")
        and date >= 2024.01.01
        and date <= 2024.01.31
        order by symbol, date
        """
        
        result = collector.session.run(custom_query)
        
        if result is not None and not result.empty:
            print(f"自定义查询返回 {len(result)} 条数据")
            print(result.head(10))
        else:
            print("查询无结果")
            
    except Exception as e:
        print(f"查询失败: {e}")
    finally:
        collector.disconnect()


def example_check_database_info():
    """检查数据库信息示例"""
    print("=" * 50)
    print("示例4: 检查数据库信息")
    print("=" * 50)
    
    collector = DolphinDBDataCollector()
    
    try:
        if not collector.connect():
            print("连接失败")
            return
            
        # 列出所有表
        tables = collector.list_tables("dfs://stock")
        if tables:
            print("可用的表:")
            for table in tables:
                print(f"  - {table}")
        
        # 如果表存在，获取表结构
        if tables and "stock_daily" in tables:
            print("\nstock_daily 表结构:")
            schema = collector.get_table_schema("stock_daily", "dfs://stock")
            if schema is not None:
                print(schema)
        
    finally:
        collector.disconnect()


def main():
    """运行所有示例"""
    print("DolphinDB 数据获取器使用示例")
    print("请确保：")
    print("1. DolphinDB 服务器正在运行")
    print("2. .env 文件中的连接信息正确")
    print("3. 数据库和表已经存在")
    print()
    
    # 运行示例（根据需要注释/取消注释）
    
    # 示例1: 获取日线数据
    # example_get_daily_data()
    
    # 示例2: 获取分钟线数据
    # example_get_minute_data()
    
    # 示例4: 检查数据库信息
    example_check_database_info()


if __name__ == "__main__":
    main()
