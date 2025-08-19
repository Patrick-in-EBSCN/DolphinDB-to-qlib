#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DolphinDB 表配置
定义不同数据表的配置信息
"""

# 常用的表配置
TABLE_CONFIGS = {
    # 日线数据表
    "daily": {
        "table_name": "stock_daily",
        "database_name": "dfs://stock",
        "description": "股票日线数据",
        "columns": [
            "symbol", "date", "open", "high", "low", "close", 
            "volume", "amount", "adjustflag"
        ]
    },
    
    # 分钟线数据表
    "15 minute": {
        "table_name": "stock_minute", 
        "database_name": "dfs://stock",
        "description": "股票分钟线数据",
        "columns": [
            "symbol", "datetime", "open", "high", "low", "close", 
            "volume", "amount"
        ]
    }
}

# 数据库配置
DATABASE_CONFIGS = {
    "stock": {
        "name": "dfs://stock",
        "description": "股票行情数据库"
    }
}
