#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DolphinDB 数据获取器
用于从DolphinDB数据库获取股票数据
"""

import os
import pandas as pd
import dolphindb as ddb
from typing import List, Dict, Optional, Tuple
from dotenv import load_dotenv
import logging
from datetime import datetime


class DolphinDBDataCollector:
    """DolphinDB数据获取器类"""
    
    def __init__(self, env_file: str = ".env"):
        """
        初始化数据获取器
        
        Args:
            env_file: 环境变量文件路径
        """
        # 加载环境变量
        load_dotenv(env_file)
        
        # 从环境变量获取连接信息
        self.host = os.getenv("DOLPHINDB_HOST", "localhost")
        self.port = int(os.getenv("DOLPHINDB_PORT", "8848"))
        self.username = os.getenv("DOLPHINDB_USERNAME", "admin")
        self.password = os.getenv("DOLPHINDB_PASSWORD", "123456")
        
        # 初始化连接对象
        self.session = None
        
        # 设置日志
        self._setup_logging()
        
    def _setup_logging(self):
        """设置日志配置"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('dolphindb_collector.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def connect(self) -> bool:
        """
        连接到DolphinDB服务器
        
        Returns:
            bool: 连接是否成功
        """
        try:
            self.session = ddb.session()
            self.session.connect(self.host, self.port, self.username, self.password)
            self.logger.info(f"成功连接到DolphinDB服务器: {self.host}:{self.port}")
            return True
        except Exception as e:
            self.logger.error(f"连接DolphinDB失败: {e}")
            return False
            
    def disconnect(self):
        """断开DolphinDB连接"""
        if self.session:
            self.session.close()
            self.logger.info("已断开DolphinDB连接")
            
    def load_stock_codes(self, file_path: str) -> pd.DataFrame:
        """
        从文件加载股票代码和时间范围
        
        Args:
            file_path: 股票代码文件路径
            
        Returns:
            pd.DataFrame: 包含symbol, start_date, end_date的DataFrame
        """
        try:
            df = pd.read_csv(
                file_path, 
                sep='\t', 
                header=None, 
                names=['symbol', 'start_date', 'end_date']
            )
            
            # 转换日期格式
            df['start_date'] = pd.to_datetime(df['start_date'])
            df['end_date'] = pd.to_datetime(df['end_date'])
            
            self.logger.info(f"成功加载{len(df)}条股票代码记录")
            return df
            
        except Exception as e:
            self.logger.error(f"加载股票代码文件失败: {e}")
            return pd.DataFrame()
            
    def get_stock_data(self, 
                      symbol: str, 
                      start_date: str, 
                      end_date: str, 
                      table_name: str = "stock_daily",
                      database_name: str = "dfs://stock") -> Optional[pd.DataFrame]:
        """
        获取单个股票的数据
        
        Args:
            symbol: 股票代码
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            table_name: 表名
            database_name: 数据库名
            
        Returns:
            pd.DataFrame: 股票数据，如果失败返回None
        """
        if not self.session:
            self.logger.error("未连接到DolphinDB服务器")
            return None
            
        try:
            # 构建DolphinDB查询语句
            query = f"""
            select * from loadTable("{database_name}", "{table_name}")
            where symbol = "{symbol}" 
            and date >= {start_date}
            and date <= {end_date}
            """
            
            # 执行查询
            result = self.session.run(query)
            
            if result is not None and not result.empty:
                self.logger.info(f"成功获取股票 {symbol} 的数据，共 {len(result)} 条记录")
                return result
            else:
                self.logger.warning(f"股票 {symbol} 在指定时间范围内无数据")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"获取股票 {symbol} 数据失败: {e}")
            return None
            
    def get_batch_stock_data(self, 
                           stock_codes_df: pd.DataFrame,
                           table_name: str = "stock_daily",
                           database_name: str = "dfs://stock",
                           save_to_file: bool = True,
                           output_dir: str = "data") -> Dict[str, pd.DataFrame]:
        """
        批量获取股票数据
        
        Args:
            stock_codes_df: 包含股票代码和时间范围的DataFrame
            table_name: 表名
            database_name: 数据库名
            save_to_file: 是否保存到文件
            output_dir: 输出根目录
            
        Returns:
            Dict[str, pd.DataFrame]: 股票代码到数据的映射
        """
        if not self.session:
            self.logger.error("未连接到DolphinDB服务器")
            return {}
            
        results = {}
        
        # 根据table_name创建对应的输出目录
        if save_to_file:
            table_output_dir = os.path.join(output_dir, table_name)
            os.makedirs(table_output_dir, exist_ok=True)
            
        total_stocks = len(stock_codes_df)
        self.logger.info(f"开始批量获取 {total_stocks} 只股票的数据")
        
        for idx, row in stock_codes_df.iterrows():
            symbol = row['symbol']
            start_date = row['start_date'].strftime('%Y.%m.%d')
            end_date = row['end_date'].strftime('%Y.%m.%d')
            
            self.logger.info(f"正在获取股票 {symbol} 数据 ({idx+1}/{total_stocks})")
            
            # 获取数据
            data = self.get_stock_data(symbol, start_date, end_date, table_name, database_name)
            
            if data is not None and not data.empty:
                results[symbol] = data
                
                # 保存到文件
                if save_to_file:
                    filename = f"{symbol}_{start_date.replace('.', '_')}_{end_date.replace('.', '_')}.csv"
                    filepath = os.path.join(table_output_dir, filename)
                    data.to_csv(filepath, index=False)
                    self.logger.info(f"数据已保存到: {filepath}")
                    
        self.logger.info(f"批量获取完成，成功获取 {len(results)} 只股票的数据")
        return results
        
    def get_table_schema(self, table_name: str, database_name: str = "dfs://stock") -> Optional[pd.DataFrame]:
        """
        获取表结构信息
        
        Args:
            table_name: 表名
            database_name: 数据库名
            
        Returns:
            pd.DataFrame: 表结构信息
        """
        if not self.session:
            self.logger.error("未连接到DolphinDB服务器")
            return None
            
        try:
            query = f'schema(loadTable("{database_name}", "{table_name}"))'
            result = self.session.run(query)
            self.logger.info(f"成功获取表 {table_name} 的结构信息")
            return result
        except Exception as e:
            self.logger.error(f"获取表结构失败: {e}")
            return None
            
    def list_tables(self, database_name: str = "dfs://stock") -> Optional[List[str]]:
        """
        列出数据库中的所有表
        
        Args:
            database_name: 数据库名
            
        Returns:
            List[str]: 表名列表
        """
        if not self.session:
            self.logger.error("未连接到DolphinDB服务器")
            return None
            
        try:
            query = f'exec name from getTables("{database_name}")'
            result = self.session.run(query)
            if isinstance(result, list):
                self.logger.info(f"数据库 {database_name} 中共有 {len(result)} 张表")
                return result
            else:
                return []
        except Exception as e:
            self.logger.error(f"列出表失败: {e}")
            return None
            
    def get_data_by_table_type(self, 
                              stock_codes_df: pd.DataFrame,
                              table_type: str = "daily",
                              save_to_file: bool = True,
                              output_dir: str = "data") -> Dict[str, pd.DataFrame]:
        """
        根据表类型获取股票数据（使用config.py中的预定义配置）
        
        Args:
            stock_codes_df: 包含股票代码和时间范围的DataFrame
            table_type: 表类型（"daily", "minute", "fundamental", "financial"等）
            save_to_file: 是否保存到文件
            output_dir: 输出根目录
            
        Returns:
            Dict[str, pd.DataFrame]: 股票代码到数据的映射
        """
        try:
            from config import TABLE_CONFIGS
            
            if table_type not in TABLE_CONFIGS:
                self.logger.error(f"未找到表类型 {table_type} 的配置")
                return {}
                
            config = TABLE_CONFIGS[table_type]
            table_name = config["table_name"]
            database_name = config["database_name"]
            
            self.logger.info(f"使用配置获取{config['description']}")
            
            return self.get_batch_stock_data(
                stock_codes_df=stock_codes_df,
                table_name=table_name,
                database_name=database_name,
                save_to_file=save_to_file,
                output_dir=output_dir
            )
            
        except ImportError:
            self.logger.warning("无法导入config.py，请检查配置文件")
            return {}
        except Exception as e:
            self.logger.error(f"根据表类型获取数据失败: {e}")
            return {}


def main():
    """主函数示例"""
    # 创建数据获取器实例
    collector = DolphinDBDataCollector()
    
    try:
        # 连接到DolphinDB
        if not collector.connect():
            print("连接失败，程序退出")
            return
            
        # 加载股票代码
        stock_codes = collector.load_stock_codes("code/csi300.txt")
        if stock_codes.empty:
            print("加载股票代码失败")
            return
            
        # 显示前几条记录
        print("股票代码示例：")
        print(stock_codes.head())
        
        # 列出可用的表（可选）
        print("\n检查可用的表...")
        tables = collector.list_tables()
        if tables:
            print(f"可用的表: {tables}")
        
        # 获取表结构（可选，需要指定具体的表名）
        # table_name = "stock_daily"  # 请根据实际情况修改
        # schema = collector.get_table_schema(table_name)
        # if schema is not None:
        #     print(f"\n表 {table_name} 的结构:")
        #     print(schema)
        
        # 获取前5只股票的数据作为示例
        print("\n开始获取前5只股票的数据...")
        sample_data = stock_codes.head(5)
        
        # 批量获取数据（请根据实际情况修改表名）
        # 数据将保存到 data/stock_daily/ 目录下
        results = collector.get_batch_stock_data(
            sample_data, 
            table_name="stock_daily",  # 请根据实际表名修改
            database_name="dfs://stock"  # 请根据实际数据库名修改
        )
        
        # 或者使用预定义的配置
        # results = collector.get_data_by_table_type(sample_data, "daily")
        
        print(f"\n成功获取了 {len(results)} 只股票的数据")
        print(f"数据已保存到 data/stock_daily/ 目录下")
        
    except KeyboardInterrupt:
        print("\n用户中断程序")
    except Exception as e:
        print(f"程序执行出错: {e}")
    finally:
        # 断开连接
        collector.disconnect()


if __name__ == "__main__":
    main()
