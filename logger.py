import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional


class DataCollectorLogger:
    """
    数据收集器日志系统
    负责记录处理过程中的信息、警告、错误和失败的股票代码
    """
    
    def __init__(self, log_dir: str = "log", log_level: str = "INFO"):
        """
        初始化日志系统
        
        Parameters
        ----------
        log_dir : str
            日志文件目录
        log_level : str
            日志级别 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        
        # 生成时间戳文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 设置日志文件路径
        self.main_log_file = self.log_dir / f"data_collector_{timestamp}.log"
        self.error_log_file = self.log_dir / f"errors_{timestamp}.log"
        self.failed_codes_file = self.log_dir / f"failed_codes_{timestamp}.txt"
        
        # 存储失败的股票代码
        self.failed_codes = []
        
        # 设置主日志记录器
        self.logger = self._setup_logger("DataCollector", self.main_log_file, log_level)
        
        # 设置错误日志记录器
        self.error_logger = self._setup_logger("ErrorLogger", self.error_log_file, "ERROR")
        
        self.logger.info(f"Logger initialized. Log files: {self.main_log_file}")
        self.logger.info(f"Error log: {self.error_log_file}")
        self.logger.info(f"Failed codes will be saved to: {self.failed_codes_file}")
    
    def _setup_logger(self, name: str, log_file: Path, level: str) -> logging.Logger:
        """
        设置日志记录器
        
        Parameters
        ----------
        name : str
            日志记录器名称
        log_file : Path
            日志文件路径
        level : str
            日志级别
            
        Returns
        -------
        logging.Logger
            配置好的日志记录器
        """
        logger = logging.getLogger(name)
        logger.setLevel(getattr(logging, level.upper()))
        
        # 清除现有的处理器
        logger.handlers.clear()
        
        # 文件处理器
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(getattr(logging, level.upper()))
        
        # 控制台处理器
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, level.upper()))
        
        # 设置格式
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
    
    def info(self, message: str):
        """记录信息级别日志"""
        self.logger.info(message)
    
    def warning(self, message: str):
        """记录警告级别日志"""
        self.logger.warning(message)
    
    def error(self, message: str, exception: Optional[Exception] = None):
        """记录错误级别日志"""
        self.logger.error(message)
        self.error_logger.error(message)
        if exception:
            self.logger.error(f"Exception details: {str(exception)}")
            self.error_logger.error(f"Exception details: {str(exception)}")
    
    def debug(self, message: str):
        """记录调试级别日志"""
        self.logger.debug(message)
    
    def critical(self, message: str):
        """记录严重错误日志"""
        self.logger.critical(message)
        self.error_logger.critical(message)
    
    def log_stock_start(self, symbol: str, original_symbol: str, index: int, total: int):
        """记录开始处理股票"""
        message = f"[{index}/{total}] 开始处理股票: {symbol} ({original_symbol})"
        self.info(message)
    
    def log_stock_success(self, symbol: str, original_symbol: str, record_count: int):
        """记录股票处理成功"""
        message = f"✅ {symbol} ({original_symbol}) 处理成功 - {record_count} 条记录"
        self.info(message)
    
    def log_stock_failure(self, symbol: str, original_symbol: str, error_msg: str, exception: Optional[Exception] = None):
        """记录股票处理失败"""
        message = f"❌ {symbol} ({original_symbol}) 处理失败: {error_msg}"
        self.error(message, exception)
        
        # 添加到失败列表
        self.failed_codes.append({
            'symbol': symbol,
            'original_symbol': original_symbol,
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        })
    
    def log_data_statistics(self, df, stage: str):
        """记录数据统计信息"""
        if df is None or df.empty:
            self.warning(f"{stage}: 数据为空")
            return
        
        nan_stats = df.isnull().sum()
        total_records = len(df)
        
        self.info(f"{stage} 数据统计:")
        self.info(f"  总记录数: {total_records}")
        self.info(f"  数据列: {list(df.columns)}")
        
        if nan_stats.sum() > 0:
            self.warning(f"  NaN 统计:")
            for col, nan_count in nan_stats.items():
                if nan_count > 0:
                    percentage = (nan_count / total_records) * 100
                    self.warning(f"    {col}: {nan_count} ({percentage:.2f}%)")
        else:
            self.info("  ✅ 无 NaN 值")
    
    def log_calendar_info(self, calendar_count: int, start_date: str, end_date: str):
        """记录交易日历信息"""
        self.info(f"交易日历加载成功: {calendar_count} 个交易日")
        self.info(f"日历范围: {start_date} 至 {end_date}")
    
    def log_processing_summary(self, total_stocks: int, success_count: int, failed_count: int):
        """记录处理总结"""
        self.info("=" * 60)
        self.info("处理完成总结:")
        self.info(f"  总股票数: {total_stocks}")
        self.info(f"  成功处理: {success_count}")
        self.info(f"  处理失败: {failed_count}")
        
        if failed_count > 0:
            success_rate = (success_count / total_stocks) * 100
            self.warning(f"  成功率: {success_rate:.2f}%")
            self.save_failed_codes()
        else:
            self.info("  🎉 所有股票处理成功!")
        
        self.info("=" * 60)
    
    def save_failed_codes(self):
        """保存失败的股票代码到文件"""
        if not self.failed_codes:
            return
        
        try:
            with open(self.failed_codes_file, 'w', encoding='utf-8') as f:
                f.write(f"# 失败的股票代码记录 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"# 格式: 原始代码 -> 标准代码 | 错误信息 | 时间戳\n\n")
                
                for failed_code in self.failed_codes:
                    f.write(f"{failed_code['original_symbol']} -> {failed_code['symbol']} | "
                           f"{failed_code['error']} | {failed_code['timestamp']}\n")
            
            self.info(f"失败代码已保存到: {self.failed_codes_file}")
            
        except Exception as e:
            self.error(f"保存失败代码文件时出错: {str(e)}", e)
    
    def get_failed_codes(self) -> List[dict]:
        """获取失败的股票代码列表"""
        return self.failed_codes.copy()
    
    def close(self):
        """关闭日志系统，保存失败代码"""
        if self.failed_codes:
            self.save_failed_codes()
        
        # 关闭所有处理器
        for handler in self.logger.handlers:
            handler.close()
        for handler in self.error_logger.handlers:
            handler.close()
        
        self.info("日志系统已关闭")


# 全局日志实例
_global_logger = None


def get_logger(log_dir: str = "log", log_level: str = "INFO") -> DataCollectorLogger:
    """
    获取全局日志实例
    
    Parameters
    ----------
    log_dir : str
        日志目录
    log_level : str
        日志级别
        
    Returns
    -------
    DataCollectorLogger
        日志实例
    """
    global _global_logger
    if _global_logger is None:
        _global_logger = DataCollectorLogger(log_dir, log_level)
    return _global_logger


def close_logger():
    """关闭全局日志实例"""
    global _global_logger
    if _global_logger is not None:
        _global_logger.close()
        _global_logger = None


if __name__ == "__main__":
    # 测试日志系统
    logger = DataCollectorLogger()
    
    logger.info("这是一条信息日志")
    logger.warning("这是一条警告日志")
    logger.error("这是一条错误日志")
    
    # 测试股票处理日志
    logger.log_stock_start("000001.SZ", "SZ000001", 1, 100)
    logger.log_stock_success("000001.SZ", "SZ000001", 1250)
    logger.log_stock_failure("000002.SZ", "SZ000002", "连接数据库失败")
    
    # 测试处理总结
    logger.log_processing_summary(100, 99, 1)
    
    logger.close()