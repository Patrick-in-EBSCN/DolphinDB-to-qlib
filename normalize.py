# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import copy
import datetime
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


class WindNormalize1d:
    """
    Wind数        # 9. 处理无效成交量数据
        d        # 13. 设置索引名称
        df.index.names = [self._date_field_name]
        
        # 最终检查NaN情况
        self._check_and_print_nan_stats(df, "标准化完成前")
        
        return df.reset_index()c[(df["volume"] <= 0) | np.isnan(df["volume"]), df.columns] = np.nan
        
        # 检查处理无效成交量数据后的NaN情况
        self._check_and_print_nan_stats(df, "处理无效成交量后")

        # 10. 检测并修正异常数据 (参考Yahoo处理逻辑)处理类，参考YahooNormalize1d实现
    处理Wind数据库字段到qlib标准格式的转换
    """
    
    # Wind字段到标准字段的映射
    WIND_FIELD_MAPPING = {
        'TRADE_DT': 'date',
        'S_DQ_OPEN': 'open',
        'S_DQ_HIGH': 'high',
        'S_DQ_LOW': 'low',
        'S_DQ_CLOSE': 'close',
        'S_DQ_VOLUME': 'volume',
        'S_DQ_AMOUNT': 'amount',
        'S_DQ_ADJCLOSE': 'adjclose'
    }
    
    # 标准化处理的价格字段
    COLUMNS = ["open", "close", "high", "low", "volume"]
    DATE_FORMAT = "%Y-%m-%d"

    def __init__(self, calendar_file_path: str = None, date_field_name: str = "date", logger=None):
        """
        初始化Wind数据标准化处理器
        
        Parameters
        ----------
        calendar_file_path : str
            交易日历文件路径 (calendar/day.txt)
        date_field_name : str
            日期字段名称，默认为 "date"
        logger : object
            日志记录器实例，可选
        """
        self._date_field_name = date_field_name
        self._logger = logger
        
        # 如果没有提供日历文件路径，使用默认路径
        if calendar_file_path is None:
            msg = "No calendar file path provided, using default 'calendar/day.txt'"
            if self._logger:
                self._logger.info(msg)
            else:
                print(f"Info: {msg}")
            calendar_file_path = "calendar/day.txt"
            
        self._calendar_list = self._load_calendar(calendar_file_path)

    def _load_calendar(self, calendar_file_path: str = "calendar/day.txt") -> list:
        """
        加载交易日历
        
        Parameters
        ----------
        calendar_file_path : str
            交易日历文件路径
            
        Returns
        -------
        list
            交易日期列表
        """
        try:
            calendar_path = Path(calendar_file_path)
            if not calendar_path.exists():
                msg = f"Calendar file not found: {calendar_file_path}"
                if self._logger:
                    self._logger.warning(msg)
                else:
                    print(f"Warning: {msg}")
                return None
                
            with open(calendar_path, 'r') as f:
                dates = [line.strip() for line in f.readlines() if line.strip()]
            
            calendar_list = pd.to_datetime(dates).tolist()
            msg = f"Loaded {len(calendar_list)} trading days from calendar"
            if self._logger:
                self._logger.log_calendar_info(len(calendar_list), str(calendar_list[0].date()), str(calendar_list[-1].date()))
            else:
                print(f"Info: {msg}")
            return calendar_list
            
        except Exception as e:
            msg = f"Error loading calendar: {e}"
            if self._logger:
                self._logger.error(msg, e)
            else:
                print(f"Error: {msg}")
            return None

    def _check_and_print_nan_stats(self, df: pd.DataFrame, stage: str = ""):
        """
        检查并打印DataFrame中NaN的统计情况
        
        Parameters
        ----------
        df : pd.DataFrame
            要检查的DataFrame
        stage : str
            当前处理阶段的描述
        """
        total_cells = df.size
        total_nan = df.isna().sum().sum()
        nan_percentage = (total_nan / total_cells * 100) if total_cells > 0 else 0
        
        print(f"Info: NaN statistics {stage}:")
        print(f"  Total cells: {total_cells}, NaN cells: {total_nan} ({nan_percentage:.2f}%)")
        
        # 按列统计NaN
        nan_by_column = df.isna().sum()
        if nan_by_column.sum() > 0:
            print("  NaN by column:")
            for col, count in nan_by_column.items():
                if count > 0:
                    percentage = (count / len(df) * 100) if len(df) > 0 else 0
                    print(f"    {col}: {count} ({percentage:.2f}%)")
        else:
            print("  No NaN values found")

    def map_wind_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        将Wind字段名映射到标准字段名
        
        Parameters
        ----------
        df : pd.DataFrame
            包含Wind字段的DataFrame
            
        Returns
        -------
        pd.DataFrame
            映射后的DataFrame
        """
        df = df.copy()
        
        # 检查并映射字段
        available_mappings = {}
        for wind_field, standard_field in self.WIND_FIELD_MAPPING.items():
            if wind_field in df.columns:
                available_mappings[wind_field] = standard_field
            else:
                print(f"Warning: Wind field '{wind_field}' not found in data")
        
        # 执行字段重命名
        df = df.rename(columns=available_mappings)
        
        # 删除股票代码列（如果存在）
        if 'S_INFO_WINDCODE' in df.columns:
            df = df.drop(columns=['S_INFO_WINDCODE'])
            print("Info: Removed S_INFO_WINDCODE column")
        
        # 记录映射信息
        print(f"Info: Mapped fields: {available_mappings}")
        
        return df

    @staticmethod
    def calc_change(df: pd.DataFrame, last_close: float = None) -> pd.Series:
        """
        计算涨跌幅
        
        Parameters
        ----------
        df : pd.DataFrame
            包含收盘价的DataFrame
        last_close : float
            前一个交易日的收盘价
            
        Returns
        -------
        pd.Series
            涨跌幅序列
        """
        df = df.copy()
        _tmp_series = df["close"].ffill()
        _tmp_shift_series = _tmp_series.shift(1)
        if last_close is not None:
            _tmp_shift_series.iloc[0] = float(last_close)
        change_series = _tmp_series / _tmp_shift_series - 1
        return change_series

    def normalize_wind_data(self, df: pd.DataFrame, last_close: float = None) -> pd.DataFrame:
        """
        标准化Wind数据
        
        Parameters
        ----------
        df : pd.DataFrame
            原始Wind数据
        last_close : float
            前一个交易日的收盘价
            
        Returns
        -------
        pd.DataFrame
            标准化后的数据
        """
        if df.empty:
            return df
            
        # 1. 映射字段名
        df = self.map_wind_fields(df)
        
        # 检查原始数据NaN情况
        self._check_and_print_nan_stats(df, "字段映射后")
        
        # 2. 基础数据处理
        columns = copy.deepcopy(self.COLUMNS)
        df = df.copy()
        
        # 3. 处理日期索引
        df[self._date_field_name] = pd.to_datetime(df[self._date_field_name], format='%Y%m%d', errors='coerce')
        df.set_index(self._date_field_name, inplace=True)
        
        # 4. 去重
        df = df[~df.index.duplicated(keep="first")]
        
        # 5. 根据交易日历重新索引
        if self._calendar_list is not None:
            df = df.reindex(
                pd.DataFrame(index=self._calendar_list)
                .loc[
                    pd.Timestamp(df.index.min()).date() : pd.Timestamp(df.index.max()).date()
                    + pd.Timedelta(hours=23, minutes=59)
                ]
                .index
            )
            print('重新索引成功')
        
        # 6. 排序
        df.sort_index(inplace=True)
        
        # 7. 检查并打印NaN情况
        self._check_and_print_nan_stats(df, "交易日历重新索引后")
        
        # 8. 对close列进行前向填充
        if "close" in df.columns:
            nan_count_before = df["close"].isna().sum()
            df["close"] = df["close"].ffill()
            nan_count_after = df["close"].isna().sum()
            print(f"Info: Close column forward fill - NaN before: {nan_count_before}, after: {nan_count_after}")
        
        # 9. 处理无效成交量数据
        df.loc[(df["volume"] <= 0) | np.isnan(df["volume"]), df.columns] = np.nan

        # 8. 检测并修正异常数据 (参考Yahoo处理逻辑)
        _count = 0
        while True:
            change_series = self.calc_change(df, last_close)
            _mask = (change_series >= 89) & (change_series <= 111)
            if not _mask.any():
                break
            _tmp_cols = ["high", "close", "low", "open", "adjclose"]
            available_cols = [col for col in _tmp_cols if col in df.columns]
            df.loc[_mask, available_cols] = df.loc[_mask, available_cols] / 100
            _count += 1
            if _count >= 10:
                print(f"Warning: Stock `change` is abnormal for {_count} consecutive days, please check the data carefully")
                break

        # 9. 计算涨跌幅
        df["change"] = self.calc_change(df, last_close)

        # 10. 处理无效数据
        columns += ["change"]
        available_columns = [col for col in columns if col in df.columns]
        df.loc[(df["volume"] <= 0) | np.isnan(df["volume"]), available_columns] = np.nan

        # 11. 设置索引名称
        df.index.names = [self._date_field_name]
        
        return df.reset_index()

    def adjusted_price(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        价格复权处理
        
        Parameters
        ----------
        df : pd.DataFrame
            包含价格数据的DataFrame
            
        Returns
        -------
        pd.DataFrame
            复权后的DataFrame
        """
        if df.empty:
            return df
            
        df = df.copy()
        df.set_index(self._date_field_name, inplace=True)
        
        # 计算复权因子
        if "adjclose" in df.columns:
            df["factor"] = df["adjclose"] / df["close"]
            df["factor"] = df["factor"].ffill()
        else:
            df["factor"] = 1
            
        # 对价格字段进行复权
        for _col in self.COLUMNS:
            if _col not in df.columns:
                continue
            if _col == "volume":
                df[_col] = df[_col] / df["factor"]
            else:
                df[_col] = df[_col] * df["factor"]
                
        df.index.names = [self._date_field_name]
        return df.reset_index()

    def _get_first_close(self, df: pd.DataFrame) -> float:
        """
        获取第一个有效的收盘价
        
        Parameters
        ----------
        df : pd.DataFrame
            包含收盘价的DataFrame
            
        Returns
        -------
        float
            第一个有效收盘价
        """
        df = df.loc[df["close"].first_valid_index() :]
        _close = df["close"].iloc[0]
        return _close

    def _manual_adj_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        手动调整数据：所有字段（除change外）根据第一天的收盘价进行标准化
        
        Parameters
        ----------
        df : pd.DataFrame
            输入DataFrame
            
        Returns
        -------
        pd.DataFrame
            调整后的DataFrame
        """
        if df.empty:
            return df
            
        df = df.copy()
        df.sort_values(self._date_field_name, inplace=True)
        df = df.set_index(self._date_field_name)
        _close = self._get_first_close(df)
        
        for _col in df.columns:
            # 跳过非数值列和特殊字段
            if _col in ["adjclose", "change", self._date_field_name] or not pd.api.types.is_numeric_dtype(df[_col]):
                continue
            if _col == "volume":
                df[_col] = df[_col] * _close
            else:
                df[_col] = df[_col] / _close
                
        return df.reset_index()

    def normalize(self, df: pd.DataFrame, last_close: float = None) -> pd.DataFrame:
        """
        完整的数据标准化流程
        
        Parameters
        ----------
        df : pd.DataFrame
            原始Wind数据
        last_close : float
            前一个交易日的收盘价
            
        Returns
        -------
        pd.DataFrame
            标准化后的数据
        """
        # 基础标准化
        df = self.normalize_wind_data(df, last_close)
        
        # 价格复权
        df = self.adjusted_price(df)
        
        # 手动数据调整
        df = self._manual_adj_data(df)
        
        return df


def process_wind_data(input_file: str, output_file: str, calendar_file: str = None):
    """
    处理Wind数据的主函数
    
    Parameters
    ----------
    input_file : str
        输入的Wind数据文件路径（CSV格式）
    output_file : str
        输出的标准化数据文件路径
    calendar_file : str
        交易日历文件路径
        
    Examples
    --------
    >>> process_wind_data(
    ...     input_file="wind_data.csv",
    ...     output_file="normalized_data.csv", 
    ...     calendar_file="calendar/day.txt"
    ... )
    """
    pass


if __name__ == "__main__":
    pass