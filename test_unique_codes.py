#!/usr/bin/env python3
"""
测试脚本：检查CSI300股票代码去重结果
"""
import os
import csv
from typing import Set, List, Dict


def analyze_csi300_codes(file_path: str) -> Dict:
    """分析CSI300股票代码文件，统计去重前后的数量"""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"文件不存在: {file_path}")
    
    all_codes = []  # 所有代码（包含重复）
    unique_codes = set()  # 去重后的代码
    
    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter='\t')
        for row_num, row in enumerate(reader, 1):
            if not row:
                continue
            if len(row) < 1:
                continue
            
            symbol = row[0].strip()
            if symbol.lower() in ("symbol", "代码", "ticker"):
                continue
            
            all_codes.append(symbol)
            unique_codes.add(symbol)
    
    return {
        "total_records": len(all_codes),
        "unique_codes": len(unique_codes),
        "duplicate_count": len(all_codes) - len(unique_codes),
        "unique_list": sorted(list(unique_codes))
    }


def get_unique_csi300_codes(file_path: str) -> List[Dict[str, str]]:
    """获取CSI300股票代码去重，固定时间范围为2008-01-01到2025-08-01"""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"文件不存在: {file_path}")
    
    unique_symbols = set()
    results = []
    
    # 固定的时间范围
    fixed_start_date = "2008-01-01"
    fixed_end_date = "2025-08-01"
    
    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter='\t')
        for row in reader:
            if not row:
                continue
            if len(row) < 1:
                continue
            
            symbol = row[0].strip()
            if symbol.lower() in ("symbol", "代码", "ticker"):
                continue
            
            if symbol not in unique_symbols:
                unique_symbols.add(symbol)
                
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
                
                results.append({
                    "symbol": symbol_fmt, 
                    "start_date": fixed_start_date, 
                    "end_date": fixed_end_date, 
                    "original_symbol": symbol
                })
    
    return results


if __name__ == "__main__":
    file_path = os.path.join(os.path.dirname(__file__), "code", "csi300.txt")
    
    print("="*60)
    print("CSI300股票代码分析")
    print("="*60)
    
    # 分析原始数据
    analysis = analyze_csi300_codes(file_path)
    print(f"原始记录总数: {analysis['total_records']}")
    print(f"去重后股票数: {analysis['unique_codes']}")
    print(f"重复记录数: {analysis['duplicate_count']}")
    
    # 获取去重后的股票列表
    unique_codes = get_unique_csi300_codes(file_path)
    print(f"\n去重后股票列表 (共{len(unique_codes)}支):")
    print(f"数据时间范围: 2008-01-01 到 2025-08-01")
    
    # 显示前20支股票
    print(f"\n前20支股票:")
    for i, code in enumerate(unique_codes[:20]):
        print(f"  {i+1:2d}. {code['original_symbol']:8s} -> {code['symbol']:10s}")
    
    if len(unique_codes) > 20:
        print(f"  ... 还有 {len(unique_codes) - 20} 支股票")
    
    # 按交易所分类统计
    sz_count = sum(1 for code in unique_codes if code['original_symbol'].startswith('SZ'))
    sh_count = sum(1 for code in unique_codes if code['original_symbol'].startswith('SH'))
    other_count = len(unique_codes) - sz_count - sh_count
    
    print(f"\n按交易所分类:")
    print(f"  深交所 (SZ): {sz_count} 支")
    print(f"  上交所 (SH): {sh_count} 支")
    print(f"  其他: {other_count} 支")
    
    print("="*60)
