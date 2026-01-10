"""
将本地Excel数据转换为VeighNa需要的CSV格式
"""
import pandas as pd
import os
from pathlib import Path
from datetime import datetime

# 数据目录（支持多个数据源）
INDEX_DATA_DIR = Path(r"D:\vnpy\vnpy\trade\index_data")  # 指数数据
STOCK_DATA_DIR = Path(r"D:\vnpy\vnpy\trade\stock_data")  # 股票数据
OUTPUT_DIR = Path(r"D:\vnpy\vnpy\converted_data")  # 输出目录

# 创建输出目录
OUTPUT_DIR.mkdir(exist_ok=True)

# 交易所映射（根据文件名判断）
EXCHANGE_MAP = {
    "sh": "SSE",      # 上海证券交易所
    "sz": "SZSE",     # 深圳证券交易所
    "bj": "BSE",      # 北京证券交易所
    "^": "NONE",      # 外盘指数（需要特殊处理）
}

# 合约代码映射（从文件名提取）
SYMBOL_MAP = {
    "sh000001": ("000001", "SSE"),      # 上证指数
    "sz399001": ("399001", "SZSE"),     # 深证成指
    "sz399006": ("399006", "SZSE"),     # 创业板指
    "sh000688": ("000688", "SSE"),      # 科创50
    "bj899050": ("899050", "BSE"),      # 北证50
    "^HSI": ("HSI", "NONE"),            # 恒生指数
    "^HSTECH": ("HSTECH", "NONE"),      # 恒生科技
    "^GSPC": ("GSPC", "NONE"),          # 标普500
    "^IXIC": ("IXIC", "NONE"),          # 纳斯达克
    "^DJI": ("DJI", "NONE"),            # 道琼斯
}

def extract_symbol_and_exchange(filename: str) -> tuple[str, str]:
    """从文件名提取合约代码和交易所"""
    # 移除扩展名
    name = filename.replace("_数据.xlsx", "").replace("_技术数据.xlsx", "").replace("_data.xlsx", "").replace(".xlsx", "")
    
    # 提取中文名称和代码部分
    parts = name.split("_")
    
    # 股票数据格式：{代码}_{名称}_技术数据.xlsx 或 指数数据格式：{名称}_{代码}_data.xlsx
    if len(parts) >= 2:
        # 尝试判断是股票数据还是指数数据
        # 如果第一部分是纯数字（可能是股票代码）
        first_part = parts[0]
        if first_part.isdigit() or (first_part.startswith("^") and len(first_part) > 1):
            # 股票数据格式：代码在第一位
            code_part = first_part
        else:
            # 指数数据格式：代码在最后
            code_part = parts[-1]
    else:
        code_part = name
    
    # 查找映射（优先使用映射表）
    if code_part in SYMBOL_MAP:
        return SYMBOL_MAP[code_part]
    
    # 根据代码前缀判断交易所（股票数据）
    if code_part.startswith("sh"):
        return (code_part[2:], "SSE")
    elif code_part.startswith("sz"):
        return (code_part[2:], "SZSE")
    elif code_part.startswith("bj"):
        return (code_part[2:], "BSE")
    elif code_part.startswith("^"):
        return (code_part[1:], "GLOBAL")  # 外盘指数使用GLOBAL
    elif code_part.isdigit():
        # 纯数字代码，根据前缀判断
        if code_part.startswith("6"):  # 6开头，上交所
            return (code_part, "SSE")
        elif code_part.startswith("0") or code_part.startswith("3"):  # 0或3开头，深交所
            return (code_part, "SZSE")
        elif code_part.startswith("688") or code_part.startswith("689"):  # 科创板
            return (code_part, "SSE")
        elif code_part.startswith("43") or code_part.startswith("83") or code_part.startswith("87"):  # 北交所
            return (code_part, "BSE")
        elif len(code_part) == 5:  # 5位数字，可能是港股
            return (code_part, "SEHK")  # 港股使用SEHK
        elif len(code_part) == 4 or len(code_part) == 6:  # 4位或6位数字，可能是港股（如01952, 700等）
            # 检查是否在港股代码范围（港股通常是4-5位数字）
            if code_part.startswith("0") or code_part.startswith("7") or code_part.startswith("9"):
                return (code_part, "SEHK")  # 港股
        else:
            return (code_part, "SSE")  # 默认上交所
    else:
        return (code_part, "SSE")  # 默认上交所

def convert_excel_to_csv(excel_path: Path) -> Path | None:
    """将Excel文件转换为VeighNa需要的CSV格式"""
    print(f"\n处理文件: {excel_path.name}")
    
    try:
        # 读取Excel文件
        df = pd.read_excel(excel_path)
        
        print(f"  原始列名: {df.columns.tolist()}")
        print(f"  数据行数: {len(df)}")
        print(f"  前3行数据:")
        print(df.head(3))
        
        # 提取合约代码和交易所
        symbol, exchange = extract_symbol_and_exchange(excel_path.name)
        print(f"  合约代码: {symbol}, 交易所: {exchange}")
        
        # 创建输出DataFrame
        output_df = pd.DataFrame()
        
        # 映射列名（常见的中文和英文列名）
        column_mapping = {
            # 时间列
            "日期": "datetime",
            "时间": "datetime",
            "date": "datetime",  # 添加小写date
            "Date": "datetime",
            "Time": "datetime",
            "datetime": "datetime",
            "时间戳": "datetime",
            "Timestamp": "datetime",
            
            # 价格列
            "开盘": "open",
            "开盘价": "open",
            "Open": "open",
            "open": "open",
            "开": "open",
            
            "最高": "high",
            "最高价": "high",
            "High": "high",
            "high": "high",
            "高": "high",
            
            "最低": "low",
            "最低价": "low",
            "Low": "low",
            "low": "low",
            "低": "low",
            
            "收盘": "close",
            "收盘价": "close",
            "Close": "close",
            "close": "close",
            "收": "close",
            
            # 成交量列
            "成交量": "volume",
            "Volume": "volume",
            "volume": "volume",
            "量": "volume",
            "成交额": "turnover",
            "成交金额": "turnover",
            "Turnover": "turnover",
            "turnover": "turnover",
            "amount": "turnover",  # 用户数据中成交额列名为amount
            "Amount": "turnover",
            "额": "turnover",
            
            # 持仓量列（可选）
            "持仓量": "open_interest",
            "OpenInterest": "open_interest",
            "open_interest": "open_interest",
        }
        
        # 转换列名
        df_renamed = df.copy()
        for old_col, new_col in column_mapping.items():
            if old_col in df_renamed.columns:
                df_renamed[new_col] = df_renamed[old_col]
        
        # 如果列名已经是标准格式，直接使用
        if "date" in df_renamed.columns and "datetime" not in df_renamed.columns:
            df_renamed["datetime"] = df_renamed["date"]
        
        # 检查必需的列
        required_cols = ["datetime", "open", "high", "low", "close"]
        missing_cols = [col for col in required_cols if col not in df_renamed.columns]
        
        if missing_cols:
            print(f"  [警告] 缺少必需的列: {missing_cols}")
            print(f"  可用列: {df_renamed.columns.tolist()}")
            return None
        
        # 构建输出DataFrame
        output_df["datetime"] = df_renamed["datetime"]
        output_df["open"] = df_renamed["open"]
        output_df["high"] = df_renamed["high"]
        output_df["low"] = df_renamed["low"]
        output_df["close"] = df_renamed["close"]
        
        # 成交量（必需）
        if "volume" in df_renamed.columns:
            output_df["volume"] = df_renamed["volume"]
        else:
            output_df["volume"] = 0
            print("  [警告] 没有成交量数据，设置为0")
        
        # 成交额（可选）
        if "turnover" in df_renamed.columns:
            output_df["turnover"] = df_renamed["turnover"]
        
        # 持仓量（可选，股票数据通常没有）
        if "open_interest" in df_renamed.columns:
            output_df["open_interest"] = df_renamed["open_interest"]
        else:
            output_df["open_interest"] = 0
        
        # 处理时间格式
        if output_df["datetime"].dtype == "object":
            # 尝试多种时间格式
            for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"]:
                try:
                    output_df["datetime"] = pd.to_datetime(output_df["datetime"], format=fmt)
                    break
                except:
                    continue
            else:
                # 如果都失败，使用pandas自动解析
                output_df["datetime"] = pd.to_datetime(output_df["datetime"], errors="coerce")
        
        # 确保时间格式为 datetime
        output_df["datetime"] = pd.to_datetime(output_df["datetime"])
        
        # 排序
        output_df = output_df.sort_values("datetime")
        
        # 删除空值
        output_df = output_df.dropna(subset=["datetime", "open", "high", "low", "close"])
        
        # 保存为CSV（使用UTF-8无BOM，避免编码问题）
        csv_filename = excel_path.stem + ".csv"
        csv_path = OUTPUT_DIR / csv_filename
        output_df.to_csv(csv_path, index=False, encoding="utf-8")
        
        print(f"  [成功] 转换成功: {csv_path}")
        print(f"  输出行数: {len(output_df)}")
        print(f"  时间范围: {output_df['datetime'].min()} 到 {output_df['datetime'].max()}")
        
        # 保存合约信息到单独文件
        info_filename = excel_path.stem + "_info.txt"
        info_path = OUTPUT_DIR / info_filename
        with open(info_path, "w", encoding="utf-8") as f:
            f.write(f"合约代码: {symbol}\n")
            f.write(f"交易所: {exchange}\n")
            f.write(f"VeighNa代码: {symbol}.{exchange}\n")
            f.write(f"数据行数: {len(output_df)}\n")
            f.write(f"时间范围: {output_df['datetime'].min()} 到 {output_df['datetime'].max()}\n")
        
        return csv_path
        
    except Exception as e:
        print(f"  [失败] 转换失败: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """主函数"""
    print("=" * 70)
    print("VeighNa 数据格式转换工具")
    print("=" * 70)
    print(f"指数数据目录: {INDEX_DATA_DIR}")
    print(f"股票数据目录: {STOCK_DATA_DIR}")
    print(f"输出目录: {OUTPUT_DIR}")
    
    # 获取两个目录的所有Excel文件
    index_files = list(INDEX_DATA_DIR.glob("*.xlsx")) if INDEX_DATA_DIR.exists() else []
    stock_files = list(STOCK_DATA_DIR.glob("*.xlsx")) if STOCK_DATA_DIR.exists() else []
    excel_files = index_files + stock_files
    
    if not excel_files:
        print(f"\n未找到Excel文件")
        if not INDEX_DATA_DIR.exists():
            print(f"  [警告] 指数数据目录不存在: {INDEX_DATA_DIR}")
        if not STOCK_DATA_DIR.exists():
            print(f"  [警告] 股票数据目录不存在: {STOCK_DATA_DIR}")
        return
    
    print(f"\n找到 {len(excel_files)} 个Excel文件")
    print(f"  - 指数数据: {len(index_files)} 个")
    print(f"  - 股票数据: {len(stock_files)} 个")
    
    # 转换每个文件
    success_count = 0
    failed_count = 0
    
    for excel_file in excel_files:
        result = convert_excel_to_csv(excel_file)
        if result:
            success_count += 1
        else:
            failed_count += 1
    
    print("\n" + "=" * 70)
    print("转换完成！")
    print(f"成功: {success_count} 个")
    print(f"失败: {failed_count} 个")
    print(f"输出目录: {OUTPUT_DIR}")
    print("\n下一步:")
    print("1. 检查转换后的CSV文件")
    print("2. 在VeighNa Trader中使用'数据管理'模块导入CSV文件")
    print("3. 导入时参考每个文件对应的_info.txt文件中的合约信息")
    print("=" * 70)

if __name__ == "__main__":
    main()

