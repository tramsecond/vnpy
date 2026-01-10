"""
批量导入CSV数据到VeighNa数据库
自动识别合约信息并批量导入
"""
import sys
from pathlib import Path
from datetime import datetime

# 设置输出编码
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# 导入VeighNa模块
from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.constant import Exchange, Interval
from vnpy_datamanager import DataManagerApp

CSV_DIR = Path(r"D:\vnpy\vnpy\converted_data")

# 合约信息映射（从文件名提取）
SYMBOL_MAP = {
    "sh000001": ("000001", Exchange.SSE, "上证指数"),
    "sz399001": ("399001", Exchange.SZSE, "深证成指"),
    "sz399006": ("399006", Exchange.SZSE, "创业板指"),
    "sh000688": ("000688", Exchange.SSE, "科创50"),
    "bj899050": ("899050", Exchange.BSE, "北证50"),
    "^HSI": ("HSI", Exchange.GLOBAL, "恒生指数"),
    "^HSTECH": ("HSTECH", Exchange.GLOBAL, "恒生科技"),
    "^GSPC": ("GSPC", Exchange.GLOBAL, "标普500"),
    "^IXIC": ("IXIC", Exchange.GLOBAL, "纳斯达克"),
    "^DJI": ("DJI", Exchange.GLOBAL, "道琼斯"),
}

def extract_symbol_and_exchange(filename: str) -> tuple[str, Exchange, str]:
    """从文件名提取合约代码和交易所"""
    # 支持多种文件名格式
    name = filename.replace("_数据.csv", "").replace("_技术数据.csv", "").replace("_data.csv", "").replace(".csv", "").replace(".xlsx", "")
    parts = name.split("_")
    
    # 股票数据格式：{代码}_{名称}_技术数据.xlsx（如：01952_云顶新耀_技术数据.xlsx）
    # 指数数据格式：{名称}_{代码}_data.xlsx（如：上证指数_sh000001_data.xlsx）
    if len(parts) >= 2:
        first_part = parts[0]
        # 如果第一部分是纯数字（可能是股票代码）
        if first_part.isdigit() or (first_part.startswith("^") and len(first_part) > 1):
            code_part = first_part  # 股票数据：代码在第一位
            display_name = parts[1] if len(parts) > 1 else name  # 股票名称
        else:
            code_part = parts[-1]  # 指数数据：代码在最后
            display_name = parts[0] if len(parts) > 1 else name  # 指数名称
    else:
        code_part = name
        display_name = name
    
    if code_part in SYMBOL_MAP:
        symbol, exchange, mapped_name = SYMBOL_MAP[code_part]
        return (symbol, exchange, mapped_name)
    
    # 如果没有映射，尝试从代码判断
    if code_part.startswith("sh"):
        return (code_part[2:], Exchange.SSE, display_name)
    elif code_part.startswith("sz"):
        return (code_part[2:], Exchange.SZSE, display_name)
    elif code_part.startswith("bj"):
        return (code_part[2:], Exchange.BSE, display_name)
    elif code_part.startswith("^"):
        return (code_part[1:], Exchange.GLOBAL, display_name)
    elif code_part.isdigit():
        # 纯数字代码，根据前缀判断
        if code_part.startswith("6"):  # 6开头，上交所
            return (code_part, Exchange.SSE, display_name)
        elif code_part.startswith("0") or code_part.startswith("3"):  # 0或3开头，深交所
            return (code_part, Exchange.SZSE, display_name)
        elif code_part.startswith("688") or code_part.startswith("689"):  # 科创板
            return (code_part, Exchange.SSE, display_name)
        elif code_part.startswith("43") or code_part.startswith("83") or code_part.startswith("87"):  # 北交所
            return (code_part, Exchange.BSE, display_name)
        elif len(code_part) == 4 or len(code_part) == 5 or len(code_part) == 6:
            # 4-6位数字，可能是港股（如01952, 700, 09636等）
            if code_part.startswith("0") or code_part.startswith("7") or code_part.startswith("9"):
                return (code_part, Exchange.SEHK, display_name)  # 港股使用SEHK
            else:
                return (code_part, Exchange.SSE, display_name)  # 默认上交所
        else:
            return (code_part, Exchange.SSE, display_name)  # 默认上交所
    else:
        return (code_part, Exchange.SSE, display_name)  # 默认上交所

def detect_interval_from_csv(csv_path: Path) -> Interval:
    """从CSV文件检测周期"""
    import pandas as pd
    
    try:
        df = pd.read_csv(csv_path, encoding='utf-8', nrows=10)
        if "datetime" not in df.columns:
            return Interval.DAILY
        
        df["datetime"] = pd.to_datetime(df["datetime"])
        if len(df) < 2:
            return Interval.DAILY
        
        time_diffs = df["datetime"].diff().dropna()
        if len(time_diffs) == 0:
            return Interval.DAILY
        
        avg_diff_seconds = time_diffs.mean().total_seconds()
        
        if avg_diff_seconds < 120:
            return Interval.MINUTE
        elif avg_diff_seconds < 7200:
            return Interval.HOUR
        elif avg_diff_seconds < 86400 * 2:
            return Interval.DAILY
        elif avg_diff_seconds < 86400 * 8:
            return Interval.WEEKLY
        else:
            return Interval.DAILY
    except:
        return Interval.DAILY

def detect_datetime_format(csv_path: Path) -> str:
    """检测CSV文件中的时间格式"""
    import pandas as pd
    
    try:
        df = pd.read_csv(csv_path, encoding='utf-8', nrows=1)
        if "datetime" not in df.columns:
            return "%Y-%m-%d"
        
        sample_time = str(df["datetime"].iloc[0])
        if len(sample_time) == 10:  # 只有日期
            return "%Y-%m-%d"
        elif " " in sample_time and ":" in sample_time:
            if sample_time.count(":") == 2:
                return "%Y-%m-%d %H:%M:%S"
            else:
                return "%Y-%m-%d %H:%M"
        else:
            return "%Y-%m-%d"
    except:
        return "%Y-%m-%d"

def detect_interval_from_filename(filename: str) -> Interval | None:
    """从文件名提取周期信息"""
    filename_upper = filename.upper()
    
    # 文件名格式：xxx_DAILY.csv, xxx_HOUR.csv, xxx_WEEKLY.csv, xxx_MONTHLY.csv
    # 注意：文件名中周期标识在最后，如：000063_中兴通讯_技术数据_DAILY.csv
    if "_DAILY" in filename_upper:
        return Interval.DAILY
    elif "_HOUR" in filename_upper:
        return Interval.HOUR
    elif "_WEEKLY" in filename_upper:
        return Interval.WEEKLY
    elif "_MINUTE" in filename_upper:
        return Interval.MINUTE
    elif "_MONTHLY" in filename_upper:
        # 月线数据，VeighNa可能不支持MONTHLY周期，但我们可以使用DAILY作为替代
        # 或者直接使用DAILY，因为月线数据在导入时可能需要选择DAILY周期
        return Interval.DAILY
    elif "_D_" in filename_upper:
        return Interval.DAILY
    elif "_H_" in filename_upper or "小时" in filename:
        return Interval.HOUR
    elif "_W_" in filename_upper or "_周" in filename:
        return Interval.WEEKLY
    elif "_M_" in filename_upper or "分钟" in filename:
        return Interval.MINUTE
    elif "月" in filename:
        return Interval.DAILY  # 月线数据使用DAILY
    
    return None

def detect_csv_columns(csv_path: Path) -> dict:
    """检测CSV文件中的列名"""
    import pandas as pd
    
    try:
        df = pd.read_csv(csv_path, encoding='utf-8', nrows=1)
        columns = [col.lower().strip() for col in df.columns]
        
        result = {
            "turnover": "",
            "open_interest": "",
        }
        
        # 检测turnover列（可能的列名：turnover, amount, 成交额, 成交金额等）
        turnover_names = ["turnover", "amount", "成交额", "成交金额", "成交"]
        for col in df.columns:
            col_lower = col.lower().strip()
            if col_lower in turnover_names or "成交额" in col or "成交金额" in col:
                result["turnover"] = col
                break
        
        # 检测open_interest列（可能的列名：open_interest, 持仓量, 持仓等）
        oi_names = ["open_interest", "openinterest", "持仓量", "持仓", "interest"]
        for col in df.columns:
            col_lower = col.lower().strip()
            if col_lower in oi_names or "持仓量" in col or "持仓" in col:
                result["open_interest"] = col
                break
        
        return result
    except:
        return {"turnover": "", "open_interest": ""}

def batch_import():
    """批量导入CSV文件"""
    print("=" * 70)
    print("VeighNa 批量数据导入工具")
    print("=" * 70)
    
    # 创建引擎
    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    data_manager_engine = main_engine.add_app(DataManagerApp)
    
    # 获取所有CSV文件
    csv_files = [f for f in CSV_DIR.glob("*.csv") if not f.name.startswith(".")]
    
    if not csv_files:
        print(f"\n未找到CSV文件在 {CSV_DIR}")
        return
    
    print(f"\n找到 {len(csv_files)} 个CSV文件")
    print("-" * 70)
    
    success_count = 0
    failed_count = 0
    
    for csv_file in csv_files:
        # 跳过MONTHLY（月线）数据，不导入
        if "_MONTHLY" in csv_file.name.upper() or "月" in csv_file.name:
            print(f"\n跳过: {csv_file.name} (MONTHLY周期，已忽略)")
            failed_count += 1
            continue
        
        # 跳过WEEKLY（周线）数据，不导入
        if "_WEEKLY" in csv_file.name.upper() or "_W_" in csv_file.name.upper() or "周" in csv_file.name:
            print(f"\n跳过: {csv_file.name} (WEEKLY周期，已忽略)")
            failed_count += 1
            continue
        
        print(f"\n处理: {csv_file.name}")
        
        try:
            # 提取合约信息
            symbol, exchange, display_name = extract_symbol_and_exchange(csv_file.name)
            print(f"  合约代码: {symbol}")
            print(f"  交易所: {exchange.value}")
            print(f"  名称: {display_name}")
            
            # 优先从文件名提取周期，如果失败则从数据检测
            interval = detect_interval_from_filename(csv_file.name)
            if interval is None:
                interval = detect_interval_from_csv(csv_file)
                print(f"  周期: {interval.value} (从数据检测)")
            else:
                print(f"  周期: {interval.value} (从文件名识别)")
            
            # 检测时间格式
            datetime_format = detect_datetime_format(csv_file)
            print(f"  时间格式: {datetime_format}")
            
            # 检测CSV文件中的列名（特别是turnover和open_interest）
            detected_columns = detect_csv_columns(csv_file)
            turnover_head = detected_columns["turnover"]
            open_interest_head = detected_columns["open_interest"]
            
            if turnover_head:
                print(f"  检测到成交额列: {turnover_head}")
            else:
                print(f"  未检测到成交额列（将留空）")
            
            if open_interest_head:
                print(f"  检测到持仓量列: {open_interest_head}")
            else:
                print(f"  未检测到持仓量列（将留空，股票数据正常）")
            
            # 准备导入参数
            file_path = str(csv_file.absolute())
            
            # 调用导入方法
            print(f"  开始导入...")
            # 参数顺序：file_path, symbol, exchange, interval, tz_name, datetime_head, open_head, high_head, low_head, close_head, volume_head, turnover_head, open_interest_head, datetime_format
            start, end, count = data_manager_engine.import_data_from_csv(
                file_path,              # file_path
                symbol,                 # symbol
                exchange,               # exchange
                interval,               # interval
                "Asia/Shanghai",        # tz_name (时区)
                "datetime",             # datetime_head
                "open",                 # open_head
                "high",                 # high_head
                "low",                  # low_head
                "close",                # close_head
                "volume",               # volume_head
                turnover_head,          # turnover_head (如果CSV中有则导入，否则留空)
                open_interest_head,     # open_interest_head (如果CSV中有则导入，否则留空)
                datetime_format         # datetime_format (时间格式，最后一个)
            )
            
            print(f"  ✓ 导入成功!")
            print(f"    时间范围: {start} 到 {end}")
            print(f"    数据条数: {count}")
            success_count += 1
            
        except Exception as e:
            print(f"  ✗ 导入失败: {e}")
            import traceback
            traceback.print_exc()
            failed_count += 1
    
    print("\n" + "=" * 70)
    print("批量导入完成!")
    print(f"成功: {success_count} 个")
    print(f"失败: {failed_count} 个")
    print("=" * 70)
    
    # 关闭引擎
    main_engine.close()

if __name__ == "__main__":
    batch_import()

