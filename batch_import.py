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
    name = filename.replace("_data.csv", "").replace(".csv", "")
    parts = name.split("_")
    
    if len(parts) >= 2:
        code_part = parts[-1]
    else:
        code_part = name
    
    if code_part in SYMBOL_MAP:
        return SYMBOL_MAP[code_part]
    
    # 如果没有映射，尝试从代码判断
    if code_part.startswith("sh"):
        return (code_part[2:], Exchange.SSE, name)
    elif code_part.startswith("sz"):
        return (code_part[2:], Exchange.SZSE, name)
    elif code_part.startswith("bj"):
        return (code_part[2:], Exchange.BSE, name)
    elif code_part.startswith("^"):
        return (code_part[1:], Exchange.GLOBAL, name)
    else:
        return (code_part, Exchange.SSE, name)

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
        print(f"\n处理: {csv_file.name}")
        
        try:
            # 提取合约信息
            symbol, exchange, display_name = extract_symbol_and_exchange(csv_file.name)
            print(f"  合约代码: {symbol}")
            print(f"  交易所: {exchange.value}")
            print(f"  名称: {display_name}")
            
            # 检测周期
            interval = detect_interval_from_csv(csv_file)
            print(f"  周期: {interval.value}")
            
            # 检测时间格式
            datetime_format = detect_datetime_format(csv_file)
            print(f"  时间格式: {datetime_format}")
            
            # 准备导入参数
            file_path = str(csv_file.absolute())
            
            # 表头信息（根据CSV文件的实际列名）
            header_dict = {
                "datetime": "datetime",
                "open": "open",
                "high": "high",
                "low": "low",
                "close": "close",
                "volume": "volume",
                "turnover": "",  # 可选
                "open_interest": "open_interest",  # 可选
            }
            
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
                "",                     # turnover_head (可选，留空)
                "open_interest",        # open_interest_head (可选)
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

