"""
多周期数据转换脚本
支持将不同周期的Excel数据转换为VeighNa需要的CSV格式
"""
import pandas as pd
from pathlib import Path
import sys
from datetime import datetime

# 设置输出编码
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

DATA_DIR = Path(r"D:\tradegit\trade\index_data")
OUTPUT_DIR = Path(r"D:\vnpy\vnpy\converted_data")
OUTPUT_DIR.mkdir(exist_ok=True)

# 周期检测规则（根据时间间隔自动判断）
def detect_interval(df: pd.DataFrame) -> str:
    """根据时间间隔自动检测周期"""
    if len(df) < 2:
        return "DAILY"  # 默认日线
    
    # 计算时间差
    time_diffs = df["datetime"].diff().dropna()
    
    if len(time_diffs) == 0:
        return "DAILY"
    
    # 计算平均时间间隔（秒）
    avg_diff_seconds = time_diffs.mean().total_seconds()
    
    # 判断周期
    if avg_diff_seconds < 120:  # 小于2分钟，可能是1分钟
        return "MINUTE"
    elif avg_diff_seconds < 7200:  # 小于2小时，可能是1小时
        return "HOUR"
    elif avg_diff_seconds < 86400 * 2:  # 小于2天，可能是日线
        return "DAILY"
    elif avg_diff_seconds < 86400 * 8:  # 小于8天，可能是周线
        return "WEEKLY"
    else:
        return "DAILY"  # 默认日线

def convert_file(excel_path: Path, interval: str = None):
    """转换单个Excel文件"""
    print(f"\n处理: {excel_path.name}")
    
    try:
        # 读取Excel
        df = pd.read_excel(excel_path)
        print(f"  原始列: {list(df.columns)[:10]}...")
        
        # 创建输出DataFrame
        output = pd.DataFrame()
        
        # 处理时间列
        if "date" in df.columns:
            output["datetime"] = pd.to_datetime(df["date"])
        elif "datetime" in df.columns:
            output["datetime"] = pd.to_datetime(df["datetime"])
        else:
            print("  [错误] 找不到时间列 (date 或 datetime)")
            return False
        
        # 处理价格列
        for col in ["open", "high", "low", "close"]:
            if col in df.columns:
                output[col] = df[col]
            else:
                print(f"  [错误] 找不到 {col} 列")
                return False
        
        # 处理成交量
        if "volume" in df.columns:
            output["volume"] = df["volume"]
        else:
            output["volume"] = 0
            print("  [警告] 没有volume列，设置为0")
        
        # 处理可选列
        if "turnover" in df.columns:
            output["turnover"] = df["turnover"]
        
        if "open_interest" in df.columns:
            output["open_interest"] = df["open_interest"]
        else:
            output["open_interest"] = 0
        
        # 排序和清理
        output = output.sort_values("datetime").dropna()
        
        # 自动检测周期（如果未指定）
        if interval is None:
            detected_interval = detect_interval(output)
            print(f"  [检测] 自动检测周期: {detected_interval}")
            interval = detected_interval
        else:
            print(f"  [指定] 使用周期: {interval}")
        
        # 保存CSV（文件名包含周期信息，使用UTF-8无BOM）
        csv_filename = f"{excel_path.stem}_{interval}.csv"
        csv_path = OUTPUT_DIR / csv_filename
        output.to_csv(csv_path, index=False, encoding="utf-8")
        
        print(f"  [成功] {csv_path}")
        print(f"  行数: {len(output)}, 时间: {output['datetime'].min()} 到 {output['datetime'].max()}")
        
        # 保存合约信息
        symbol, exchange = extract_symbol_and_exchange(excel_path.name)
        info_filename = f"{excel_path.stem}_{interval}_info.txt"
        info_path = OUTPUT_DIR / info_filename
        with open(info_path, "w", encoding="utf-8") as f:
            f.write(f"合约代码: {symbol}\n")
            f.write(f"交易所: {exchange}\n")
            f.write(f"周期: {interval}\n")
            f.write(f"VeighNa代码: {symbol}.{exchange}\n")
            f.write(f"数据行数: {len(output)}\n")
            f.write(f"时间范围: {output['datetime'].min()} 到 {output['datetime'].max()}\n")
            f.write(f"\n导入时在VeighNa中选择周期: {interval}\n")
        
        return True
        
    except Exception as e:
        print(f"  [失败] {e}")
        import traceback
        traceback.print_exc()
        return False

def extract_symbol_and_exchange(filename: str) -> tuple[str, str]:
    """从文件名提取合约代码和交易所"""
    name = filename.replace("_data.xlsx", "").replace(".xlsx", "")
    parts = name.split("_")
    
    if len(parts) >= 2:
        code_part = parts[-1]
    else:
        code_part = name
    
    # 合约代码映射
    SYMBOL_MAP = {
        "sh000001": ("000001", "SSE"),
        "sz399001": ("399001", "SZSE"),
        "sz399006": ("399006", "SZSE"),
        "sh000688": ("000688", "SSE"),
        "bj899050": ("899050", "BSE"),
        "^HSI": ("HSI", "NONE"),
        "^HSTECH": ("HSTECH", "NONE"),
        "^GSPC": ("GSPC", "NONE"),
        "^IXIC": ("IXIC", "NONE"),
        "^DJI": ("DJI", "NONE"),
    }
    
    if code_part in SYMBOL_MAP:
        return SYMBOL_MAP[code_part]
    
    if code_part.startswith("sh"):
        return (code_part[2:], "SSE")
    elif code_part.startswith("sz"):
        return (code_part[2:], "SZSE")
    elif code_part.startswith("bj"):
        return (code_part[2:], "BSE")
    elif code_part.startswith("^"):
        return (code_part[1:], "NONE")
    else:
        return (code_part, "SSE")

def main():
    """主函数"""
    print("=" * 70)
    print("VeighNa 多周期数据转换工具")
    print("=" * 70)
    print(f"数据目录: {DATA_DIR}")
    print(f"输出目录: {OUTPUT_DIR}")
    print("\n支持的周期:")
    print("  - MINUTE (1分钟K线)")
    print("  - HOUR (1小时K线)")
    print("  - DAILY (日K线)")
    print("  - WEEKLY (周K线)")
    print("\n脚本会自动检测周期，也可以手动指定")
    print("=" * 70)
    
    excel_files = list(DATA_DIR.glob("*.xlsx"))
    
    if not excel_files:
        print(f"\n未找到Excel文件在 {DATA_DIR}")
        return
    
    print(f"\n找到 {len(excel_files)} 个文件")
    
    # 如果数据是按周期分类的，可以指定周期
    # 例如：如果文件名包含周期信息，可以提取
    # 这里先使用自动检测
    
    success = 0
    for f in excel_files:
        # 可以从文件名提取周期信息
        interval = None
        filename_lower = f.name.lower()
        
        if "1m" in filename_lower or "minute" in filename_lower or "分钟" in filename_lower:
            interval = "MINUTE"
        elif "1h" in filename_lower or "hour" in filename_lower or "小时" in filename_lower:
            interval = "HOUR"
        elif "1d" in filename_lower or "daily" in filename_lower or "日" in filename_lower:
            interval = "DAILY"
        elif "1w" in filename_lower or "weekly" in filename_lower or "周" in filename_lower:
            interval = "WEEKLY"
        
        if convert_file(f, interval):
            success += 1
    
    print("\n" + "=" * 70)
    print(f"完成: {success}/{len(excel_files)} 个文件转换成功")
    print(f"输出目录: {OUTPUT_DIR}")
    print("\n下一步:")
    print("1. 检查转换后的CSV文件（文件名包含周期信息）")
    print("2. 在VeighNa Trader中使用'数据管理'模块导入CSV文件")
    print("3. 导入时在'周期'字段选择对应的周期（MINUTE/HOUR/DAILY/WEEKLY）")
    print("4. 参考每个文件对应的_info.txt文件中的合约信息")
    print("=" * 70)

if __name__ == "__main__":
    main()

