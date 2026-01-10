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

# 数据目录（支持多个数据源）
INDEX_DATA_DIR = Path(r"D:\vnpy\vnpy\trade\index_data")  # 指数数据
STOCK_DATA_DIR = Path(r"D:\vnpy\vnpy\trade\stock_data")  # 股票数据
OUTPUT_DIR = Path(r"D:\vnpy\vnpy\converted_data")  # 输出目录
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

def get_sheet_interval(sheet_name: str) -> str | None:
    """根据Sheet名称识别周期"""
    sheet_lower = sheet_name.lower()
    
    # 匹配中文周期名称（按优先级排序）
    if "月" in sheet_name or "month" in sheet_lower or "monthly" in sheet_lower:
        # 月线数据：VeighNa可能不支持MONTHLY周期，使用"MONTHLY"作为标识
        # 导入时需要用户手动选择DAILY周期
        return "MONTHLY"
    elif "周" in sheet_name or "weekly" in sheet_lower or "week" in sheet_lower or "w" == sheet_lower:
        return "WEEKLY"
    elif "日" in sheet_name or "daily" in sheet_lower or "d" == sheet_lower or "天" in sheet_name:
        return "DAILY"
    elif "小时" in sheet_name or "hour" in sheet_lower or "h" == sheet_lower:
        return "HOUR"
    elif "分钟" in sheet_name or "minute" in sheet_lower or "min" in sheet_lower:
        if "1" in sheet_name or "1m" in sheet_lower:
            return "MINUTE"
        else:
            return "MINUTE"  # 默认1分钟
    elif "分" in sheet_name:
        return "MINUTE"
    elif "时" in sheet_name:
        return "HOUR"
    
    return None

def convert_file(excel_path: Path, interval: str = None, sheet_name: str = None):
    """转换单个Excel文件或Sheet"""
    sheet_info = f" (Sheet: {sheet_name})" if sheet_name else ""
    print(f"\n处理: {excel_path.name}{sheet_info}")
    
    try:
        # 读取Excel（如果指定了Sheet名称，则读取指定Sheet）
        if sheet_name:
            df = pd.read_excel(excel_path, sheet_name=sheet_name)
        else:
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
        # 成交额（turnover）：如果原数据有则保留，如果没有则不创建（可选字段）
        # 注意：用户数据中成交额列名为 "amount"
        if "turnover" in df.columns:
            output["turnover"] = df["turnover"]
            print(f"  [信息] 找到成交额列（turnover），已保留")
        elif "amount" in df.columns:
            output["turnover"] = df["amount"]
            print(f"  [信息] 找到成交额列（amount），已转换")
        elif "成交额" in df.columns:
            output["turnover"] = df["成交额"]
            print(f"  [信息] 找到成交额列（中文），已转换")
        elif "成交金额" in df.columns:
            output["turnover"] = df["成交金额"]
            print(f"  [信息] 找到成交金额列，已转换")
        else:
            print(f"  [信息] 未找到成交额列（这是正常的，后续可以补充）")
        
        # 持仓量（open_interest）：股票数据通常没有，期货数据才有
        if "open_interest" in df.columns:
            output["open_interest"] = df["open_interest"]
        elif "持仓量" in df.columns:
            output["open_interest"] = df["持仓量"]
            print(f"  [信息] 找到持仓量列（中文），已转换")
        else:
            output["open_interest"] = 0  # 股票数据没有持仓量，默认0
        
        # 排序和清理
        output = output.sort_values("datetime").dropna()
        
        # 确定周期（优先级：指定周期 > Sheet名称识别 > 自动检测）
        if interval is None and sheet_name:
            # 尝试从Sheet名称识别周期
            detected_from_sheet = get_sheet_interval(sheet_name)
            if detected_from_sheet:
                interval = detected_from_sheet
                print(f"  [检测] 从Sheet名称识别周期: {interval}")
        
        if interval is None:
            detected_interval = detect_interval(output)
            print(f"  [检测] 自动检测周期: {detected_interval}")
            interval = detected_interval
        else:
            print(f"  [使用] 周期: {interval}")
        
        # 保存CSV（文件名包含周期信息，使用UTF-8无BOM）
        # 文件名格式：{原文件名}_{周期}.csv
        # 如果同一个文件有多个Sheet，周期会不同，所以文件名不会冲突
        base_name = excel_path.stem
        csv_filename = f"{base_name}_{interval}.csv"
        csv_path = OUTPUT_DIR / csv_filename
        
        # 检查文件是否已存在（可能之前已经处理过同一个周期）
        if csv_path.exists():
            print(f"  [警告] 文件已存在，将覆盖: {csv_path}")
        
        output.to_csv(csv_path, index=False, encoding="utf-8")
        
        print(f"  [成功] {csv_path}")
        print(f"  行数: {len(output)}, 时间: {output['datetime'].min()} 到 {output['datetime'].max()}")
        
        # 保存合约信息
        symbol, exchange = extract_symbol_and_exchange(excel_path.name)
        info_filename = f"{base_name}_{interval}_info.txt"
        info_path = OUTPUT_DIR / info_filename
        with open(info_path, "w", encoding="utf-8") as f:
            f.write(f"合约代码: {symbol}\n")
            f.write(f"交易所: {exchange}\n")
            f.write(f"周期: {interval}\n")
            if sheet_name:
                f.write(f"Sheet名称: {sheet_name}\n")
            f.write(f"VeighNa代码: {symbol}.{exchange}\n")
            f.write(f"数据行数: {len(output)}\n")
            f.write(f"时间范围: {output['datetime'].min()} 到 {output['datetime'].max()}\n")
            # 对于月线数据，提示用户导入时选择DAILY周期
            if interval == "MONTHLY":
                f.write(f"\n注意：这是月线数据，导入VeighNa时请选择 DAILY 周期（VeighNa可能不支持MONTHLY周期）\n")
            else:
                f.write(f"\n导入时在VeighNa中选择周期: {interval}\n")
        
        return True
        
    except Exception as e:
        print(f"  [失败] {e}")
        import traceback
        traceback.print_exc()
        return False

def extract_symbol_and_exchange(filename: str) -> tuple[str, str]:
    """从文件名提取合约代码和交易所"""
    # 支持多种文件名格式
    name = filename.replace("_数据.xlsx", "").replace("_技术数据.xlsx", "").replace("_data.xlsx", "").replace(".xlsx", "").replace(".csv", "")
    parts = name.split("_")
    
    # 股票数据格式：{代码}_{名称}_技术数据.xlsx（如：01952_云顶新耀_技术数据.xlsx）
    # 指数数据格式：{名称}_{代码}_data.xlsx（如：上证指数_sh000001_data.xlsx）
    if len(parts) >= 2:
        first_part = parts[0]
        # 如果第一部分是纯数字（可能是股票代码）
        if first_part.isdigit() or (first_part.startswith("^") and len(first_part) > 1):
            code_part = first_part  # 股票数据：代码在第一位
        else:
            code_part = parts[-1]  # 指数数据：代码在最后
    else:
        code_part = name
    
    # 合约代码映射
    SYMBOL_MAP = {
        "sh000001": ("000001", "SSE"),
        "sz399001": ("399001", "SZSE"),
        "sz399006": ("399006", "SZSE"),
        "sh000688": ("000688", "SSE"),
        "bj899050": ("899050", "BSE"),
        "^HSI": ("HSI", "GLOBAL"),
        "^HSTECH": ("HSTECH", "GLOBAL"),
        "^GSPC": ("GSPC", "GLOBAL"),
        "^IXIC": ("IXIC", "GLOBAL"),
        "^DJI": ("DJI", "GLOBAL"),
    }
    
    if code_part in SYMBOL_MAP:
        return SYMBOL_MAP[code_part]
    
    # 根据代码前缀判断交易所
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
        elif len(code_part) == 4 or len(code_part) == 5 or len(code_part) == 6:
            # 4-6位数字，可能是港股（如01952, 700, 09636等）
            if code_part.startswith("0") or code_part.startswith("7") or code_part.startswith("9"):
                return (code_part, "SEHK")  # 港股使用SEHK
            else:
                return (code_part, "SSE")  # 默认上交所
        else:
            return (code_part, "SSE")  # 默认上交所
    else:
        return (code_part, "SSE")  # 默认上交所

def main():
    """主函数"""
    print("=" * 70)
    print("VeighNa 多周期数据转换工具")
    print("=" * 70)
    print(f"指数数据目录: {INDEX_DATA_DIR}")
    print(f"股票数据目录: {STOCK_DATA_DIR}")
    print(f"输出目录: {OUTPUT_DIR}")
    print("\n支持的周期:")
    print("  - MINUTE (1分钟K线)")
    print("  - HOUR (1小时K线)")
    print("  - DAILY (日K线)")
    print("  - WEEKLY (周K线)")
    print("\n脚本会自动从Excel的Sheet（页签）中识别周期，每个Sheet代表一个周期")
    print("=" * 70)
    
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
    
    print(f"\n找到 {len(excel_files)} 个文件")
    print(f"  - 指数数据: {len(index_files)} 个")
    print(f"  - 股票数据: {len(stock_files)} 个")
    
    total_sheets = 0
    success_count = 0
    failed_count = 0
    
    for excel_file in excel_files:
        print(f"\n{'='*70}")
        print(f"处理文件: {excel_file.name}")
        
        try:
            # 读取Excel文件，获取所有Sheet名称
            xls = pd.ExcelFile(excel_file)
            sheet_names = xls.sheet_names
            print(f"  发现 {len(sheet_names)} 个Sheet（页签）: {sheet_names}")
            total_sheets += len(sheet_names)
            
            # 如果只有一个Sheet，尝试按旧方式处理
            if len(sheet_names) == 1:
                # 尝试从文件名提取周期信息
                interval = None
                filename_lower = excel_file.name.lower()
                
                if "1m" in filename_lower or "minute" in filename_lower or "分钟" in filename_lower:
                    interval = "MINUTE"
                elif "1h" in filename_lower or "hour" in filename_lower or "小时" in filename_lower:
                    interval = "HOUR"
                elif "1d" in filename_lower or "daily" in filename_lower or "日" in filename_lower:
                    interval = "DAILY"
                elif "1w" in filename_lower or "weekly" in filename_lower or "周" in filename_lower:
                    interval = "WEEKLY"
                
                if convert_file(excel_file, interval, sheet_names[0]):
                    success_count += 1
                else:
                    failed_count += 1
            else:
                # 多个Sheet，每个Sheet代表一个周期
                file_success = 0
                for sheet_name in sheet_names:
                    try:
                        if convert_file(excel_file, None, sheet_name):
                            file_success += 1
                            success_count += 1
                        else:
                            failed_count += 1
                    except Exception as e:
                        print(f"  [失败] Sheet '{sheet_name}' 处理失败: {e}")
                        failed_count += 1
                
                print(f"  文件处理完成: {file_success}/{len(sheet_names)} 个Sheet转换成功")
        
        except Exception as e:
            print(f"  [错误] 读取Excel文件失败: {e}")
            import traceback
            traceback.print_exc()
            failed_count += 1
    
    print("\n" + "=" * 70)
    print(f"批量转换完成！")
    print(f"  处理文件数: {len(excel_files)}")
    print(f"  处理Sheet数: {total_sheets}")
    print(f"  成功: {success_count} 个Sheet")
    print(f"  失败: {failed_count} 个Sheet")
    print(f"  输出目录: {OUTPUT_DIR}")
    print("\n下一步:")
    print("1. 检查转换后的CSV文件（文件名包含周期信息）")
    print("2. 在VeighNa Trader中使用'数据管理'模块导入CSV文件")
    print("3. 导入时在'周期'字段选择对应的周期（MINUTE/HOUR/DAILY/WEEKLY）")
    print("4. 参考每个文件对应的_info.txt文件中的合约信息")
    print("=" * 70)

if __name__ == "__main__":
    main()

