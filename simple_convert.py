"""
简化版数据转换脚本 - 直接处理标准格式的Excel文件
"""
import pandas as pd
from pathlib import Path
import sys

# 设置输出编码
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# 数据目录（支持多个数据源）
INDEX_DATA_DIR = Path(r"D:\vnpy\vnpy\trade\index_data")  # 指数数据
STOCK_DATA_DIR = Path(r"D:\vnpy\vnpy\trade\stock_data")  # 股票数据
OUTPUT_DIR = Path(r"D:\vnpy\vnpy\converted_data")  # 输出目录
OUTPUT_DIR.mkdir(exist_ok=True)

def convert_file(excel_path):
    """转换单个Excel文件"""
    print(f"\n处理: {excel_path.name}")
    
    try:
        # 读取Excel
        df = pd.read_excel(excel_path)
        print(f"  原始列: {list(df.columns)[:10]}...")  # 只显示前10列
        
        # 创建输出DataFrame
        output = pd.DataFrame()
        
        # 处理时间列
        if "date" in df.columns:
            output["datetime"] = pd.to_datetime(df["date"])
        elif "datetime" in df.columns:
            output["datetime"] = pd.to_datetime(df["datetime"])
        else:
            print("  错误: 找不到时间列 (date 或 datetime)")
            return False
        
        # 处理价格列
        for col in ["open", "high", "low", "close"]:
            if col in df.columns:
                output[col] = df[col]
            else:
                print(f"  错误: 找不到 {col} 列")
                return False
        
        # 处理成交量
        if "volume" in df.columns:
            output["volume"] = df["volume"]
        else:
            output["volume"] = 0
            print("  警告: 没有volume列，设置为0")
        
        # 处理可选列
        # 成交额（turnover）：如果原数据有则保留，如果没有则不创建（可选字段）
        # 注意：用户数据中成交额列名为 "amount"
        if "turnover" in df.columns:
            output["turnover"] = df["turnover"]
            print("  [信息] 找到成交额列（turnover），已保留")
        elif "amount" in df.columns:
            output["turnover"] = df["amount"]
            print("  [信息] 找到成交额列（amount），已转换")
        elif "成交额" in df.columns:
            output["turnover"] = df["成交额"]
            print("  [信息] 找到成交额列（中文），已转换")
        elif "成交金额" in df.columns:
            output["turnover"] = df["成交金额"]
            print("  [信息] 找到成交金额列，已转换")
        else:
            print("  [信息] 未找到成交额列（这是正常的，后续可以补充）")
        
        # 持仓量（open_interest）：股票数据通常没有，期货数据才有
        if "open_interest" in df.columns:
            output["open_interest"] = df["open_interest"]
        elif "持仓量" in df.columns:
            output["open_interest"] = df["持仓量"]
            print("  [信息] 找到持仓量列（中文），已转换")
        else:
            output["open_interest"] = 0  # 股票数据没有持仓量，默认0
        
        # 排序和清理
        output = output.sort_values("datetime").dropna()
        
        # 保存CSV（使用UTF-8无BOM，避免编码问题）
        csv_path = OUTPUT_DIR / (excel_path.stem + ".csv")
        output.to_csv(csv_path, index=False, encoding="utf-8")
        
        print(f"  成功: {csv_path}")
        print(f"  行数: {len(output)}, 时间: {output['datetime'].min()} 到 {output['datetime'].max()}")
        
        return True
        
    except Exception as e:
        print(f"  失败: {e}")
        return False

def main():
    print("=" * 60)
    print("VeighNa 数据转换工具")
    print("=" * 60)
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
    
    print(f"\n找到 {len(excel_files)} 个文件")
    print(f"  - 指数数据: {len(index_files)} 个")
    print(f"  - 股票数据: {len(stock_files)} 个")
    
    success = 0
    for f in excel_files:
        if convert_file(f):
            success += 1
    
    print("\n" + "=" * 60)
    print(f"完成: {success}/{len(excel_files)} 个文件转换成功")
    print(f"输出目录: {OUTPUT_DIR}")
    print("=" * 60)

if __name__ == "__main__":
    main()

