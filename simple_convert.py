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

DATA_DIR = Path(r"D:\tradegit\trade\index_data")
OUTPUT_DIR = Path(r"D:\vnpy\vnpy\converted_data")
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
        if "turnover" in df.columns:
            output["turnover"] = df["turnover"]
        
        if "open_interest" in df.columns:
            output["open_interest"] = df["open_interest"]
        else:
            output["open_interest"] = 0
        
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
    
    excel_files = list(DATA_DIR.glob("*.xlsx"))
    print(f"\n找到 {len(excel_files)} 个文件")
    
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

