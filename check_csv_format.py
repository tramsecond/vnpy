"""
检查CSV文件格式，确保符合VeighNa导入要求
"""
import pandas as pd
from pathlib import Path
import sys

if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

CSV_DIR = Path(r"D:\vnpy\vnpy\converted_data")

def check_csv(csv_path: Path):
    """检查CSV文件格式"""
    print(f"\n检查文件: {csv_path.name}")
    print("-" * 60)
    
    try:
        # 读取CSV文件
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        
        print(f"✓ 文件读取成功")
        print(f"  行数: {len(df)}")
        print(f"  列数: {len(df.columns)}")
        print(f"\n列名:")
        for i, col in enumerate(df.columns, 1):
            print(f"  {i}. {col}")
        
        # 检查必需的列
        required_cols = ["datetime", "open", "high", "low", "close", "volume"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            print(f"\n✗ 缺少必需的列: {missing_cols}")
        else:
            print(f"\n✓ 所有必需的列都存在")
        
        # 检查时间列
        if "datetime" in df.columns:
            print(f"\n时间列检查:")
            print(f"  数据类型: {df['datetime'].dtype}")
            print(f"  前3个值: {df['datetime'].head(3).tolist()}")
            
            # 尝试转换为datetime
            try:
                df['datetime'] = pd.to_datetime(df['datetime'])
                print(f"  ✓ 时间格式正确，可以解析")
                print(f"  时间范围: {df['datetime'].min()} 到 {df['datetime'].max()}")
            except Exception as e:
                print(f"  ✗ 时间格式错误: {e}")
        else:
            # 检查是否有date列
            if "date" in df.columns:
                print(f"\n⚠ 注意: CSV文件使用 'date' 列，不是 'datetime'")
                print(f"  在VeighNa导入时，表头信息应该填写: datetime: date")
        
        # 检查数值列
        print(f"\n数值列检查:")
        for col in ["open", "high", "low", "close", "volume"]:
            if col in df.columns:
                print(f"  {col}: {df[col].dtype}, 非空值: {df[col].notna().sum()}/{len(df)}")
            else:
                print(f"  ✗ {col}: 缺失")
        
        # 生成表头信息配置
        print(f"\n" + "=" * 60)
        print("VeighNa导入配置建议:")
        print("=" * 60)
        print("\n表头信息（复制到VeighNa导入对话框）:")
        print("-" * 60)
        
        # 确定datetime列的实际名称
        datetime_col = "datetime"
        if "datetime" not in df.columns and "date" in df.columns:
            datetime_col = "date"
        
        print(f"datetime: {datetime_col}")
        print(f"open: open" if "open" in df.columns else "open: ")
        print(f"high: high" if "high" in df.columns else "high: ")
        print(f"low: low" if "low" in df.columns else "low: ")
        print(f"close: close" if "close" in df.columns else "close: ")
        print(f"volume: volume" if "volume" in df.columns else "volume: ")
        if "open_interest" in df.columns:
            print(f"open_interest: open_interest")
        else:
            print(f"open_interest: ")
        
        # 时间格式建议
        print(f"\n时间格式建议:")
        if datetime_col in df.columns:
            sample_time = str(df[datetime_col].iloc[0])
            if len(sample_time) == 10:  # 只有日期
                print(f"  %Y-%m-%d")
            elif " " in sample_time and ":" in sample_time:
                if sample_time.count(":") == 2:
                    print(f"  %Y-%m-%d %H:%M:%S")
                else:
                    print(f"  %Y-%m-%d %H:%M")
            else:
                print(f"  请根据实际格式调整")
        
        print("\n" + "=" * 60)
        
    except Exception as e:
        print(f"✗ 检查失败: {e}")
        import traceback
        traceback.print_exc()

def main():
    """主函数"""
    print("=" * 60)
    print("CSV文件格式检查工具")
    print("=" * 60)
    
    csv_files = list(CSV_DIR.glob("*.csv"))
    
    if not csv_files:
        print(f"\n未找到CSV文件在 {CSV_DIR}")
        return
    
    print(f"\n找到 {len(csv_files)} 个CSV文件")
    
    for csv_file in csv_files:
        check_csv(csv_file)
    
    print("\n" + "=" * 60)
    print("检查完成！")
    print("=" * 60)

if __name__ == "__main__":
    main()

