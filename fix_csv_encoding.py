"""
修复CSV文件编码问题
移除BOM标记，确保列名正确
"""
import pandas as pd
from pathlib import Path
import sys

if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

CSV_DIR = Path(r"D:\vnpy\vnpy\converted_data")
BACKUP_DIR = CSV_DIR / "backup"
BACKUP_DIR.mkdir(exist_ok=True)

def fix_csv_encoding(csv_path: Path):
    """修复CSV文件编码"""
    print(f"\n修复文件: {csv_path.name}")
    
    try:
        # 备份原文件
        backup_path = BACKUP_DIR / csv_path.name
        import shutil
        shutil.copy2(csv_path, backup_path)
        print(f"  已备份到: {backup_path}")
        
        # 读取CSV文件（尝试多种编码）
        df = None
        encodings = ['utf-8-sig', 'utf-8', 'gbk', 'gb2312']
        
        for enc in encodings:
            try:
                df = pd.read_csv(csv_path, encoding=enc)
                print(f"  使用编码 {enc} 读取成功")
                break
            except Exception as e:
                continue
        
        if df is None:
            print(f"  ✗ 无法读取文件")
            return False
        
        # 检查列名
        print(f"  原始列名: {list(df.columns)}")
        
        # 清理列名（移除BOM和空白字符）
        df.columns = df.columns.str.strip().str.replace('\ufeff', '', regex=False)
        print(f"  清理后列名: {list(df.columns)}")
        
        # 确保列名正确
        column_mapping = {
            'datetime': 'datetime',
            'date': 'datetime',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'volume': 'volume',
            'turnover': 'turnover',
            'open_interest': 'open_interest',
        }
        
        # 重命名列（如果列名不标准）
        rename_dict = {}
        for col in df.columns:
            col_clean = col.strip().replace('\ufeff', '')
            if col_clean.lower() in column_mapping:
                if col_clean != column_mapping[col_clean.lower()]:
                    rename_dict[col] = column_mapping[col_clean.lower()]
        
        if rename_dict:
            df = df.rename(columns=rename_dict)
            print(f"  重命名列: {rename_dict}")
        
        # 保存为UTF-8无BOM格式
        # 使用utf-8而不是utf-8-sig，避免BOM问题
        df.to_csv(csv_path, index=False, encoding='utf-8')
        print(f"  ✓ 已保存为UTF-8无BOM格式")
        
        # 验证保存的文件
        df_check = pd.read_csv(csv_path, encoding='utf-8')
        print(f"  验证列名: {list(df_check.columns)}")
        
        if 'datetime' in df_check.columns:
            print(f"  ✓ datetime列存在，修复成功")
            return True
        else:
            print(f"  ✗ datetime列不存在，修复失败")
            return False
        
    except Exception as e:
        print(f"  ✗ 修复失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """主函数"""
    print("=" * 70)
    print("CSV文件编码修复工具")
    print("=" * 70)
    print("此工具将:")
    print("1. 备份原始CSV文件")
    print("2. 移除BOM标记")
    print("3. 确保列名正确")
    print("4. 保存为UTF-8无BOM格式")
    print("=" * 70)
    
    csv_files = list(CSV_DIR.glob("*.csv"))
    
    if not csv_files:
        print(f"\n未找到CSV文件在 {CSV_DIR}")
        return
    
    print(f"\n找到 {len(csv_files)} 个CSV文件")
    
    success = 0
    for csv_file in csv_files:
        if fix_csv_encoding(csv_file):
            success += 1
    
    print("\n" + "=" * 70)
    print(f"修复完成: {success}/{len(csv_files)} 个文件")
    print(f"备份目录: {BACKUP_DIR}")
    print("\n现在可以重新在VeighNa中导入数据了")
    print("=" * 70)

if __name__ == "__main__":
    main()

