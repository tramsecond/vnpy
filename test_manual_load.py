"""手动测试策略加载流程"""
import sys
from pathlib import Path
import importlib
import traceback

sys.path.insert(0, str(Path.cwd()))

print(f"当前目录: {Path.cwd()}")

# 模拟BacktesterEngine的加载流程
strategies_path = Path.cwd() / "strategies"
print(f"策略目录: {strategies_path}")
print(f"目录存在: {strategies_path.exists()}")

if strategies_path.exists():
    print(f"\n目录内容:")
    for f in strategies_path.iterdir():
        print(f"  - {f.name}")

# 尝试导入
module_name = "strategies.atr_rsi_position_strategy"
print(f"\n尝试导入: {module_name}")

try:
    module = importlib.import_module(module_name)
    print(f"[OK] 模块导入成功")
    
    # 查找策略类
    from vnpy_ctastrategy import CtaTemplate
    
    found_classes = []
    for name in dir(module):
        value = getattr(module, name)
        if isinstance(value, type) and issubclass(value, CtaTemplate) and value != CtaTemplate:
            found_classes.append(name)
            print(f"[OK] 找到策略类: {name}")
    
    if not found_classes:
        print("[ERROR] 没有找到策略类")
        
except Exception as e:
    print(f"[ERROR] 导入失败: {e}")
    traceback.print_exc()

