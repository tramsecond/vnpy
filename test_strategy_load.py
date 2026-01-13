"""测试策略加载"""
import sys
from pathlib import Path

# 添加当前目录到路径
sys.path.insert(0, str(Path.cwd()))

print(f"当前工作目录: {Path.cwd()}")
print(f"Python路径: {sys.path[:3]}")

# 测试导入
try:
    from strategies.atr_rsi_position_strategy import AtrRsiPositionStrategy
    print(f"[OK] 成功导入策略: {AtrRsiPositionStrategy.__name__}")
    print(f"   策略参数: {AtrRsiPositionStrategy.parameters}")
except Exception as e:
    print(f"[ERROR] 导入失败: {e}")
    import traceback
    traceback.print_exc()

# 测试BacktesterEngine加载
try:
    from vnpy.trader.engine import MainEngine
    from vnpy.event import EventEngine
    from vnpy_ctabacktester import APP_NAME
    
    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    
    from vnpy_ctabacktester import CtaBacktesterApp
    main_engine.add_app(CtaBacktesterApp)
    
    backtester_engine = main_engine.get_engine(APP_NAME)
    backtester_engine.init_engine()
    
    strategies = backtester_engine.get_strategy_class_names()
    print(f"\n加载的策略列表 ({len(strategies)}个):")
    for s in sorted(strategies):
        print(f"  - {s}")
    
    if "AtrRsiPositionStrategy" in strategies:
        print("\n[OK] AtrRsiPositionStrategy 已成功加载！")
    else:
        print("\n[ERROR] AtrRsiPositionStrategy 未加载")
        
except Exception as e:
    print(f"\n[ERROR] BacktesterEngine测试失败: {e}")
    import traceback
    traceback.print_exc()

