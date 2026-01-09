"""
测试 run.py 的逻辑
验证所有导入和条件判断是否正确
"""
import sys
import traceback

print("=" * 60)
print("VeighNa Trader 启动脚本测试")
print("=" * 60)

# 测试1: 核心模块
print("\n[测试1] 核心模块导入")
try:
    from vnpy.event import EventEngine
    from vnpy.trader.engine import MainEngine
    from vnpy.trader.ui import MainWindow, create_qapp
    print("  ✓ 核心模块导入成功")
    core_ok = True
except Exception as e:
    print(f"  ✗ 核心模块导入失败: {e}")
    traceback.print_exc()
    core_ok = False
    sys.exit(1)

# 测试2: CTP接口
print("\n[测试2] CTP交易接口")
try:
    from vnpy_ctp import CtpGateway
    HAS_CTP = True
    print("  ✓ CTP接口已安装，HAS_CTP = True")
except ImportError:
    HAS_CTP = False
    print("  ✗ CTP接口未安装，HAS_CTP = False")

# 测试3: CTA策略
print("\n[测试3] CTA策略模块")
try:
    from vnpy_ctastrategy import CtaStrategyApp
    HAS_CTA_STRATEGY = True
    print("  ✓ CTA策略已安装，HAS_CTA_STRATEGY = True")
except ImportError:
    HAS_CTA_STRATEGY = False
    print("  ✗ CTA策略未安装，HAS_CTA_STRATEGY = False")

# 测试4: CTA回测
print("\n[测试4] CTA回测模块")
try:
    from vnpy_ctabacktester import CtaBacktesterApp
    HAS_CTA_BACKTESTER = True
    print("  ✓ CTA回测已安装，HAS_CTA_BACKTESTER = True")
except ImportError:
    HAS_CTA_BACKTESTER = False
    print("  ✗ CTA回测未安装，HAS_CTA_BACKTESTER = False")

# 测试5: 数据管理
print("\n[测试5] 数据管理模块")
try:
    from vnpy_datamanager import DataManagerApp
    HAS_DATA_MANAGER = True
    print("  ✓ 数据管理已安装，HAS_DATA_MANAGER = True")
except ImportError:
    HAS_DATA_MANAGER = False
    print("  ✗ 数据管理未安装，HAS_DATA_MANAGER = False")

# 测试6: 模拟main函数逻辑
print("\n[测试6] 模拟启动逻辑")
if core_ok:
    try:
        # 不实际创建GUI，只验证逻辑
        print("  模拟创建事件引擎...")
        event_engine = EventEngine()
        print("  ✓ 事件引擎创建成功")
        
        print("  模拟创建主引擎...")
        main_engine = MainEngine(event_engine)
        print("  ✓ 主引擎创建成功")
        
        # 统计要添加的模块
        gateway_count = 0
        app_count = 0
        
        if HAS_CTP:
            print("  → 将添加CTP交易接口")
            gateway_count += 1
        
        if HAS_CTA_STRATEGY:
            print("  → 将添加CTA策略模块")
            app_count += 1
        
        if HAS_CTA_BACKTESTER:
            print("  → 将添加CTA回测模块")
            app_count += 1
        
        if HAS_DATA_MANAGER:
            print("  → 将添加数据管理模块")
            app_count += 1
        
        print(f"\n  总结: 将添加 {gateway_count} 个交易接口, {app_count} 个功能模块")
        
        if app_count == 0:
            print("  ⚠ 警告: 没有功能模块，功能菜单将为空！")
        else:
            print("  ✓ 功能菜单将显示", app_count, "个选项")
            
    except Exception as e:
        print(f"  ✗ 模拟启动失败: {e}")
        traceback.print_exc()

print("\n" + "=" * 60)
print("测试完成！")
print("=" * 60)
print("\n如果所有测试通过，运行 'python run.py' 应该能正常启动GUI")
print("功能菜单应该显示", sum([HAS_CTA_STRATEGY, HAS_CTA_BACKTESTER, HAS_DATA_MANAGER]), "个选项")

