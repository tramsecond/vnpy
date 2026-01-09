"""测试脚本 - 验证所有模块是否能正常导入"""
print("=" * 50)
print("VeighNa 模块导入测试")
print("=" * 50)

# 测试核心模块
print("\n[1] 测试核心模块...")
try:
    from vnpy.event import EventEngine
    from vnpy.trader.engine import MainEngine
    from vnpy.trader.ui import MainWindow, create_qapp
    print("✓ 核心模块导入成功")
except Exception as e:
    print(f"✗ 核心模块导入失败: {e}")

# 测试交易接口
print("\n[2] 测试交易接口...")
try:
    from vnpy_ctp import CtpGateway
    print("✓ CTP接口导入成功")
except ImportError:
    print("✗ CTP接口未安装")

# 测试功能模块
print("\n[3] 测试功能模块...")
modules = [
    ("vnpy_ctastrategy", "CtaStrategyApp", "CTA策略"),
    ("vnpy_ctabacktester", "CtaBacktesterApp", "CTA回测"),
    ("vnpy_datamanager", "DataManagerApp", "数据管理"),
]

for module_name, class_name, display_name in modules:
    try:
        module = __import__(module_name, fromlist=[class_name])
        cls = getattr(module, class_name)
        print(f"✓ {display_name} 导入成功")
    except ImportError:
        print(f"✗ {display_name} 未安装")

print("\n" + "=" * 50)
print("测试完成！")
print("=" * 50)

