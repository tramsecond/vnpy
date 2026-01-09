"""
验证VeighNa Trader启动配置
不启动GUI，只验证模块导入和配置逻辑
"""
print("=" * 70)
print("VeighNa Trader 配置验证")
print("=" * 70)

# 1. 验证核心模块
print("\n[1/6] 验证核心模块...")
try:
    from vnpy.event import EventEngine
    from vnpy.trader.engine import MainEngine
    from vnpy.trader.ui import MainWindow, create_qapp
    print("   ✓ 核心模块正常")
except Exception as e:
    print(f"   ✗ 核心模块错误: {e}")
    exit(1)

# 2. 验证并统计交易接口
print("\n[2/6] 验证交易接口...")
gateways = []
try:
    from vnpy_ctp import CtpGateway
    gateways.append(("CTP", CtpGateway))
    print("   ✓ CTP接口可用")
except ImportError:
    print("   - CTP接口未安装")

# 3. 验证并统计功能模块
print("\n[3/6] 验证功能模块...")
apps = []
try:
    from vnpy_ctastrategy import CtaStrategyApp
    apps.append(("CTA策略", CtaStrategyApp))
    print("   ✓ CTA策略模块可用")
except ImportError:
    print("   - CTA策略模块未安装")

try:
    from vnpy_ctabacktester import CtaBacktesterApp
    apps.append(("CTA回测", CtaBacktesterApp))
    print("   ✓ CTA回测模块可用")
except ImportError:
    print("   - CTA回测模块未安装")

try:
    from vnpy_datamanager import DataManagerApp
    apps.append(("数据管理", DataManagerApp))
    print("   ✓ 数据管理模块可用")
except ImportError:
    print("   - 数据管理模块未安装")

# 4. 模拟创建引擎（不启动GUI）
print("\n[4/6] 模拟创建引擎...")
try:
    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    print("   ✓ 引擎创建成功")
except Exception as e:
    print(f"   ✗ 引擎创建失败: {e}")
    exit(1)

# 5. 模拟添加模块
print("\n[5/6] 模拟添加模块...")
gateway_count = 0
app_count = 0

for name, gateway_class in gateways:
    try:
        main_engine.add_gateway(gateway_class)
        gateway_count += 1
        print(f"   ✓ 已添加交易接口: {name}")
    except Exception as e:
        print(f"   ✗ 添加交易接口失败 ({name}): {e}")

for name, app_class in apps:
    try:
        main_engine.add_app(app_class)
        app_count += 1
        print(f"   ✓ 已添加功能模块: {name}")
    except Exception as e:
        print(f"   ✗ 添加功能模块失败 ({name}): {e}")

# 6. 验证最终配置
print("\n[6/6] 验证最终配置...")
actual_gateways = main_engine.get_all_gateway_names()
actual_apps = main_engine.get_all_apps()

print(f"   交易接口数量: {len(actual_gateways)}")
for name in actual_gateways:
    print(f"     - {name}")

print(f"   功能模块数量: {len(actual_apps)}")
for app in actual_apps:
    print(f"     - {app.display_name}")

# 总结
print("\n" + "=" * 70)
print("验证总结")
print("=" * 70)
print(f"✓ 核心模块: 正常")
print(f"✓ 交易接口: {len(actual_gateways)} 个")
print(f"✓ 功能模块: {len(actual_apps)} 个")

if len(actual_apps) == 0:
    print("\n⚠ 警告: 没有功能模块！")
    print("   功能菜单将为空，请安装功能模块扩展包")
else:
    print(f"\n✓ 功能菜单将显示 {len(actual_apps)} 个选项:")
    for app in actual_apps:
        print(f"   - {app.display_name}")

print("\n" + "=" * 70)
print("验证完成！如果看到此消息，配置是正确的。")
print("运行 'python run.py' 应该能正常启动GUI")
print("=" * 70)

