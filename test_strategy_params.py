#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""测试策略参数加载"""

import sys
from pathlib import Path

# 添加策略目录到路径
sys.path.insert(0, str(Path(__file__).parent / "strategies"))

try:
    from grid_trend_strategy import GridTrendStrategy
    
    print("=" * 60)
    print("✅ 策略类加载成功: GridTrendStrategy")
    print("=" * 60)
    
    # 检查parameters
    print("\n📋 策略参数 (parameters):")
    print("-" * 60)
    if hasattr(GridTrendStrategy, 'parameters'):
        for param in GridTrendStrategy.parameters:
            if hasattr(GridTrendStrategy, param):
                value = getattr(GridTrendStrategy, param)
                print(f"  {param:30s} = {value}")
            else:
                print(f"  {param:30s} = [未定义]")
    else:
        print("  ❌ 策略没有定义 parameters 列表")
    
    # 检查variables
    print("\n📊 策略变量 (variables):")
    print("-" * 60)
    if hasattr(GridTrendStrategy, 'variables'):
        for var in GridTrendStrategy.variables:
            if hasattr(GridTrendStrategy, var):
                value = getattr(GridTrendStrategy, var)
                print(f"  {var:30s} = {value}")
            else:
                print(f"  {var:30s} = [未定义]")
    else:
        print("  ❌ 策略没有定义 variables 列表")
    
    # 检查author
    print("\n👤 策略作者:")
    print("-" * 60)
    if hasattr(GridTrendStrategy, 'author'):
        print(f"  {GridTrendStrategy.author}")
    
    print("\n" + "=" * 60)
    print("✅ 所有检查完成！策略参数应该可以正常显示")
    print("=" * 60)
    print("\n💡 如果在VnPy中仍然看不到参数，请：")
    print("   1. 完全关闭VnPy应用")
    print("   2. 重新运行: python run.py")
    print("   3. 打开CTA回测，选择GridTrendStrategy")
    
except ImportError as e:
    print(f"❌ 策略导入失败: {e}")
    import traceback
    traceback.print_exc()
except Exception as e:
    print(f"❌ 发生错误: {e}")
    import traceback
    traceback.print_exc()

