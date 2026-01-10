#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import time
from datetime import datetime, timedelta

# 导入配置文件
from config import START_DATE

# 强制设置UTF-8编码环境
sys.stdout.reconfigure(encoding='utf-8') if hasattr(sys.stdout, 'reconfigure') else None
sys.stderr.reconfigure(encoding='utf-8') if hasattr(sys.stderr, 'reconfigure') else None

if sys.platform.startswith('win'):
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    os.environ['LANG'] = 'zh_CN.UTF-8'
    os.environ['LC_ALL'] = 'zh_CN.UTF-8'

def main():
    """每日数据更新主函数"""
    print("=" * 80)
    print("🚀 每日数据更新系统启动")
    print(f"🕐 当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # 检查是否为交易日
    today = datetime.now().date()
    if today.weekday() > 4:  # 周末
        print("📅 今天是周末，无需更新数据")
        return
    
    print("📅 今天是交易日，开始更新数据...")
    
    try:
        # 导入更新管理器
        from data_update_manager import DataUpdateManager
        update_manager = DataUpdateManager()
        
        # 获取股票列表
        from data_preparation import get_hs300_stocks
        stock_list = get_hs300_stocks()
        
        # 获取指数列表
        from data_index_preparation_improved import load_indices_from_csv
        index_list = load_indices_from_csv()
        
        print(f"📋 加载完成: {len(stock_list)} 只股票 + {len(index_list)} 个指数")
        
        # 自动更新所有数据
        print("\n🚀 开始自动更新所有数据...")
        
        # 更新股票数据
        print("\n📈 更新股票数据...")
        stock_success = update_manager.batch_update_stocks(stock_list, batch_size=20)
        
        # 更新指数数据
        print("\n📊 更新指数数据...")
        index_success = update_manager.batch_update_indices(index_list, batch_size=5)
        
        # 打印更新结果
        print("\n" + "=" * 80)
        print("📊 每日更新完成总结")
        print("=" * 80)
        print(f"📈 股票更新: {stock_success}/{len(stock_list)} 成功")
        print(f"📊 指数更新: {index_success}/{len(index_list)} 成功")
        print(f"🕐 完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
    except Exception as e:
        print(f"❌ 每日更新过程中出错: {e}")
        import traceback
        print(traceback.format_exc())

if __name__ == "__main__":
    main()
