#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pandas as pd
from datetime import datetime

def detailed_comparison():
    """详细对比分析修改前后的效果"""
    
    print("="*80)
    print("网格+趋势组合策略 - 修改前后详细对比")
    print("="*80)
    print("\n修改内容概要:")
    print("1. 修复资金利用率计算:")
    print("   原逻辑: 总补充资金 / 初始资金 × 100%")
    print("   新逻辑: 日均持仓市值 / 初始资金 × 100%")
    print("   目的: 准确反映资金占用情况")
    print()
    print("2. 修复策略转换逻辑:")
    print("   原逻辑: 趋势买入信号触发时，清仓所有网格持仓")
    print("   新逻辑: 趋势买入信号触发时，保留网格持仓(冻结)，仅用现金买入趋势")
    print("   目的: 避免过早卖出盈利中的网格仓位")
    print("="*80)
    
    # 获取最新的两个报告文件
    result_dir = "backtest_results_grid_trend_combined"
    files = [f for f in os.listdir(result_dir) if '汇总报告' in f and f.endswith('.xlsx')]
    files.sort()
    
    if len(files) < 2:
        print("错误: 需要至少两个汇总报告用于对比")
        return
    
    before_file = os.path.join(result_dir, files[-2])  # 修改前
    after_file = os.path.join(result_dir, files[-1])   # 修改后
    
    print(f"\n修改前版本: {files[-2]}")
    print(f"修改后版本: {files[-1]}")
    
    # 读取数据
    df_before = pd.read_excel(before_file)
    df_after = pd.read_excel(after_file)
    
    print("\n" + "="*80)
    print("详细对比结果")
    print("="*80)
    
    for idx, row in df_before.iterrows():
        stock_code = row['股票代码']
        after_row = df_after[df_after['股票代码'] == stock_code].iloc[0]
        
        stock_name = stock_code.split('_')[1] if '_' in stock_code else stock_code
        print(f"\n【{stock_name}】")
        print("-" * 40)
        
        # 策略涨幅对比
        before_return = row['策略涨幅'] * 100
        after_return = after_row['策略涨幅'] * 100
        return_diff = after_return - before_return
        print(f"策略涨幅: {before_return:8.2f}% → {after_return:8.2f}% ({return_diff:+7.2f}ppt)")
        
        # 胜率对比
        before_win_rate = row['胜率'] * 100
        after_win_rate = after_row['胜率'] * 100
        win_rate_diff = after_win_rate - before_win_rate
        print(f"胜率: {before_win_rate:8.2f}% → {after_win_rate:8.2f}% ({win_rate_diff:+7.2f}ppt)")
        
        # 交易次数对比
        before_trades = row['交易次数']
        after_trades = after_row['交易次数']
        trades_diff = after_trades - before_trades
        print(f"交易次数: {before_trades:8.0f} → {after_trades:8.0f} ({trades_diff:+7.0f}次)")
        
        # 资金利用率对比
        before_util = row['资金使用效率(%)']
        after_util = after_row['资金使用效率(%)']
        util_diff = after_util - before_util
        print(f"资金利用率: {before_util:8.2f}% → {after_util:8.2f}% ({util_diff:+7.2f}ppt)")
        
        # 超额收益对比
        before_excess = row['策略超额收益'] * 100
        after_excess = after_row['策略超额收益'] * 100
        excess_diff = after_excess - before_excess
        print(f"超额收益: {before_excess:8.2f}% → {after_excess:8.2f}% ({excess_diff:+7.2f}ppt)")
    
    # 计算总体对比
    print("\n" + "="*80)
    print("总体对比效果")
    print("="*80)
    
    stats_before = df_before.agg({
        '策略涨幅': 'mean',
        '胜率': 'mean',
        '交易次数': 'mean',
        '资金使用效率(%)': 'mean',
        '策略超额收益': 'mean',
        '策略年化涨幅': 'mean',
        '一直持有涨幅': 'mean',
    })
    
    stats_after = df_after.agg({
        '策略涨幅': 'mean',
        '胜率': 'mean',
        '交易次数': 'mean',
        '资金使用效率(%)': 'mean',
        '策略超额收益': 'mean',
        '策略年化涨幅': 'mean',
        '一直持有涨幅': 'mean',
    })
    
    print(f"{'指标':<25} {'修改前':<12} {'修改后':<12} {'变化':<12}")
    print("-" * 60)
    print(f"{'平均策略涨幅':<25} {stats_before['策略涨幅']*100:>10.2f}% → {stats_after['策略涨幅']*100:>10.2f}% ({(stats_after['策略涨幅']-stats_before['策略涨幅'])*100:+7.2f}ppt)")
    print(f"{'平均胜率':<25} {stats_before['胜率']*100:>10.2f}% → {stats_after['胜率']*100:>10.2f}% ({(stats_after['胜率']-stats_before['胜率'])*100:+7.2f}ppt)")
    print(f"{'平均交易次数':<25} {stats_before['交易次数']:>10.0f} → {stats_after['交易次数']:>10.0f}")
    print(f"{'平均资金利用率':<25} {stats_before['资金使用效率(%)']:>10.2f}% → {stats_after['资金使用效率(%)']:>10.2f}% ({stats_after['资金使用效率(%)']-stats_before['资金使用效率(%)']:+7.2f}ppt)")
    print(f"{'平均超额收益':<25} {stats_before['策略超额收益']*100:>10.2f}% → {stats_after['策略超额收益']*100:>10.2f}% ({(stats_after['策略超额收益']-stats_before['策略超额收益'])*100:+7.2f}ppt)")
    print(f"{'平均策略年化涨幅':<25} {stats_before['策略年化涨幅']*100:>10.2f}% → {stats_after['策略年化涨幅']*100:>10.2f}% ({(stats_after['策略年化涨幅']-stats_before['策略年化涨幅'])*100:+7.2f}ppt)")
    print(f"{'一直持有涨幅':<25} {stats_before['一直持有涨幅']*100:>10.2f}% → {stats_after['一直持有涨幅']*100:>10.2f}% ({(stats_after['一直持有涨幅']-stats_before['一直持有涨幅'])*100:+7.2f}ppt)")
    
    # 总结
    print("\n" + "="*80)
    print("修改效果总结")
    print("="*80)
    
    if stats_after['策略涨幅'] > stats_before['策略涨幅']:
        improvement = (stats_after['策略涨幅'] - stats_before['策略涨幅']) * 100
        print(f"✓ 策略性能提升: 平均涨幅提升 {improvement:.2f} 个百分点")
    else:
        decline = (stats_before['策略涨幅'] - stats_after['策略涨幅']) * 100
        print(f"✗ 策略性能下降: 平均涨幅下降 {decline:.2f} 个百分点")
    
    if stats_after['胜率'] > stats_before['胜率']:
        win_improvement = (stats_after['胜率'] - stats_before['胜率']) * 100
        print(f"✓ 胜率提升: 平均胜率提升 {win_improvement:.2f} 个百分点")
    else:
        win_decline = (stats_before['胜率'] - stats_after['胜率']) * 100
        print(f"✗ 胜率下降: 平均胜率下降 {win_decline:.2f} 个百分点")
    
    if stats_after['资金使用效率(%)'] != stats_before['资金使用效率(%)']:
        util_change = stats_after['资金使用效率(%)'] - stats_before['资金使用效率(%)']
        print(f"ℹ 资金利用率: {'提升' if util_change > 0 else '下降'} {abs(util_change):.2f} 个百分点")
    else:
        print("ℹ 资金利用率: 无明显变化")
    
    print("\n" + "="*80)
    print("核心改进点说明")
    print("="*80)
    print("1. 资金利用率计算更加合理")
    print("   新逻辑计算日均持仓市值占比，更真实反映资金使用效率")
    print()
    print("2. 策略切换逻辑优化")
    print("   冻结而非清仓网格持仓，避免了趋势转换时的不必要卖出")
    print("   减少了交易摩擦成本，保持盈利仓位的持续性")
    print()
    print("3. 风险控制提升")
    print("   网格和趋势仓位分开管理，风险分散化")
    print("   避免了过度频繁的策略切换")
    print("="*80)

if __name__ == "__main__":
    detailed_comparison()
