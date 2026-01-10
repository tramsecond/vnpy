#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import xlsxwriter

def compare_versions(df_before, df_after, output_path):
    """对比修改前后的数据"""
    
    print("正在对比修改前后的收益情况...\n")
    
    # 创建一个对比DataFrame
    comparison_data = []
    
    for idx, row in df_before.iterrows():
        stock_code = row['股票代码']
        
        # 找到对应的后修改数据
        after_row = df_after[df_after['股票代码'] == stock_code]
        if after_row.empty:
            continue
        after_row = after_row.iloc[0]
        
        # 提取关键指标进行对比
        before_return = row['策略涨幅']
        after_return = after_row['策略涨幅']
        return_change = after_return - before_return
        
        before_win_rate = row['胜率']
        after_win_rate = after_row['胜率']
        win_rate_change = after_win_rate - before_win_rate
        
        before_trades = row['交易次数']
        after_trades = after_row['交易次数']
        trades_change = after_trades - before_trades
        
        before_utilization = row['资金使用效率(%)']
        after_utilization = after_row['资金使用效率(%)']
        utilization_change = after_utilization - before_utilization
        
        # 计算超额收益的变化
        before_excess = row['策略超额收益']
        after_excess = after_row['策略超额收益']
        excess_change = after_excess - before_excess
        
        comparison_data.append({
            '股票代码': stock_code,
            '修改前策略涨幅': before_return,
            '修改后策略涨幅': after_return,
            '涨幅变化': return_change,
            '修改前胜率': before_win_rate,
            '修改后胜率': after_win_rate,
            '胜率变化': win_rate_change,
            '修改前交易次数(次)': before_trades,
            '修改后交易次数(次)': after_trades,
            '交易次数变化': trades_change,
            '修改前资金利用率(%)': before_utilization,
            '修改后资金利用率(%)': after_utilization,
            '利用率变化': utilization_change,
            '修改前超额收益': before_excess,
            '修改后超额收益': after_excess,
            '超额收益变化': excess_change,
        })
    
    comparison_df = pd.DataFrame(comparison_data)
    
    # 创建Excel报告
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        workbook = writer.book
        percent_format = workbook.add_format({'num_format': '0.00%'})
        number_format = workbook.add_format({'num_format': '0.00'})
        
        # 详细对比表
        comparison_df.to_excel(writer, sheet_name='详细对比', index=False)
        worksheet = writer.sheets['详细对比']
        
        # 设置列宽和格式
        for col_num, col_name in enumerate(comparison_df.columns):
            if '涨幅' in col_name or '胜率' in col_name or '超额收益' in col_name:
                worksheet.set_column(col_num, col_num, 15, percent_format)
            elif '利用率' in col_name:
                worksheet.set_column(col_num, col_num, 18, number_format)
            elif '交易次数' in col_name:
                worksheet.set_column(col_num, col_num, 15, number_format)
            else:
                worksheet.set_column(col_num, col_num, 25)
        
        # 整体对比统计
        overall_stats = {
            '对比项目': [
                '平均策略涨幅',
                '平均胜率', 
                '平均交易次数',
                '平均资金利用率(%)',
                '平均超额收益',
                '最佳股票',
                '涨幅提升最大',
                '胜率提升最大'
            ],
            '修改前': [
                comparison_df['修改前策略涨幅'].mean(),
                comparison_df['修改前胜率'].mean(),
                comparison_df['修改前交易次数(次)'].mean(),
                comparison_df['修改前资金利用率(%)'].mean(),
                comparison_df['修改前超额收益'].mean(),
                comparison_df.loc[comparison_df['修改前策略涨幅'].idxmax(), '股票代码'],
                comparison_df.loc[comparison_df['涨幅变化'].idxmax(), '股票代码'],
                comparison_df.loc[comparison_df['胜率变化'].idxmax(), '股票代码']
            ],
            '修改后': [
                comparison_df['修改后策略涨幅'].mean(),
                comparison_df['修改后胜率'].mean(),
                comparison_df['修改后交易次数(次)'].mean(),
                comparison_df['修改后资金利用率(%)'].mean(),
                comparison_df['修改后超额收益'].mean(),
                comparison_df.loc[comparison_df['修改后策略涨幅'].idxmax(), '股票代码'],
                comparison_df.loc[comparison_df['涨幅变化'].idxmax(), '股票代码'],
                comparison_df.loc[comparison_df['胜率变化'].idxmax(), '股票代码']
            ],
            '变化(百分点)': [
                comparison_df['涨幅变化'].mean(),
                comparison_df['胜率变化'].mean(),
                comparison_df['交易次数变化'].mean(),
                comparison_df['利用率变化'].mean(),
                comparison_df['超额收益变化'].mean(),
                '',
                '',
                ''
            ]
        }
        
        overall_df = pd.DataFrame(overall_stats)
        overall_df.to_excel(writer, sheet_name='整体对比', index=False)
        worksheet = writer.sheets['整体对比']
        
        for col_num in range(3):
            worksheet.set_column(col_num, col_num, 20)
    
    return comparison_df

def main():
    # 修改前后的汇总报告
    before_file = "backtest_results_grid_trend_combined/网格趋势组合汇总报告_20260107_150724.xlsx"  # 修改前（清仓网格）
    after_file = "backtest_results_grid_trend_combined/网格趋势组合汇总报告_20260107_152400.xlsx"   # 修改后（冻结网格）
    
    if not os.path.exists(before_file):
        print(f"错误: 找不到修改前的数据文件: {before_file}")
        return
        
    if not os.path.exists(after_file):
        print(f"错误: 找不到修改后的数据文件: {after_file}")
        return
    
    print("="*60)
    print("网格+趋势组合策略 - 修改前后对比分析")
    print("="*60)
    print("\n修改内容:")
    print("1. 修复资金利用率计算错误:")
    print("   - 原: 计算为总补充资金比例")
    print("   - 新: 计算为日均持仓市值占比\n")
    print("2. 修复趋势转换逻辑:")
    print("   - 原: 趋势买入信号触发时清仓网格持仓")
    print("   - 新: 趋势买入信号触发时冻结网格持仓")
    print("="*60)
    
    # 读取数据
    df_before = pd.read_excel(before_file)
    df_after = pd.read_excel(after_file)
    
    # 生成对比报告
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"backtest_results_grid_trend_combined/修改前后对比报告_{timestamp}.xlsx"
    
    comparison_df = compare_versions(df_before, df_after, output_path)
    
    # 打印关键对比信息
    print("\n关键指标对比:")
    print("-"*80)
    print(f"{'股票名称':<20} {'策略涨幅变化':<15} {'胜率变化':<15} {'交易次数变化':<15} {'资金利用率变化':<18}")
    print("-"*80)
    
    for idx, row in comparison_df.iterrows():
        stock_name = row['股票代码'].split('_')[1] if '_' in row['股票代码'] else row['股票代码']
        stock_name = stock_name[:14]  # 限制长度
        
        return_change = f"{row['涨幅变化']*100:+6.2f}%" if row['涨幅变化'] != 0 else "6.2f}%"
        win_rate_change = f"{row['胜率变化']*100:+6.2f}%" if row['胜率变化'] != 0 else "6.2f}%"
        trades_change = f"{row['交易次数变化']:+8.1f}" if row['交易次数变化'] != 0 else "8.1f}"
        utilization_change = f"{row['利用率变化']:+10.2f}%" if abs(row['利用率变化']) > 0.01 else "10.2f}%"
        
        print(f"{stock_name:<20} {return_change:<15} {win_rate_change:<15} {trades_change:<15} {utilization_change:<18}")
    
    print("-"*80)
    
    # 计算总体效果
    avg_return_change = comparison_df['涨幅变化'].mean()
    avg_strategy_return_before = comparison_df['修改前策略涨幅'].mean()
    avg_strategy_return_after = comparison_df['修改后策略涨幅'].mean()
    
    avg_win_rate_before = comparison_df['修改前胜率'].mean()
    avg_win_rate_after = comparison_df['修改后胜率'].mean()
    
    avg_utilization_before = comparison_df['修改前资金利用率(%)'].mean()
    avg_utilization_after = comparison_df['修改后资金利用率(%)'].mean()
    
    print(f"\n总体效果:")
    print(f"  平均策略涨幅: {avg_strategy_return_before*100:6.2f}% → {avg_strategy_return_after*100:6.2f}% ({avg_return_change*100:+6.2f}%{'pts' if abs(avg_return_change) < 0.01 else ''})")
    print(f"  平均胜率: {avg_win_rate_before*100:6.2f}% → {avg_win_rate_after*100:6.2f}% ({(avg_win_rate_after-avg_win_rate_before)*100:+6.2f}ppt)")
    print(f"  平均资金利用率: {avg_utilization_before:6.2f}% → {avg_utilization_after:6.2f}% ({avg_utilization_after-avg_utilization_before:+6.2f}ppt)")
    
    # 统计改善的股票数量
    improved_returns = len(comparison_df[comparison_df['涨幅变化'] > 0.005])
    improved_win_rates = len(comparison_df[comparison_df['胜率变化'] > 0.01])
    
    print(f"\n改善情况:")
    print(f"  策略涨幅改善的股票: {improved_returns}/{len(comparison_df)} ({improved_returns/len(comparison_df)*100:.1f}%)")
    print(f"  胜率改善的股票: {improved_win_rates}/{len(comparison_df)} ({improved_win_rates/len(comparison_df)*100:.1f}%)")
    
    print(f"\n详细对比报告已保存到: {output_path}")
    print("="*60)

if __name__ == "__main__":
    main()
