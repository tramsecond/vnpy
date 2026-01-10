# -*- coding: utf-8 -*-
"""
回测可视化工具
提供回测结果的图表可视化功能
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from typing import Dict, List
import numpy as np

# 设置中文显示
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False


def visualize_backtest_results(results: Dict, output_path: str = None):
    """
    可视化单个策略的回测结果
    
    Parameters:
    -----------
    results : Dict
        回测结果字典
    output_path : str
        输出图片路径（可选）
    """
    if '日收益数据' not in results:
        print("错误：回测结果中没有日收益数据")
        return
    
    df = results['日收益数据']
    
    # 创建图表
    fig = plt.figure(figsize=(16, 12))
    
    # 1. 资金曲线和持仓
    ax1 = plt.subplot(3, 2, 1)
    ax1.plot(df['date'], df['total_value'], label='总资产', color='#2E86C1', linewidth=2)
    ax1.axhline(y=results['初始资金'], color='gray', linestyle='--', alpha=0.5, label='初始资金')
    ax1.fill_between(df['date'], results['初始资金'], df['total_value'], 
                      where=df['total_value'] >= results['初始资金'], 
                      color='green', alpha=0.2, label='盈利区')
    ax1.fill_between(df['date'], results['初始资金'], df['total_value'], 
                      where=df['total_value'] < results['初始资金'], 
                      color='red', alpha=0.2, label='亏损区')
    ax1.set_title('资金曲线', fontsize=14, fontweight='bold')
    ax1.set_xlabel('日期')
    ax1.set_ylabel('资产 (元)')
    ax1.legend(loc='upper left')
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
    
    # 2. 收益率曲线
    ax2 = plt.subplot(3, 2, 2)
    ax2.plot(df['date'], df['return_pct'], color='#28B463', linewidth=2)
    ax2.axhline(y=0, color='gray', linestyle='--', alpha=0.5)
    ax2.fill_between(df['date'], 0, df['return_pct'], 
                      where=df['return_pct'] >= 0, color='green', alpha=0.3)
    ax2.fill_between(df['date'], 0, df['return_pct'], 
                      where=df['return_pct'] < 0, color='red', alpha=0.3)
    ax2.set_title('累计收益率', fontsize=14, fontweight='bold')
    ax2.set_xlabel('日期')
    ax2.set_ylabel('收益率 (%)')
    ax2.grid(True, alpha=0.3)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
    
    # 3. 回撤曲线
    ax3 = plt.subplot(3, 2, 3)
    df['peak'] = df['total_value'].cummax()
    df['drawdown'] = (df['total_value'] - df['peak']) / df['peak'] * 100
    ax3.fill_between(df['date'], 0, df['drawdown'], color='red', alpha=0.5)
    ax3.plot(df['date'], df['drawdown'], color='darkred', linewidth=2)
    ax3.axhline(y=results['最大回撤(%)'], color='red', linestyle='--', 
                label=f"最大回撤: {results['最大回撤(%)']}%", linewidth=2)
    ax3.set_title('回撤曲线', fontsize=14, fontweight='bold')
    ax3.set_xlabel('日期')
    ax3.set_ylabel('回撤 (%)')
    ax3.legend(loc='lower left')
    ax3.grid(True, alpha=0.3)
    ax3.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45)
    
    # 4. 持仓价值
    ax4 = plt.subplot(3, 2, 4)
    ax4.fill_between(df['date'], 0, df['position_value'], 
                      where=df['position_value'] > 0, color='blue', alpha=0.5)
    ax4.plot(df['date'], df['position_value'], color='darkblue', linewidth=2)
    ax4.set_title('持仓价值', fontsize=14, fontweight='bold')
    ax4.set_xlabel('日期')
    ax4.set_ylabel('价值 (元)')
    ax4.grid(True, alpha=0.3)
    ax4.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.setp(ax4.xaxis.get_majorticklabels(), rotation=45)
    
    # 5. 统计指标
    ax5 = plt.subplot(3, 2, 5)
    ax5.axis('off')
    
    stats_text = f"""
    ╔══════════════════════════════════════╗
    ║         回测统计摘要                  ║
    ╠══════════════════════════════════════╣
    ║  策略名称: {results['策略名称']}
    ║  标的代码: {results['标的代码']}
    ╠══════════════════════════════════════╣
    ║  初始资金: {results['初始资金']:,.0f} 元
    ║  最终资金: {results['最终资金']:,.0f} 元
    ║  总收益: {results['总收益']:,.0f} 元
    ╠══════════════════════════════════════╣
    ║  总收益率: {results['总收益率(%)']}%
    ║  年化收益率: {results['年化收益率(%)']}%
    ║  最大回撤: {results['最大回撤(%)']}%
    ║  夏普比率: {results['夏普比率']}
    ╠══════════════════════════════════════╣
    ║  交易次数: {results['交易次数']}
    ║  盈利次数: {results['盈利次数']}
    ║  亏损次数: {results['亏损次数']}
    ║  胜率: {results['胜率(%)']}%
    ╚══════════════════════════════════════╝
    """
    
    ax5.text(0.1, 0.5, stats_text, fontsize=11, family='monospace',
             verticalalignment='center', bbox=dict(boxstyle='round', 
             facecolor='wheat', alpha=0.3))
    
    # 6. 月度收益热力图
    ax6 = plt.subplot(3, 2, 6)
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    
    # 计算月度收益
    monthly_returns = df.groupby(['year', 'month'])['return_pct'].last().unstack(fill_value=0)
    
    if len(monthly_returns) > 0:
        im = ax6.imshow(monthly_returns.values, cmap='RdYlGn', aspect='auto', 
                        vmin=-10, vmax=10)
        ax6.set_xticks(range(len(monthly_returns.columns)))
        ax6.set_xticklabels([f'{m}月' for m in monthly_returns.columns])
        ax6.set_yticks(range(len(monthly_returns.index)))
        ax6.set_yticks(range(len(monthly_returns.index)))
        ax6.set_yticklabels(monthly_returns.index)
        ax6.set_title('月度收益率热力图 (%)', fontsize=14, fontweight='bold')
        
        # 添加数值标注
        for i in range(len(monthly_returns.index)):
            for j in range(len(monthly_returns.columns)):
                value = monthly_returns.values[i, j]
                if abs(value) > 0.1:  # 只显示非零值
                    text_color = 'white' if abs(value) > 5 else 'black'
                    ax6.text(j, i, f'{value:.1f}', ha='center', va='center',
                            color=text_color, fontsize=8)
        
        plt.colorbar(im, ax=ax6)
    else:
        ax6.text(0.5, 0.5, '数据不足', ha='center', va='center', fontsize=14)
    
    plt.tight_layout()
    
    # 保存或显示
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"可视化图表已保存至: {output_path}")
    else:
        plt.show()
    
    plt.close()


def compare_strategies(results_list: List[Dict], output_path: str = None):
    """
    对比多个策略的回测结果
    
    Parameters:
    -----------
    results_list : List[Dict]
        多个回测结果的列表
    output_path : str
        输出图片路径（可选）
    """
    if not results_list:
        print("错误：没有提供回测结果")
        return
    
    fig = plt.figure(figsize=(16, 10))
    
    # 1. 资金曲线对比
    ax1 = plt.subplot(2, 2, 1)
    for results in results_list:
        if '日收益数据' in results:
            df = results['日收益数据']
            label = results.get('策略名称', 'Unknown')
            ax1.plot(df['date'], df['total_value'], label=label, linewidth=2)
    
    ax1.set_title('资金曲线对比', fontsize=14, fontweight='bold')
    ax1.set_xlabel('日期')
    ax1.set_ylabel('总资产 (元)')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
    
    # 2. 收益率对比
    ax2 = plt.subplot(2, 2, 2)
    for results in results_list:
        if '日收益数据' in results:
            df = results['日收益数据']
            label = results.get('策略名称', 'Unknown')
            ax2.plot(df['date'], df['return_pct'], label=label, linewidth=2)
    
    ax2.set_title('累计收益率对比', fontsize=14, fontweight='bold')
    ax2.set_xlabel('日期')
    ax2.set_ylabel('收益率 (%)')
    ax2.axhline(y=0, color='gray', linestyle='--', alpha=0.5)
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
    
    # 3. 关键指标柱状图对比
    ax3 = plt.subplot(2, 2, 3)
    
    strategy_names = [r.get('策略名称', f'策略{i+1}') for i, r in enumerate(results_list)]
    total_returns = [r.get('总收益率(%)', 0) for r in results_list]
    max_drawdowns = [r.get('最大回撤(%)', 0) for r in results_list]
    sharpe_ratios = [r.get('夏普比率', 0) for r in results_list]
    
    x = np.arange(len(strategy_names))
    width = 0.25
    
    ax3.bar(x - width, total_returns, width, label='总收益率(%)', color='#28B463')
    ax3.bar(x, max_drawdowns, width, label='最大回撤(%)', color='#E74C3C')
    ax3.bar(x + width, [s*10 for s in sharpe_ratios], width, label='夏普比率(×10)', color='#3498DB')
    
    ax3.set_title('关键指标对比', fontsize=14, fontweight='bold')
    ax3.set_xlabel('策略')
    ax3.set_ylabel('数值')
    ax3.set_xticks(x)
    ax3.set_xticklabels(strategy_names, rotation=15, ha='right')
    ax3.legend()
    ax3.grid(True, alpha=0.3, axis='y')
    ax3.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
    
    # 4. 胜率和交易次数对比
    ax4 = plt.subplot(2, 2, 4)
    
    win_rates = [r.get('胜率(%)', 0) for r in results_list]
    trade_counts = [r.get('交易次数', 0) for r in results_list]
    
    ax4_twin = ax4.twinx()
    
    bars1 = ax4.bar(x - width/2, win_rates, width, label='胜率(%)', color='#F39C12')
    bars2 = ax4_twin.bar(x + width/2, trade_counts, width, label='交易次数', color='#9B59B6')
    
    ax4.set_title('胜率与交易次数对比', fontsize=14, fontweight='bold')
    ax4.set_xlabel('策略')
    ax4.set_ylabel('胜率 (%)', color='#F39C12')
    ax4_twin.set_ylabel('交易次数', color='#9B59B6')
    ax4.set_xticks(x)
    ax4.set_xticklabels(strategy_names, rotation=15, ha='right')
    ax4.tick_params(axis='y', labelcolor='#F39C12')
    ax4_twin.tick_params(axis='y', labelcolor='#9B59B6')
    ax4.grid(True, alpha=0.3, axis='y')
    
    # 添加图例
    lines1, labels1 = ax4.get_legend_handles_labels()
    lines2, labels2 = ax4_twin.get_legend_handles_labels()
    ax4.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
    
    plt.tight_layout()
    
    # 保存或显示
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"策略对比图表已保存至: {output_path}")
    else:
        plt.show()
    
    plt.close()


def plot_optimization_results(optimization_results: List[Dict], param_names: List[str], 
                               target_metric: str = '夏普比率', output_path: str = None):
    """
    可视化参数优化结果
    
    Parameters:
    -----------
    optimization_results : List[Dict]
        参数优化结果列表
    param_names : List[str]
        参数名称列表
    target_metric : str
        目标指标名称
    output_path : str
        输出图片路径（可选）
    """
    if len(param_names) != 2:
        print("注意：当前仅支持2个参数的可视化")
        return
    
    # 提取数据
    param1_values = [r['参数'][param_names[0]] for r in optimization_results]
    param2_values = [r['参数'][param_names[1]] for r in optimization_results]
    metric_values = [r.get(target_metric, 0) for r in optimization_results]
    
    # 创建图表
    fig = plt.figure(figsize=(14, 6))
    
    # 1. 3D散点图
    ax1 = fig.add_subplot(121, projection='3d')
    scatter = ax1.scatter(param1_values, param2_values, metric_values, 
                          c=metric_values, cmap='viridis', s=100, alpha=0.6)
    ax1.set_xlabel(param_names[0], fontsize=12)
    ax1.set_ylabel(param_names[1], fontsize=12)
    ax1.set_zlabel(target_metric, fontsize=12)
    ax1.set_title(f'参数优化3D视图\n{target_metric}', fontsize=14, fontweight='bold')
    plt.colorbar(scatter, ax=ax1, shrink=0.5)
    
    # 2. 热力图
    ax2 = fig.add_subplot(122)
    
    # 创建网格数据
    param1_unique = sorted(list(set(param1_values)))
    param2_unique = sorted(list(set(param2_values)))
    grid = np.zeros((len(param2_unique), len(param1_unique)))
    
    for r in optimization_results:
        i = param2_unique.index(r['参数'][param_names[1]])
        j = param1_unique.index(r['参数'][param_names[0]])
        grid[i, j] = r.get(target_metric, 0)
    
    im = ax2.imshow(grid, cmap='RdYlGn', aspect='auto')
    ax2.set_xticks(range(len(param1_unique)))
    ax2.set_xticklabels(param1_unique)
    ax2.set_yticks(range(len(param2_unique)))
    ax2.set_yticklabels(param2_unique)
    ax2.set_xlabel(param_names[0], fontsize=12)
    ax2.set_ylabel(param_names[1], fontsize=12)
    ax2.set_title(f'参数优化热力图\n{target_metric}', fontsize=14, fontweight='bold')
    
    # 添加数值标注
    for i in range(len(param2_unique)):
        for j in range(len(param1_unique)):
            value = grid[i, j]
            if abs(value) > 0.01:
                text_color = 'white' if abs(value) > np.mean(metric_values) else 'black'
                ax2.text(j, i, f'{value:.2f}', ha='center', va='center',
                        color=text_color, fontsize=9)
    
    plt.colorbar(im, ax=ax2)
    
    plt.tight_layout()
    
    # 保存或显示
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"参数优化图表已保存至: {output_path}")
    else:
        plt.show()
    
    plt.close()


if __name__ == "__main__":
    print("可视化工具模块 - 使用示例请参考 backtest_examples.py")
