# -*- coding: utf-8 -*-
"""
回测引擎使用示例
展示如何使用新的回测框架进行策略开发和测试
"""

from backtest_engine import BacktestEngine, StrategyTemplate, BarData, save_backtest_results
from backtest_visualizer import visualize_backtest_results, compare_strategies
from typing import Dict
import os


# ============================================================================
# 示例策略 1: 双均线策略
# ============================================================================
class DualMAStrategy(StrategyTemplate):
    """
    双均线策略
    当短期均线上穿长期均线时买入
    当短期均线下穿长期均线时卖出
    """
    
    def on_init(self):
        """初始化策略"""
        # 获取策略参数
        self.fast_period = self.params.get('fast_period', 5)
        self.slow_period = self.params.get('slow_period', 20)
        
        print(f"双均线策略初始化:")
        print(f"  快线周期: MA{self.fast_period}")
        print(f"  慢线周期: MA{self.slow_period}")
    
    def on_bar(self, bar: BarData):
        """处理K线数据"""
        # 获取均线值
        fast_ma = self.get_indicator(f'MA{self.fast_period}')
        slow_ma = self.get_indicator(f'MA{self.slow_period}')
        
        # 如果指标值不存在，跳过
        if fast_ma is None or slow_ma is None:
            return
        
        # 计算可买入数量（使用80%资金）
        available_capital = self.engine.capital * 0.8
        volume = int(available_capital / bar.close / 100) * 100  # 整百股
        
        # 交易逻辑
        if self.pos == 0:  # 空仓时
            if fast_ma > slow_ma and volume > 0:
                # 金叉买入
                self.buy(bar.close, volume)
                print(f"{bar.datetime.strftime('%Y-%m-%d')}: 买入 {volume}股 @{bar.close:.2f}")
        
        else:  # 持仓时
            if fast_ma < slow_ma:
                # 死叉卖出
                self.sell(bar.close, self.pos)
                print(f"{bar.datetime.strftime('%Y-%m-%d')}: 卖出 {self.pos}股 @{bar.close:.2f}")


# ============================================================================
# 示例策略 2: SuperTrend趋势策略
# ============================================================================
class SuperTrendStrategy(StrategyTemplate):
    """
    SuperTrend趋势跟踪策略
    基于SuperTrend信号进行交易
    """
    
    def on_init(self):
        """初始化策略"""
        self.enable_stop_loss = self.params.get('enable_stop_loss', True)
        self.stop_loss_pct = self.params.get('stop_loss_pct', 5.0)
        self.enable_profit_take = self.params.get('enable_profit_take', True)
        self.profit_take_pct = self.params.get('profit_take_pct', 10.0)
        
        self.entry_price = 0.0
        
        print(f"SuperTrend策略初始化:")
        print(f"  止损: {'开启' if self.enable_stop_loss else '关闭'} ({self.stop_loss_pct}%)")
        print(f"  止盈: {'开启' if self.enable_profit_take else '关闭'} ({self.profit_take_pct}%)")
    
    def on_bar(self, bar: BarData):
        """处理K线数据"""
        # 获取SuperTrend信号
        supertrend_signal = self.get_indicator('SuperTrend_信号')
        
        if supertrend_signal is None:
            return
        
        # 计算可买入数量
        available_capital = self.engine.capital * 0.8
        volume = int(available_capital / bar.close / 100) * 100
        
        # 空仓时
        if self.pos == 0:
            if supertrend_signal == '看多' and volume > 0:
                self.buy(bar.close, volume)
                self.entry_price = bar.close
                print(f"{bar.datetime.strftime('%Y-%m-%d')}: SuperTrend买入 {volume}股 @{bar.close:.2f}")
        
        # 持仓时
        else:
            # 检查止损
            if self.enable_stop_loss:
                loss_pct = (bar.close - self.entry_price) / self.entry_price * 100
                if loss_pct <= -self.stop_loss_pct:
                    self.sell(bar.close, self.pos)
                    print(f"{bar.datetime.strftime('%Y-%m-%d')}: 止损卖出 @{bar.close:.2f} (亏损{loss_pct:.2f}%)")
                    return
            
            # 检查止盈
            if self.enable_profit_take:
                profit_pct = (bar.close - self.entry_price) / self.entry_price * 100
                if profit_pct >= self.profit_take_pct:
                    self.sell(bar.close, self.pos)
                    print(f"{bar.datetime.strftime('%Y-%m-%d')}: 止盈卖出 @{bar.close:.2f} (盈利{profit_pct:.2f}%)")
                    return
            
            # SuperTrend信号卖出
            if supertrend_signal == '看空':
                self.sell(bar.close, self.pos)
                print(f"{bar.datetime.strftime('%Y-%m-%d')}: SuperTrend卖出 @{bar.close:.2f}")


# ============================================================================
# 示例策略 3: MACD+KDJ组合策略
# ============================================================================
class MACDKDJStrategy(StrategyTemplate):
    """
    MACD+KDJ组合策略
    同时满足MACD和KDJ买入条件时买入
    """
    
    def on_init(self):
        """初始化策略"""
        self.kdj_oversold = self.params.get('kdj_oversold', 20)
        self.kdj_overbought = self.params.get('kdj_overbought', 80)
        
        print(f"MACD+KDJ组合策略初始化:")
        print(f"  KDJ超卖阈值: {self.kdj_oversold}")
        print(f"  KDJ超买阈值: {self.kdj_overbought}")
    
    def on_bar(self, bar: BarData):
        """处理K线数据"""
        # 获取指标值
        macd = self.get_indicator('MACD')
        macd_signal = self.get_indicator('MACD_Signal')
        kdj_j = self.get_indicator('KDJ_J')
        
        if macd is None or macd_signal is None or kdj_j is None:
            return
        
        # 计算可买入数量
        available_capital = self.engine.capital * 0.8
        volume = int(available_capital / bar.close / 100) * 100
        
        # 空仓时
        if self.pos == 0:
            # MACD金叉 且 KDJ超卖
            if macd > macd_signal and kdj_j < self.kdj_oversold and volume > 0:
                self.buy(bar.close, volume)
                print(f"{bar.datetime.strftime('%Y-%m-%d')}: 组合信号买入 @{bar.close:.2f} (MACD:{macd:.2f}, KDJ_J:{kdj_j:.2f})")
        
        # 持仓时
        else:
            # MACD死叉 或 KDJ超买
            if macd < macd_signal or kdj_j > self.kdj_overbought:
                self.sell(bar.close, self.pos)
                print(f"{bar.datetime.strftime('%Y-%m-%d')}: 组合信号卖出 @{bar.close:.2f}")


# ============================================================================
# 示例 1: 单策略回测
# ============================================================================
def example_single_backtest():
    """单策略回测示例"""
    print("\n" + "="*80)
    print("示例 1: 单策略回测 - 双均线策略")
    print("="*80)
    
    # 创建回测引擎
    engine = BacktestEngine(
        strategy_class=DualMAStrategy,
        symbol="000651",
        data_path="analyzed_results/000651_格力电器_技术数据.xlsx",
        start_date="2022-01-01",
        end_date="2024-12-31",
        initial_capital=100000,
        commission_rate=0.0003,
        slippage=0.01
    )
    
    # 加载数据
    if not engine.load_data(sheet_name="日线"):
        return
    
    # 运行回测
    results = engine.run_backtest(strategy_params={
        'fast_period': 5,
        'slow_period': 20
    })
    
    # 打印结果
    print("\n" + "-"*80)
    print("回测结果:")
    print("-"*80)
    for key, value in results.items():
        if key != '日收益数据':
            print(f"{key}: {value}")
    
    # 保存结果
    output_dir = "backtest_results_new_engine"
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/双均线策略_格力电器_回测结果.xlsx"
    save_backtest_results(results, output_path)
    
    # 可视化
    visualize_backtest_results(results, f"{output_dir}/双均线策略_格力电器_可视化.png")


# ============================================================================
# 示例 2: 参数优化
# ============================================================================
def example_parameter_optimization():
    """参数优化示例"""
    print("\n" + "="*80)
    print("示例 2: 参数优化 - 寻找最佳均线组合")
    print("="*80)
    
    # 创建回测引擎
    engine = BacktestEngine(
        strategy_class=DualMAStrategy,
        symbol="000651",
        data_path="analyzed_results/000651_格力电器_技术数据.xlsx",
        start_date="2022-01-01",
        end_date="2024-12-31",
        initial_capital=100000,
        commission_rate=0.0003
    )
    
    # 加载数据
    if not engine.load_data(sheet_name="日线"):
        return
    
    # 定义参数范围
    param_ranges = {
        'fast_period': [5, 10, 15],
        'slow_period': [20, 30, 60]
    }
    
    # 运行优化
    optimization_results = engine.optimize_parameters(
        param_ranges=param_ranges,
        target_metric='夏普比率',
        optimize_mode='max'
    )
    
    # 打印前5个最佳结果
    print("\n" + "-"*80)
    print("前5个最佳参数组合:")
    print("-"*80)
    for i, result in enumerate(optimization_results[:5], 1):
        print(f"\n排名 {i}:")
        print(f"  参数: {result['参数']}")
        print(f"  总收益率: {result['总收益率(%)']}%")
        print(f"  年化收益率: {result['年化收益率(%)']}%")
        print(f"  最大回撤: {result['最大回撤(%)']}%")
        print(f"  夏普比率: {result['夏普比率']}")
        print(f"  胜率: {result['胜率(%)']}%")
    
    # 保存优化结果
    output_dir = "backtest_results_new_engine"
    os.makedirs(output_dir, exist_ok=True)
    
    import pandas as pd
    df_optimization = pd.DataFrame([
        {
            '快线周期': r['参数']['fast_period'],
            '慢线周期': r['参数']['slow_period'],
            '总收益率(%)': r['总收益率(%)'],
            '年化收益率(%)': r['年化收益率(%)'],
            '最大回撤(%)': r['最大回撤(%)'],
            '夏普比率': r['夏普比率'],
            '胜率(%)': r['胜率(%)'],
            '交易次数': r['交易次数']
        }
        for r in optimization_results
    ])
    
    output_path = f"{output_dir}/参数优化结果_双均线策略.xlsx"
    df_optimization.to_excel(output_path, index=False)
    print(f"\n优化结果已保存至: {output_path}")


# ============================================================================
# 示例 3: 多策略对比
# ============================================================================
def example_strategy_comparison():
    """多策略对比示例"""
    print("\n" + "="*80)
    print("示例 3: 多策略对比")
    print("="*80)
    
    strategies = [
        (DualMAStrategy, {'fast_period': 5, 'slow_period': 20}, "双均线(5,20)"),
        (SuperTrendStrategy, {'enable_stop_loss': True, 'stop_loss_pct': 5}, "SuperTrend+止损"),
        (MACDKDJStrategy, {'kdj_oversold': 20, 'kdj_overbought': 80}, "MACD+KDJ组合")
    ]
    
    all_results = []
    
    for strategy_class, params, name in strategies:
        print(f"\n正在回测: {name}")
        print("-"*80)
        
        engine = BacktestEngine(
            strategy_class=strategy_class,
            symbol="000651",
            data_path="analyzed_results/000651_格力电器_技术数据.xlsx",
            start_date="2022-01-01",
            end_date="2024-12-31",
            initial_capital=100000,
            commission_rate=0.0003
        )
        
        if not engine.load_data(sheet_name="日线"):
            continue
        
        results = engine.run_backtest(strategy_params=params)
        results['策略名称'] = name
        all_results.append(results)
    
    # 对比分析
    print("\n" + "="*80)
    print("策略对比结果:")
    print("="*80)
    
    import pandas as pd
    df_comparison = pd.DataFrame([
        {
            '策略': r['策略名称'],
            '总收益率(%)': r['总收益率(%)'],
            '年化收益率(%)': r['年化收益率(%)'],
            '最大回撤(%)': r['最大回撤(%)'],
            '夏普比率': r['夏普比率'],
            '胜率(%)': r['胜率(%)'],
            '交易次数': r['交易次数']
        }
        for r in all_results
    ])
    
    print("\n", df_comparison.to_string(index=False))
    
    # 保存对比结果
    output_dir = "backtest_results_new_engine"
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/策略对比结果.xlsx"
    df_comparison.to_excel(output_path, index=False)
    print(f"\n对比结果已保存至: {output_path}")
    
    # 可视化对比
    compare_strategies(all_results, f"{output_dir}/策略对比可视化.png")


# ============================================================================
# 示例 4: 批量回测多只股票
# ============================================================================
def example_batch_backtest():
    """批量回测示例"""
    print("\n" + "="*80)
    print("示例 4: 批量回测多只股票")
    print("="*80)
    
    # 定义要回测的股票列表
    stocks = [
        ("000651", "格力电器"),
        ("002594", "比亚迪"),
        ("600031", "三一重工")
    ]
    
    batch_results = []
    
    for code, name in stocks:
        print(f"\n正在回测: {code} {name}")
        print("-"*80)
        
        data_path = f"analyzed_results/{code}_{name}_技术数据.xlsx"
        
        # 检查文件是否存在
        if not os.path.exists(data_path):
            print(f"  文件不存在，跳过")
            continue
        
        engine = BacktestEngine(
            strategy_class=SuperTrendStrategy,
            symbol=code,
            data_path=data_path,
            start_date="2022-01-01",
            end_date="2024-12-31",
            initial_capital=100000,
            commission_rate=0.0003
        )
        
        if not engine.load_data(sheet_name="日线"):
            continue
        
        results = engine.run_backtest(strategy_params={
            'enable_stop_loss': True,
            'stop_loss_pct': 5,
            'enable_profit_take': True,
            'profit_take_pct': 10
        })
        
        results['股票代码'] = code
        results['股票名称'] = name
        batch_results.append(results)
        
        print(f"  总收益率: {results['总收益率(%)']}%")
        print(f"  最大回撤: {results['最大回撤(%)']}%")
    
    # 汇总结果
    if batch_results:
        print("\n" + "="*80)
        print("批量回测汇总:")
        print("="*80)
        
        import pandas as pd
        df_batch = pd.DataFrame([
            {
                '代码': r['股票代码'],
                '名称': r['股票名称'],
                '总收益率(%)': r['总收益率(%)'],
                '年化收益率(%)': r['年化收益率(%)'],
                '最大回撤(%)': r['最大回撤(%)'],
                '夏普比率': r['夏普比率'],
                '胜率(%)': r['胜率(%)'],
                '交易次数': r['交易次数']
            }
            for r in batch_results
        ])
        
        print("\n", df_batch.to_string(index=False))
        
        # 保存批量结果
        output_dir = "backtest_results_new_engine"
        os.makedirs(output_dir, exist_ok=True)
        output_path = f"{output_dir}/批量回测结果_SuperTrend策略.xlsx"
        df_batch.to_excel(output_path, index=False)
        print(f"\n批量回测结果已保存至: {output_path}")


# ============================================================================
# 主函数
# ============================================================================
def main():
    """主函数 - 选择要运行的示例"""
    print("\n" + "="*80)
    print("回测引擎使用示例")
    print("="*80)
    print("\n请选择要运行的示例:")
    print("1. 单策略回测")
    print("2. 参数优化")
    print("3. 多策略对比")
    print("4. 批量回测多只股票")
    print("5. 运行所有示例")
    print("0. 退出")
    
    choice = input("\n请输入选项 (0-5): ").strip()
    
    if choice == '1':
        example_single_backtest()
    elif choice == '2':
        example_parameter_optimization()
    elif choice == '3':
        example_strategy_comparison()
    elif choice == '4':
        example_batch_backtest()
    elif choice == '5':
        example_single_backtest()
        example_parameter_optimization()
        example_strategy_comparison()
        example_batch_backtest()
    elif choice == '0':
        print("退出")
        return
    else:
        print("无效选项")
    
    print("\n" + "="*80)
    print("示例运行完成！")
    print("="*80)


if __name__ == "__main__":
    main()
