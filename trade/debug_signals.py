#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import akshare as ak
from data_preparation import calculate_supertrend, calculate_qqe_mod, calculate_trend_indicator_a
from data_trend import add_signals_to_dataframe

def debug_signal_logic():
    """调试信号判断逻辑"""
    print("=== 调试信号判断逻辑 ===")
    
    try:
        # 获取测试数据
        print("获取测试数据...")
        df_raw = ak.stock_zh_a_hist(symbol="600519", period="daily", adjust="qfq", start_date="2024-12-01")
        
        # 重命名列
        rename_map = {
            '日期': 'date',
            '开盘': 'open', 
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume'
        }
        
        for old_name, new_name in rename_map.items():
            if old_name in df_raw.columns:
                df_raw = df_raw.rename(columns={old_name: new_name})
        
        # 取最近30行数据进行测试
        df_test = df_raw.tail(30).copy()
        df_test = df_test.reset_index(drop=True)
        
        print(f"测试数据形状: {df_test.shape}")
        print(f"价格范围: {df_test['close'].min():.2f} - {df_test['close'].max():.2f}")
        
        # 确保数据类型正确
        for col in ['open', 'high', 'low', 'close']:
            df_test[col] = pd.to_numeric(df_test[col], errors='coerce')
        
        # 计算三个指标
        print("\n=== 计算SuperTrend ===")
        df_test = calculate_supertrend(df_test)
        print(f"SuperTrend方向变化:")
        direction_changes = df_test['SUPERTREND_DIRECTION'].diff().fillna(0)
        print(f"  方向变化次数: {(direction_changes != 0).sum()}")
        print(f"  上升趋势天数: {(df_test['SUPERTREND_DIRECTION'] == 1).sum()}")
        print(f"  下降趋势天数: {(df_test['SUPERTREND_DIRECTION'] == -1).sum()}")
        
        print("\n=== 计算QQE MOD ===")
        df_test = calculate_qqe_mod(df_test)
        print(f"QQE MOD信号统计:")
        print(f"  Primary RSI范围: {df_test['QQE_PRIMARY_RSI'].min():.2f} - {df_test['QQE_PRIMARY_RSI'].max():.2f}")
        print(f"  Secondary RSI范围: {df_test['QQE_SECONDARY_RSI'].min():.2f} - {df_test['QQE_SECONDARY_RSI'].max():.2f}")
        print(f"  Bollinger Upper范围: {df_test['QQE_BOLLINGER_UPPER'].min():.2f} - {df_test['QQE_BOLLINGER_UPPER'].max():.2f}")
        print(f"  Bollinger Lower范围: {df_test['QQE_BOLLINGER_LOWER'].min():.2f} - {df_test['QQE_BOLLINGER_LOWER'].max():.2f}")
        
        # 检查QQE信号条件
        up_condition = ((df_test['QQE_SECONDARY_RSI'] - 50) > 3.0) & ((df_test['QQE_PRIMARY_RSI'] - 50) > df_test['QQE_BOLLINGER_UPPER'])
        down_condition = ((df_test['QQE_SECONDARY_RSI'] - 50) < -3.0) & ((df_test['QQE_PRIMARY_RSI'] - 50) < df_test['QQE_BOLLINGER_LOWER'])
        print(f"  看多信号次数: {up_condition.sum()}")
        print(f"  看空信号次数: {down_condition.sum()}")
        
        print("\n=== 计算Trend Indicator A ===")
        df_test = calculate_trend_indicator_a(df_test)
        print(f"Trend A信号统计:")
        print(f"  TREND_A_STRENGTH范围: {df_test['TREND_A_STRENGTH'].min():.2f} - {df_test['TREND_A_STRENGTH'].max():.2f}")
        print(f"  上升趋势天数: {(df_test['TREND_A_DIRECTION'] == 1).sum()}")
        print(f"  下降趋势天数: {(df_test['TREND_A_DIRECTION'] == -1).sum()}")
        
        # 添加信号判断
        print("\n=== 添加信号判断 ===")
        df_test = add_signals_to_dataframe(df_test)
        
        # 分析信号分布
        print(f"\n=== 信号分布分析 ===")
        signal_counts = {}
        for col in ['SuperTrend信号', 'QQE_MOD信号', 'TrendA信号', '综合判断']:
            if col in df_test.columns:
                counts = df_test[col].value_counts()
                signal_counts[col] = counts
                print(f"{col}:")
                for signal, count in counts.items():
                    print(f"  {signal}: {count}")
        
        # 分析信号与价格关系
        print(f"\n=== 信号与价格关系分析 ===")
        df_test['价格变化'] = df_test['close'].pct_change()
        
        # 分析SuperTrend信号
        if 'SuperTrend信号' in df_test.columns:
            buy_signals = df_test[df_test['SuperTrend信号'] == '买入信号']
            sell_signals = df_test[df_test['SuperTrend信号'] == '卖出信号']
            
            if len(buy_signals) > 0:
                buy_returns = buy_signals['价格变化'].mean()
                print(f"SuperTrend买入信号平均收益: {buy_returns:.4f}")
            
            if len(sell_signals) > 0:
                sell_returns = sell_signals['价格变化'].mean()
                print(f"SuperTrend卖出信号平均收益: {sell_returns:.4f}")
        
        # 分析QQE MOD信号
        if 'QQE_MOD信号' in df_test.columns:
            up_signals = df_test[df_test['QQE_MOD信号'] == '看多信号']
            down_signals = df_test[df_test['QQE_MOD信号'] == '看空信号']
            
            if len(up_signals) > 0:
                up_returns = up_signals['价格变化'].mean()
                print(f"QQE MOD看多信号平均收益: {up_returns:.4f}")
            
            if len(down_signals) > 0:
                down_returns = down_signals['价格变化'].mean()
                print(f"QQE MOD看空信号平均收益: {down_returns:.4f}")
        
        # 分析综合判断
        if '综合判断' in df_test.columns:
            bullish_signals = df_test[df_test['综合判断'] == '看多']
            bearish_signals = df_test[df_test['综合判断'] == '看空']
            
            if len(bullish_signals) > 0:
                bullish_returns = bullish_signals['价格变化'].mean()
                print(f"综合判断看多信号平均收益: {bullish_returns:.4f}")
            
            if len(bearish_signals) > 0:
                bearish_returns = bearish_signals['价格变化'].mean()
                print(f"综合判断看空信号平均收益: {bearish_returns:.4f}")
        
        # 检查是否有反向信号
        print(f"\n=== 检查反向信号 ===")
        if len(buy_signals) > 0 and len(sell_signals) > 0:
            buy_avg_return = buy_signals['价格变化'].mean()
            sell_avg_return = sell_signals['价格变化'].mean()
            
            if buy_avg_return < 0 and sell_avg_return > 0:
                print("⚠️  发现反向信号！SuperTrend买入信号收益为负，卖出信号收益为正")
            elif buy_avg_return > 0 and sell_avg_return < 0:
                print("✅ SuperTrend信号方向正确")
        
        # 输出详细信号数据
        print(f"\n=== 详细信号数据（最近10行）===")
        signal_cols = ['date', 'close', 'SuperTrend信号', 'QQE_MOD信号', 'TrendA信号', '综合判断', '价格变化']
        available_cols = [col for col in signal_cols if col in df_test.columns]
        print(df_test[available_cols].tail(10).to_string())
        
    except Exception as e:
        print(f"调试失败: {e}")
        import traceback
        print(traceback.format_exc())

if __name__ == "__main__":
    debug_signal_logic()
