#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import pandas as pd
import numpy as np
import glob
import time
from datetime import datetime
import xlsxwriter
import shutil

# 强制设置UTF-8编码环境
sys.stdout.reconfigure(encoding='utf-8') if hasattr(sys.stdout, 'reconfigure') else None
sys.stderr.reconfigure(encoding='utf-8') if hasattr(sys.stderr, 'reconfigure') else None

if sys.platform.startswith('win'):
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    os.environ['LANG'] = 'zh_CN.UTF-8'
    os.environ['LC_ALL'] = 'zh_CN.UTF-8'

# 技术指标配置
TECH_CONFIG = {
    'SUPERTREND': {'atr_length': 10, 'multiplier': 3.0},
    'QQEMOD': {
        'rsi_length_primary': 6, 'rsi_smoothing_primary': 5, 'qqe_factor_primary': 3.0, 'threshold_primary': 3.0,
        'rsi_length_secondary': 6, 'rsi_smoothing_secondary': 5, 'qqe_factor_secondary': 1.61, 'threshold_secondary': 3.0,
        'bollinger_length': 50, 'bollinger_multiplier': 0.35
    },
    'TREND_A': {'ma_period': 9, 'ma_type': 'EMA'}
}

# 技术信号列定义 - 使用新的三个指标
TECH_SIGNAL_COLUMNS = [
    'SuperTrend信号', 'QQE_MOD信号', 'Trend_A_V2信号', '综合判断'
]

# 支持的不同周期类型
TIME_FRAMES = ['小时线', '日线', '周线', '月线']

# ===== 新增：数值保留三位小数的函数 =====
def round_value(value, decimals=3):
    """保留指定小数位数，仅处理数值类型"""
    if isinstance(value, (int, float, np.number)):
        return round(value, decimals)
    return value

# ===== 技术指标计算函数 =====
def calculate_atr(df, period=14):
    """计算ATR (Average True Range)"""
    df = df.copy()
    
    # 检查必要的列
    if 'high' not in df.columns or 'low' not in df.columns or 'close' not in df.columns:
        print("  警告：ATR计算缺少必要列")
        df['ATR'] = np.nan
        return df
    
    # 确保数据是数值类型
    for col in ['high', 'low', 'close']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 计算True Range
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['close'].shift())
    
    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    
    # 计算ATR (使用Wilder's smoothing)
    df['ATR'] = true_range.ewm(span=period, adjust=False).mean()
    
    return df

def calculate_supertrend(df, atr_length=None, multiplier=None):
    """计算SuperTrend指标"""
    # 从配置文件获取参数
    if atr_length is None:
        atr_length = TECH_CONFIG.get('SUPERTREND', {}).get('atr_length', 10)
    if multiplier is None:
        multiplier = TECH_CONFIG.get('SUPERTREND', {}).get('multiplier', 3.0)
    
    df = df.copy()
    
    # 检查必要的列是否存在
    required_cols = ['high', 'low', 'close']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"  警告：SuperTrend计算缺少必要列: {missing_cols}")
        print(f"  当前可用列: {list(df.columns)}")
        # 返回空的SuperTrend列
        df['SuperTrend'] = np.nan
        df['SuperTrend_DIRECTION'] = np.nan
        return df
    
    # 确保数据是数值类型
    for col in ['high', 'low', 'close']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 检查是否有有效的数值数据
    if df[['high', 'low', 'close']].isnull().all().any():
        print(f"  警告：SuperTrend计算遇到全空数据")
        df['SuperTrend'] = np.nan
        df['SuperTrend_DIRECTION'] = np.nan
        return df
    
    # 计算ATR
    atr = calculate_atr(df, atr_length)['ATR']
    
    # 计算HL2
    hl2 = (df['high'] + df['low']) / 2
    
    # 计算基础上下轨
    basic_upper_band = hl2 + (multiplier * atr)
    basic_lower_band = hl2 - (multiplier * atr)
    
    # 初始化最终上下轨
    upper_band = pd.Series(index=df.index, dtype=float)
    lower_band = pd.Series(index=df.index, dtype=float)
    
    upper_band.iloc[0] = basic_upper_band.iloc[0]
    lower_band.iloc[0] = basic_lower_band.iloc[0]
    
    # 计算最终上下轨
    for i in range(1, len(df)):
        # 上轨计算
        if basic_upper_band.iloc[i] < upper_band.iloc[i-1] or df['close'].iloc[i-1] > upper_band.iloc[i-1]:
            upper_band.iloc[i] = basic_upper_band.iloc[i]
        else:
            upper_band.iloc[i] = upper_band.iloc[i-1]
        
        # 下轨计算
        if basic_lower_band.iloc[i] > lower_band.iloc[i-1] or df['close'].iloc[i-1] < lower_band.iloc[i-1]:
            lower_band.iloc[i] = basic_lower_band.iloc[i]
        else:
            lower_band.iloc[i] = lower_band.iloc[i-1]
    
    # 计算SuperTrend
    supertrend = pd.Series(index=df.index, dtype=float)
    trend_direction = pd.Series(index=df.index, dtype=int)
    
    # 初始化 - 在ATR计算完成之前，趋势方向默认为下降趋势
    trend_direction.iloc[0] = -1  # 下降趋势
    supertrend.iloc[0] = upper_band.iloc[0]
    
    for i in range(1, len(df)):
        # 根据SuperTrend的标准逻辑
        if supertrend.iloc[i-1] == upper_band.iloc[i-1]:
            # 前一个SuperTrend等于前一个上轨，检查当前收盘价是否突破上轨
            if df['close'].iloc[i] > upper_band.iloc[i]:
                trend_direction.iloc[i] = 1  # 转为上升趋势
                supertrend.iloc[i] = lower_band.iloc[i]
            else:
                trend_direction.iloc[i] = -1  # 保持下降趋势
                supertrend.iloc[i] = upper_band.iloc[i]
        else:
            # 前一个SuperTrend等于前一个下轨，检查当前收盘价是否突破下轨
            if df['close'].iloc[i] < lower_band.iloc[i]:
                trend_direction.iloc[i] = -1  # 转为下降趋势
                supertrend.iloc[i] = upper_band.iloc[i]
            else:
                trend_direction.iloc[i] = 1  # 保持上升趋势
                supertrend.iloc[i] = lower_band.iloc[i]
    
    df['SuperTrend'] = supertrend
    df['SuperTrend_DIRECTION'] = trend_direction
    
    return df

def calculate_qqe_mod(df, 
                     rsi_length_primary=None, rsi_smoothing_primary=None, qqe_factor_primary=None, threshold_primary=None,
                     rsi_length_secondary=None, rsi_smoothing_secondary=None, qqe_factor_secondary=None, threshold_secondary=None,
                     bollinger_length=None, bollinger_multiplier=None):
    """计算QQE MOD指标"""
    # 从配置文件获取参数
    config = TECH_CONFIG.get('QQEMOD', {})
    if rsi_length_primary is None:
        rsi_length_primary = config.get('rsi_length_primary', 6)
    if rsi_smoothing_primary is None:
        rsi_smoothing_primary = config.get('rsi_smoothing_primary', 5)
    if qqe_factor_primary is None:
        qqe_factor_primary = config.get('qqe_factor_primary', 3.0)
    if threshold_primary is None:
        threshold_primary = config.get('threshold_primary', 3.0)
    if rsi_length_secondary is None:
        rsi_length_secondary = config.get('rsi_length_secondary', 6)
    if rsi_smoothing_secondary is None:
        rsi_smoothing_secondary = config.get('rsi_smoothing_secondary', 5)
    if qqe_factor_secondary is None:
        qqe_factor_secondary = config.get('qqe_factor_secondary', 1.61)
    if threshold_secondary is None:
        threshold_secondary = config.get('threshold_secondary', 3.0)
    if bollinger_length is None:
        bollinger_length = config.get('bollinger_length', 50)
    if bollinger_multiplier is None:
        bollinger_multiplier = config.get('bollinger_multiplier', 0.35)
    
    df = df.copy()
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    
    def calculate_qqe_bands(source, rsi_length, smoothing_factor, qqe_factor):
        """计算QQE bands"""
        wilders_length = rsi_length * 2 - 1
        
        # 计算RSI
        delta = source.diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.ewm(alpha=1/rsi_length, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/rsi_length, adjust=False).mean()
        rs = avg_gain / (avg_loss + 1e-9)
        rsi = 100 - (100 / (1 + rs))
        
        # 平滑RSI
        smoothed_rsi = rsi.ewm(span=smoothing_factor, adjust=False).mean()
        
        # 计算ATR-like指标
        atr_rsi = np.abs(smoothed_rsi.diff())
        smoothed_atr_rsi = atr_rsi.ewm(span=wilders_length, adjust=False).mean()
        dynamic_atr_rsi = smoothed_atr_rsi * qqe_factor
        
        # 初始化bands
        long_band = pd.Series(index=source.index, dtype=float)
        short_band = pd.Series(index=source.index, dtype=float)
        trend_direction = pd.Series(index=source.index, dtype=int)
        
        # 计算第一个值
        long_band.iloc[0] = smoothed_rsi.iloc[0] - dynamic_atr_rsi.iloc[0]
        short_band.iloc[0] = smoothed_rsi.iloc[0] + dynamic_atr_rsi.iloc[0]
        trend_direction.iloc[0] = 1
        
        for i in range(1, len(source)):
            new_long_band = smoothed_rsi.iloc[i] - dynamic_atr_rsi.iloc[i]
            new_short_band = smoothed_rsi.iloc[i] + dynamic_atr_rsi.iloc[i]
            
            # 更新long_band
            if smoothed_rsi.iloc[i-1] > long_band.iloc[i-1] and smoothed_rsi.iloc[i] > long_band.iloc[i-1]:
                long_band.iloc[i] = max(long_band.iloc[i-1], new_long_band)
            else:
                long_band.iloc[i] = new_long_band
            
            # 更新short_band
            if smoothed_rsi.iloc[i-1] < short_band.iloc[i-1] and smoothed_rsi.iloc[i] < short_band.iloc[i-1]:
                short_band.iloc[i] = min(short_band.iloc[i-1], new_short_band)
            else:
                short_band.iloc[i] = new_short_band
            
            # 更新趋势方向
            if smoothed_rsi.iloc[i] > short_band.iloc[i-1]:
                trend_direction.iloc[i] = 1
            elif smoothed_rsi.iloc[i] < long_band.iloc[i-1]:
                trend_direction.iloc[i] = -1
            else:
                trend_direction.iloc[i] = trend_direction.iloc[i-1]
        
        # 确定趋势线
        qqe_trend_line = pd.Series(index=source.index, dtype=float)
        for i in range(len(source)):
            qqe_trend_line.iloc[i] = long_band.iloc[i] if trend_direction.iloc[i] == 1 else short_band.iloc[i]
        
        return qqe_trend_line, smoothed_rsi
    
    # 计算主QQE
    primary_qqe_trend, primary_rsi = calculate_qqe_bands(
        df['close'], rsi_length_primary, rsi_smoothing_primary, qqe_factor_primary
    )
    
    # 计算副QQE
    secondary_qqe_trend, secondary_rsi = calculate_qqe_bands(
        df['close'], rsi_length_secondary, rsi_smoothing_secondary, qqe_factor_secondary
    )
    
    # 计算布林带
    bollinger_basis = (primary_qqe_trend - 50).rolling(window=bollinger_length, min_periods=1).mean()
    bollinger_deviation = bollinger_multiplier * (primary_qqe_trend - 50).rolling(window=bollinger_length, min_periods=1).std()
    bollinger_upper = bollinger_basis + bollinger_deviation
    bollinger_lower = bollinger_basis - bollinger_deviation
    
    # 存储结果
    df['QQE_PRIMARY_TREND'] = primary_qqe_trend
    df['QQE_PRIMARY_RSI'] = primary_rsi
    df['QQE_SECONDARY_TREND'] = secondary_qqe_trend
    df['QQE_SECONDARY_RSI'] = secondary_rsi
    df['QQE_BOLLINGER_UPPER'] = bollinger_basis
    df['QQE_BOLLINGER_LOWER'] = bollinger_lower
    
    # 计算信号
    df['QQE_UP_SIGNAL'] = np.where(
        (secondary_rsi - 50 > threshold_secondary) & (primary_rsi - 50 > bollinger_upper), 1, 0
    )
    df['QQE_DOWN_SIGNAL'] = np.where(
        (secondary_rsi - 50 < -threshold_secondary) & (primary_rsi - 50 < bollinger_lower), 1, 0
    )
    
    # 添加QQE_MOD列用于信号判断
    df['QQE_MOD'] = np.where(df['QQE_UP_SIGNAL'] == 1, 1, np.where(df['QQE_DOWN_SIGNAL'] == 1, -1, 0))
    
    return df

def calculate_trend_indicator_a_v2(df, ma_period=None, ma_type=None):
    """计算Trend Indicator A-V2 (Smoothed Heikin Ashi Cloud)"""
    # 从配置文件获取参数，如果没有则使用默认值
    if ma_period is None:
        ma_period = TECH_CONFIG.get('TREND_A', {}).get('ma_period', 9)  # 修正默认值为9
    if ma_type is None:
        ma_type = TECH_CONFIG.get('TREND_A', {}).get('ma_type', 'EMA')
    
    df = df.copy()
    
    # 检查必要的列是否存在
    required_cols = ['open', 'high', 'low', 'close']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"  警告：Trend Indicator A计算缺少必要列: {missing_cols}")
        print(f"  当前可用列: {list(df.columns)}")
        # 返回空的TrendA列
        df['TREND_A_OPEN'] = np.nan
        df['TREND_A_CLOSE'] = np.nan
        df['TREND_A_HIGH'] = np.nan
        df['TREND_A_LOW'] = np.nan
        df['TREND_A_STRENGTH'] = np.nan
        df['TREND_A_DIRECTION'] = np.nan
        return df
    
    # 确保数据是数值类型
    for col in ['open', 'high', 'low', 'close']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 检查是否有有效的数值数据
    if df[['open', 'high', 'low', 'close']].isnull().all().any():
        print(f"  警告：Trend Indicator A计算遇到全空数据")
        df['TREND_A_OPEN'] = np.nan
        df['TREND_A_CLOSE'] = np.nan
        df['TREND_A_HIGH'] = np.nan
        df['TREND_A_LOW'] = np.nan
        df['TREND_A_STRENGTH'] = np.nan
        df['TREND_A_DIRECTION'] = np.nan
        return df
    
    # 计算Heikin Ashi
    ha_close = (df['open'] + df['high'] + df['low'] + df['close']) / 4
    ha_open = pd.Series(index=df.index, dtype=float)
    ha_open.iloc[0] = (df['open'].iloc[0] + df['close'].iloc[0]) / 2
    
    for i in range(1, len(df)):
        ha_open.iloc[i] = (ha_open.iloc[i-1] + ha_close.iloc[i-1]) / 2
    
    ha_high = pd.concat([df['high'], ha_open, ha_close], axis=1).max(axis=1)
    ha_low = pd.concat([df['low'], ha_open, ha_close], axis=1).min(axis=1)
    
    # 根据2024年4月23日更新，移除双重平滑，只使用单级EMA平滑
    if ma_type == 'EMA':
        # 只使用单级EMA平滑，减少延迟
        df['TREND_A_OPEN'] = ha_open.ewm(span=ma_period, adjust=False).mean()
        df['TREND_A_CLOSE'] = ha_close.ewm(span=ma_period, adjust=False).mean()
        df['TREND_A_HIGH'] = ha_high.ewm(span=ma_period, adjust=False).mean()
        df['TREND_A_LOW'] = ha_low.ewm(span=ma_period, adjust=False).mean()
    elif ma_type == 'SMA':
        # 只使用单级SMA平滑
        df['TREND_A_OPEN'] = ha_open.rolling(window=ma_period, min_periods=1).mean()
        df['TREND_A_CLOSE'] = ha_close.rolling(window=ma_period, min_periods=1).mean()
        df['TREND_A_HIGH'] = ha_high.rolling(window=ma_period, min_periods=1).mean()
        df['TREND_A_LOW'] = ha_low.rolling(window=ma_period, min_periods=1).mean()
    else:  # 默认使用EMA
        # 只使用单级EMA平滑
        df['TREND_A_OPEN'] = ha_open.ewm(span=ma_period, adjust=False).mean()
        df['TREND_A_CLOSE'] = ha_close.ewm(span=ma_period, adjust=False).mean()
        df['TREND_A_HIGH'] = ha_high.ewm(span=ma_period, adjust=False).mean()
        df['TREND_A_LOW'] = ha_low.ewm(span=ma_period, adjust=False).mean()
    
    # 计算趋势强度 - 根据原始PineScript逻辑
    df['TREND_A_STRENGTH'] = 100 * (df['TREND_A_CLOSE'] - df['TREND_A_OPEN']) / (df['TREND_A_HIGH'] - df['TREND_A_LOW'] + 1e-9)
    
    # 趋势方向 - 根据原始PineScript逻辑：trend > 0 为看涨，trend < 0 为看跌
    df['TREND_A_DIRECTION'] = np.where(df['TREND_A_STRENGTH'] > 0, 1, -1)
    
    # 添加Trend_A_V2列用于信号判断
    df['Trend_A_V2'] = df['TREND_A_DIRECTION']
    
    return df

# 日志记录函数
def log_error(file_name, sheet_name, reason, exception=None):
    """记录错误信息到日志"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] 文件: {file_name} | 工作表: {sheet_name} | 原因: {reason}"
    if exception:
        log_entry += f" | 错误: {str(exception)}"
    
    # 打印到控制台
    print(log_entry)
    
    # 写入日志文件（使用index_analysis_errors.log以区分股票和指数）
    with open("index_analysis_errors.log", "a", encoding="utf-8") as log_file:
        log_file.write(log_entry + "\n")

# 安全字符串转换函数
def safe_str(value):
    """安全转换值为字符串，处理编码问题"""
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        try:
            # 尝试使用UTF-8解码
            return value.decode('utf-8', errors='replace')
        except:
            try:
                # 尝试使用GBK解码
                return value.decode('gbk', errors='replace')
            except:
                return "编码错误"
    if isinstance(value, (pd.Timestamp)):
        # 只保留日期
        return value.strftime('%Y-%m-%d')
    if isinstance(value, (np.int64, np.int32, np.float64)):
        return str(round_value(value))  # 数值类型保留三位小数
    return str(value)

# 添加信号的函数 - 使用三个新指标
def add_signals_to_dataframe(df):
    """为数据框添加技术分析信号列 - 使用三个新指标（支持小时线、日线、周线、月线）"""
    if df is None or len(df) < 8:
        return df
    
    # 计算技术指标
    df = calculate_supertrend(df)
    df = calculate_qqe_mod(df)
    df = calculate_trend_indicator_a_v2(df)
    
    # 添加下个周期涨跌幅列
    df['下个周期收盘价'] = df['close'].shift(-1)
    df['涨跌幅度'] = (df['下个周期收盘价'] - df['close']).apply(lambda x: round_value(x) if pd.notna(x) else None)
    df['涨跌幅百分比'] = (df['涨跌幅度'] / df['close'] * 100).apply(lambda x: round_value(x) if pd.notna(x) else None)
    
    # 对于最后一行，没有下个周期数据，设为空
    df.loc[df.index[-1], '下个周期收盘价'] = None
    df.loc[df.index[-1], '涨跌幅度'] = None
    df.loc[df.index[-1], '涨跌幅百分比'] = None
    
    # 初始化信号列
    df['SuperTrend信号'] = '中性'
    df['QQE_MOD信号'] = '中性'
    df['Trend_A_V2信号'] = '中性'
    df['综合判断'] = '中性'
    
    # 根据指标值设置初始信号
    df.loc[df['SuperTrend_DIRECTION'] == 1, 'SuperTrend信号'] = '买入信号'
    df.loc[df['SuperTrend_DIRECTION'] == -1, 'SuperTrend信号'] = '卖出信号'
    
    df.loc[df['QQE_MOD'] == 1, 'QQE_MOD信号'] = '看多信号'
    df.loc[df['QQE_MOD'] == -1, 'QQE_MOD信号'] = '看空信号'
    
    df.loc[df['Trend_A_V2'] == 1, 'Trend_A_V2信号'] = '上升趋势'
    df.loc[df['Trend_A_V2'] == -1, 'Trend_A_V2信号'] = '下降趋势'
    
    # 实现信号延续逻辑：信号持续直到出现反转信号
    for i in range(1, len(df)):
        # SuperTrend信号延续逻辑
        prev_signal = df.iloc[i-1]['SuperTrend信号']
        curr_direction = df.iloc[i]['SuperTrend_DIRECTION']
        
        # 如果前一个信号是买入信号，且当前方向仍然是1，则延续为持有信号
        if prev_signal == '买入信号' and curr_direction == 1:
            df.iloc[i, df.columns.get_loc('SuperTrend信号')] = '持有信号'
        # 如果前一个信号是持有信号，且当前方向仍然是1，则继续持有
        elif prev_signal == '持有信号' and curr_direction == 1:
            df.iloc[i, df.columns.get_loc('SuperTrend信号')] = '持有信号'
        # 如果前一个信号是卖出信号，且当前方向仍然是-1，则延续为谨慎观望信号
        elif prev_signal == '卖出信号' and curr_direction == -1:
            df.iloc[i, df.columns.get_loc('SuperTrend信号')] = '谨慎观望信号'
        # 如果前一个信号是谨慎观望信号，且当前方向仍然是-1，则继续谨慎观望
        elif prev_signal == '谨慎观望信号' and curr_direction == -1:
            df.iloc[i, df.columns.get_loc('SuperTrend信号')] = '谨慎观望信号'
        # 如果方向发生变化，则使用新的信号
        elif curr_direction == 1:
            df.iloc[i, df.columns.get_loc('SuperTrend信号')] = '买入信号'
        elif curr_direction == -1:
            df.iloc[i, df.columns.get_loc('SuperTrend信号')] = '卖出信号'
    
    # 综合判断逻辑
    for i in range(len(df)):
        supertrend = df.iloc[i]['SuperTrend信号']
        qqe = df.iloc[i]['QQE_MOD信号']
        trend = df.iloc[i]['Trend_A_V2信号']
        
        # 如果SuperTrend是持有或谨慎观望信号，直接使用
        if supertrend in ['持有信号', '谨慎观望信号']:
            df.iloc[i, df.columns.get_loc('综合判断')] = supertrend
        # 否则按原来的逻辑判断
        elif supertrend == '买入信号' and qqe == '看多信号' and trend == '上升趋势':
            df.iloc[i, df.columns.get_loc('综合判断')] = '买入信号'
        elif supertrend == '卖出信号' and qqe == '看空信号' and trend == '下降趋势':
            df.iloc[i, df.columns.get_loc('综合判断')] = '卖出信号'
        elif supertrend == '买入信号' or qqe == '看多信号' or trend == '上升趋势':
            df.iloc[i, df.columns.get_loc('综合判断')] = '看多信号'
        elif supertrend == '卖出信号' or qqe == '看空信号' or trend == '下降趋势':
            df.iloc[i, df.columns.get_loc('综合判断')] = '看空信号'
        else:
            df.iloc[i, df.columns.get_loc('综合判断')] = '中性'
    
    return df

# 处理单个Excel文件 - 修复公式问题并保留小数位
def process_excel_file(file_path, output_dir):
    """处理单个Excel文件，在输出目录创建分析结果 - 修复公式问题并保留小数位"""
    file_name = os.path.basename(file_path)
    output_path = os.path.join(output_dir, file_name)
    start_time = time.time()
    print(f"处理: {file_name}")
    
    try:
        # 创建新的Excel文件 - 禁用公式和URL转换
        workbook = xlsxwriter.Workbook(output_path, {
            'strings_to_urls': False,
            'strings_to_formulas': False,
            'constant_memory': True
        })
        
        # 准备存储所有周期的信号数据
        all_signals = []
        
        # 处理所有周期数据（小时线、日线、周线、月线）
        for time_frame in TIME_FRAMES:
            sheet_name = f"{time_frame}数据"
            
            try:
                # 尝试读取特定工作表
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                # 规范化列名
                df.columns = [safe_str(col).strip().replace(' ', '') for col in df.columns]
                
                if df.empty:
                    log_error(file_name, sheet_name, "工作表为空")
                    continue
                    
                # 添加信号列
                df_with_signals = add_signals_to_dataframe(df)
                
                # 创建原始工作表
                worksheet = workbook.add_worksheet(sheet_name)
                
                # 写入列名
                for col_idx, col_name in enumerate(df_with_signals.columns):
                    worksheet.write(0, col_idx, safe_str(col_name))
                
                # 添加数据
                for row_idx, (_, row) in enumerate(df_with_signals.iterrows(), start=1):
                    for col_idx, value in enumerate(row):
                        # 安全处理所有值
                        safe_value = safe_str(value)
                        
                        # 写入值
                        if pd.isna(value) or value is None:
                            worksheet.write(row_idx, col_idx, "")
                        elif isinstance(value, (np.int64, np.int32, np.float64)):
                            # 数值类型保留三位小数
                            worksheet.write_number(row_idx, col_idx, round_value(value))
                        elif isinstance(value, (pd.Timestamp)):
                            worksheet.write(row_idx, col_idx, safe_value)
                        else:
                            # 防止Excel将字符串解释为公式
                            if safe_value.startswith('='):
                                safe_value = "'" + safe_value
                            worksheet.write(row_idx, col_idx, safe_value)
                
                # 生成数据总结
                stats_summary = []
                if not df_with_signals.empty:
                    # 计算关键统计数据
                    start_price = df_with_signals.iloc[0]['close']
                    end_price = df_with_signals.iloc[-1]['close']
                    price_change = round_value((end_price - start_price) / start_price * 100, 3)  # 保留三位小数
                    
                    high_point = df_with_signals['close'].max()
                    low_point = df_with_signals['close'].min()
                    
                    # 趋势期数统计（适用于所有时间周期）
                    bullish_periods = ((df_with_signals['综合判断'] == '看多') | (df_with_signals['综合判断'] == '强烈看多')).sum()
                    bearish_periods = ((df_with_signals['综合判断'] == '看空') | (df_with_signals['综合判断'] == '强烈看空')).sum()
                    total_periods = bullish_periods + bearish_periods
                    
                    bullish_percent = round_value((bullish_periods / total_periods * 100), 3) if total_periods > 0 else 0
                    bearish_percent = round_value((bearish_periods / total_periods * 100), 3) if total_periods > 0 else 0
                    
                    # 构建总结文本
                    stats_summary.append(f"[{time_frame}趋势总结]")
                    stats_summary.append(f"价格变动: {price_change:+.2f}% ({start_price:.2f} → {end_price:.2f})")
                    stats_summary.append(f"最高价: {round_value(high_point, 3)}")
                    stats_summary.append(f"最低价: {round_value(low_point, 3)}")
                    stats_summary.append(f"趋势方向: 看多 {bullish_percent:.1f}% | 看空 {bearish_percent:.1f}%")
                    
                    # 当前趋势状态
                    current_trend = df_with_signals.iloc[-1]['综合判断'] if '综合判断' in df_with_signals.columns else '未知'
                    stats_summary.append(f"当前趋势状态: {current_trend}")
                
                # 提取信号列和下个周期涨跌幅列
                signal_columns = ['date'] + [col for col in TECH_SIGNAL_COLUMNS if col in df_with_signals.columns]
                # 添加下个周期涨跌幅列
                signal_columns += ['涨跌幅度', '涨跌幅百分比']
                
                # 根据周期类型确定保留的记录数
                if time_frame == '小时线':
                    record_count = 60  # 小时线保留60条记录（约2.5天的数据）
                elif time_frame == '日线':
                    record_count = 30  # 日线保留30条记录（约1个月的数据）
                else:
                    record_count = 12  # 周线和月线保留12条记录
                df_summary = df_with_signals[signal_columns].tail(record_count)
                
                # 存储信号数据
                all_signals.append({
                    'time_frame': time_frame,
                    'stats': stats_summary,
                    'df_signals': df_summary
                })
                
                print(f"成功处理{time_frame}数据，添加技术信号列和下个周期涨跌幅")
                
            except Exception as e:
                log_error(file_name, sheet_name, "读取工作表失败", e)
        
        # 创建信号总结工作表
        if all_signals:
            worksheet = workbook.add_worksheet('信号总结')
            
            # 写入文件头
            worksheet.write(0, 0, "指数技术信号综合分析报告")
            worksheet.write(1, 0, "生成时间: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            row_index = 2
            
            # 写入所有周期数据
            for signals in all_signals:
                time_frame = signals['time_frame']
                stats = signals['stats']
                df_signals = signals['df_signals']
                
                # 添加分隔行和时间周期标题
                worksheet.write(row_index, 0, f"=== {time_frame}趋势分析 ===")
                row_index += 1
                
                # 写入统计总结
                if stats:
                    for line in stats:
                        safe_line = safe_str(line)
                        # 防止Excel将字符串解释为公式
                        if safe_line.startswith('='):
                            safe_line = "'" + safe_line
                        worksheet.write(row_index, 0, safe_line)
                        row_index += 1
                
                row_index += 1
                
                # 写入信号表头
                header = ["日期"] + [col for col in TECH_SIGNAL_COLUMNS if col in df_signals.columns] + ["涨跌幅度", "涨跌幅百分比"]
                for col_idx, col_name in enumerate(header):
                    safe_col_name = safe_str(col_name)
                    # 防止Excel将字符串解释为公式
                    if safe_col_name.startswith('='):
                        safe_col_name = "'" + safe_col_name
                    worksheet.write(row_index, col_idx, safe_col_name)
                row_index += 1
                
                # 写入信号数据
                for _, row in df_signals.iterrows():
                    row_data = [row['date']] 
                    row_data += [row[col] for col in TECH_SIGNAL_COLUMNS if col in df_signals.columns]
                    row_data += [row['涨跌幅度'], row['涨跌幅百分比']]
                    
                    for col_idx, value in enumerate(row_data):
                        safe_value = safe_str(value)
                        # 防止Excel将字符串解释为公式
                        if safe_value.startswith('='):
                            safe_value = "'" + safe_value
                            
                        if pd.isna(value) or value is None:
                            worksheet.write(row_index, col_idx, "")
                        elif isinstance(value, (np.int64, np.int32, np.float64)):
                            # 数值类型保留三位小数
                            if isinstance(value, (np.float64)) and col_idx >= len(header) - 2:
                                worksheet.write_number(row_index, col_idx, round_value(value))
                            else:
                                worksheet.write(row_index, col_idx, safe_value)
                        elif isinstance(value, (pd.Timestamp)):
                            worksheet.write(row_index, col_idx, safe_value)
                        else:
                            worksheet.write(row_index, col_idx, safe_value)
                    row_index += 1
                
                # 添加空行分隔不同周期
                row_index += 2
        
        # 关闭工作簿
        workbook.close()
        
        process_time = time.time() - start_time
        print(f"完成: {file_name} → 已保存到输出目录 (用时: {process_time:.1f}秒)")
        return True
    
    except Exception as e:
        log_error(file_name, "", "处理文件失败", e)
        return False

# 主函数
def main():
    # 数据目录
    data_dir = "index_data"
    output_dir = "analyzed_results"
    
    # 检查目录是否存在
    if not os.path.exists(data_dir):
        print(f"错误: 数据目录 '{data_dir}' 不存在")
        print("请先运行指数数据准备脚本")
        return
    
    # 创建输出目录
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"创建输出目录: {output_dir}")
    
    # 获取所有Excel文件
    excel_files = glob.glob(os.path.join(data_dir, "*.xlsx"))
    
    if not excel_files:
        print("警告: 未找到任何Excel文件")
        print(f"请确保指数数据准备脚本已在 '{data_dir}' 目录中生成了Excel文件")
        return
    
    print(f"找到 {len(excel_files)} 个Excel文件，开始添加技术信号和下个周期涨跌幅...")
    print(f"分析后文件将保存到: {output_dir}")
    print("=" * 60)
    print(f"将分析以下周期: {', '.join(TIME_FRAMES)}")
    
    total_files = len(excel_files)
    success_count = 0
    skipped_files = []
    
    # 初始化错误日志
    open("index_analysis_errors.log", "w", encoding="utf-8").close()
    
    # 处理每个文件
    for idx, file_path in enumerate(excel_files):
        print(f"\n[{idx+1}/{total_files}] ", end="")
        result = process_excel_file(file_path, output_dir)
        if result:
            success_count += 1
        else:
            skipped_files.append(os.path.basename(file_path))
        
        if idx < total_files - 1:
            print("-" * 60)
    
    print("\n" + "=" * 60)
    print(f"处理完成! 成功处理 {success_count}/{total_files} 个文件")
    print(f"所有分析后文件已保存到 '{output_dir}' 目录中")
    
    if skipped_files:
        print("\n跳过文件报告:")
        print(f"共跳过 {len(skipped_files)} 个文件:")
        for file in skipped_files:
            print(f"- {file}")
            
        # 写入跳过文件日志
        with open("skipped_index_files.log", "w", encoding="utf-8") as log:
            log.write("\n".join(skipped_files))
    
    print("\n详细错误信息请查看: index_analysis_errors.log")
    print("跳过文件列表请查看: skipped_index_files.log")
    print("=" * 60)

if __name__ == "__main__":
    main()
