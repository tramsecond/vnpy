#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import io
import locale
import os
import time
import warnings
import numpy as np
import pandas as pd
import akshare as ak
from datetime import datetime, timedelta
import xlsxwriter
import traceback
import json
import threading
import random

# 导入配置文件
from config import START_DATE, HOURLY_DATA_YEARS

# 加载技术指标参数配置
def load_technical_indicators_config():
    """从CSV文件加载技术指标参数配置"""
    config_file = "technical_indicators_config.csv"
    
    if not os.path.exists(config_file):
        print(f"警告：配置文件 {config_file} 不存在，使用默认参数")
        return {}
    
    try:
        df_config = pd.read_csv(config_file, encoding='utf-8-sig')
        config_dict = {}
        
        for _, row in df_config.iterrows():
            indicator = row['指标名称']
            param_name = row['参数名称']
            param_value = row['参数值']
            
            # 处理特殊格式的参数值
            if param_name == 'periods' and param_value.startswith('['):
                # 解析列表格式的参数
                import ast
                param_value = ast.literal_eval(param_value)
            else:
                # 尝试转换为数值类型
                try:
                    param_value = float(param_value)
                    if param_value.is_integer():
                        param_value = int(param_value)
                except ValueError:
                    pass  # 保持字符串格式
            
            if indicator not in config_dict:
                config_dict[indicator] = {}
            config_dict[indicator][param_name] = param_value
        
        print(f"成功加载技术指标配置: {config_file}")
        return config_dict
    except Exception as e:
        print(f"加载配置文件失败: {e}")
        return {}

# 全局配置变量
TECH_CONFIG = load_technical_indicators_config()

# 解决中文编码问题
if sys.platform.startswith('win'):
    if sys.getdefaultencoding() != 'utf-8':
        import importlib
        importlib.reload(sys)
    if isinstance(sys.stdout, io.TextIOWrapper):
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    if isinstance(sys.stderr, io.TextIOWrapper):
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
    try:
        locale.setlocale(locale.LC_ALL, 'zh_CN.UTF-8')
    except:
        locale.setlocale(locale.LC_ALL, '')
        
# 设置环境变量
os.environ['PYTHONIOENCODING'] = 'utf-8'
os.environ['LANG'] = 'zh_CN.UTF-8'
os.environ['LC_ALL'] = 'zh_CN.UTF-8'

# 忽略警告
warnings.filterwarnings("ignore")

# 指标计算函数
def calculate_macd(df, fast=None, slow=None, signal=None):
    """计算MACD指标"""
    # 从配置文件获取参数，如果没有则使用默认值
    if fast is None:
        fast = TECH_CONFIG.get('MACD', {}).get('fast', 12)
    if slow is None:
        slow = TECH_CONFIG.get('MACD', {}).get('slow', 26)
    if signal is None:
        signal = TECH_CONFIG.get('MACD', {}).get('signal', 9)
    
    df = df.copy()
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    
    df['EMA_fast'] = df['close'].ewm(span=fast, adjust=False).mean()
    df['EMA_slow'] = df['close'].ewm(span=slow, adjust=False).mean()
    df['DIF'] = df['EMA_fast'] - df['EMA_slow']
    df['DEA'] = df['DIF'].ewm(span=signal, adjust=False).mean()
    df['MACD'] = (df['DIF'] - df['DEA']) * 2
    return df

def calculate_kdj(df, n=None, m1=None, m2=None):
    """计算KDJ指标"""
    # 从配置文件获取参数，如果没有则使用默认值
    if n is None:
        n = TECH_CONFIG.get('KDJ', {}).get('n', 9)
    if m1 is None:
        m1 = TECH_CONFIG.get('KDJ', {}).get('m1', 3)
    if m2 is None:
        m2 = TECH_CONFIG.get('KDJ', {}).get('m2', 3)
    
    df = df.copy()
    
    # 确保输入的数据是数值类型
    for col in ['high', 'low', 'close']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 计算最低价和最高价
    low_min = df['low'].rolling(window=n, min_periods=1).min()
    high_max = df['high'].rolling(window=n, min_periods=1).max()
    
    # 计算RSV
    rsv = (df['close'] - low_min) / (high_max - low_min + 1e-9) * 100
    
    # 使用ewm计算K和D值
    df['K'] = rsv.ewm(alpha=1/m1, adjust=False).mean()
    df['D'] = df['K'].ewm(alpha=1/m2, adjust=False).mean()
    df['J'] = 3 * df['K'] - 2 * df['D']
    return df

def calculate_rsi(df, periods=None):
    """计算RSI指标"""
    # 从配置文件获取参数，如果没有则使用默认值
    if periods is None:
        periods = TECH_CONFIG.get('RSI', {}).get('periods', 14)
    
    df = df.copy()
    
    # 确保输入的数据是数值类型
    if 'close' in df.columns:
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
    
    # 计算价格变化
    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    
    # 使用ewm计算平均增益和损失
    avg_gain = gain.ewm(alpha=1/periods, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/periods, adjust=False).mean()
    
    # 计算RS和RSI
    rs = avg_gain / (avg_loss + 1e-9)
    df[f'RSI_{periods}'] = 100 - (100 / (1 + rs))
    
    # 确保没有NaN或无穷大值
    df[f'RSI_{periods}'] = df[f'RSI_{periods}'].replace([np.inf, -np.inf], np.nan)
    df[f'RSI_{periods}'] = df[f'RSI_{periods}'].ffill().bfill().fillna(50)
    return df

def calculate_boll(df, window=None, std_multiplier=None):
    """计算布林带指标"""
    # 从配置文件获取参数，如果没有则使用默认值
    if window is None:
        window = TECH_CONFIG.get('BOLL', {}).get('window', 20)
    if std_multiplier is None:
        std_multiplier = TECH_CONFIG.get('BOLL', {}).get('std_multiplier', 2)
    
    df = df.copy()
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    
    df[f'MA{window}'] = df['close'].rolling(window=window).mean()
    df[f'STD{window}'] = df['close'].rolling(window=window).std()
    df['BOLL_UPPER'] = df[f'MA{window}'] + std_multiplier * df[f'STD{window}']
    df['BOLL_LOWER'] = df[f'MA{window}'] - std_multiplier * df[f'STD{window}']
    return df

def calculate_ma(df, periods=None):
    """计算移动平均线"""
    # 从配置文件获取参数，如果没有则使用默认值
    if periods is None:
        periods = TECH_CONFIG.get('MA', {}).get('periods', [5, 10, 20, 30, 60])
    
    df = df.copy()
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    
    for period in periods:
        df[f'MA{period}'] = df['close'].rolling(window=period).mean()
    return df

def calculate_ema(df, periods=None):
    """计算指数移动平均线"""
    # 从配置文件获取参数，如果没有则使用默认值
    if periods is None:
        periods = TECH_CONFIG.get('EMA', {}).get('periods', [10, 52])
    
    df = df.copy()
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    
    for period in periods:
        df[f'EMA{period}'] = df['close'].ewm(span=period, adjust=False).mean()
    return df

def calculate_atr(df, period=14):
    """计算ATR (Average True Range)"""
    df = df.copy()
    
    # 确保数据是数值类型
    for col in ['high', 'low', 'close']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 计算True Range
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['close'].shift())
    
    # 对于第一行，high_close和low_close会是NaN，我们用high_low代替
    true_range = np.maximum(high_low, np.maximum(high_close, low_close))
    
    # 对第一行，如果是NaN，用high_low填充
    true_range.iloc[0] = high_low.iloc[0] if pd.isna(true_range.iloc[0]) else true_range.iloc[0]
    
    # 计算ATR，保持索引一致
    atr = pd.Series(true_range, index=df.index).rolling(window=period, min_periods=1).mean()
    
    return atr

def calculate_trend_indicator_a(df, ma_period=None, ma_type=None):
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
        df['SUPERTREND'] = np.nan
        df['SUPERTREND_DIRECTION'] = np.nan
        df['SUPERTREND_UPPER'] = np.nan
        df['SUPERTREND_LOWER'] = np.nan
        return df
    
    # 确保数据是数值类型
    for col in ['high', 'low', 'close']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 检查是否有有效的数值数据
    if df[['high', 'low', 'close']].isnull().all().any():
        print(f"  警告：SuperTrend计算遇到全空数据")
        df['SUPERTREND'] = np.nan
        df['SUPERTREND_DIRECTION'] = np.nan
        df['SUPERTREND_UPPER'] = np.nan
        df['SUPERTREND_LOWER'] = np.nan
        return df
    
    # 计算ATR
    atr = calculate_atr(df, atr_length)
    
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
        # 根据SuperTrend.txt的标准逻辑
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
    
    df['SUPERTREND'] = supertrend
    df['SUPERTREND_DIRECTION'] = trend_direction
    df['SUPERTREND_UPPER'] = upper_band
    df['SUPERTREND_LOWER'] = lower_band
    
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
    df['QQE_BOLLINGER_UPPER'] = bollinger_upper
    df['QQE_BOLLINGER_LOWER'] = bollinger_lower
    df['QQE_BOLLINGER_BASIS'] = bollinger_basis
    
    # 计算信号
    df['QQE_UP_SIGNAL'] = np.where(
        (secondary_rsi - 50 > threshold_secondary) & (primary_rsi - 50 > bollinger_upper), 1, 0
    )
    df['QQE_DOWN_SIGNAL'] = np.where(
        (secondary_rsi - 50 < -threshold_secondary) & (primary_rsi - 50 < bollinger_lower), 1, 0
    )
    
    return df

def generate_weekly_view(df):
    """生成正确的周线视图"""
    # 创建数据副本
    df_copy = df.copy()
    
    # 确保有有效的日期列
    if 'date' not in df_copy.columns:
        df_copy['date'] = pd.to_datetime(df_copy.index)
    
    # 设置日期为索引
    df_copy = df_copy.set_index('date')
    
    # 按周分组（每周从周一开始，周五结束）
    agg_dict = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }
    # 如果存在成交额列，则聚合成交额（求和）
    if 'amount' in df_copy.columns:
        agg_dict['amount'] = 'sum'
    weekly = df_copy.resample('W-FRI').agg(agg_dict)
    
    # 删除空行
    weekly = weekly.dropna()
    
    # 重置索引并将日期列命名为'date'
    weekly = weekly.reset_index()
    weekly = weekly.rename(columns={'date': 'date'})
    return weekly

def generate_monthly_view(df):
    """生成正确的月线视图"""
    # 创建数据副本
    df_copy = df.copy()
    
    # 确保有有效的日期列
    if 'date' not in df_copy.columns:
        df_copy['date'] = pd.to_datetime(df_copy.index)
    
    # 设置日期为索引
    df_copy = df_copy.set_index('date')
    
    # 按月分组
    agg_dict = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }
    # 如果存在成交额列，则聚合成交额（求和）
    if 'amount' in df_copy.columns:
        agg_dict['amount'] = 'sum'
    monthly = df_copy.resample('ME').agg(agg_dict)
    
    # 删除空行
    monthly = monthly.dropna()
    
    # 重置索引并将日期列命名为'date'
    monthly = monthly.reset_index()
    monthly = monthly.rename(columns={'date': 'date'})
    return monthly

def fetch_stock_data_hourly_china(stock_code, max_retries=3, base_delay=1):
    """使用国内数据源获取股票小时线数据"""
    for attempt in range(max_retries):
        try:
            print(f"  使用国内数据源获取股票小时线数据({stock_code})... 尝试 {attempt + 1}/{max_retries}")
            
            # 添加随机延迟
            if attempt > 0:
                delay = base_delay * (2 ** attempt) + random.uniform(0, 0.5)
                print(f"  等待 {delay:.1f} 秒后重试...")
                time.sleep(delay)
            
            # 尝试获取分钟数据然后聚合为小时线
            df_hourly = pd.DataFrame()
            
            # 使用 stock_zh_a_hist_min_em 接口获取小时线数据（period="60"表示60分钟）
            # 注意：该接口只能返回近期数据，不能返回所有历史数据
            stock_code_clean = stock_code[2:] if stock_code.startswith(('sh', 'sz')) else stock_code
            
            # 计算日期范围（尝试获取最近N年的数据，逐步回退）
            end_date = datetime.now()
            max_days = HOURLY_DATA_YEARS * 365  # 配置的年数转换为天数
            
            # 尝试多个时间范围，从最长到最短
            time_ranges = [
                max_days,      # 配置的年数
                int(max_days * 0.75),  # 75%
                int(max_days * 0.5),   # 50%
                int(max_days * 0.25),  # 25%
                90,            # 3个月
                30,            # 1个月
            ]
            
            df_minute = pd.DataFrame()
            for days in time_ranges:
                try:
                    start_date = end_date - timedelta(days=days)
                    start_date_str = start_date.strftime("%Y-%m-%d 09:30:00")
                    end_date_str = end_date.strftime("%Y-%m-%d 15:00:00")
                    
                    print(f"    尝试获取最近 {days} 天的小时线数据...")
                    # 使用 stock_zh_a_hist_min_em 获取60分钟数据
                    # 先尝试带前复权参数
                    try:
                        df_minute = ak.stock_zh_a_hist_min_em(
                            symbol=stock_code_clean, 
                            period="60", 
                            start_date=start_date_str, 
                            end_date=end_date_str,
                            adjust="qfq"  # 前复权
                        )
                    except:
                        # 如果失败，尝试不带adjust参数
                        try:
                            df_minute = ak.stock_zh_a_hist_min_em(
                                symbol=stock_code_clean, 
                                period="60", 
                                start_date=start_date_str, 
                                end_date=end_date_str
                            )
                        except Exception as e2:
                            print(f"    ⚠️  API调用失败: {str(e2)[:50]}")
                            continue
                    
                    if not df_minute.empty:
                        df_hourly = df_minute.copy()
                        print(f"    ✅ 成功获取小时线数据: {len(df_hourly)} 条（最近 {days} 天）")
                        break
                except Exception as e:
                    print(f"    ⚠️  获取最近 {days} 天数据失败: {str(e)[:50]}")
                    continue
            
            if df_minute.empty:
                print(f"    ❌ 所有时间范围都失败，将使用日线数据生成小时线")
            
            if not df_hourly.empty:
                # 统一数据格式 - stock_zh_a_hist_min_em 返回的列名是中文
                column_mapping = {
                    '时间': 'date',
                    '开盘': 'open',
                    '收盘': 'close',
                    '最高': 'high',
                    '最低': 'low',
                    '成交量': 'volume',
                    '成交额': 'amount',
                    '均价': 'average'
                }
                
                # 检查并应用列名映射
                available_mapping = {k: v for k, v in column_mapping.items() if k in df_hourly.columns}
                if available_mapping:
                    df_hourly = df_hourly.rename(columns=available_mapping)
                
                # 确保必要的列存在
                required_cols = ['date', 'open', 'high', 'low', 'close']
                if all(col in df_hourly.columns for col in required_cols):
                    print(f"  成功获取小时线数据，共 {len(df_hourly)} 行")
                    return df_hourly
                else:
                    missing_cols = [col for col in required_cols if col not in df_hourly.columns]
                    print(f"  小时线数据格式不完整，缺少必要列: {missing_cols}")
                    print(f"  当前可用列: {list(df_hourly.columns)}")
            else:
                print(f"  尝试 {attempt + 1} 失败：小时线数据为空")
                
        except Exception as e:
            error_msg = str(e)
            print(f"  尝试 {attempt + 1} 失败：{error_msg}")
            
            # 最后一次尝试失败
            if attempt == max_retries - 1:
                print(f"  所有 {max_retries} 次尝试都失败了")
                return None
    
    return None

def generate_hourly_view(df):
    """生成小时线数据（从日线数据生成，每个交易日按小时分组）- 仅作为后备方案，优先使用API拉取"""
    if df.empty:
        return pd.DataFrame()
    
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    
    # 由于日线数据没有小时信息，我们需要为每个交易日生成小时线数据
    # 这里我们使用日线数据，但标记为小时线（实际是日线数据的复制）
    # 如果需要真正的小时线数据，需要从分钟数据聚合
    # 注意：此函数仅作为无法从API拉取小时线数据时的后备方案
    hourly = df.copy()
    
    # 为每个交易日生成4个小时线数据点（9:30, 10:30, 13:00, 14:00）
    # 这里简化处理，直接使用日线数据作为小时线数据
    hourly_list = []
    for _, row in df.iterrows():
        date = row['date']
        # 生成4个小时线数据点
        for hour in [9, 10, 13, 14]:
            hour_date = pd.Timestamp(date.year, date.month, date.day, hour, 30 if hour < 12 else 0)
            hourly_row = row.copy()
            hourly_row['date'] = hour_date
            hourly_list.append(hourly_row)
    
    if hourly_list:
        hourly = pd.DataFrame(hourly_list)
        hourly = hourly.sort_values('date')
    
    return hourly


def get_hs300_stocks():
    """从CSV文件获取沪深300成分股列表"""
    csv_file = "hs300_stocks.csv"
    
    # 如果CSV文件不存在，则创建并使用默认列表
    if not os.path.exists(csv_file):
        print(f"CSV文件不存在: {csv_file}，将使用预定义股票列表并创建CSV文件")
        
        # 预定义股票列表
        default_stocks = [
            ('sh600519', '贵州茅台'), ('sz000001', '平安银行'), ('sh601398', '工商银行'),
            ('sh600036', '招商银行'), ('sz000333', '美的集团'), ('sh600031', '三一重工'),
            ('sz002415', '海康威视'), ('sh601088', '中国神华'), ('sh600585', '海螺水泥'),
            ('sh601857', '中国石油'), ('sh600690', '海尔智家'), ('sh601818', '光大银行'),
            ('sh600104', '上汽集团'), ('sz000002', '万科A'), ('sh601988', '中国银行'),
            ('sh600028', '中国石化'), ('sh601288', '农业银行'), ('sh600019', '宝钢股份'),
            ('sh601328', '交通银行'), ('sh601166', '兴业银行'), ('sh601939', '建设银行'),
            ('sh600016', '民生银行'), ('sh600000', '浦发银行'), ('sh601998', '中信银行'),
            ('sh601169', '北京银行'), ('sh601229', '上海银行'), ('sz300085', '银之杰')
        ]
        
        # 创建CSV文件
        df = pd.DataFrame(default_stocks, columns=['symbol', 'name'])
        df.to_csv(csv_file, index=False, encoding='utf-8-sig')  # 使用带BOM的UTF-8
        print(f"已创建CSV文件: {csv_file}")
        
        return default_stocks
    
    try:
        # 尝试多种编码读取CSV文件
        encodings = ['utf-8-sig', 'gbk', 'latin1', 'utf-16']
        df = None
        
        for encoding in encodings:
            try:
                df = pd.read_csv(csv_file, encoding=encoding)
                print(f"使用 {encoding} 编码成功读取CSV文件")
                break
            except UnicodeDecodeError:
                continue
            except Exception as e:
                print(f"尝试 {encoding} 编码时出错: {e}")
                continue
        
        if df is None:
            raise ValueError("无法使用任何支持的编码读取CSV文件")
        
        # 确保文件包含必要的列
        if 'symbol' not in df.columns or 'name' not in df.columns:
            # 尝试自动检测列名
            possible_columns = {
                '代码': 'symbol', '股票代码': 'symbol', '证券代码': 'symbol',
                '名称': 'name', '股票名称': 'name', '证券名称': 'name'
            }
            
            for col in df.columns:
                if col in possible_columns:
                    df = df.rename(columns={col: possible_columns[col]})
            
            if 'symbol' not in df.columns or 'name' not in df.columns:
                raise ValueError("CSV文件缺少必要的列（'symbol'或'name')")
        
        # 转换为元组列表
        stock_list = list(df[['symbol', 'name']].itertuples(index=False, name=None))
        
        print(f"从CSV文件读取 {len(stock_list)} 只股票")
        return stock_list
    except Exception as e:
        print(f"读取股票列表CSV文件失败: {e}")
        print(traceback.format_exc())
        return []


def save_data_to_excel(df_daily, df_hourly, df_weekly, df_monthly, filename):
    """将数据保存到Excel文件，包含多个工作表（纯表格，支持小时线）"""
    # 确保所有数据框都有'date'列
    if 'date' not in df_daily.columns:
        if df_daily.index.name == 'date':
            df_daily = df_daily.reset_index()
        else:
            df_daily['date'] = df_daily.index
    
    if df_hourly is not None and not df_hourly.empty and 'date' not in df_hourly.columns:
        if df_hourly.index.name == 'date':
            df_hourly = df_hourly.reset_index()
        else:
            df_hourly['date'] = df_hourly.index
    
    if df_weekly is not None and 'date' not in df_weekly.columns:
        if df_weekly.index.name == 'date':
            df_weekly = df_weekly.reset_index()
        else:
            df_weekly['date'] = df_weekly.index
    
    if df_monthly is not None and 'date' not in df_monthly.columns:
        if df_monthly.index.name == 'date':
            df_monthly = df_monthly.reset_index()
        else:
            df_monthly['date'] = df_monthly.index
    
    # 创建Excel写入器
    with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
        # 保存日线数据
        df_daily.to_excel(writer, sheet_name='日线数据', index=False)
        
        # 保存小时线数据
        if df_hourly is not None and not df_hourly.empty:
            df_hourly.to_excel(writer, sheet_name='小时线数据', index=False)
        
        # 保存周线数据
        if df_weekly is not None and not df_weekly.empty:
            df_weekly.to_excel(writer, sheet_name='周线数据', index=False)
        
        # 保存月线数据
        if df_monthly is not None and not df_monthly.empty:
            df_monthly.to_excel(writer, sheet_name='月线数据', index=False)
    
    print(f"  数据已保存到Excel文件: {filename}")

def prepare_stock_data(stock_code, stock_name):
    """准备股票数据并保存到Excel文件"""
    print(f"  📈 开始准备 {stock_name}({stock_code}) 的数据...")
    
    # 创建数据目录
    if not os.path.exists("stock_data"):
        os.makedirs("stock_data")
        print("  📁 创建stock_data目录")
    
    # 数据文件路径（港股和A股都去掉前2个字符）
    excel_file = f"stock_data/{stock_code[2:]}_{stock_name}_技术数据.xlsx"
    
    # 如果文件已经存在，刷新数据
    if os.path.exists(excel_file):
        file_date = datetime.fromtimestamp(os.path.getmtime(excel_file)).date()
        if file_date == datetime.today().date():
            print(f"  🔄 刷新数据: {excel_file}")
            os.remove(excel_file)  # 强制每次运行刷新数据

    
    # 判断股票类型（A股、港股、北交所）
    is_hk_stock = stock_code.startswith('hk')
    # 北交所股票代码格式：sz430xxx, sz837xxx, sz889xxx, sz43xxxx, sz83xxxx, sz88xxxx
    is_bj_stock = (stock_code.startswith('sz430') or 
                   stock_code.startswith('sz837') or 
                   stock_code.startswith('sz889') or
                   (stock_code.startswith('sz43') and len(stock_code) >= 6) or
                   (stock_code.startswith('sz83') and len(stock_code) >= 6) or
                   (stock_code.startswith('sz88') and len(stock_code) >= 6))
    
    # 拉取数据
    if is_hk_stock:
        # 港股：去掉 'hk' 前缀，保留数字代码，并补齐前导0到5位
        # 例如：hk2331 -> 02331, hk700 -> 00700
        symbol_code_raw = stock_code[2:]
        # 补齐前导0到5位（港股代码通常是5位数字）
        symbol_code = symbol_code_raw.zfill(5)
    elif is_bj_stock:
        # 北交所：去掉 'sz' 前缀，保留数字代码（如 sz430090 -> 430090）
        symbol_code = stock_code[2:]
    else:
        # A股：去掉交易所前缀（如 sh600519 -> 600519）
        symbol_code = stock_code[2:]
    
    # 设置超时时间（秒）
    TIMEOUT_SECONDS = 30
    
    # 使用threading.Timer实现超时控制（Windows兼容）
    def fetch_with_timeout(func, *args, **kwargs):
        result = [None]
        exception = [None]
        
        def target():
            try:
                result[0] = func(*args, **kwargs)
            except Exception as e:
                exception[0] = e
        
        thread = threading.Thread(target=target)
        thread.daemon = True
        thread.start()
        thread.join(TIMEOUT_SECONDS)
        
        if thread.is_alive():
            # 超时，线程仍在运行
            return None, TimeoutError("API调用超时")
        
        if exception[0]:
            return None, exception[0]
        
        return result[0], None
    
    try:
        if is_hk_stock:
            # 港股数据获取
            print(f"  📥 拉取港股 {symbol_code} 的日数据...")
            
            # 转换日期格式：2020-01-01 -> 20200101
            start_date_str = START_DATE.replace('-', '')
            end_date_str = datetime.now().strftime('%Y%m%d')
            
            # 使用 stock_hk_hist 获取港股历史数据
            df_daily, error = fetch_with_timeout(
                ak.stock_hk_hist, 
                symbol=symbol_code, 
                period="daily", 
                start_date=start_date_str, 
                end_date=end_date_str, 
                adjust=""
            )
            
            if df_daily is not None and not df_daily.empty:
                print(f"  ✅ 成功获取港股 {len(df_daily)} 条日线数据")
            elif isinstance(error, TimeoutError):
                print(f"  ❌ 港股日线数据获取超时")
                return None
            else:
                print(f"  ❌ 港股数据获取失败: {error}")
                return None
        elif is_bj_stock:
            # 北交所数据获取（北交所不支持qfq前复权）
            print(f"  📥 拉取北交所 {symbol_code} 的日数据...")
            
            # 北交所股票使用A股接口，但不带qfq参数
            df_daily, error = fetch_with_timeout(
                ak.stock_zh_a_hist, 
                symbol=symbol_code, 
                period="daily", 
                start_date=START_DATE
            )
            
            if df_daily is not None and not df_daily.empty:
                print(f"  ✅ 成功获取北交所 {len(df_daily)} 条日线数据")
            elif isinstance(error, TimeoutError):
                print(f"  ❌ 北交所日线数据获取超时")
                return None
            else:
                print(f"  ❌ 北交所数据获取失败: {error}")
                # 尝试其他方法（如果有）
                return None
        else:
            # A股数据获取（原有逻辑）
            print(f"  📥 拉取 {symbol_code} 的日数据...")
            
            # 尝试获取日线数据（带qfq参数）
            df_daily, error = fetch_with_timeout(ak.stock_zh_a_hist, symbol=symbol_code, period="daily", adjust="qfq", start_date=START_DATE)
            
            if df_daily is not None:
                print(f"  ✅ 成功获取 {len(df_daily)} 条日线数据")
            elif isinstance(error, TimeoutError):
                print(f"  ⏰ 日线数据获取超时，尝试不带qfq参数...")
                # 尝试不带qfq参数
                df_daily, error = fetch_with_timeout(ak.stock_zh_a_hist, symbol=symbol_code, period="daily", start_date=START_DATE)
                if df_daily is not None:
                    print(f"  ✅ 成功获取 {len(df_daily)} 条日线数据")
                elif isinstance(error, TimeoutError):
                    print(f"  ❌ 日线数据获取超时")
                    return None
                else:
                    print(f"  ❌ 二次尝试也失败: {error}")
                    return None
            else:
                print(f"  ❌ 拉取数据失败: {error}")
                # 尝试不带qfq参数
                df_daily, error = fetch_with_timeout(ak.stock_zh_a_hist, symbol=symbol_code, period="daily", start_date=START_DATE)
                if df_daily is not None:
                    print(f"  ✅ 成功获取 {len(df_daily)} 条日线数据")
                elif isinstance(error, TimeoutError):
                    print(f"  ❌ 日线数据获取超时")
                    return None
                else:
                    print(f"  ❌ 二次尝试也失败: {error}")
                    return None
        
        if df_daily is None or df_daily.empty:
            return None
    
    except Exception as e:
        print(f"  ❌ 拉取日线数据时发生异常: {e}")
        print(traceback.format_exc())
        return None
    
    return process_and_save_data(df_daily, stock_name, stock_code, excel_file)


def process_and_save_data(df_daily, name, code, excel_file):
    """处理数据并保存到Excel文件（仅处理股票数据）"""
    # 检查数据是否为空
    if df_daily is None or df_daily.empty:
        print(f"❌ 警告: {name}({code})没有获取到数据")
        return None
    
    # 数据清洗
    print("  🧹 数据清洗中...")
    
    # 重命名列
    rename_map = {
        '日期': 'date',
        '时间': 'date',
        '收盘价': 'close',
        '开盘价': 'open',
        '最高价': 'high',
        '最低价': 'low',
        '成交': 'volume',
        '成交量': 'volume',
        '交易量': 'volume',
        '收盘': 'close',
        '开盘': 'open',
        '最高': 'high',
        '最低': 'low',
        'Close': 'close',
        'Open': 'open',
        'High': 'high',
        'Low': 'low',
        'Volume': 'volume',
        '成交额': 'amount',
        'Amount': 'amount',
        'AMOUNT': 'amount',
        'turnover': 'amount',
        'Turnover': 'amount',
        '金额': 'amount'
    }
    
    # 只重命名实际存在的列
    for old_name, new_name in rename_map.items():
        if old_name in df_daily.columns:
            df_daily = df_daily.rename(columns={old_name: new_name})
    
    # 添加日期列如果不存在
    if 'date' not in df_daily.columns:
        if df_daily.index.name == 'date' or df_daily.index.name == '日期':
            df_daily = df_daily.reset_index()
        else:
            df_daily['date'] = pd.to_datetime(df_daily.index)
    
    # 确保日期列存在且有效
    df_daily['date'] = pd.to_datetime(df_daily['date'], errors='coerce')
    
    # 过滤起始日期
    df_daily = df_daily[df_daily['date'] >= START_DATE]
    
    # 检查并删除重复日期
    duplicate_dates = df_daily['date'].duplicated()
    if duplicate_dates.any():
        print(f"  🔍 发现 {duplicate_dates.sum()} 个重复日期，正在删除...")
        df_daily = df_daily[~duplicate_dates]
    
    # 确保数值列是数值类型
    for col in ['open', 'high', 'low', 'close', 'volume', 'amount']:
        if col in df_daily.columns:
            if df_daily[col].dtype != 'float64' and df_daily[col].dtype != 'int64':
                df_daily[col] = pd.to_numeric(df_daily[col], errors='coerce')
    
    # 剔除停牌日或零交易量日
    if 'volume' in df_daily.columns:
        df_daily = df_daily[df_daily['volume'] > 0]
    
    # 排序数据
    df_daily = df_daily.sort_values('date', ascending=True)
    
    # 填充缺失值
    if df_daily.isnull().values.any():
        print("  🔧 发现缺失值，正在填充...")
        for col in ['open', 'high', 'low', 'close', 'volume', 'amount']:
            if col in df_daily.columns:
                df_daily[col] = df_daily[col].ffill().bfill()
        df_daily = df_daily.dropna()
    
    # 检查并打印日期范围
    if len(df_daily) > 0:
        start_date = df_daily['date'].min()
        end_date = df_daily['date'].max()
        print(f"  📊 日线数据量: {len(df_daily)} 条记录")
        print(f"  📅 日期范围: {start_date.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}")
    else:
        print("  ❌ 警告：清洗后没有有效数据")
        return None
    
    # 计算技术指标
    print("  🧮 计算日线技术指标...")
    try:
        print("    📈 计算移动平均线(MA)...")
        df_daily = calculate_ma(df_daily)
        
        print("    📈 计算指数移动平均线(EMA)...")
        df_daily = calculate_ema(df_daily)
        
        print("    📊 计算MACD指标...")
        df_daily = calculate_macd(df_daily)
        
        print("    📊 计算KDJ指标...")
        df_daily = calculate_kdj(df_daily)
        
        print("    📊 计算RSI指标...")
        df_daily = calculate_rsi(df_daily)
        
        print("    📊 计算布林带指标...")
        df_daily = calculate_boll(df_daily)
        
        print("    📊 计算Trend Indicator A-V2...")
        df_daily = calculate_trend_indicator_a(df_daily)
        
        print("    📊 计算SuperTrend指标...")
        df_daily = calculate_supertrend(df_daily)
        
        print("    📊 计算QQE MOD指标...")
        df_daily = calculate_qqe_mod(df_daily)
        
        print("  ✅ 日线技术指标计算完成")
    except Exception as e:
        print(f"  ❌ 计算技术指标时出错: {e}")
        print(traceback.format_exc())
        return None
    
    # 获取小时线数据（先尝试拉取，失败后从日线生成）
    print("    📈 获取小时线数据...")
    df_hourly = pd.DataFrame()
    
    # 判断股票类型
    is_hk_stock = code.startswith('hk')
    is_bj_stock = code.startswith(('sz430', 'sz837', 'sz889', 'sz43', 'sz83', 'sz88'))
    
    # 先尝试从API拉取小时线数据
    try:
        if is_hk_stock:
            # 港股暂时不支持小时线数据拉取，直接从日线生成
            print(f"    ⚠️  港股暂不支持小时线数据拉取，将从日线数据生成...")
            df_hourly_raw = None
        elif is_bj_stock:
            # 北交所暂时不支持小时线数据拉取，直接从日线生成
            print(f"    ⚠️  北交所暂不支持小时线数据拉取，将从日线数据生成...")
            df_hourly_raw = None
        else:
            # A股尝试拉取小时线数据
            df_hourly_raw = fetch_stock_data_hourly_china(code)
        
        if df_hourly_raw is not None and not df_hourly_raw.empty:
            # 清洗小时线数据（使用与日线数据相同的清洗逻辑）
            print("    🧹 清洗小时线数据...")
            # 确保日期列存在且有效
            if 'date' not in df_hourly_raw.columns:
                if df_hourly_raw.index.name == 'date' or df_hourly_raw.index.name == '日期':
                    df_hourly_raw = df_hourly_raw.reset_index()
                else:
                    df_hourly_raw['date'] = pd.to_datetime(df_hourly_raw.index)
            
            df_hourly_raw['date'] = pd.to_datetime(df_hourly_raw['date'], errors='coerce')
            
            # 处理时区：如果日期列是tz-aware，转换为UTC时区然后去掉时区信息
            if pd.api.types.is_datetime64tz_dtype(df_hourly_raw['date']):
                df_hourly_raw['date'] = df_hourly_raw['date'].dt.tz_convert('UTC').dt.tz_localize(None)
            
            # 过滤起始日期
            start_date_pd = pd.to_datetime(START_DATE)
            df_hourly_raw = df_hourly_raw[df_hourly_raw['date'] >= start_date_pd]
            
            # 删除重复日期
            if df_hourly_raw['date'].duplicated().any():
                print(f"    🔍 发现重复日期，正在删除...")
                df_hourly_raw = df_hourly_raw.drop_duplicates(subset=['date'])
            
            # 确保数值列是数值类型
            for col in ['open', 'high', 'low', 'close', 'volume', 'amount']:
                if col in df_hourly_raw.columns:
                    if not pd.api.types.is_numeric_dtype(df_hourly_raw[col]):
                        df_hourly_raw[col] = pd.to_numeric(df_hourly_raw[col], errors='coerce')
            
            # 排序数据
            df_hourly_raw = df_hourly_raw.sort_values('date', ascending=True)
            
            # 填充缺失值
            if df_hourly_raw.isnull().values.any():
                print("    🔧 发现缺失值，正在填充...")
                for col in ['open', 'high', 'low', 'close', 'volume', 'amount']:
                    if col in df_hourly_raw.columns:
                        df_hourly_raw[col] = df_hourly_raw[col].ffill().bfill()
                df_hourly_raw = df_hourly_raw.dropna()
            
            df_hourly = df_hourly_raw.copy()
            
            # 检查清洗后的数据是否为空
            if len(df_hourly) > 0:
                print(f"    ✅ 成功拉取小时线数据: {len(df_hourly)} 条记录")
            else:
                print(f"    ⚠️  清洗后小时线数据为空，将从日线数据生成...")
                df_hourly = generate_hourly_view(df_daily.copy())
                print(f"    📊 从日线生成小时线数据: {len(df_hourly)} 条记录")
        else:
            print(f"    ⚠️  无法拉取小时线数据，将从日线数据生成...")
            df_hourly = generate_hourly_view(df_daily.copy())
            print(f"    📊 从日线生成小时线数据: {len(df_hourly)} 条记录")
    except Exception as e:
        print(f"    ⚠️  拉取小时线数据失败: {e}，将从日线数据生成...")
        print(traceback.format_exc())
        try:
            df_hourly = generate_hourly_view(df_daily.copy())
            print(f"    📊 从日线生成小时线数据: {len(df_hourly)} 条记录")
        except Exception as e2:
            print(f"    ❌ 生成小时线数据时出错: {e2}")
            df_hourly = pd.DataFrame()
    
    # 计算小时线指标
    if not df_hourly.empty:
        try:
            print("    🧮 计算小时线技术指标...")
            df_hourly = calculate_ma(df_hourly)
            df_hourly = calculate_ema(df_hourly)
            df_hourly = calculate_macd(df_hourly)
            df_hourly = calculate_kdj(df_hourly)
            df_hourly = calculate_rsi(df_hourly)
            df_hourly = calculate_boll(df_hourly)
            df_hourly = calculate_trend_indicator_a(df_hourly)
            df_hourly = calculate_supertrend(df_hourly)
            df_hourly = calculate_qqe_mod(df_hourly)
            print("    ✅ 小时线技术指标计算完成")
        except Exception as e:
            print(f"    ❌ 计算小时线技术指标时出错: {e}")
            print(traceback.format_exc())
    
    # 生成周线视图
    try:
        print("    📈 生成周线数据...")
        df_weekly = generate_weekly_view(df_daily.copy())
        print(f"    📊 周线数据量: {len(df_weekly)} 条记录")
        
        # 计算周线指标
        if not df_weekly.empty:
            print("    🧮 计算周线技术指标...")
            df_weekly = calculate_ma(df_weekly)
            df_weekly = calculate_ema(df_weekly)
            df_weekly = calculate_macd(df_weekly)
            df_weekly = calculate_kdj(df_weekly)
            df_weekly = calculate_rsi(df_weekly)
            df_weekly = calculate_boll(df_weekly)
            df_weekly = calculate_trend_indicator_a(df_weekly)
            df_weekly = calculate_supertrend(df_weekly)
            df_weekly = calculate_qqe_mod(df_weekly)
            print("    ✅ 周线技术指标计算完成")
    except Exception as e:
        print(f"  ❌ 生成周线数据时出错: {e}")
        print(traceback.format_exc())
        df_weekly = pd.DataFrame()
    
    # 生成月线视图
    try:
        print("    📈 生成月线数据...")
        df_monthly = generate_monthly_view(df_daily.copy())
        print(f"    📊 月线数据量: {len(df_monthly)} 条记录")
        
        # 计算月线指标
        if not df_monthly.empty:
            print("    🧮 计算月线技术指标...")
            df_monthly = calculate_ma(df_monthly)
            df_monthly = calculate_ema(df_monthly)
            df_monthly = calculate_macd(df_monthly)
            df_monthly = calculate_kdj(df_monthly)
            df_monthly = calculate_rsi(df_monthly)
            df_monthly = calculate_boll(df_monthly)
            df_monthly = calculate_trend_indicator_a(df_monthly)
            df_monthly = calculate_supertrend(df_monthly)
            df_monthly = calculate_qqe_mod(df_monthly)
            print("    ✅ 月线技术指标计算完成")
    except Exception as e:
        print(f"  ❌ 生成月线数据时出错: {e}")
        print(traceback.format_exc())
        df_monthly = pd.DataFrame()
    
    # 保存到Excel文件
    print("  💾 保存数据到Excel文件...")
    save_data_to_excel(df_daily, df_hourly, df_weekly, df_monthly, excel_file)
    print(f"  ✅ 数据已保存: {excel_file}")
    
    return excel_file

def main():
    """主函数：准备所有股票数据 - 支持分段处理和断点续传"""
    start_time = time.time()
    
    print("=" * 80)
    print("📊 股票数据准备系统启动 - 分段处理模式")
    print(f"🕐 开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # 获取沪深300成分股列表
    print("📋 正在加载股票列表...")
    stock_list = get_hs300_stocks()
    total_stocks = len(stock_list)
    
    if total_stocks == 0:
        print("❌ 没有找到股票数据，请检查hs300_stocks.csv文件")
        return
    
    print(f"✅ 加载完成: {total_stocks} 只股票")
    
    # 分段处理配置
    BATCH_SIZE = 20  # 每批处理20只股票
    BATCH_REST_TIME = 10  # 每批之间休息10秒
    STOCK_REST_TIME = 3  # 每只股票之间休息3秒
    PROGRESS_FILE = "processing_progress.json"  # 进度保存文件
    
    # 检查是否有断点续传
    start_index = 0
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                progress_data = json.load(f)
                start_index = progress_data.get('last_processed_index', 0)
                print(f"🔄 发现断点续传文件，从第 {start_index + 1} 只股票开始继续处理")
        except Exception as e:
            print(f"⚠️  读取进度文件失败: {e}，从头开始处理")
            start_index = 0
    
    # 开始处理股票
    print("\n" + "=" * 80)
    print(f"🚀 开始处理股票数据 ({total_stocks} 只股票) - 分段处理模式")
    print(f"📊 批次大小: {BATCH_SIZE} | 批次间隔: {BATCH_REST_TIME}秒 | 股票间隔: {STOCK_REST_TIME}秒")
    print("=" * 80)
    
    # 准备所有股票数据
    processed_files = []
    stock_start_time = time.time()
    
    # 计算总批次数
    total_batches = (total_stocks - start_index + BATCH_SIZE - 1) // BATCH_SIZE
    
    for batch_idx in range(total_batches):
        batch_start = start_index + batch_idx * BATCH_SIZE
        batch_end = min(start_index + (batch_idx + 1) * BATCH_SIZE, total_stocks)
        current_batch_size = batch_end - batch_start
        
        print(f"\n" + "=" * 60)
        print(f"📦 批次 {batch_idx + 1}/{total_batches}: 处理第 {batch_start + 1}-{batch_end} 只股票")
        print(f"📊 本批次: {current_batch_size} 只股票")
        print("=" * 60)
        
        batch_start_time = time.time()
        
        for idx in range(batch_start, batch_end):
            current_time = time.time()
            elapsed_time = current_time - start_time
            progress_percent = ((idx + 1) / total_stocks) * 100
            remaining_stocks = total_stocks - idx - 1
            
            print(f"\n📈 [{idx+1}/{total_stocks}] ({progress_percent:.1f}%) 处理股票: {stock_list[idx][1]}({stock_list[idx][0]})")
            if idx > 0:
                print(f"⏱️  已用时间: {elapsed_time:.1f}秒 | 预计剩余: {((elapsed_time/(idx+1))*(remaining_stocks)):.1f}秒")
            
            try:
                # 设置单个股票的超时时间
                stock_start = time.time()
                excel_file = prepare_stock_data(stock_list[idx][0], stock_list[idx][1])
                stock_elapsed = time.time() - stock_start
                
                if excel_file:
                    processed_files.append((f"股票_{stock_list[idx][0]}", stock_list[idx][1], excel_file))
                    print(f"✅ 股票 {stock_list[idx][1]} 处理成功，耗时: {stock_elapsed:.2f}秒")
                else:
                    print(f"❌ 股票 {stock_list[idx][1]} 处理失败，耗时: {stock_elapsed:.2f}秒")
            except Exception as e:
                print(f"❌ 处理股票 {stock_list[idx][1]}({stock_list[idx][0]}) 时出错: {e}")
                print(traceback.format_exc())
            
            # 保存进度
            try:
                progress_data = {
                    'last_processed_index': idx,
                    'last_processed_time': datetime.now().isoformat(),
                    'total_processed': len(processed_files),
                    'total_stocks': total_stocks
                }
                with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
                    json.dump(progress_data, f, ensure_ascii=False, indent=2)
            except Exception as e:
                print(f"⚠️  保存进度失败: {e}")
            
            # 每只股票之间休息
            if idx + 1 < batch_end:
                print(f"⏸️  休息 {STOCK_REST_TIME} 秒...")
                time.sleep(STOCK_REST_TIME)
        
        # 批次完成统计
        batch_end_time = time.time()
        batch_elapsed = batch_end_time - batch_start_time
        print(f"\n✅ 批次 {batch_idx + 1} 完成! 本批次用时: {batch_elapsed:.1f}秒")
        
        # 批次间休息（除了最后一批）
        if batch_idx + 1 < total_batches:
            print(f"⏸️  批次间休息 {BATCH_REST_TIME} 秒...")
            time.sleep(BATCH_REST_TIME)
    
    stock_end_time = time.time()
    stock_elapsed = stock_end_time - stock_start_time
    print(f"\n✅ 所有股票数据处理完成! 总用时: {stock_elapsed:.1f}秒")
    
    # 删除进度文件
    if os.path.exists(PROGRESS_FILE):
        try:
            os.remove(PROGRESS_FILE)
            print("🗑️  进度文件已清理")
        except Exception as e:
            print(f"⚠️  清理进度文件失败: {e}")
    
    # 计算总用时
    total_elapsed = time.time() - start_time
    
    # 打印处理结果摘要
    print("\n" + "=" * 80)
    print("📊 股票数据处理完成总结")
    print("=" * 80)
    print(f"✅ 成功处理: {len(processed_files)}/{total_stocks} 只股票")
    print(f"⏱️  总用时: {total_elapsed:.1f}秒")
    print(f"⚡ 平均速度: {total_stocks/total_elapsed:.2f} 只/分钟")
    
    # 找出失败的股票
    stock_failures = []
    for code, name in stock_list:
        if not any(p_id == f"股票_{code}" for p_id, _, _ in processed_files):
            stock_failures.append((f"股票_{code}", name))
    
    # 报告失败情况
    if stock_failures:
        print(f"\n❌ 失败的股票 ({len(stock_failures)} 只):")
        for code, name in stock_failures:
            print(f"  - {name}({code.replace('股票_', '')})")
    
    if not stock_failures:
        print("\n🎉 所有股票都处理成功！")
    
    print("\n" + "=" * 80)
    print("📁 数据保存位置")
    print("=" * 80)
    print(f"📈 股票数据: 'stock_data' 目录")
    print(f"🕐 完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # 分段处理完成提示
    print("\n" + "=" * 80)
    print("🎯 分段处理模式说明")
    print("=" * 80)
    print("✅ 支持断点续传：如果脚本中断，重新运行会从上次停止的地方继续")
    print("✅ 批次处理：每批处理20只股票，批次间休息10秒")
    print("✅ 智能休息：每只股票间休息3秒，避免API限流")
    print("✅ 进度保存：实时保存处理进度到 processing_progress.json")
    print("✅ 自动清理：处理完成后自动清理进度文件")
    print("=" * 80)


if __name__ == "__main__":
    # 只处理股票数据
    print("🎯 运行模式：处理股票数据")
    main()