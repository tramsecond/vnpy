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
import yfinance as yf
import pytz
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

# ================= 通用函数 =================

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

def generate_hourly_view(df):
    """生成小时线数据（从日线数据生成，每个交易日按小时分组）"""
    if df.empty:
        return pd.DataFrame()
    
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    
    # 由于日线数据没有小时信息，我们需要为每个交易日生成小时线数据
    # 这里我们使用日线数据，但标记为小时线（实际是日线数据的复制）
    # 如果需要真正的小时线数据，需要从分钟数据聚合
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

def generate_weekly_view(df):
    """生成周线数据（按交易日分组，避免休息日导致的空白）"""
    if df.empty:
        return pd.DataFrame()
    
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    
    # 按周分组（每周从周一开始，周五结束）
    # 使用 'W-FRI' 表示每周以周五结束
    agg_dict = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }
    # 如果存在成交额列，则聚合成交额（求和）
    if 'amount' in df.columns:
        agg_dict['amount'] = 'sum'
    weekly = df.resample('W-FRI').agg(agg_dict)
    
    # 计算每周的交易天数，只保留至少有3个交易日的周
    # 这样可以避免不完整的周（比如只有1-2个交易日）被计入
    weekly_with_count = df.resample('W-FRI').size()
    weekly['trading_days'] = weekly_with_count
    
    # 删除空行（没有交易日的周会产生空行）
    weekly = weekly.dropna(subset=['open', 'high', 'low', 'close'])
    
    # 只保留至少有3个交易日的周（过滤掉不完整的周）
    weekly = weekly[weekly['trading_days'] >= 3]
    
    # 删除辅助列
    weekly = weekly.drop(columns=['trading_days'], errors='ignore')
    
    # 重置索引
    weekly = weekly.reset_index()
    return weekly

def generate_monthly_view(df):
    """生成月线数据"""
    if df.empty:
        return pd.DataFrame()
    
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    
    # 重采样为月线
    agg_dict = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }
    # 如果存在成交额列，则聚合成交额（求和）
    if 'amount' in df.columns:
        agg_dict['amount'] = 'sum'
    monthly = df.resample('ME').agg(agg_dict)
    
    monthly = monthly.reset_index()
    return monthly

def save_data_to_excel(df_daily, df_hourly, df_weekly, df_monthly, filename):
    """将数据保存到Excel文件，包含多个工作表（参考data_preparation.py的实现）"""
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
    
    if df_weekly is not None and not df_weekly.empty and 'date' not in df_weekly.columns:
        if df_weekly.index.name == 'date':
            df_weekly = df_weekly.reset_index()
        else:
            df_weekly['date'] = df_weekly.index
    
    if df_monthly is not None and not df_monthly.empty and 'date' not in df_monthly.columns:
        if df_monthly.index.name == 'date':
            df_monthly = df_monthly.reset_index()
        else:
            df_monthly['date'] = df_monthly.index
    
    # 创建Excel写入器
    try:
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            # 保存日线数据
            if not df_daily.empty:
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
        
    except Exception as e:
        print(f"  保存Excel文件时出错: {str(e)}")
        traceback.print_exc()

def clean_and_prepare_data(df):
    """
    数据清洗和准备（参考data_preparation.py的实现）
    :param df: 原始数据DataFrame
    :return: 清洗后的数据
    """
    # 检查数据是否为空
    if df is None or df.empty:
        print("  ❌ 警告: 数据为空")
        return pd.DataFrame()
    
    print("  🧹 数据清洗中...")
    
    # 重命名列（参考data_preparation.py的列名映射）
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
        if old_name in df.columns:
            df = df.rename(columns={old_name: new_name})
    
    # 添加日期列如果不存在
    if 'date' not in df.columns:
        if df.index.name == 'date' or df.index.name == '日期':
            df = df.reset_index()
        else:
            df['date'] = pd.to_datetime(df.index)
    
    # 确保日期列存在且有效
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    # 处理时区：如果日期列是tz-aware，转换为UTC时区然后去掉时区信息
    if pd.api.types.is_datetime64tz_dtype(df['date']):
        df['date'] = df['date'].dt.tz_convert('UTC').dt.tz_localize(None)
    
    # 过滤起始日期
    start_date_pd = pd.to_datetime(START_DATE)
    df = df[df['date'] >= start_date_pd]
    
    # 检查并删除重复日期
    duplicate_dates = df['date'].duplicated()
    if duplicate_dates.any():
        print(f"  🔍 发现 {duplicate_dates.sum()} 个重复日期，正在删除...")
        df = df[~duplicate_dates]
    
    # 确保数值列是数值类型
    for col in ['open', 'high', 'low', 'close', 'volume', 'amount']:
        if col in df.columns:
            if df[col].dtype != 'float64' and df[col].dtype != 'int64':
                df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 剔除停牌日或零交易量日（如果有volume列）
    if 'volume' in df.columns:
        df = df[df['volume'] > 0]
    
    # 排序数据
    df = df.sort_values('date', ascending=True)
    
    # 填充缺失值
    if df.isnull().values.any():
        print("  🔧 发现缺失值，正在填充...")
        for col in ['open', 'high', 'low', 'close', 'volume', 'amount']:
            if col in df.columns:
                df[col] = df[col].ffill().bfill()
        df = df.dropna()
    
    # 检查并打印日期范围
    if len(df) > 0:
        start_date = df['date'].min()
        end_date = df['date'].max()
        print(f"  📊 数据量: {len(df)} 条记录")
        print(f"  📅 日期范围: {start_date.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}")
    else:
        print("  ❌ 警告：清洗后没有有效数据")
        return pd.DataFrame()
    
    return df

def fetch_index_data_china(index_code, max_retries=3, base_delay=1):
    """使用国内数据源获取指数数据"""
    for attempt in range(max_retries):
        try:
            print(f"  使用国内数据源获取指数数据({index_code})... 尝试 {attempt + 1}/{max_retries}")
            
            # 添加随机延迟
            if attempt > 0:
                delay = base_delay * (2 ** attempt) + random.uniform(0, 0.5)
                print(f"  等待 {delay:.1f} 秒后重试...")
                time.sleep(delay)
            
            # 根据指数代码选择不同的获取方法
            if index_code == 'sh000001':
                # 上证指数 - 使用新浪数据源
                df_data = ak.stock_zh_index_daily(symbol="sh000001")
            elif index_code == 'sz399001':
                # 深证成指 - 使用新浪数据源
                df_data = ak.stock_zh_index_daily(symbol="sz399001")
            elif index_code == 'sz399006':
                # 创业板指 - 使用新浪数据源
                df_data = ak.stock_zh_index_daily(symbol="sz399006")
            elif index_code == 'bj899050':
                # 北证50 - 使用新浪数据源
                df_data = ak.stock_zh_index_daily(symbol="bj899050")
            elif index_code == 'sh000688':
                # 科创50 - 使用新浪数据源
                df_data = ak.stock_zh_index_daily(symbol="sh000688")
            elif index_code == '^HSI':
                # 恒生指数 - 使用新浪港股指数数据
                df_data = ak.stock_hk_index_daily_sina(symbol="HSI")
            elif index_code == '^HSTECH':
                # 恒生科技 - 使用新浪港股指数数据
                df_data = ak.stock_hk_index_daily_sina(symbol="HSTECH")
            elif index_code == '^DJI':
                # 道琼斯 - 使用新浪美股指数数据
                df_data = ak.index_us_stock_sina(symbol=".DJI")
            elif index_code == '^IXIC':
                # 纳斯达克指数 - 使用新浪美股指数数据
                df_data = ak.index_us_stock_sina(symbol=".IXIC")
            elif index_code == '^GSPC':
                # 标普500 - 使用新浪美股指数数据
                df_data = ak.index_us_stock_sina(symbol=".INX")
            else:
                # 其他指数尝试通用方法
                try:
                    df_data = ak.stock_zh_index_daily(symbol=index_code)
                except:
                    df_data = pd.DataFrame()
            
            if not df_data.empty:
                # 统一数据格式 - 处理各种可能的列名
                column_mapping = {}
                
                # 日期列映射
                date_cols = ['date', '日期', 'Date', 'DATE', 'time', 'Time', 'TIME', 'datetime', 'DateTime']
                for col in date_cols:
                    if col in df_data.columns:
                        column_mapping[col] = 'date'
                        break
                
                # 开盘价列映射
                open_cols = ['open', '开盘', 'Open', 'OPEN', 'o', 'O']
                for col in open_cols:
                    if col in df_data.columns:
                        column_mapping[col] = 'open'
                        break
                
                # 最高价列映射
                high_cols = ['high', '最高', 'High', 'HIGH', 'h', 'H']
                for col in high_cols:
                    if col in df_data.columns:
                        column_mapping[col] = 'high'
                        break
                
                # 最低价列映射
                low_cols = ['low', '最低', 'Low', 'LOW', 'l', 'L']
                for col in low_cols:
                    if col in df_data.columns:
                        column_mapping[col] = 'low'
                        break
                
                # 收盘价列映射
                close_cols = ['close', '收盘', 'Close', 'CLOSE', 'c', 'C', 'price', 'Price']
                for col in close_cols:
                    if col in df_data.columns:
                        column_mapping[col] = 'close'
                        break
                
                # 成交量列映射（可选）
                volume_cols = ['volume', '成交量', 'Volume', 'VOLUME', 'v', 'V', 'vol', 'Vol']
                for col in volume_cols:
                    if col in df_data.columns:
                        column_mapping[col] = 'volume'
                        break
                
                # 成交额列映射（可选）
                amount_cols = ['amount', '成交额', 'Amount', 'AMOUNT', 'turnover', 'Turnover', '成交', '金额']
                for col in amount_cols:
                    if col in df_data.columns:
                        column_mapping[col] = 'amount'
                        break
                
                # 应用列名映射
                if column_mapping:
                    df_data = df_data.rename(columns=column_mapping)
                
                # 确保必要的列存在
                required_cols = ['date', 'open', 'high', 'low', 'close']
                if all(col in df_data.columns for col in required_cols):
                    print(f"  成功获取数据，共 {len(df_data)} 行")
                    return df_data
                else:
                    missing_cols = [col for col in required_cols if col not in df_data.columns]
                    print(f"  数据格式不完整，缺少必要列: {missing_cols}")
                    print(f"  当前可用列: {list(df_data.columns)}")
            else:
                print(f"  尝试 {attempt + 1} 失败：数据为空")
                
        except Exception as e:
            error_msg = str(e)
            print(f"  尝试 {attempt + 1} 失败：{error_msg}")
            
            # 最后一次尝试失败
            if attempt == max_retries - 1:
                print(f"  所有 {max_retries} 次尝试都失败了")
                return None
    
    return None

def fetch_index_data_hourly_china(index_code, max_retries=3, base_delay=1):
    """使用国内数据源获取指数小时线数据"""
    for attempt in range(max_retries):
        try:
            print(f"  使用国内数据源获取指数小时线数据({index_code})... 尝试 {attempt + 1}/{max_retries}")
            
            # 添加随机延迟
            if attempt > 0:
                delay = base_delay * (2 ** attempt) + random.uniform(0, 0.5)
                print(f"  等待 {delay:.1f} 秒后重试...")
                time.sleep(delay)
            
            # 尝试获取分钟数据然后聚合为小时线
            df_hourly = pd.DataFrame()
            
            # 根据指数代码选择不同的获取方法
            # 使用 index_zh_a_hist_min_em 接口获取小时线数据（period="60"表示60分钟）
            # 注意：该接口只能返回近期数据，不能返回所有历史数据
            index_code_clean = index_code.replace('sh', '').replace('sz', '').replace('bj', '')
            
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
                    # 使用 index_zh_a_hist_min_em 获取60分钟数据
                    df_minute = ak.index_zh_a_hist_min_em(
                        symbol=index_code_clean, 
                        period="60", 
                        start_date=start_date_str, 
                        end_date=end_date_str
                    )
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
                # 统一数据格式 - index_zh_a_hist_min_em 返回的列名是中文
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

def fetch_index_data_hourly_hk(index_code, max_retries=3, base_delay=1):
    """获取恒生指数小时线数据"""
    for attempt in range(max_retries):
        try:
            print(f"  获取恒生指数小时线数据({index_code})... 尝试 {attempt + 1}/{max_retries}")
            
            # 添加随机延迟
            if attempt > 0:
                delay = base_delay * (2 ** attempt) + random.uniform(0, 0.5)
                print(f"  等待 {delay:.1f} 秒后重试...")
                time.sleep(delay)
            
            df_hourly = pd.DataFrame()
            
            # 恒生指数代码映射（yfinance使用的代码）
            symbol_map = {
                '^HSI': '^HSI',  # 恒生指数
                '^HSTECH': '^HSTECH'  # 恒生科技（yfinance中可能不支持，尝试其他代码）
            }
            
            # 方法1: 尝试使用yfinance获取小时线数据
            try:
                import yfinance as yf
                # 对于恒生科技，尝试不同的代码格式
                if index_code == '^HSTECH':
                    # 尝试多个可能的代码格式（已验证：HSTECH.HK 是有效代码）
                    ticker_codes = [
                        'HSTECH.HK',    # 港股格式（已验证有效！）
                        '^HSTECH',      # 标准格式
                        'HSTECH',       # 无前缀
                    ]
                else:
                    ticker_codes = [index_code]
                
                ticker = None
                df_hourly_temp = pd.DataFrame()
                for ticker_code in ticker_codes:
                    try:
                        ticker = yf.Ticker(ticker_code)
                        # 尝试获取最近N年的小时线数据，逐步回退
                        end_date = datetime.now()
                        max_days = HOURLY_DATA_YEARS * 365
                        
                        # 尝试多个时间范围，从最长到最短
                        time_ranges = [
                            (max_days, f"{HOURLY_DATA_YEARS}y"),      # 配置的年数
                            (int(max_days * 0.75), "18mo"),           # 18个月
                            (int(max_days * 0.5), "1y"),              # 1年
                            (int(max_days * 0.25), "6mo"),            # 6个月
                            (90, "3mo"),                               # 3个月
                            (30, "1mo"),                               # 1个月
                        ]
                        
                        df_hourly_temp = pd.DataFrame()
                        for days, period in time_ranges:
                            try:
                                start_date = end_date - timedelta(days=days)
                                print(f"    尝试获取最近 {days} 天的小时线数据（{period}）...")
                                
                                # 先尝试使用start和end参数
                                try:
                                    df_hourly_temp = ticker.history(
                                        start=start_date.strftime("%Y-%m-%d"),
                                        end=end_date.strftime("%Y-%m-%d"),
                                        interval="1h"
                                    )
                                except:
                                    # 如果失败，尝试使用period参数
                                    df_hourly_temp = ticker.history(period=period, interval="1h")
                                
                                if not df_hourly_temp.empty:
                                    print(f"    ✅ 成功获取小时线数据: {len(df_hourly_temp)} 条（最近 {days} 天）")
                                    break
                            except Exception as e:
                                print(f"    ⚠️  获取最近 {days} 天数据失败: {str(e)[:50]}")
                                continue
                        
                        if not df_hourly_temp.empty:
                            print(f"    使用代码 {ticker_code} 成功获取数据")
                            break
                    except:
                        continue
                
                if not df_hourly_temp.empty:
                    df_hourly = df_hourly_temp
                    # 转换列名
                    df_hourly = df_hourly.reset_index()
                    df_hourly.columns = [col.lower().replace(' ', '_') for col in df_hourly.columns]
                    # 重命名日期列
                    if 'datetime' in df_hourly.columns:
                        df_hourly = df_hourly.rename(columns={'datetime': 'date'})
                    elif 'date' not in df_hourly.columns and df_hourly.index.name == 'Date':
                        df_hourly = df_hourly.reset_index()
                        df_hourly = df_hourly.rename(columns={'Date': 'date'})
                    
                    # 确保列名正确
                    column_mapping = {
                        'open': 'open',
                        'high': 'high',
                        'low': 'low',
                        'close': 'close',
                        'volume': 'volume'
                    }
                    for old_col, new_col in column_mapping.items():
                        if old_col in df_hourly.columns and new_col not in df_hourly.columns:
                            df_hourly = df_hourly.rename(columns={old_col: new_col})
                    
                    print(f"    成功通过yfinance获取小时线数据: {len(df_hourly)} 条")
            except Exception as e:
                print(f"    yfinance获取失败: {e}")
            
            if not df_hourly.empty:
                # 确保必要的列存在
                required_cols = ['date', 'open', 'high', 'low', 'close']
                if all(col in df_hourly.columns for col in required_cols):
                    print(f"  成功获取恒生指数小时线数据，共 {len(df_hourly)} 行")
                    return df_hourly
                else:
                    missing_cols = [col for col in required_cols if col not in df_hourly.columns]
                    print(f"  小时线数据格式不完整，缺少必要列: {missing_cols}")
            else:
                print(f"  尝试 {attempt + 1} 失败：小时线数据为空")
                
        except Exception as e:
            error_msg = str(e)
            print(f"  尝试 {attempt + 1} 失败：{error_msg}")
            
            if attempt == max_retries - 1:
                print(f"  所有 {max_retries} 次尝试都失败了")
                return None
    
    return None

def fetch_index_data_hourly_us(index_code, max_retries=3, base_delay=1):
    """获取美股指数小时线数据"""
    for attempt in range(max_retries):
        try:
            print(f"  获取美股指数小时线数据({index_code})... 尝试 {attempt + 1}/{max_retries}")
            
            # 添加随机延迟
            if attempt > 0:
                delay = base_delay * (2 ** attempt) + random.uniform(0, 0.5)
                print(f"  等待 {delay:.1f} 秒后重试...")
                time.sleep(delay)
            
            df_hourly = pd.DataFrame()
            
            # 方法1: 尝试使用yfinance获取小时线数据
            try:
                import yfinance as yf
                ticker = yf.Ticker(index_code)
                # 尝试获取最近N年的小时线数据，逐步回退
                end_date = datetime.now()
                max_days = HOURLY_DATA_YEARS * 365
                
                # 尝试多个时间范围，从最长到最短
                time_ranges = [
                    (max_days, f"{HOURLY_DATA_YEARS}y"),      # 配置的年数
                    (int(max_days * 0.75), "18mo"),           # 18个月
                    (int(max_days * 0.5), "1y"),              # 1年
                    (int(max_days * 0.25), "6mo"),            # 6个月
                    (90, "3mo"),                               # 3个月
                    (30, "1mo"),                               # 1个月
                ]
                
                df_hourly = pd.DataFrame()
                for days, period in time_ranges:
                    try:
                        start_date = end_date - timedelta(days=days)
                        print(f"    尝试获取最近 {days} 天的小时线数据（{period}）...")
                        
                        # 先尝试使用start和end参数
                        try:
                            df_hourly = ticker.history(
                                start=start_date.strftime("%Y-%m-%d"),
                                end=end_date.strftime("%Y-%m-%d"),
                                interval="1h"
                            )
                        except:
                            # 如果失败，尝试使用period参数
                            df_hourly = ticker.history(period=period, interval="1h")
                        
                        if not df_hourly.empty:
                            print(f"    ✅ 成功获取小时线数据: {len(df_hourly)} 条（最近 {days} 天）")
                            break
                    except Exception as e:
                        print(f"    ⚠️  获取最近 {days} 天数据失败: {str(e)[:50]}")
                        continue
                if not df_hourly.empty:
                    # 转换列名
                    df_hourly = df_hourly.reset_index()
                    df_hourly.columns = [col.lower().replace(' ', '_') for col in df_hourly.columns]
                    # 重命名日期列
                    if 'datetime' in df_hourly.columns:
                        df_hourly = df_hourly.rename(columns={'datetime': 'date'})
                    elif 'date' not in df_hourly.columns and df_hourly.index.name == 'Date':
                        df_hourly = df_hourly.reset_index()
                        df_hourly = df_hourly.rename(columns={'Date': 'date'})
                    
                    # 确保列名正确
                    column_mapping = {
                        'open': 'open',
                        'high': 'high',
                        'low': 'low',
                        'close': 'close',
                        'volume': 'volume'
                    }
                    for old_col, new_col in column_mapping.items():
                        if old_col in df_hourly.columns and new_col not in df_hourly.columns:
                            df_hourly = df_hourly.rename(columns={old_col: new_col})
                    
                    print(f"    成功通过yfinance获取小时线数据: {len(df_hourly)} 条")
            except Exception as e:
                print(f"    yfinance获取失败: {e}")
            
            if not df_hourly.empty:
                # 确保必要的列存在
                required_cols = ['date', 'open', 'high', 'low', 'close']
                if all(col in df_hourly.columns for col in required_cols):
                    print(f"  成功获取美股指数小时线数据，共 {len(df_hourly)} 行")
                    return df_hourly
                else:
                    missing_cols = [col for col in required_cols if col not in df_hourly.columns]
                    print(f"  小时线数据格式不完整，缺少必要列: {missing_cols}")
            else:
                print(f"  尝试 {attempt + 1} 失败：小时线数据为空")
                
        except Exception as e:
            error_msg = str(e)
            print(f"  尝试 {attempt + 1} 失败：{error_msg}")
            
            if attempt == max_retries - 1:
                print(f"  所有 {max_retries} 次尝试都失败了")
                return None
    
    return None

def fetch_index_data(index_code, name, source='yfinance'):
    """获取指数数据"""
    print(f"📊 获取指数数据: {name}({index_code})")
    
    # 获取日线数据
    df_data = pd.DataFrame()
    
    try:
        # 对于所有国内数据源（包括A股、港股、美股），都使用fetch_index_data_china函数
        # 该函数使用新浪数据源，并带有重试机制
        if source == 'china':
            print(f"  📥 使用新浪数据源获取{name}日线数据...")
            df_data = fetch_index_data_china(index_code)
            
            if df_data is None or df_data.empty:
                print(f"  ❌ 无法获取{name}数据")
                return False
                
        elif source == 'hk':
            print(f"  📥 获取{name}日线数据...")
            df_data = ak.stock_hk_index_daily_em(symbol=index_code.replace('^', ''))
                
        elif source == 'us':
            print(f"  📥 获取{name}日线数据...")
            # 尝试不同的符号格式
            try:
                df_data = ak.stock_us_hist_min_em(symbol=f"{index_code}.NASDAQ")
            except:
                try:
                    df_data = ak.stock_us_hist_min_em(symbol=index_code)
                except:
                    print(f"  ❌ 无法获取{name}数据，跳过")
                    return False
        
        if df_data is not None and not df_data.empty:
            print(f"  ✅ 成功获取日线数据: {len(df_data)}条")
        else:
            print(f"  ❌ 无法获取{name}数据")
            return False
            
    except Exception as e:
        print(f"  ❌ 获取{name}数据失败: {e}")
        traceback.print_exc()
        return False
    
    # 数据清洗和准备
    if df_data is not None and not df_data.empty:
        print(f"  🧹 清洗{name}数据...")
        df_data = clean_and_prepare_data(df_data)
        
        # 计算日线技术指标
        print(f"  🧮 计算{name}日线技术指标...")
        try:
            df_data = calculate_ma(df_data)
            df_data = calculate_ema(df_data)
            df_data = calculate_macd(df_data)
            df_data = calculate_kdj(df_data)
            df_data = calculate_rsi(df_data)
            df_data = calculate_boll(df_data)
            df_data = calculate_trend_indicator_a(df_data)
            df_data = calculate_supertrend(df_data)
            df_data = calculate_qqe_mod(df_data)
            print(f"  ✅ {name}日线技术指标计算完成")
        except Exception as e:
            print(f"  ⚠️  计算日线技术指标时出错: {e}")
            traceback.print_exc()
        
        # 获取小时线数据
        print(f"  📈 获取{name}小时线数据...")
        df_hourly = pd.DataFrame()
        
        # 根据指数代码判断数据源类型（因为恒生和美股在fetch_index_data中source是'china'，但实际需要特殊处理）
        if index_code in ['^HSI', '^HSTECH']:
            # 恒生指数使用专门的函数
            df_hourly = fetch_index_data_hourly_hk(index_code)
            # 如果无法获取小时线数据，从日线数据生成
            if df_hourly is None or df_hourly.empty:
                print(f"  ⚠️  无法获取小时线数据，从日线数据生成...")
                df_hourly = generate_hourly_view(df_data.copy())
        elif index_code in ['^DJI', '^IXIC', '^GSPC']:
            # 美股指数使用专门的函数
            df_hourly = fetch_index_data_hourly_us(index_code)
            # 如果无法获取小时线数据，从日线数据生成
            if df_hourly is None or df_hourly.empty:
                print(f"  ⚠️  无法获取小时线数据，从日线数据生成...")
                df_hourly = generate_hourly_view(df_data.copy())
        elif source == 'china':
            # 国内A股指数
            df_hourly = fetch_index_data_hourly_china(index_code)
            # 如果无法获取小时线数据，从日线数据生成
            if df_hourly is None or df_hourly.empty:
                print(f"  ⚠️  无法获取小时线数据，从日线数据生成...")
                df_hourly = generate_hourly_view(df_data.copy())
        elif source == 'hk':
            # 恒生指数使用专门的函数
            df_hourly = fetch_index_data_hourly_hk(index_code)
            # 如果无法获取小时线数据，从日线数据生成
            if df_hourly is None or df_hourly.empty:
                print(f"  ⚠️  无法获取小时线数据，从日线数据生成...")
                df_hourly = generate_hourly_view(df_data.copy())
        elif source == 'us':
            # 美股指数使用专门的函数
            df_hourly = fetch_index_data_hourly_us(index_code)
            # 如果无法获取小时线数据，从日线数据生成
            if df_hourly is None or df_hourly.empty:
                print(f"  ⚠️  无法获取小时线数据，从日线数据生成...")
                df_hourly = generate_hourly_view(df_data.copy())
        else:
            # 其他数据源暂时从日线数据生成小时线
            df_hourly = generate_hourly_view(df_data.copy())
        
        # 计算小时线技术指标
        if not df_hourly.empty:
            print(f"  🧮 计算{name}小时线技术指标...")
            try:
                df_hourly = clean_and_prepare_data(df_hourly)
                df_hourly = calculate_ma(df_hourly)
                df_hourly = calculate_ema(df_hourly)
                df_hourly = calculate_macd(df_hourly)
                df_hourly = calculate_kdj(df_hourly)
                df_hourly = calculate_rsi(df_hourly)
                df_hourly = calculate_boll(df_hourly)
                df_hourly = calculate_trend_indicator_a(df_hourly)
                df_hourly = calculate_supertrend(df_hourly)
                df_hourly = calculate_qqe_mod(df_hourly)
                print(f"  ✅ {name}小时线技术指标计算完成")
            except Exception as e:
                print(f"  ⚠️  计算小时线技术指标时出错: {e}")
                traceback.print_exc()
        
        # 生成周线视图
        print(f"  📈 生成{name}周线数据...")
        df_weekly = generate_weekly_view(df_data.copy())
        
        # 计算周线技术指标
        if not df_weekly.empty:
            print(f"  🧮 计算{name}周线技术指标...")
            try:
                df_weekly = calculate_ma(df_weekly)
                df_weekly = calculate_ema(df_weekly)
                df_weekly = calculate_macd(df_weekly)
                df_weekly = calculate_kdj(df_weekly)
                df_weekly = calculate_rsi(df_weekly)
                df_weekly = calculate_boll(df_weekly)
                df_weekly = calculate_trend_indicator_a(df_weekly)
                df_weekly = calculate_supertrend(df_weekly)
                df_weekly = calculate_qqe_mod(df_weekly)
                print(f"  ✅ {name}周线技术指标计算完成")
            except Exception as e:
                print(f"  ⚠️  计算周线技术指标时出错: {e}")
                traceback.print_exc()
        
        # 生成月线视图
        print(f"  📈 生成{name}月线数据...")
        df_monthly = generate_monthly_view(df_data.copy())
        
        # 计算月线技术指标
        if not df_monthly.empty:
            print(f"  🧮 计算{name}月线技术指标...")
            try:
                df_monthly = calculate_ma(df_monthly)
                df_monthly = calculate_ema(df_monthly)
                df_monthly = calculate_macd(df_monthly)
                df_monthly = calculate_kdj(df_monthly)
                df_monthly = calculate_rsi(df_monthly)
                df_monthly = calculate_boll(df_monthly)
                df_monthly = calculate_trend_indicator_a(df_monthly)
                df_monthly = calculate_supertrend(df_monthly)
                df_monthly = calculate_qqe_mod(df_monthly)
                print(f"  ✅ {name}月线技术指标计算完成")
            except Exception as e:
                print(f"  ⚠️  计算月线技术指标时出错: {e}")
                traceback.print_exc()
        
        # 保存数据到Excel
        excel_file = f"index_data/{name}_{index_code}_data.xlsx"
        save_data_to_excel(df_data, df_hourly, df_weekly, df_monthly, excel_file)
        print(f"  ✅ {name}数据保存完成: {excel_file}")
        return True
    else:
        print(f"  ❌ {name}数据为空，无法处理")
        return False

# ================= 主函数 =================

def load_indices_from_csv(csv_file="indices.csv"):
    """从CSV文件加载指数列表"""
    try:
        if not os.path.exists(csv_file):
            print(f"错误：找不到指数配置文件 {csv_file}")
            print("请确保 indices.csv 文件存在并包含正确的指数信息")
            return []
        
        df = pd.read_csv(csv_file, encoding='utf-8')
        print(f"从 {csv_file} 加载了 {len(df)} 个指数")
        
        # 转换为字典列表
        indexes = []
        for _, row in df.iterrows():
            indexes.append({
                'code': row['code'],
                'name': row['name'],
                'source': row['source'],
                'description': row.get('description', '')
            })
        
        return indexes
    except Exception as e:
        print(f"读取指数配置文件时出错: {str(e)}")
        return []

def main():
    """主函数：准备指数数据"""
    # 准备指数数据
    print("开始准备指数数据...")
    
    # 从CSV文件加载指数列表
    indexes = load_indices_from_csv()
    
    if not indexes:
        print("未能加载指数列表，程序退出")
        return
    
    # 显示将要处理的指数
    print(f"\n将要处理以下 {len(indexes)} 个指数：")
    for i, index in enumerate(indexes, 1):
        desc = index.get('description', '')
        print(f"  {i}. {index['name']} ({index['code']}) - {desc}")
    
    # 准备指数数据
    index_files = []
    for idx, index in enumerate(indexes):
        print(f"\n[{idx+1}/{len(indexes)}] 处理指数 {index['name']}({index['code']})")
        excel_file = fetch_index_data(index['code'], index['name'], index['source'])
        if excel_file:
            print(f"  指数数据已保存到: {excel_file}")
            index_files.append(excel_file)
        else:
            print(f"  {index['name']}数据获取失败")
    
    print(f"\n指数数据处理完成！成功处理 {len(index_files)} 个指数")

if __name__ == "__main__":
    main()
