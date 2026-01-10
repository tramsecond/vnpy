#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
KDJ战法（BBIKDJSelector）策略回测脚本

策略逻辑：
1. 价格波动约束：最近 max_window 根收盘价的波动（high/low-1）≤ price_range_pct
2. BBI 上升：bbi_deriv_uptrend，允许一阶差分在 bbi_q_threshold 分位内为负（容忍回撤）
3. KDJ 低位：当周期 J 值 < j_threshold 或 ≤ 最近 max_window 的 j_q_threshold 分位
4. MACD：DIF > 0
5. MA60 条件：当周期 close ≥ MA60 且最近 max_window 内存在"有效上穿 MA60"
6. 知行当日约束：收盘 > 长期线 且 短期线 > 长期线
"""

import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime
import xlsxwriter
import math

# 导入配置文件
from config import INITIAL_CAPITAL, BACKTEST_START_DATE, BBI_KDJ_STRATEGY

# 支持的周期类型（暂时只进行日线级别回测）
TIME_FRAMES = ['日线']

# 强制设置UTF-8编码环境
import sys
sys.stdout.reconfigure(encoding='utf-8') if hasattr(sys.stdout, 'reconfigure') else None
sys.stderr.reconfigure(encoding='utf-8') if hasattr(sys.stderr, 'reconfigure') else None

if sys.platform.startswith('win'):
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    os.environ['LANG'] = 'zh_CN.UTF-8'
    os.environ['LC_ALL'] = 'zh_CN.UTF-8'

def safe_str(value):
    """安全转换值为字符串，处理编码问题"""
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        try:
            return value.decode('utf-8', errors='replace')
        except:
            try:
                return value.decode('gbk', errors='replace')
            except:
                return "编码错误"
    if isinstance(value, (pd.Timestamp)):
        return value.strftime('%Y-%m-%d')
    if isinstance(value, (np.int64, np.int32, np.float64)):
        return str(round(value, 3))
    return str(value)

def calculate_annualized_return(start_date, end_date, final_value, initial_capital):
    """计算年化收益率"""
    if not start_date or not end_date:
        return 0.0
    
    try:
        days = (end_date - start_date).days
        if days <= 0:
            return 0.0
        
        years = days / 365.0
        return_ratio = final_value / initial_capital
        annualized_return = (return_ratio ** (1 / years) - 1) if years > 0 else 0.0
        return annualized_return
    except:
        return 0.0

def calculate_bbi(df, periods=[3, 6, 12, 24]):
    """计算BBI（多空指标）"""
    df = df.copy()
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    
    # 计算各周期均线
    ma_list = []
    for period in periods:
        ma_col = f'MA{period}'
        if ma_col not in df.columns:
            df[ma_col] = df['close'].rolling(window=period).mean()
        ma_list.append(df[ma_col])
    
    # BBI = (MA3 + MA6 + MA12 + MA24) / 4
    df['BBI'] = pd.concat(ma_list, axis=1).mean(axis=1)
    
    return df

def check_bbi_uptrend(df, current_idx, max_window, bbi_q_threshold, bbi_min_window=None):
    """
    检查BBI是否上升趋势（允许一阶差分在分位内为负）
    参考原版策略的bbi_deriv_uptrend函数：
    1. 先归一化BBI（除以窗口起始值）
    2. 计算一阶差分
    3. 检查分位数是否 >= 0
    4. 支持自适应窗口搜索（从最长窗口向下搜索）
    """
    if bbi_min_window is None:
        bbi_min_window = max_window
    
    if current_idx < bbi_min_window:
        return False
    
    # 获取BBI序列（从开始到当前位置）
    bbi_series = df['BBI'].iloc[:current_idx + 1].dropna()
    
    if len(bbi_series) < bbi_min_window:
        return False
    
    # 自适应窗口搜索：从最长窗口向下搜索
    longest = min(len(bbi_series), max_window)
    
    for w in range(longest, bbi_min_window - 1, -1):
        # 获取窗口数据 [T-w+1, T]
        seg = bbi_series.iloc[-w:]
        if len(seg) < 2:
            continue
        
        # 归一化：除以窗口起始值
        norm = seg / seg.iloc[0]
        
        # 计算一阶差分
        diffs = np.diff(norm.values)
        
        if len(diffs) == 0:
            continue
        
        # 检查分位数是否 >= 0
        if np.quantile(diffs, bbi_q_threshold) >= 0:
            return True
    
    return False

def check_price_range(df, current_idx, max_window, price_range_pct):
    """
    检查价格波动约束：最近max_window根收盘价的波动（close.max/close.min-1）≤ price_range_pct
    
    原版策略逻辑：
    - price_range_pct 是小数形式：1 表示 100%，0.01 表示 1%
    - 计算：(close_max / close_min - 1)，结果也是小数形式
    - 条件：(close_max / close_min - 1) <= price_range_pct
    
    例如：
    - price_range_pct = 1：表示波动不超过100%（即最高价不超过最低价的2倍）
    - price_range_pct = 0.01：表示波动不超过1%（即最高价不超过最低价的1.01倍）
    
    注意：如果df中已有'price_range_check'列（预计算结果），直接使用该列的值
    """
    # 如果已经预计算，直接使用
    if 'price_range_check' in df.columns:
        if current_idx >= len(df):
            return False
        return bool(df.iloc[current_idx]['price_range_check'])
    
    # 否则使用原始计算方法（向后兼容）
    if current_idx < max_window - 1:
        return False
    
    # 获取最近max_window的数据
    window_data = df.iloc[max(0, current_idx - max_window + 1):current_idx + 1]
    
    if len(window_data) < max_window:
        return False
    
    # 原版策略：使用收盘价的最大值和最小值（不是最高价和最低价）
    close_max = window_data['close'].max()
    close_min = window_data['close'].min()
    
    if close_min <= 0:
        return False
    
    # 原版策略逻辑：price_range_pct 是小数形式（1表示100%，0.01表示1%）
    # 计算波动率（小数形式）
    price_range = (close_max / close_min - 1)
    
    # 条件：波动率 <= price_range_pct
    return price_range <= price_range_pct

def check_kdj_low(df, current_idx, max_window, j_threshold, j_q_threshold):
    """检查KDJ低位：J值 < j_threshold 或 ≤ 最近max_window的j_q_threshold分位"""
    if current_idx < max_window - 1:
        return False
    
    if 'J' not in df.columns:
        return False
    
    current_j = df.iloc[current_idx]['J']
    
    if pd.isna(current_j):
        return False
    
    # 条件1：J值 < j_threshold
    if current_j < j_threshold:
        return True
    
    # 条件2：J值 ≤ 最近max_window的j_q_threshold分位
    window_data = df.iloc[max(0, current_idx - max_window + 1):current_idx + 1]
    j_values = window_data['J'].dropna()
    
    if len(j_values) == 0:
        return False
    
    j_quantile = j_values.quantile(j_q_threshold)
    
    return current_j <= j_quantile

def check_ma60_condition(df, current_idx, max_window):
    """检查MA60条件：当周期close ≥ MA60 且最近max_window内存在"有效上穿MA60" """
    if 'MA60' not in df.columns or 'close' not in df.columns:
        return False
    
    current_close = df.iloc[current_idx]['close']
    current_ma60 = df.iloc[current_idx]['MA60']
    
    if pd.isna(current_close) or pd.isna(current_ma60):
        return False
    
    # 条件1：当前收盘价 >= MA60
    if current_close < current_ma60:
        return False
    
    # 条件2：最近max_window内存在"有效上穿MA60"
    if current_idx < max_window - 1:
        window_start = 0
    else:
        window_start = current_idx - max_window + 1
    
    window_data = df.iloc[window_start:current_idx + 1]
    
    # 检查是否有上穿（前一天close < MA60，当天close >= MA60）
    for i in range(1, len(window_data)):
        prev_close = window_data.iloc[i-1]['close']
        prev_ma60 = window_data.iloc[i-1]['MA60']
        curr_close = window_data.iloc[i]['close']
        curr_ma60 = window_data.iloc[i]['MA60']
        
        if pd.isna(prev_close) or pd.isna(prev_ma60) or pd.isna(curr_close) or pd.isna(curr_ma60):
            continue
        
        # 有效上穿：前一天close < MA60，当天close >= MA60
        if prev_close < prev_ma60 and curr_close >= curr_ma60:
            return True
    
    return False

def compute_zx_lines(df):
    """
    计算知行线（参考原版策略）
    返回 (ZXDQ, ZXDKX)
    ZXDQ = EMA(EMA(C,10),10)  # 短期线
    ZXDKX = (MA(C,14)+MA(C,28)+MA(C,57)+MA(C,114))/4  # 长期线
    """
    close = df['close'].astype(float)
    # 短期线：EMA(EMA(C,10),10)
    zxdq = close.ewm(span=10, adjust=False).mean().ewm(span=10, adjust=False).mean()
    
    # 长期线：(MA14+MA28+MA57+MA114)/4
    ma14 = close.rolling(window=14, min_periods=14).mean()
    ma28 = close.rolling(window=28, min_periods=28).mean()
    ma57 = close.rolling(window=57, min_periods=57).mean()
    ma114 = close.rolling(window=114, min_periods=114).mean()
    zxdkx = (ma14 + ma28 + ma57 + ma114) / 4.0
    
    return zxdq, zxdkx

def check_zhixing_constraint(df, current_idx, use_zx_lines=True):
    """
    检查知行当日约束：收盘 > 长期线 且 短期线 > 长期线
    如果use_zx_lines=True，使用原版策略的知行线计算方式
    如果use_zx_lines=False，使用配置的MA周期（向后兼容）
    
    注意：如果df中已有'ZXDQ'和'ZXDKX'列（预计算结果），直接使用该列的值
    """
    if use_zx_lines:
        # 如果已经预计算，直接使用
        if 'ZXDQ' in df.columns and 'ZXDKX' in df.columns:
            if current_idx >= len(df):
                return False
            s = float(df.iloc[current_idx]['ZXDQ'])
            l = float(df.iloc[current_idx]['ZXDKX']) if pd.notna(df.iloc[current_idx]['ZXDKX']) else float("nan")
            c = float(df.iloc[current_idx]['close'])
        else:
            # 否则重新计算（向后兼容）
            zxdq, zxdkx = compute_zx_lines(df)
            
            if current_idx >= len(zxdq) or current_idx >= len(zxdkx):
                return False
            
            s = float(zxdq.iloc[current_idx])
            l = float(zxdkx.iloc[current_idx]) if pd.notna(zxdkx.iloc[current_idx]) else float("nan")
            c = float(df.iloc[current_idx]['close'])
        
        if not np.isfinite(l) or not np.isfinite(s):
            return False
        
        # 收盘 > 长期线 且 短期线 > 长期线
        return (c > l) and (s > l)
    else:
        # 使用配置的MA周期（向后兼容）
        short_ma_period = BBI_KDJ_STRATEGY.get('SHORT_MA_PERIOD', 5)
        long_ma_period = BBI_KDJ_STRATEGY.get('LONG_MA_PERIOD', 20)
        short_ma_col = f'MA{short_ma_period}'
        long_ma_col = f'MA{long_ma_period}'
        
        if short_ma_col not in df.columns or long_ma_col not in df.columns or 'close' not in df.columns:
            return False
        
        current_close = df.iloc[current_idx]['close']
        current_short_ma = df.iloc[current_idx][short_ma_col]
        current_long_ma = df.iloc[current_idx][long_ma_col]
        
        if pd.isna(current_close) or pd.isna(current_short_ma) or pd.isna(current_long_ma):
            return False
        
        return current_close > current_long_ma and current_short_ma > current_long_ma

def check_buy_signal(df, current_idx, params, debug=False):
    """检查买入信号（KDJ战法）"""
    if current_idx < params['max_window']:
        if debug:
            print(f"      条件0失败: 索引不足 (current_idx={current_idx}, max_window={params['max_window']})")
        return False
    
    # 1. 价格波动约束（可通过配置启用/禁用）
    if params.get('ENABLE_PRICE_RANGE', True):  # 默认启用，如果配置中设置为False则跳过
        if not check_price_range(df, current_idx, params['max_window'], params['price_range_pct']):
            if debug:
                print(f"      条件1失败: 价格波动约束")
            return False
    
    # 2. BBI上升
    bbi_min_window = params.get('bbi_min_window', params['max_window'])
    if not check_bbi_uptrend(df, current_idx, params['max_window'], params['bbi_q_threshold'], bbi_min_window):
        if debug:
            print(f"      条件2失败: BBI上升")
        return False
    
    # 3. KDJ低位
    if not check_kdj_low(df, current_idx, params['max_window'], params['j_threshold'], params['j_q_threshold']):
        if debug:
            print(f"      条件3失败: KDJ低位")
        return False
    
    # 4. MACD：DIF > 0
    if 'DIF' not in df.columns:
        if debug:
            print(f"      条件4失败: DIF列不存在")
        return False
    current_dif = df.iloc[current_idx]['DIF']
    if pd.isna(current_dif) or current_dif <= 0:
        if debug:
            print(f"      条件4失败: DIF={current_dif} <= 0")
        return False
    
    # 5. MA60条件
    if not check_ma60_condition(df, current_idx, params['max_window']):
        if debug:
            print(f"      条件5失败: MA60条件")
        return False
    
    # 6. 知行当日约束（使用原版策略的知行线计算方式）
    if not check_zhixing_constraint(df, current_idx, use_zx_lines=True):
        if debug:
            print(f"      条件6失败: 知行约束")
        return False
    
    return True

def backtest_bbi_kdj_strategy(df, time_frame):
    """
    执行KDJ战法策略回测
    """
    if df is None or len(df) < BBI_KDJ_STRATEGY['max_window'] + 1:
        return None, None, None
    
    # 确保有必要的列
    required_columns = ['date', 'open', 'close', 'high', 'low']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"  缺少必要列: {', '.join(missing_columns)}")
        return None, None, None
    
    # 准备数据
    df = df.copy()
    
    # 1. 转换日期列
    df['date'] = pd.to_datetime(df['date'])
    
    # 2. 检查是否存在负数价格
    has_negative_prices = (df['open'] <= 0).any() or (df['close'] <= 0).any()
    
    # 3. 如果有负数价格，过滤配置的起始日期之前的数据
    if has_negative_prices:
        print(f"  检测到负数价格，过滤{BACKTEST_START_DATE}之前的数据")
        start_date = pd.Timestamp(BACKTEST_START_DATE)
        df = df[df['date'] >= start_date].copy()
        if len(df) < BBI_KDJ_STRATEGY['max_window'] + 1:
            print("  过滤后数据不足，跳过回测")
            return None, None, None
    
    # 4. 排序数据
    df.sort_values('date', inplace=True)
    
    # 5. 确保数值列是数值类型
    for col in ['open', 'high', 'low', 'close']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 6. 计算BBI指标（如果不存在）
    if 'BBI' not in df.columns:
        df = calculate_bbi(df)
    
    # 7. 确保必要的指标存在
    required_indicators = ['MA60', 'J', 'DIF', f"MA{BBI_KDJ_STRATEGY['SHORT_MA_PERIOD']}", f"MA{BBI_KDJ_STRATEGY['LONG_MA_PERIOD']}"]
    missing_indicators = [ind for ind in required_indicators if ind not in df.columns]
    if missing_indicators:
        print(f"  缺少必要指标: {', '.join(missing_indicators)}")
        return None, None, None
    
    # 8. 去除NaN值（保留足够的行数）
    df = df.dropna(subset=['date', 'open', 'close', 'high', 'low', 'BBI', 'MA60', 'J', 'DIF'])
    if len(df) < BBI_KDJ_STRATEGY['max_window'] + 1:
        print("  数据清理后不足，跳过回测")
        return None, None, None
    
    # 9. 预计算知行线（避免在循环中重复计算）
    zxdq, zxdkx = compute_zx_lines(df)
    df['ZXDQ'] = zxdq
    df['ZXDKX'] = zxdkx
    
    # 10. 预计算价格波动约束结果（向量化计算，大幅提升性能）
    if BBI_KDJ_STRATEGY.get('ENABLE_PRICE_RANGE', True):
        max_window = BBI_KDJ_STRATEGY['max_window']
        price_range_pct = BBI_KDJ_STRATEGY['price_range_pct']
        
        # 使用rolling窗口计算每个位置的价格波动
        close_max = df['close'].rolling(window=max_window, min_periods=max_window).max()
        close_min = df['close'].rolling(window=max_window, min_periods=max_window).min()
        price_range = (close_max / close_min - 1)
        df['price_range_check'] = (price_range <= price_range_pct) | (close_min <= 0)
    else:
        df['price_range_check'] = True  # 如果禁用，所有位置都通过
    
    # 初始化变量
    cash = INITIAL_CAPITAL
    position = 0
    trades = []
    equity_values = []
    buy_price = 0
    buy_date = None
    win_count = 0
    trade_count = 0
    total_hold_days = 0  # 总持有天数（自然天）
    
    # 记录开始和结束日期
    start_date = df['date'].min()
    end_date = df['date'].max()
    
    # 获取回测开始和结束时的价格
    backtest_start_price = df.iloc[0]['close']
    backtest_end_price = df.iloc[-1]['close']
    backtest_price_change_pct = ((backtest_end_price - backtest_start_price) / backtest_start_price * 100) if backtest_start_price > 0 else 0
    
    # 计算买入并持有策略的收益
    buy_and_hold_return = backtest_price_change_pct / 100
    
    # 统计信号检查情况（用于调试）
    signal_check_stats = {
        'total_checks': 0,
        'price_range_pass': 0,
        'bbi_uptrend_pass': 0,
        'kdj_low_pass': 0,
        'dif_pass': 0,
        'ma60_pass': 0,
        'zhixing_pass': 0,
        'all_pass': 0
    }
    
    # 遍历每一行数据（从max_window开始，因为需要足够的历史数据）
    for i in range(BBI_KDJ_STRATEGY['max_window'], len(df)):
        row = df.iloc[i]
        date = row['date']
        close_price = row['close']
        open_price = row['open']
        
        # 计算当前资产净值
        current_equity = cash + position * close_price
        equity_values.append({
            'date': date,
            'equity': current_equity,
            'position': position,
            'cash': cash,
            'close_price': close_price
        })
        
        # 检查止盈止损（如果持仓）
        if position > 0 and buy_price > 0:
            price_change_pct = ((close_price - buy_price) / buy_price * 100) if buy_price > 0 else 0
            
            # 止盈
            if BBI_KDJ_STRATEGY['ENABLE_PROFIT_TAKE'] and price_change_pct >= BBI_KDJ_STRATEGY['PROFIT_TAKE_PCT']:
                sell_price = close_price
                sell_amount = position * sell_price
                cash += sell_amount
                
                profit = (sell_price - buy_price) * position
                is_win = profit > 0
                if is_win:
                    win_count += 1
                trade_count += 1
                
                # 计算持有天数（自然天）
                if buy_date:
                    hold_days = (date - buy_date).days
                    total_hold_days += hold_days
                
                trades.append({
                    'date': date,
                    'action': '止盈卖出',
                    'price': sell_price,
                    'position': position,
                    'amount': sell_amount,
                    'buy_price': buy_price,
                    'profit': profit,
                    'profit_pct': price_change_pct,
                    'is_win': is_win,
                    'reason': f"止盈({BBI_KDJ_STRATEGY['PROFIT_TAKE_PCT']}%)",
                    'equity': cash
                })
                
                position = 0
                buy_price = 0
                buy_date = None
                continue
            
            # 止损
            if BBI_KDJ_STRATEGY['ENABLE_STOP_LOSS'] and price_change_pct <= -BBI_KDJ_STRATEGY['STOP_LOSS_PCT']:
                sell_price = close_price
                sell_amount = position * sell_price
                cash += sell_amount
                
                profit = (sell_price - buy_price) * position
                is_win = profit > 0
                if is_win:
                    win_count += 1
                trade_count += 1
                
                # 计算持有天数（自然天）
                if buy_date:
                    hold_days = (date - buy_date).days
                    total_hold_days += hold_days
                
                trades.append({
                    'date': date,
                    'action': '止损卖出',
                    'price': sell_price,
                    'position': position,
                    'amount': sell_amount,
                    'buy_price': buy_price,
                    'profit': profit,
                    'profit_pct': price_change_pct,
                    'is_win': is_win,
                    'reason': f"止损({BBI_KDJ_STRATEGY['STOP_LOSS_PCT']}%)",
                    'equity': cash
                })
                
                position = 0
                buy_price = 0
                buy_date = None
                continue
        
        # 买入逻辑：检查KDJ战法买入信号（优化：减少重复计算）
        if position == 0 and cash > 0:
            signal_check_stats['total_checks'] += 1
            
            # 快速检查：先检查简单的条件，避免复杂计算
            # 1. DIF > 0（最快检查）
            if 'DIF' in df.columns:
                current_dif = df.iloc[i]['DIF']
                if pd.isna(current_dif) or current_dif <= 0:
                    # DIF不满足，跳过后续检查
                    continue
                else:
                    signal_check_stats['dif_pass'] += 1
            else:
                continue
            
            # 2. 价格波动约束（如果已预计算，直接使用）
            if BBI_KDJ_STRATEGY.get('ENABLE_PRICE_RANGE', True):
                if not check_price_range(df, i, BBI_KDJ_STRATEGY['max_window'], BBI_KDJ_STRATEGY['price_range_pct']):
                    continue
                signal_check_stats['price_range_pass'] += 1
            else:
                signal_check_stats['price_range_pass'] += 1
            
            # 3. 检查完整买入信号（其他复杂条件）
            if check_buy_signal(df, i, BBI_KDJ_STRATEGY):
                signal_check_stats['all_pass'] += 1
                # 统计其他条件（用于调试，只在通过时统计）
                bbi_min_window = BBI_KDJ_STRATEGY.get('bbi_min_window', BBI_KDJ_STRATEGY['max_window'])
                if check_bbi_uptrend(df, i, BBI_KDJ_STRATEGY['max_window'], BBI_KDJ_STRATEGY['bbi_q_threshold'], bbi_min_window):
                    signal_check_stats['bbi_uptrend_pass'] += 1
                if check_kdj_low(df, i, BBI_KDJ_STRATEGY['max_window'], BBI_KDJ_STRATEGY['j_threshold'], BBI_KDJ_STRATEGY['j_q_threshold']):
                    signal_check_stats['kdj_low_pass'] += 1
                if check_ma60_condition(df, i, BBI_KDJ_STRATEGY['max_window']):
                    signal_check_stats['ma60_pass'] += 1
                if check_zhixing_constraint(df, i, use_zx_lines=True):
                    signal_check_stats['zhixing_pass'] += 1
                
                # 如果是最后一行，使用当前行的开盘价买入
                if i >= len(df) - 1:
                    buy_price = open_price if open_price > 0 else close_price
                    if buy_price <= 0:
                        continue
                    
                    position = cash / buy_price
                    cash = 0
                    buy_date = date
                    
                    trades.append({
                        'date': date,
                        'action': '买入（KDJ战法）',
                        'price': buy_price,
                        'position': position,
                        'amount': position * buy_price,
                        'signal': 'KDJ战法买入信号',
                        'equity': position * buy_price
                    })
                    # 买入不算交易，只有卖出才算一次完整交易
                    continue
                
                # 获取下一个交易日的数据
                next_row = df.iloc[i+1]
                next_date = next_row['date']
                next_open = next_row['open']
                
                if next_open <= 0:
                    continue
                
                # 使用下一个交易日的开盘价买入
                buy_price = next_open
                position = cash / buy_price
                cash = 0
                buy_date = next_date
                
                trades.append({
                    'date': next_date,
                    'action': '买入（KDJ战法）',
                    'price': buy_price,
                    'position': position,
                    'amount': position * buy_price,
                    'signal': 'KDJ战法买入信号',
                    'equity': position * buy_price
                })
                # 买入不算交易，只有卖出才算一次完整交易
        
        # 卖出逻辑：信号卖出（可以添加其他卖出条件）
        elif position > 0:
            # 这里可以添加卖出信号判断，暂时只通过止盈止损卖出
            # 如果需要信号卖出，可以检查SELL_SIGNALS
            pass
    
    # 处理最终持仓
    final_price = df.iloc[-1]['close']
    final_equity = cash + position * final_price
    
    # 如果最后还有持仓，记录未实现的盈亏
    if position > 0 and buy_price > 0:
        final_profit = (final_price - buy_price) * position
        final_profit_pct = ((final_price - buy_price) / buy_price * 100) if buy_price > 0 else 0
        
        final_equity = cash + position * final_price
        
        # 最终持仓也算作一次交易
        is_win = final_profit > 0
        if is_win:
            win_count += 1
        trade_count += 1
        
        # 计算最终持仓的持有天数（自然天）
        if buy_date:
            hold_days = (df.iloc[-1]['date'] - buy_date).days
            total_hold_days += hold_days
        
        trades.append({
            'date': df.iloc[-1]['date'],
            'action': '持仓（未卖出）',
            'price': final_price,
            'position': position,
            'amount': position * final_price,
            'buy_price': buy_price,
            'profit': final_profit,
            'profit_pct': final_profit_pct,
            'is_win': is_win,
            'signal': '回测结束',
            'equity': final_equity
        })
    
    # 计算统计
    equity_df = pd.DataFrame(equity_values)
    if equity_df.empty:
        return None, None, None
    
    max_equity = equity_df['equity'].max()
    min_equity = equity_df['equity'].min()
    win_rate = win_count / trade_count if trade_count > 0 else 0
    
    return_ratio = (final_equity / INITIAL_CAPITAL) - 1
    annualized_return = calculate_annualized_return(start_date, end_date, final_equity, INITIAL_CAPITAL)
    buy_and_hold_annualized = calculate_annualized_return(start_date, end_date, INITIAL_CAPITAL * (1 + buy_and_hold_return), INITIAL_CAPITAL)
    
    # 计算持有年化涨幅：策略涨幅 / (持有天数 / 365)
    hold_years = total_hold_days / 365.0 if total_hold_days > 0 else 0
    hold_annualized_return = (return_ratio / hold_years) if hold_years > 0 else 0
    
    stats = {
        '股票代码': '',
        '时间周期': time_frame,
        '初始资金': INITIAL_CAPITAL,
        '最终资产净值': final_equity,
        '资产净值最大值': max_equity,
        '资产净值最小值': min_equity,
        '胜率': win_rate,
        '策略涨幅': return_ratio,
        '策略年化涨幅': annualized_return,
        '持有年化涨幅': hold_annualized_return,
        '总持有天数': total_hold_days,
        '一直持有涨幅': buy_and_hold_return,
        '一直持有年化涨幅': buy_and_hold_annualized,
        '策略超额收益': return_ratio - buy_and_hold_return,
        '策略超额年化收益': annualized_return - buy_and_hold_annualized,
        '交易次数': trade_count,
        '盈利交易次数': win_count,
        '开始日期': start_date,
        '结束日期': end_date,
        '回测开始价格': backtest_start_price,
        '回测结束价格': backtest_end_price,
        '期间价格涨跌幅': backtest_price_change_pct / 100,
        '最终持仓': position,
        '最终现金': cash
    }
    
    return stats, trades, equity_df

def backtest_single_file(file_path, output_dir):
    """对单个文件进行回测"""
    try:
        file_name = os.path.splitext(os.path.basename(file_path))[0]
        print(f"开始回测: {file_name}")
        
        all_stats = []
        all_trades = []
        all_equity = []
        
        # 处理所有周期
        for time_frame in TIME_FRAMES:
            sheet_name = f"{time_frame}数据"
            
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name)
            except Exception as e:
                print(f"  读取{time_frame}数据失败: {e}")
                continue
            
            if df.empty:
                print(f"  {time_frame}数据为空，跳过")
                continue
            
            # 规范列名
            df.columns = [safe_str(col).strip().replace(' ', '') for col in df.columns]
            
            # 清理数据
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df = df.dropna(subset=['date'])
            df = df[(df['open'] > 0) & (df['close'] > 0)]
            
            if len(df) < BBI_KDJ_STRATEGY['max_window'] + 1:
                print(f"  {time_frame}数据清理后不足{BBI_KDJ_STRATEGY['max_window'] + 1}行，跳过回测")
                continue
            
            print(f"  回测{time_frame}...")
            stats, trades, equity_df = backtest_bbi_kdj_strategy(df, time_frame)
            
            if stats is None:
                print(f"  {time_frame}回测失败")
                continue
            
            stats['股票代码'] = file_name
            
            # 为交易记录和资产净值添加时间周期标识
            if trades:
                for trade in trades:
                    trade['时间周期'] = time_frame
                all_trades.extend(trades)
            
            if equity_df is not None and not equity_df.empty:
                equity_df['时间周期'] = time_frame
                all_equity.append(equity_df)
            
            all_stats.append(stats)
            
            # 打印关键信息
            print(f"    {time_frame}策略涨幅: {stats['策略涨幅']:.2%}")
            print(f"    {time_frame}一直持有涨幅: {stats['一直持有涨幅']:.2%}")
            print(f"    {time_frame}策略超额收益: {stats['策略超额收益']:.2%}")
            print(f"    {time_frame}胜率: {stats['胜率']:.2%}")
            print(f"    {time_frame}交易次数: {stats['交易次数']}")
        
        if not all_stats:
            print("  所有周期回测失败")
            return None
        
        # 生成输出文件名
        output_filename = f"{file_name}_KDJ战法_回测结果.xlsx"
        output_path = os.path.join(output_dir, output_filename)
        
        # 创建Excel文件
        try:
            writer = pd.ExcelWriter(output_path, engine='xlsxwriter')
            workbook = writer.book
            
            number_format = workbook.add_format({'num_format': '0.00'})
            integer_format = workbook.add_format({'num_format': '0'})  # 整数格式
            day_format = workbook.add_format({'num_format': '0.0'})  # 天数格式（1位小数）
            percent_format = workbook.add_format({'num_format': '0.00%'})
            currency_format = workbook.add_format({'num_format': '¥#,##0.00'})
            
            # 写入统计数据
            stats_df = pd.DataFrame(all_stats)
            stats_df.to_excel(writer, sheet_name='回测统计', index=False)
            
            worksheet = writer.sheets["回测统计"]
            for col_num, col_name in enumerate(stats_df.columns):
    # 整数字段（交易次数、股票数等）
    if any(keyword in col_name for keyword in ['交易次数', '盈利交易次数', '总交易次数', '总盈利交易次数', '总股票数', '盈利股票数', '亏损股票数', '趋势状态次数', '网格份数', '最终网格份数']):
        worksheet.set_column(col_num + 1, col_num + 1, 15, integer_format)
    # 天数字段（保留1位小数）
    elif any(keyword in col_name for keyword in ['持有天数', '总持有天数', '平均持股天数']):
        worksheet.set_column(col_num + 1, col_num + 1, 15, day_format)
    # 百分比字段
    elif '涨跌幅' in col_name or '胜率' in col_name or '收益' in col_name or '利用率' in col_name or '占比' in col_name:
        worksheet.set_column(col_num + 1, col_num + 1, 15, percent_format)
    # 货币字段
    elif any(keyword in col_name for keyword in ['成本', '市值', '资金', '资产', '价格', '金额', '盈亏', '现金']):
        worksheet.set_column(col_num + 1, col_num + 1, 15, currency_format)
    # 默认数字格式
    else:
        worksheet.set_column(col_num + 1, col_num + 1, 15, number_format)
            
            # 写入交易记录
            if all_trades:
                trades_df = pd.DataFrame(all_trades)
                trades_df.to_excel(writer, sheet_name='交易记录', index=False)
                
                worksheet = writer.sheets["交易记录"]
                for col_num, col_name in enumerate(trades_df.columns):
                    worksheet.set_column(col_num + 1, col_num + 1, 15, number_format)
            
            # 写入资产净值曲线（合并所有周期）
            if all_equity:
                equity_combined = pd.concat(all_equity, ignore_index=True)
                equity_combined.to_excel(writer, sheet_name='资产净值', index=False)
            
            writer.close()
            print(f"  回测完成，结果保存到: {output_path}")
            
        except PermissionError as e:
            print(f"  文件访问权限错误: {e}")
            return None
        
        return all_stats
        
    except Exception as e:
        print(f"  回测过程中出现错误: {e}")
        import traceback
        traceback.print_exc()
        return None

def generate_summary_report(all_stats, output_dir):
    """生成汇总报告"""
    if not all_stats:
        print("没有统计数据，无法生成汇总报告")
        return
    
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        summary_filename = f"KDJ战法_汇总报告_{timestamp}.xlsx"
        summary_path = os.path.join(output_dir, summary_filename)
        
        writer = pd.ExcelWriter(summary_path, engine='xlsxwriter')
        workbook = writer.book
        
        number_format = workbook.add_format({'num_format': '0.00'})
            integer_format = workbook.add_format({'num_format': '0'})  # 整数格式
            day_format = workbook.add_format({'num_format': '0.0'})  # 天数格式（1位小数）
        percent_format = workbook.add_format({'num_format': '0.00%'})
        currency_format = workbook.add_format({'num_format': '¥#,##0.00'})
        
        # 汇总所有统计数据
        summary_data = []
        for file_stats in all_stats:
            if file_stats:
                for stats in file_stats:
                    summary_data.append(stats)
        
        if summary_data:
            summary_df = pd.DataFrame(summary_data)
            
            # 按股票代码和时间周期排序
            summary_df = summary_df.sort_values(['股票代码', '时间周期'])
            summary_df.to_excel(writer, sheet_name='全部数据', index=False)
            
            worksheet = writer.sheets["全部数据"]
            for col_num, col_name in enumerate(summary_df.columns):
    # 整数字段（交易次数、股票数等）
    if any(keyword in col_name for keyword in ['交易次数', '盈利交易次数', '总交易次数', '总盈利交易次数', '总股票数', '盈利股票数', '亏损股票数', '趋势状态次数', '网格份数', '最终网格份数']):
        worksheet.set_column(col_num + 1, col_num + 1, 15, integer_format)
    # 天数字段（保留1位小数）
    elif any(keyword in col_name for keyword in ['持有天数', '总持有天数', '平均持股天数']):
        worksheet.set_column(col_num + 1, col_num + 1, 15, day_format)
    # 百分比字段
    elif '涨跌幅' in col_name or '胜率' in col_name or '收益' in col_name or '利用率' in col_name or '占比' in col_name:
        worksheet.set_column(col_num + 1, col_num + 1, 15, percent_format)
    # 货币字段
    elif any(keyword in col_name for keyword in ['成本', '市值', '资金', '资产', '价格', '金额', '盈亏', '现金']):
        worksheet.set_column(col_num + 1, col_num + 1, 15, currency_format)
    # 默认数字格式
    else:
        worksheet.set_column(col_num + 1, col_num + 1, 15, number_format)
            
            # 为每个周期创建单独的工作表
            for time_frame in TIME_FRAMES:
                frame_df = summary_df[summary_df['时间周期'] == time_frame].copy()
                if not frame_df.empty:
                    # 删除时间周期列
                    if '时间周期' in frame_df.columns:
                        frame_df = frame_df.drop(columns=['时间周期'])
                    
                    # 按股票代码排序
                    frame_df = frame_df.sort_values('股票代码')
                    frame_df.to_excel(writer, sheet_name=time_frame, index=False)
                    
                    worksheet = writer.sheets[time_frame]
                    for col_num, col_name in enumerate(frame_df.columns):
    # 整数字段（交易次数、股票数等）
    if any(keyword in col_name for keyword in ['交易次数', '盈利交易次数', '总交易次数', '总盈利交易次数', '总股票数', '盈利股票数', '亏损股票数', '趋势状态次数', '网格份数', '最终网格份数']):
        worksheet.set_column(col_num + 1, col_num + 1, 15, integer_format)
    # 天数字段（保留1位小数）
    elif any(keyword in col_name for keyword in ['持有天数', '总持有天数', '平均持股天数']):
        worksheet.set_column(col_num + 1, col_num + 1, 15, day_format)
    # 百分比字段
    elif '涨跌幅' in col_name or '胜率' in col_name or '收益' in col_name or '利用率' in col_name or '占比' in col_name:
        worksheet.set_column(col_num + 1, col_num + 1, 15, percent_format)
    # 货币字段
    elif any(keyword in col_name for keyword in ['成本', '市值', '资金', '资产', '价格', '金额', '盈亏', '现金']):
        worksheet.set_column(col_num + 1, col_num + 1, 15, currency_format)
    # 默认数字格式
    else:
        worksheet.set_column(col_num + 1, col_num + 1, 15, number_format)
            
            # 整体统计
            overall_stats = {
                '统计项': '全部周期汇总',
                '总股票数': len(summary_df['股票代码'].unique()),
                '总交易次数': summary_df['交易次数'].sum(),
                '总盈利交易次数': summary_df['盈利交易次数'].sum(),
                '平均胜率': summary_df['胜率'].mean(),
                '平均策略涨幅': summary_df['策略涨幅'].mean(),
                '平均策略年化涨幅': summary_df['策略年化涨幅'].mean(),
                '平均一直持有涨幅': summary_df['一直持有涨幅'].mean(),
                '平均一直持有年化涨幅': summary_df['一直持有年化涨幅'].mean(),
                '平均策略超额收益': summary_df['策略超额收益'].mean(),
                '平均策略超额年化收益': summary_df['策略超额年化收益'].mean(),
                '最大策略涨幅': summary_df['策略涨幅'].max(),
                '最小策略涨幅': summary_df['策略涨幅'].min(),
                '亏损股票数': len(summary_df[summary_df['策略涨幅'] < 0]),
                '盈利股票平均策略涨幅': summary_df[summary_df['策略涨幅'] > 0]['策略涨幅'].mean() if len(summary_df[summary_df['策略涨幅'] > 0]) > 0 else 0,
                '亏损股票平均亏损比': summary_df[summary_df['策略涨幅'] < 0]['策略涨幅'].mean() if len(summary_df[summary_df['策略涨幅'] < 0]) > 0 else 0,
            }
            
            # 按时间周期分别统计
            period_stats_list = []
            for time_frame in TIME_FRAMES:
                frame_df = summary_df[summary_df['时间周期'] == time_frame]
                if not frame_df.empty:
                    period_stats = {
                        '统计项': time_frame,
                        '总股票数': len(frame_df['股票代码'].unique()),
                        '总交易次数': frame_df['交易次数'].sum(),
                        '总盈利交易次数': frame_df['盈利交易次数'].sum(),
                        '平均胜率': frame_df['胜率'].mean(),
                        '平均策略涨幅': frame_df['策略涨幅'].mean(),
                        '平均策略年化涨幅': frame_df['策略年化涨幅'].mean(),
                        '平均一直持有涨幅': frame_df['一直持有涨幅'].mean(),
                        '平均一直持有年化涨幅': frame_df['一直持有年化涨幅'].mean(),
                        '平均策略超额收益': frame_df['策略超额收益'].mean(),
                        '平均策略超额年化收益': frame_df['策略超额年化收益'].mean(),
                        '最大策略涨幅': frame_df['策略涨幅'].max(),
                        '最小策略涨幅': frame_df['策略涨幅'].min(),
                        '亏损股票数': len(frame_df[frame_df['策略涨幅'] < 0]),
                        '盈利股票平均策略涨幅': frame_df[frame_df['策略涨幅'] > 0]['策略涨幅'].mean() if len(frame_df[frame_df['策略涨幅'] > 0]) > 0 else 0,
                        '亏损股票平均亏损比': frame_df[frame_df['策略涨幅'] < 0]['策略涨幅'].mean() if len(frame_df[frame_df['策略涨幅'] < 0]) > 0 else 0,
                    }
                    period_stats_list.append(period_stats)
            
            # 合并整体统计和周期统计
            all_stats_list = [overall_stats] + period_stats_list
            overall_df = pd.DataFrame(all_stats_list)
            overall_df.to_excel(writer, sheet_name='整体统计', index=False)
            
            worksheet = writer.sheets["整体统计"]
            for col_num, col_name in enumerate(overall_df.columns):
                if col_name == '统计项':
                    worksheet.set_column(col_num + 1, col_num + 1, 15)
                elif '涨跌幅' in col_name or '胜率' in col_name:
                    worksheet.set_column(col_num + 1, col_num + 1, 15, percent_format)
                elif '资金' in col_name or '资产' in col_name or '价格' in col_name:
                    worksheet.set_column(col_num + 1, col_num + 1, 15, currency_format)
                else:
                    worksheet.set_column(col_num + 1, col_num + 1, 15, number_format)
        
        writer.close()
        print(f"汇总报告已生成: {summary_path}")
        
    except PermissionError as e:
        print(f"生成汇总报告时出现权限错误: {e}")
    except Exception as e:
        print(f"生成汇总报告时出现错误: {e}")
        import traceback
        traceback.print_exc()

def main():
    # 输入和输出目录
    input_dir = "analyzed_results"
    output_dir = "backtest_results_bbi_kdj"
    
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 获取所有Excel文件
    excel_files = glob.glob(os.path.join(input_dir, "*.xlsx"))
    
    if not excel_files:
        print(f"在 {input_dir} 目录中没有找到Excel文件")
        return
    
    print(f"找到 {len(excel_files)} 个文件进行KDJ战法策略回测")
    print(f"策略说明:")
    if BBI_KDJ_STRATEGY.get('ENABLE_PRICE_RANGE', True):
        print(f"  - 价格波动约束: 最近{BBI_KDJ_STRATEGY['max_window']}根K线的波动 ≤ {BBI_KDJ_STRATEGY['price_range_pct']}% [已启用]")
    else:
        print(f"  - 价格波动约束: [已禁用]")
    print(f"  - BBI上升: 允许一阶差分在{BBI_KDJ_STRATEGY['bbi_q_threshold']}分位内为负")
    print(f"  - KDJ低位: J值 < {BBI_KDJ_STRATEGY['j_threshold']} 或 ≤ {BBI_KDJ_STRATEGY['j_q_threshold']}分位")
    print(f"  - MACD: DIF > 0")
    print(f"  - MA60条件: close ≥ MA60 且最近{BBI_KDJ_STRATEGY['max_window']}内存在有效上穿MA60")
    print(f"  - 知行约束: 收盘 > MA{BBI_KDJ_STRATEGY['LONG_MA_PERIOD']} 且 MA{BBI_KDJ_STRATEGY['SHORT_MA_PERIOD']} > MA{BBI_KDJ_STRATEGY['LONG_MA_PERIOD']}")
    if BBI_KDJ_STRATEGY['ENABLE_PROFIT_TAKE']:
        print(f"  - 止盈: {BBI_KDJ_STRATEGY['PROFIT_TAKE_PCT']}%")
    else:
        print(f"  - 止盈: 未启用")
    if BBI_KDJ_STRATEGY['ENABLE_STOP_LOSS']:
        print(f"  - 止损: {BBI_KDJ_STRATEGY['STOP_LOSS_PCT']}%")
    else:
        print(f"  - 止损: 未启用")
    print(f"  - 支持周期: {', '.join(TIME_FRAMES)}")
    print("=" * 60)
    
    # 存储所有统计数据
    all_stats = []
    
    # 逐个处理文件
    for i, file_path in enumerate(excel_files, 1):
        print(f"\n[{i}/{len(excel_files)}] 回测: {os.path.basename(file_path)}")
        
        try:
            file_stats = backtest_single_file(file_path, output_dir)
            if file_stats:
                all_stats.append(file_stats)
        except Exception as e:
            print(f"处理文件 {file_path} 时出现错误: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    # 生成汇总报告
    if all_stats:
        print(f"\n生成汇总报告...")
        generate_summary_report(all_stats, output_dir)
    
    print(f"\n回测完成！共处理 {len(all_stats)} 个文件")

if __name__ == "__main__":
    main()

