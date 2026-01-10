#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
网格+趋势组合策略 - 带收益区分统计的版本（已修复）

修复内容：
- 修正收益计算逻辑，适应无限资金池模式
- 正确计算网格和趋势各自的贡献
- 策略总收益 = 已实现收益 + 未兑现收益
"""

import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime
import xlsxwriter

# 导入配置文件（包含趋势信号和网格策略参数）
from config import (INITIAL_CAPITAL, BUY_SIGNALS, SELL_SIGNALS, BACKTEST_START_DATE, GRID_STRATEGY)

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

def backtest_grid_trend_combined_profit_split_fixed(df):
    """
    执行网格+趋势组合策略回测 - 收益区分统计版（已修复）
    
    修复内容：
    - 修正收益计算逻辑，适应无限资金池模式
    - 使用已实现收益 + 未兑现收益的方式计算真实收益
    """
    if df is None or len(df) < 10:
        return None, None, None
    
    required_columns = ['date', 'open', 'close', '综合判断']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"缺少必要列: {', '.join(missing_columns)}")
        return None, None, None
    
    # 准备数据
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    
    # 检查负数价格
    has_negative_prices = (df['open'] <= 0).any() or (df['close'] <= 0).any()
    if has_negative_prices:
        print(f"  检测到负数价格，过滤{BACKTEST_START_DATE}之前的数据")
        start_date = pd.Timestamp(BACKTEST_START_DATE)
        df = df[df['date'] >= start_date].copy()
        if len(df) < 10:
            print("  过滤后数据不足10行，跳过回测")
            return None, None, None
    
    df.sort_values('date', inplace=True)
    
    # 网格策略参数
    GRID_SIZE_PCT = GRID_STRATEGY["GRID_SIZE_PCT"] / 100.0
    GRID_AMOUNT_PER_UNIT = GRID_STRATEGY["GRID_AMOUNT_PER_UNIT"]
    MIN_HOLD_UNITS = GRID_STRATEGY["MIN_HOLD_UNITS"]
    MAX_HOLD_UNITS = GRID_STRATEGY["MAX_HOLD_UNITS"]
    REQUIRED_PROFIT_PCT = GRID_STRATEGY["REQUIRED_PROFIT_PCT"] / 100.0
    
    # 初始化变量
    cash = INITIAL_CAPITAL
    position = 0  # 总持仓数量
    trades = []
    equity_values = []
    
    # 网格相关变量
    grid_position = 0  # 网格持仓数量
    grid_units = 0  # 网格持仓份数
    grid_buy_records = []  # 网格买入记录
    grid_cumulative_profit = 0.0  # 网格累计已实现收益（卖出的盈利）
    grid_reference_price = None  # 网格参考价格（用于计算目标买入价，动态更新为最高价）
    initial_grid_bought = False  # 是否已买入初始底仓
    
    # 趋势相关变量
    trend_position = 0  # 趋势持仓数量
    trend_active = False  # 是否处于趋势状态
    trend_buy_price = 0  # 趋势买入价格
    trend_buy_date = None  # 趋势买入日期
    trend_cumulative_profit = 0.0  # 趋势累计已实现收益
    
    # 用于计算日均持仓资金（资金利用率）
    daily_hold_values = []
    
    win_count = 0
    trade_count = 0
    total_hold_days = 0  # 总持有天数（自然天）
    grid_hold_start_date = None  # 网格持仓开始日期
    start_date = df['date'].min()
    end_date = df['date'].max()
    
    # 资金池追踪
    max_negative_cash = 0
    total_cash_injected = 0
    
    # 回测开始结束价格
    backtest_start_price = df.iloc[0]['close']
    backtest_end_price = df.iloc[-1]['close']
    buy_and_hold_return = ((backtest_end_price - backtest_start_price) / backtest_start_price) if backtest_start_price > 0 else 0
    
    # 使用第一天开盘价作为网格基准价格
    base_price = df.iloc[0]['open']
    if base_price <= 0:
        base_price = df.iloc[0]['close']
    
    # 遍历数据
    for i, row in df.iterrows():
        date = row['date']
        close_price = row['close']
        open_price = row['open']
        signal = row['综合判断']
        
        # 计算当前资产净值
        current_equity = cash + position * close_price
        
        # 更新最大负现金
        if cash < 0 and abs(cash) > max_negative_cash:
            max_negative_cash = abs(cash)
        
        # 检查趋势信号
        trend_buy_signal = signal in BUY_SIGNALS
        trend_sell_signal = signal in SELL_SIGNALS
        
        # 趋势状态切换检查
        if trend_active and trend_sell_signal:
            # 在趋势状态，收到卖出信号：卖出所有趋势持仓，切回网格策略
            if trend_position > 0:
                sell_price = close_price
                sell_amount = trend_position * sell_price
                cash += sell_amount
                
                # 计算盈亏（趋势部分）- 已实现收益
                profit = (sell_price - trend_buy_price) * trend_position if trend_buy_price > 0 else 0
                trend_cumulative_profit += profit
                
                is_win = profit > 0
                if is_win:
                    win_count += 1
                trade_count += 1
                
                price_change_pct = ((sell_price - trend_buy_price) / trend_buy_price * 100) if trend_buy_price > 0 else 0
                
                trades.append({
                    'date': date,
                    'action': '趋势卖出',
                    '价格': sell_price,
                    '数量': trend_position,
                    '金额': sell_amount,
                    '盈亏': profit,
                    '盈亏百分比': price_change_pct,
                    '是否盈利': is_win,
                    '卖出原因': f'趋势卖出信号({signal})',
                    '趋势持仓': 0,
                    '网格持仓': grid_position,
                    '总持仓': grid_position,
                    '现金': cash,
                    '资产净值': cash + grid_position * close_price
                })
                
                # 计算趋势持仓的持有天数（自然天）
                if trend_buy_date:
                    hold_days = (date - trend_buy_date).days
                    total_hold_days += hold_days
                
                # 清空趋势持仓，但保留网格持仓（解冻）
                trend_position = 0
                trend_buy_price = 0
                trend_buy_date = None
                position = grid_position  # 总持仓等于网格持仓
                trend_active = False
                # 趋势卖出后，卖出价格成为网格基准价格
                grid_reference_price = sell_price
        
        # 不在趋势状态时，检查是否触发趋势买入信号
        elif not trend_active and trend_buy_signal:
            # 从网格策略切换到趋势策略
            # 保留网格持仓，冻结网格交易，使用所有现金买入趋势
            total_buy_amount = cash
            
            if total_buy_amount > 0:
                trend_buy_price = close_price
                trend_buy_date = date  # 记录趋势买入日期
                trend_position = total_buy_amount / trend_buy_price
                cash = 0
                trend_active = True
                position = trend_position + grid_position  # 总持仓包括趋势持仓和网格持仓
                
                trades.append({
                    'date': date,
                    'action': '趋势买入（网格冻结）',
                    '价格': trend_buy_price,
                    '数量': trend_position,
                    '金额': total_buy_amount,
                    '趋势持仓': trend_position,
                    '冻结网格持仓': grid_position,
                    '网格份数': grid_units,
                    '总持仓': position,
                    '现金': 0,
                    '资产净值': position * close_price
                })
        
        # 在趋势状态：只监控卖出信号，不执行网格交易（网格持仓冻结）
        if trend_active:
            # 记录当日持仓市值（趋势持仓 + 冻结的网格持仓）
            today_hold_value = trend_position * close_price + grid_position * close_price
            daily_hold_values.append(today_hold_value)
            
            # 趋势持仓：全仓，算作1天
            total_hold_days += 1
            
            # 只记录资产净值，不执行网格交易
            equity_values.append({
                'date': date,
                'equity': current_equity,
                'position': position,
                'cash': cash,
                '状态': '趋势持仓',
                '趋势持仓': trend_position,
                '网格持仓': grid_position,
                '网格份数': grid_units,
                'close_price': close_price
            })
            continue
        
        # 非趋势状态：执行网格策略
        # 1. 初始状态：如果不进入趋势，先买一份底仓（只在第一次进入非趋势状态时执行）
        if not initial_grid_bought and grid_units == 0 and not trend_active:
            # 买入初始底仓
            buy_price = close_price
            buy_quantity = GRID_AMOUNT_PER_UNIT / buy_price
            
            old_cash = cash
            grid_position += buy_quantity
            position = grid_position + trend_position
            cash -= GRID_AMOUNT_PER_UNIT
            grid_units += 1
            grid_buy_records.append(buy_price)
            initial_grid_bought = True
            grid_reference_price = close_price  # 初始底仓价格作为基准
            grid_hold_start_date = date  # 记录网格持仓开始日期
            trade_count += 1
            
            # 记录补充资金
            if old_cash >= 0 and cash < 0:
                total_cash_injected += abs(cash)
            elif old_cash < 0:
                total_cash_injected += GRID_AMOUNT_PER_UNIT
            
            trades.append({
                'date': date,
                'action': '网格买入（初始底仓）',
                '价格': buy_price,
                '数量': buy_quantity,
                '金额': GRID_AMOUNT_PER_UNIT,
                '网格份数': grid_units,
                '趋势状态': '等待',
                '现金': cash,
                '资产净值': cash + position * close_price
            })
        
        # 2. 网格买入逻辑：价格从基准价格下跌1.5%时买入
        # 需要确保有基准价格（初始底仓或趋势卖出后设置）
        if grid_units < MAX_HOLD_UNITS and grid_reference_price is not None:
            # 更新基准价格：如果价格创新高，更新基准价格为当前价格
            if close_price > grid_reference_price:
                grid_reference_price = close_price
            
            # 计算目标买入价格：从基准价格下跌1.5%
            target_buy_price = grid_reference_price * (1 - GRID_SIZE_PCT)
            
            # 检查买入条件：价格从基准价格下跌1.5%
            if close_price <= target_buy_price:
                buy_price = close_price
                buy_quantity = GRID_AMOUNT_PER_UNIT / buy_price
                
                old_cash = cash
                grid_position += buy_quantity
                position = grid_position + trend_position
                cash -= GRID_AMOUNT_PER_UNIT
                grid_units += 1
                grid_buy_records.append(buy_price)
                trade_count += 1
                
                # 买入后，更新基准价格为买入价格（从买入价开始跟踪新的最高价）
                grid_reference_price = buy_price
                
                # 记录补充资金
                if old_cash >= 0 and cash < 0:
                    total_cash_injected += abs(cash)
                elif old_cash < 0:
                    total_cash_injected += GRID_AMOUNT_PER_UNIT
                
                trades.append({
                    'date': date,
                    'action': '网格买入',
                    '价格': buy_price,
                    '数量': buy_quantity,
                    '金额': GRID_AMOUNT_PER_UNIT,
                    '网格份数': grid_units,
                    '趋势状态': '等待',
                    '现金': cash,
                    '资产净值': cash + position * close_price
                })
        
        # 卖出逻辑：网格持仓达到盈利目标，且持仓超过最小份数
        if grid_units > MIN_HOLD_UNITS and grid_buy_records:
            indices_to_remove = []
            
            for idx, buy_price in enumerate(grid_buy_records):
                target_sell_price = buy_price * (1 + REQUIRED_PROFIT_PCT)
                
                if close_price >= target_sell_price:
                    sell_price = close_price
                    sell_quantity = GRID_AMOUNT_PER_UNIT / buy_price
                    
                    if grid_position >= sell_quantity:
                        grid_position -= sell_quantity
                        position = grid_position + trend_position
                        old_cash = cash
                        cash += sell_quantity * sell_price
                        grid_units -= 1
                        indices_to_remove.append(idx)
                        
                        profit = (sell_price - buy_price) * sell_quantity
                        grid_cumulative_profit += profit  # 累加网格收益
                        is_win = profit > 0
                        win_count += 1 if is_win else 0
                        trade_count += 1
                        
                        trades.append({
                            'date': date,
                            'action': '网格卖出',
                            '价格': sell_price,
                            '数量': sell_quantity,
                            '金额': sell_quantity * sell_price,
                            '买入价': buy_price,
                            '盈利': profit,
                            '是否盈利': is_win,
                            '网格份数': grid_units,
                            '趋势状态': '等待',
                            '现金': cash,
                            '资产净值': cash + position * close_price
                        })
            
            # 删除已卖出的记录
            for idx in sorted(indices_to_remove, reverse=True):
                if idx < len(grid_buy_records):
                    grid_buy_records.pop(idx)
        
        # 记录资产净值
        equity_values.append({
            'date': date,
            'equity': current_equity,
            'position': position,
            'cash': cash,
            '状态': '网格策略',
            '趋势持仓': trend_position,
            '网格持仓': grid_position,
            '网格份数': grid_units,
            'close_price': close_price
        })
        
        # 记录当日持仓市值（网格持仓）
        if not trend_active:
            daily_hold_values.append(grid_position * close_price)
        
        # 累加持有天数（自然天）
        # 网格状态：按 grid_units / MAX_HOLD_UNITS 计算天数
        # 注意：趋势状态的持有天数已在上面计算（第256行），这里只处理网格状态
        if grid_units > 0:
            # 网格持仓：按份数比例计算，全仓（MAX_HOLD_UNITS份）算作1天
            hold_days_ratio = grid_units / MAX_HOLD_UNITS if MAX_HOLD_UNITS > 0 else 0
            total_hold_days += hold_days_ratio
    
    # 处理最终持仓
    final_price = df.iloc[-1]['close']
    final_equity = cash + (trend_position + grid_position) * final_price
    
    # 计算网格部分的最终收益
    # 网格收益 = 已实现收益 + 未兑现收益
    # 未兑现收益 = 当前网格持仓的市值 - 当前网格持仓的成本
    current_grid_cost = len(grid_buy_records) * GRID_AMOUNT_PER_UNIT  # 当前持仓成本（假设按买入价计算）
    final_grid_value = grid_position * final_price
    unrealized_grid_profit = final_grid_value - current_grid_cost
    total_grid_return = grid_cumulative_profit + unrealized_grid_profit
    
    # 计算趋势部分的最终收益
    # 趋势收益 = 已实现收益 + 未兑现收益
    unrealized_trend_profit = trend_position * final_price - (0 if trend_buy_price == 0 else trend_position * trend_buy_price)
    total_trend_return = trend_cumulative_profit + unrealized_trend_profit
    
    # 计算回测期涨跌幅（供参考）
    price_return_ratio = (final_price / backtest_start_price - 1) if backtest_start_price > 0 else 0
    
    # 最终补充资金
    final_cash_needed = max(0, max_negative_cash)
    if cash < 0:
        final_cash_needed = max(final_cash_needed, abs(cash))
    
    # 计算统计
    equity_df = pd.DataFrame(equity_values)
    max_equity = equity_df['equity'].max()
    min_equity = equity_df['equity'].min()
    win_rate = win_count / trade_count if trade_count > 0 else 0
    
    return_ratio = (final_equity / INITIAL_CAPITAL) - 1
    annualized_return = calculate_annualized_return(start_date, end_date, final_equity, INITIAL_CAPITAL)
    buy_and_hold_annualized = calculate_annualized_return(start_date, end_date, INITIAL_CAPITAL * (1 + buy_and_hold_return), INITIAL_CAPITAL)
    
    # 计算资金利用率（日均持仓资金 / 初始资金）
    if daily_hold_values:
        average_daily_hold_value = sum(daily_hold_values) / len(daily_hold_values)
        capital_utilization_ratio = (average_daily_hold_value / INITIAL_CAPITAL) * 100 if INITIAL_CAPITAL > 0 else 0
    else:
        capital_utilization_ratio = 0
    
    # 计算持仓市值占比
    final_position_value = (trend_position + grid_position) * final_price
    position_ratio = (final_position_value / final_equity * 100) if final_equity > 0 else 0
    
    # 计算持有年化涨幅：策略涨幅 / (持有天数 / 365)
    hold_years = total_hold_days / 365.0 if total_hold_days > 0 else 0
    hold_annualized_return = (return_ratio / hold_years) if hold_years > 0 else 0
    
    stats = {
        '股票代码': '',
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
        '期间价格涨跌幅': price_return_ratio,
        '趋势状态次数': len([e for e in equity_values if e.get('状态') == '趋势持仓']),
        '状态': '收益区分版(已修复)',
        '当前现金': cash,
        '最终趋势持仓': trend_position,
        '最终网格持仓': grid_position,
        '最终网格份数': grid_units,
        '最大挪用资金': max_negative_cash,
        '最终需补充资金': final_cash_needed if cash < 0 else 0,
        '总补充资金': total_cash_injected,
        '资金利用率(%)': capital_utilization_ratio,
        '持仓市值占比(%)': position_ratio,
        # 新增：收益区分统计
        '网格已实现收益': grid_cumulative_profit,
        '网格未兑现收益': unrealized_grid_profit,
        '网格总收益': total_grid_return,
        '趋势已实现收益': trend_cumulative_profit,
        '趋势未兑现收益': unrealized_trend_profit,
        '趋势总收益': total_trend_return,
    }
    
    # 打印收益分析
    print(f"  网格收益分析:")
    print(f"    已实现收益: {grid_cumulative_profit:.2f}")
    print(f"    未兑现收益: {unrealized_grid_profit:.2f}")
    print(f"    总收益: {total_grid_return:.2f}")
    
    print(f"  趋势收益分析:")
    print(f"    已实现收益: {trend_cumulative_profit:.2f}")
    print(f"    未兑现收益: {unrealized_trend_profit:.2f}")
    print(f"    总收益: {total_trend_return:.2f}")
    
    return stats, trades, equity_df

def backtest_single_file(file_path, output_dir):
    """对单个文件进行回测"""
    try:
        file_name = os.path.splitext(os.path.basename(file_path))[0]
        print(f"开始回测: {file_name}")
        
        # 读取数据
        try:
            df_daily = pd.read_excel(file_path, sheet_name='日线数据')
        except Exception as e:
            print(f"  读取数据失败: {e}")
            return None
        
        if df_daily.empty:
            print(f"  日线数据为空，跳过")
            return None
        
        # 规范列名
        df_daily.columns = [safe_str(col).strip().replace(' ', '') for col in df_daily.columns]
        
        # 清理数据
        df_daily['date'] = pd.to_datetime(df_daily['date'], errors='coerce')
        df_daily = df_daily.dropna(subset=['date'])
        df_daily = df_daily[(df_daily['open'] > 0) & (df_daily['close'] > 0)]
        
        if len(df_daily) < 10:
            print(f"  数据清理后不足10行，跳过回测")
            return None
        
        stats, trades, equity_df = backtest_grid_trend_combined_profit_split_fixed(df_daily)
        
        if stats is None:
            print("  回测失败")
            return None
        
        stats['股票代码'] = file_name
        
        # 生成输出文件名
        output_filename = f"{file_name}_网格趋势组合_收益区分_回测结果.xlsx"
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
            stats_df = pd.DataFrame([stats])
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
            if trades:
                trades_df = pd.DataFrame(trades)
                trades_df.to_excel(writer, sheet_name='交易记录', index=False)
                
                worksheet = writer.sheets["交易记录"]
                for col_num, col_name in enumerate(trades_df.columns):
                    worksheet.set_column(col_num + 1, col_num + 1, 15, number_format)
            
            # 写入资产净值曲线
            equity_df.to_excel(writer, sheet_name='资产净值', index=False)
            
            writer.close()
            print(f"  回测完成，结果保存到: {output_path}")
            
        except PermissionError as e:
            print(f"  文件访问权限错误: {e}")
            return None
        
        # 打印关键信息
        print(f"  策略涨幅: {stats['策略涨幅']:.2%}")
        print(f"  一直持有涨幅: {stats['一直持有涨幅']:.2%}")
        print(f"  策略超额收益: {stats['策略超额收益']:.2%}")
        print(f"  胜率: {stats['胜率']:.2%}")
        print(f"  交易次数: {stats['交易次数']}")
        print(f"  趋势状态次数: {stats['趋势状态次数']}")
        print(f"  最终网格份数: {stats['最终网格份数']}")
        print(f"  资金利用率: {stats['资金利用率(%)']:.2f}%")
        print(f"  网格总收益: ¥{stats['网格总收益']:,.2f}")
        print(f"  趋势总收益: ¥{stats['趋势总收益']:,.2f}")
        
        return [stats]
        
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
        summary_filename = f"网格趋势组合_收益区分_汇总报告_{timestamp}.xlsx"
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
            summary_df.to_excel(writer, sheet_name='详细数据', index=False)
            
            worksheet = writer.sheets["详细数据"]
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
            
            # 整体统计
            overall_stats = {
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
                # 新增：网格和趋势收益统计
                '平均网格收益': summary_df['网格总收益'].mean(),
                '平均趋势收益': summary_df['趋势总收益'].mean(),
                '平均网格持仓成本': summary_df.get('网格持仓成本', [0]).mean() if '网格持仓成本' in summary_df.columns else 0,
                '平均趋势持仓成本': summary_df.get('趋势持仓成本', [0]).mean() if '趋势持仓成本' in summary_df.columns else 0,
                '网格收益中位数': summary_df['网格总收益'].median(),
                '趋势收益中位数': summary_df['趋势总收益'].median(),
            }
            
            overall_df = pd.DataFrame([overall_stats])
            overall_df.to_excel(writer, sheet_name='整体统计', index=False)
            
            worksheet = writer.sheets["整体统计"]
            for col_num, col_name in enumerate(overall_df.columns):
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
    output_dir = "backtest_results_grid_trend_combined"
    
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 获取所有Excel文件
    excel_files = glob.glob(os.path.join(input_dir, "*.xlsx"))
    
    if not excel_files:
        print(f"在 {input_dir} 目录中没有找到Excel文件")
        return
    
    print(f"找到 {len(excel_files)} 个文件进行网格+趋势组合策略回测")
    print(f"策略说明（收益区分版）:")
    print(f"  - 默认状态：执行网格策略（每份1万，盈利1.5%卖出）")
    print(f"  - 趋势买入信号：冻结网格持仓，使用现金全仓买入趋势")
    print(f"  - 趋势卖出信号：清空趋势持仓，恢复网格交易")
    print(f"  - 支持无限资金池")
    print(f"  - 资金利用率计算：按日均持仓市值计算")
    print(f"  - 新增：分别统计网格和趋势的收益情况")
    print("=" * 60)
    
    # 存储所有统计数据
    all_stats = []
    
    # 逐个处理文件
    for i, file_path in enumerate(excel_files, 1):
        print(f"\n[{i}/{len(excel_files)}] 回测收益区分版: {os.path.basename(file_path)}")
        
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
