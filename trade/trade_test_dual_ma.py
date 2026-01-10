#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
双均线策略回测脚本

策略逻辑：
- 使用两条移动平均线（短期均线和长期均线）
- 当短期均线上穿长期均线时买入（金叉）
- 当短期均线下穿长期均线时卖出（死叉）
- 支持小时线、日线、周线、月线
"""

import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime
import xlsxwriter
import math

# 导入配置文件
from config import INITIAL_CAPITAL, BACKTEST_START_DATE, DUAL_MA_STRATEGY

# 支持的周期类型
TIME_FRAMES = ['小时线', '日线', '周线', '月线']

# 双均线策略参数（从config.py读取）
SHORT_MA_PERIOD = DUAL_MA_STRATEGY.get("SHORT_MA_PERIOD", 5)
LONG_MA_PERIOD = DUAL_MA_STRATEGY.get("LONG_MA_PERIOD", 20)
ENABLE_PROFIT_TAKE = DUAL_MA_STRATEGY.get("ENABLE_PROFIT_TAKE", True)
PROFIT_TAKE_PCT = DUAL_MA_STRATEGY.get("PROFIT_TAKE_PCT", 10)
ENABLE_STOP_LOSS = DUAL_MA_STRATEGY.get("ENABLE_STOP_LOSS", True)
STOP_LOSS_PCT = DUAL_MA_STRATEGY.get("STOP_LOSS_PCT", 5)

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

def backtest_dual_ma_strategy(df, time_frame):
    """
    执行双均线策略回测
    
    策略逻辑：
    - 当短期均线上穿长期均线时买入（金叉）
    - 当短期均线下穿长期均线时卖出（死叉）
    - 支持止盈止损
    """
    if df is None or len(df) < max(SHORT_MA_PERIOD, LONG_MA_PERIOD) + 1:
        return None, None, None
    
    # 确保有必要的列
    short_ma_col = f'MA{SHORT_MA_PERIOD}'
    long_ma_col = f'MA{LONG_MA_PERIOD}'
    required_columns = ['date', 'open', 'close', short_ma_col, long_ma_col]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"  缺少必要列: {', '.join(missing_columns)}")
        return None, None, None
    
    # 准备数据
    df = df.copy()
    
    # 1. 转换日期列
    df['date'] = pd.to_datetime(df['date'])
    
    # 2. 检查是否存在负数价格（开盘价或收盘价）
    has_negative_prices = (df['open'] <= 0).any() or (df['close'] <= 0).any()
    
    # 3. 如果有负数价格，过滤配置的起始日期之前的数据
    if has_negative_prices:
        print(f"  检测到负数价格，过滤{BACKTEST_START_DATE}之前的数据")
        start_date = pd.Timestamp(BACKTEST_START_DATE)
        df = df[df['date'] >= start_date].copy()
        if len(df) < max(SHORT_MA_PERIOD, LONG_MA_PERIOD) + 1:
            print("  过滤后数据不足，跳过回测")
            return None, None, None
    
    # 4. 排序数据
    df.sort_values('date', inplace=True)
    
    # 5. 确保均线数据有效（去除NaN）
    df = df.dropna(subset=[short_ma_col, long_ma_col])
    if len(df) < max(SHORT_MA_PERIOD, LONG_MA_PERIOD) + 1:
        print("  均线数据不足，跳过回测")
        return None, None, None
    
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
    
    # 获取回测开始和结束时的价格（用于计算买入并持有策略的收益）
    backtest_start_price = df.iloc[0]['close']
    backtest_end_price = df.iloc[-1]['close']
    backtest_price_change_pct = ((backtest_end_price - backtest_start_price) / backtest_start_price * 100) if backtest_start_price > 0 else 0
    
    # 计算买入并持有策略的收益（一直持有）
    buy_and_hold_return = backtest_price_change_pct / 100
    
    # 遍历每一行数据（从第二行开始，因为需要比较前一行）
    for i in range(1, len(df)):
        row = df.iloc[i]
        prev_row = df.iloc[i-1]
        
        date = row['date']
        close_price = row['close']
        open_price = row['open']
        
        # 获取当前和前一天的均线值
        short_ma_current = row[short_ma_col]
        long_ma_current = row[long_ma_col]
        short_ma_prev = prev_row[short_ma_col]
        long_ma_prev = prev_row[long_ma_col]
        
        # 计算当前资产净值
        current_equity = cash + position * close_price
        equity_values.append({
            'date': date,
            'equity': current_equity,
            'position': position,
            'cash': cash,
            'close_price': close_price,
            f'MA{SHORT_MA_PERIOD}': short_ma_current,
            f'MA{LONG_MA_PERIOD}': long_ma_current
        })
        
        # 检查止盈止损（如果持仓）
        if position > 0 and buy_price > 0:
            price_change_pct = ((close_price - buy_price) / buy_price * 100) if buy_price > 0 else 0
            
            # 止盈
            if ENABLE_PROFIT_TAKE and price_change_pct >= PROFIT_TAKE_PCT:
                sell_price = close_price
                sell_amount = position * sell_price
                cash += sell_amount
                
                profit = (sell_price - buy_price) * position
                is_win = profit > 0
                if is_win:
                    win_count += 1
                trade_count += 1
                
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
                    'reason': f'止盈({PROFIT_TAKE_PCT}%)',
                    'equity': cash
                })
                
                position = 0
                buy_price = 0
                buy_date = None
                continue
            
            # 止损
            if ENABLE_STOP_LOSS and price_change_pct <= -STOP_LOSS_PCT:
                sell_price = close_price
                sell_amount = position * sell_price
                cash += sell_amount
                
                profit = (sell_price - buy_price) * position
                is_win = profit > 0
                if is_win:
                    win_count += 1
                trade_count += 1
                
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
                    'reason': f'止损({STOP_LOSS_PCT}%)',
                    'equity': cash
                })
                
                position = 0
                buy_price = 0
                buy_date = None
                continue
        
        # 检查金叉（买入信号）：短期均线上穿长期均线
        # 条件：前一天 short_ma <= long_ma，今天 short_ma > long_ma
        golden_cross = (short_ma_prev <= long_ma_prev) and (short_ma_current > long_ma_current)
        
        # 检查死叉（卖出信号）：短期均线下穿长期均线
        # 条件：前一天 short_ma >= long_ma，今天 short_ma < long_ma
        death_cross = (short_ma_prev >= long_ma_prev) and (short_ma_current < long_ma_current)
        
        # 买入逻辑：金叉且当前无持仓
        if golden_cross and position == 0 and cash > 0:
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
                    'action': '买入（金叉）',
                    'price': buy_price,
                    'position': position,
                    'amount': position * buy_price,
                    'signal': f'MA{SHORT_MA_PERIOD}上穿MA{LONG_MA_PERIOD}',
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
                'action': '买入（金叉）',
                'price': buy_price,
                'position': position,
                'amount': position * buy_price,
                'signal': f'MA{SHORT_MA_PERIOD}上穿MA{LONG_MA_PERIOD}',
                'equity': position * buy_price
            })
            # 买入不算交易，只有卖出才算一次完整交易
        
        # 卖出逻辑：死叉且当前有持仓
        elif death_cross and position > 0:
            sell_price = close_price
            sell_amount = position * sell_price
            cash += sell_amount
            
            profit = (sell_price - buy_price) * position if buy_price > 0 else 0
            is_win = profit > 0
            if is_win:
                win_count += 1
            trade_count += 1
            
            # 计算持有天数（自然天）
            if buy_date:
                hold_days = (date - buy_date).days
                total_hold_days += hold_days
            
            price_change_pct = ((sell_price - buy_price) / buy_price * 100) if buy_price > 0 else 0
            
            trades.append({
                'date': date,
                'action': '卖出（死叉）',
                'price': sell_price,
                'position': position,
                'amount': sell_amount,
                'buy_price': buy_price,
                'profit': profit,
                'profit_pct': price_change_pct,
                'is_win': is_win,
                'signal': f'MA{SHORT_MA_PERIOD}下穿MA{LONG_MA_PERIOD}',
                'equity': cash
            })
            
            position = 0
            buy_price = 0
            buy_date = None
    
    # 处理最终持仓
    final_price = df.iloc[-1]['close']
    final_equity = cash + position * final_price
    
    # 如果最后还有持仓，记录未实现的盈亏
    if position > 0 and buy_price > 0:
        final_profit = (final_price - buy_price) * position
        final_profit_pct = ((final_price - buy_price) / buy_price * 100) if buy_price > 0 else 0
        
        # 更新最终资产净值
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
        
        # 记录最后一笔未实现的交易
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
        '短期均线周期': SHORT_MA_PERIOD,
        '长期均线周期': LONG_MA_PERIOD,
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
            
            if len(df) < max(SHORT_MA_PERIOD, LONG_MA_PERIOD) + 1:
                print(f"  {time_frame}数据清理后不足{max(SHORT_MA_PERIOD, LONG_MA_PERIOD) + 1}行，跳过回测")
                continue
            
            print(f"  回测{time_frame}...")
            stats, trades, equity_df = backtest_dual_ma_strategy(df, time_frame)
            
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
        output_filename = f"{file_name}_双均线策略_回测结果.xlsx"
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
        summary_filename = f"双均线策略_汇总报告_{timestamp}.xlsx"
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
            
            # 按股票代码和时间周期排序，让同一股票的不同周期数据在一起
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
                    # 删除时间周期列（因为每个工作表只包含一个周期）
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
            
            # 整体统计（所有周期合并）
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
    output_dir = "backtest_results_dual_ma"
    
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 获取所有Excel文件
    excel_files = glob.glob(os.path.join(input_dir, "*.xlsx"))
    
    if not excel_files:
        print(f"在 {input_dir} 目录中没有找到Excel文件")
        return
    
    print(f"找到 {len(excel_files)} 个文件进行双均线策略回测")
    print(f"策略说明:")
    print(f"  - 短期均线: MA{SHORT_MA_PERIOD}")
    print(f"  - 长期均线: MA{LONG_MA_PERIOD}")
    print(f"  - 买入信号: 短期均线上穿长期均线（金叉）")
    print(f"  - 卖出信号: 短期均线下穿长期均线（死叉）")
    if ENABLE_PROFIT_TAKE:
        print(f"  - 止盈: {PROFIT_TAKE_PCT}%")
    else:
        print(f"  - 止盈: 未启用")
    if ENABLE_STOP_LOSS:
        print(f"  - 止损: {STOP_LOSS_PCT}%")
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

