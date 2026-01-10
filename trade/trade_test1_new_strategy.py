#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime
import xlsxwriter
import math

# 导入配置文件
from config import (INITIAL_CAPITAL, BUY_SIGNALS, SELL_SIGNALS, BACKTEST_START_DATE,
                    ENABLE_PROFIT_TAKE, PROFIT_TAKE_PCT, ENABLE_STOP_LOSS, STOP_LOSS_PCT)

# 支持的周期类型
TIME_FRAMES = ['小时线', '日线', '周线', '月线']

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
        # 只保留日期
            return value.strftime('%Y-%m-%d')
    if isinstance(value, (np.int64, np.int32, np.float64)):
        return str(round(value, 3))
    return str(value)

def calculate_annualized_return(start_date, end_date, final_value, initial_capital):
    """计算年化收益率"""
    if not start_date or not end_date:
        return 0.0
    
    try:
        # 计算总天数
        days = (end_date - start_date).days
        if days <= 0:
            return 0.0
        
        # 计算年数
        years = days / 365.0
        
        # 计算年化收益率
        return_ratio = final_value / initial_capital
        annualized_return = (return_ratio ** (1 / years) - 1) if years > 0 else 0.0
        return annualized_return
    except:
        return 0.0

def backtest_strategy(df, time_frame):
    """执行回测策略 - 新策略：买入后根据信号和价格变化决定卖出"""
    if df is None or len(df) < 10:
        return None, None, None
    
    # 确保有必要的列
    required_columns = ['date', 'open', 'close', '综合判断']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"缺少必要列: {', '.join(missing_columns)}")
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
        if len(df) < 10:
            print("  过滤后数据不足10行，跳过回测")
            return None, None, None
    
    # 4. 排序数据
    df.sort_values('date', inplace=True)
    
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
    backtest_start_price = df.iloc[0]['close']  # 回测开始时的价格
    backtest_end_price = df.iloc[-1]['close']   # 回测结束时的价格
    backtest_price_change_ratio = ((backtest_end_price - backtest_start_price) / backtest_start_price) if backtest_start_price > 0 else 0
    
    # 计算买入并持有策略的收益（一直持有）
    buy_and_hold_return = backtest_price_change_ratio
    
    # 遍历每一行数据
    for i in range(len(df)):
        row = df.iloc[i]
        date = row['date']
        close_price = row['close']
        open_price = row['open']
        
        # 检查信号
        signal = row['综合判断']
        
        # 买入信号 - 处理负价格问题
        if signal in BUY_SIGNALS and cash > 0:
            # 如果是最后一行，使用当前行的开盘价买入（因为周线/月线数据可能只有几行）
            if i >= len(df) - 1:
                # 使用当前行的开盘价买入
                buy_price = open_price if open_price > 0 else close_price
                if buy_price <= 0:
                    # 如果价格无效，记录当前资产净值并跳过
                    current_equity = cash + position * close_price
                    equity_values.append({
                        'date': date,
                        'equity': current_equity
                    })
                    continue
                
                # 买入
                position = cash / buy_price
                cash = 0
                buy_date = date
                
                # 买入后，使用买入价计算资产净值
                current_equity_after = cash + position * buy_price
                
                trades.append({
                    'date': date,
                    'action': '买入',
                    'price': buy_price,
                    'position': position,
                    'equity': current_equity_after,
                    'signal': signal,
                    '买入价': buy_price,
                    '信号日期': date
                })
                
                # 记录买入当天的资产净值
                equity_values.append({
                    'date': date,
                    'equity': current_equity_after
                })
                continue
            
            # 获取下一个交易日的数据
            next_row = df.iloc[i+1]
            next_date = next_row['date']
            next_open = next_row['open']
            
            # 如果价格小于等于0，跳过操作
            if next_open <= 0:
                # 计算当前资产净值（即使跳过买入也要记录）
                current_equity = cash + position * close_price
                equity_values.append({
                    'date': date,
                    'equity': current_equity
                })
                continue
            
            # 先记录信号出现当天的资产净值（买入前）
            current_equity_before = cash + position * close_price
            equity_values.append({
                'date': date,
                'equity': current_equity_before
            })
                
            # 使用下一个交易日的开盘价买入
            buy_price = next_open
            position = cash / buy_price
            cash = 0
            buy_date = next_date  # 买入日期应该是实际买入的日期（下一个交易日）
            
            # 买入后，使用买入价计算资产净值（因为已经用买入价买入了）
            current_equity_after = cash + position * buy_price
            
            trades.append({
                'date': next_date,  # 买入日期应该是实际买入的日期
                'action': '买入',
                'price': buy_price,
                'position': position,
                'equity': current_equity_after,
                'signal': signal,
                '买入价': buy_price,
                '信号日期': date  # 记录信号出现的日期
            })
            
            # 记录买入当天的资产净值（使用下一个交易日的日期）
            equity_values.append({
                'date': next_date,
                'equity': current_equity_after
            })
        
        # 卖出逻辑 - 新策略
        elif position > 0:
            # 先计算当前资产净值（基于当前收盘价）
            current_equity = cash + position * close_price
            
            should_sell = False
            sell_reason = ""
            
            # 1. 如果信号为看空，立即卖出
            if signal in SELL_SIGNALS:
                should_sell = True
                sell_reason = f"信号卖出({signal})"
            
            # 2. 如果信号不为看多，检查价格变化（根据配置的止盈止损）
            elif signal not in BUY_SIGNALS:
                # 计算从买入价到当前收盘价的价格变化百分比
                price_change_pct = ((close_price - buy_price) / buy_price * 100) if buy_price > 0 else 0
                
                # 根据配置检查止盈止损
                if ENABLE_PROFIT_TAKE and price_change_pct >= PROFIT_TAKE_PCT:
                    should_sell = True
                    sell_reason = f"止盈卖出({price_change_pct:.2f}%)"
                elif ENABLE_STOP_LOSS and price_change_pct <= -STOP_LOSS_PCT:
                    should_sell = True
                    sell_reason = f"止损卖出({price_change_pct:.2f}%)"
            
            # 执行卖出
            if should_sell:
                # 使用收盘价卖出
                sell_price = close_price
                # 计算盈亏（买入价和卖出价的差值）
                profit = (sell_price - buy_price) * position if buy_price > 0 else 0
                win = profit > 0
                win_count += 1 if win else 0
                trade_count += 1
                
                # 计算持有天数（自然天）
                if buy_date:
                    hold_days = (date - buy_date).days
                    total_hold_days += hold_days
                
                # 计算价格变化百分比
                price_change_pct = ((sell_price - buy_price) / buy_price * 100) if buy_price > 0 else 0
                
                # 卖出后，资产净值等于卖出得到的现金
                cash_after_sell = position * sell_price
                
                trades.append({
                    'date': date,
                    'action': '卖出',
                    'price': sell_price,
                    'position': 0,
                    'equity': cash_after_sell,
                    'signal': signal,
                    '买入价': buy_price,
                    '卖出价': sell_price,
                    '价格变化(%)': price_change_pct,
                    'profit': profit,
                    'win': win,
                    '卖出原因': sell_reason
                })
                
                cash = cash_after_sell
                position = 0
                buy_price = 0
                buy_date = None
                
                # 记录卖出后的资产净值
                equity_values.append({
                    'date': date,
                    'equity': cash
                })
            else:
                # 没有卖出，记录当前资产净值
                equity_values.append({
                    'date': date,
                    'equity': current_equity
                })
        
        else:
            # 既没有买入也没有卖出，记录当前资产净值
            current_equity = cash + position * close_price
            equity_values.append({
                'date': date,
                'equity': current_equity
            })
    
    # 处理最后未卖出的持仓
    if position > 0:
        # 计算最后一天的资产净值
        final_close_price = df.iloc[-1]['close']
        
        # 确保价格有效
        if final_close_price <= 0:
            print(f"  警告: 最后一天的收盘价无效({final_close_price})，使用买入价计算")
            final_close_price = buy_price if buy_price > 0 else INITIAL_CAPITAL
        
        final_equity = cash + position * final_close_price
        
        # 确保最终资产净值不为0
        if final_equity <= 0:
            print(f"  警告: 计算出的最终资产净值为0或负数，使用持仓价值计算")
            final_equity = position * final_close_price if final_close_price > 0 else INITIAL_CAPITAL
        
        # 确保最后一天的资产净值被记录（如果循环中已经记录过，这里会覆盖为最终值）
        # 检查是否已经记录过最后一天
        last_date = df.iloc[-1]['date']
        if equity_values and equity_values[-1]['date'] == last_date:
            # 更新最后一条记录
            equity_values[-1]['equity'] = final_equity
        else:
            # 添加新记录
            equity_values.append({
                'date': last_date,
                'equity': final_equity
            })
        
        # 计算盈亏（买入价和当前价格的差值）
        profit = (final_close_price - buy_price) * position if buy_price > 0 else 0
        win = profit > 0
        # 未卖出的持仓也算作一次交易，以最后的价格计算盈利
        win_count += 1 if win else 0
        trade_count += 1
        
        # 计算最终持仓的持有天数（自然天）
        if buy_date:
            hold_days = (df.iloc[-1]['date'] - buy_date).days
            total_hold_days += hold_days
        
        # 计算价格变化百分比
        price_change_pct = ((final_close_price - buy_price) / buy_price * 100) if buy_price > 0 else 0
        
        trades.append({
            'date': last_date,
            'action': '未卖出',
            'price': final_close_price,
            'position': position,
            'equity': final_equity,
            'signal': "持仓结束",
            '买入价': buy_price,
            '当前价': final_close_price,
            '股票价值': position * final_close_price,
            '价格变化(%)': price_change_pct,
            'profit': profit,
            'win': win,
            '卖出原因': "回测结束"
        })
    else:
        final_equity = cash
        # 确保最终资产净值不为0（至少等于初始资金）
        if final_equity < 0:
            print(f"  警告: 最终现金为负数({final_equity})，设置为0")
            final_equity = 0
        
        # 如果没有持仓，确保最后一天的资产净值被记录
        last_date = df.iloc[-1]['date']
        if equity_values:
            # 如果已经有记录，检查最后一条是否是最后一天
            if equity_values[-1]['date'] == last_date:
                # 更新最后一条为最终值
                equity_values[-1]['equity'] = final_equity
            else:
                # 添加新记录
                equity_values.append({
                    'date': last_date,
                    'equity': final_equity
                })
        else:
            # 如果没有记录，添加一条
            equity_values.append({
                'date': last_date,
                'equity': final_equity
            })
    
    # 计算统计数据
    equity_df = pd.DataFrame(equity_values)
    max_equity = equity_df['equity'].max()
    min_equity = equity_df['equity'].min()
    win_rate = win_count / trade_count if trade_count > 0 else 0
    
    # 计算涨幅（百分比形式：盈利50%显示为0.5）
    return_ratio = (final_equity / INITIAL_CAPITAL) - 1
    
    annualized_return = calculate_annualized_return(start_date, end_date, final_equity, INITIAL_CAPITAL)
    
    # 计算买入并持有策略的年化收益
    buy_and_hold_annualized = calculate_annualized_return(start_date, end_date, INITIAL_CAPITAL * (1 + buy_and_hold_return), INITIAL_CAPITAL)
    
    # 计算策略超额收益（策略涨幅 - 买入并持有涨幅）
    strategy_excess_return = return_ratio - buy_and_hold_return
    strategy_excess_annualized = annualized_return - buy_and_hold_annualized
    
    # 计算持有年化涨幅：策略涨幅 / (持有天数 / 365)
    hold_years = total_hold_days / 365.0 if total_hold_days > 0 else 0
    hold_annualized_return = (return_ratio / hold_years) if hold_years > 0 else 0
    
    # 汇总统计
    stats = {
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
        '策略超额收益': strategy_excess_return,
        '策略超额年化收益': strategy_excess_annualized,
        '交易次数': trade_count,
        '盈利交易次数': win_count,
        '开始日期': start_date,
        '结束日期': end_date,
        '回测开始价格': backtest_start_price,
        '回测结束价格': backtest_end_price,
        '回测期间涨跌幅': backtest_price_change_ratio
    }
    
    return stats, trades, equity_df

def backtest_single_file(file_path, output_dir):
    """对单个文件进行回测"""
    try:
        file_name = os.path.splitext(os.path.basename(file_path))[0]
        print(f"开始回测: {file_name}")
        
        all_stats = []
        all_trades = []
        
        for time_frame in TIME_FRAMES:
            print(f"  处理{time_frame}数据...")
            sheet_name = f"{time_frame}数据"
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                if df.empty:
                    print(f"    {time_frame}工作表为空，跳过")
                    continue
                # 规范化列名
                df.columns = [safe_str(col).strip().replace(' ', '') for col in df.columns]
            except Exception as e:
                print(f"    {time_frame}工作表读取失败: {e}，跳过")
                continue
            
            # Excel文件中的周线/月线数据已经是处理好的，不需要再次重采样
            # 直接使用即可，只需要确保日期格式正确
                resampled_df = df.copy()
            resampled_df['date'] = pd.to_datetime(resampled_df['date'], errors='coerce')
            
            # 检查并清理无效数据
            # 删除日期为空的行
            resampled_df = resampled_df.dropna(subset=['date'])
            # 删除价格为空或小于等于0的行
            resampled_df = resampled_df[(resampled_df['open'] > 0) & (resampled_df['close'] > 0)]
            
            if len(resampled_df) < 10:
                print(f"    {time_frame}数据清理后不足10行，跳过回测")
                continue
            
            stats, trades, equity_df = backtest_strategy(resampled_df, time_frame)
            
            if stats is not None:
                stats['周期'] = time_frame
                stats['股票代码'] = file_name
                all_stats.append(stats)
                
                if trades:
                    for trade in trades:
                        trade['周期'] = time_frame
                    all_trades.extend(trades)
                
                # 打印价格分析
                print(f"    {time_frame} - 回测开始价格: {stats['回测开始价格']:.2f}")
                print(f"    {time_frame} - 回测结束价格: {stats['回测结束价格']:.2f}")
                print(f"    {time_frame} - 一直持有涨幅: {stats['一直持有涨幅']:.2%}")
                print(f"    {time_frame} - 策略涨幅: {stats['策略涨幅']:.2%}")
                print(f"    {time_frame} - 策略超额收益: {stats['策略超额收益']:.2%}")
                print(f"    {time_frame} - 胜率: {stats['胜率']:.2%}")
                print(f"    {time_frame} - 交易次数: {stats['交易次数']}")
        
        if not all_stats:
            print("  没有有效的回测结果")
            return None
        
        # 生成输出文件名
        output_filename = f"{file_name}_新策略_回测结果.xlsx"
        output_path = os.path.join(output_dir, output_filename)
        
        # 创建Excel文件
        try:
            writer = pd.ExcelWriter(output_path, engine='xlsxwriter')
            workbook = writer.book
            
            # 创建格式
            number_format = workbook.add_format({'num_format': '0.00'})
            integer_format = workbook.add_format({'num_format': '0'})  # 整数格式
            day_format = workbook.add_format({'num_format': '0.0'})  # 天数格式（1位小数）
            
            # 写入统计数据
            stats_df = pd.DataFrame(all_stats)
            stats_df.to_excel(writer, sheet_name='回测统计', index=False)
            
            # 应用格式到回测统计表
            worksheet = writer.sheets["回测统计"]
            percent_format = workbook.add_format({'num_format': '0.00%'})
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
            
            # 为每个周期创建单独的详细交易记录页签
            for time_frame in TIME_FRAMES:
                period_trades = [trade for trade in all_trades if trade['周期'] == time_frame]
                if period_trades:
                    trades_df = pd.DataFrame(period_trades)
                    trades_df.to_excel(writer, sheet_name=f'{time_frame}交易记录', index=False)
                    
                    # 应用格式到详细数据表
                    worksheet = writer.sheets[f'{time_frame}交易记录']
                    for col_num, col_name in enumerate(trades_df.columns):
                        worksheet.set_column(col_num + 1, col_num + 1, 15, number_format)
            
            writer.close()
            print(f"  回测完成，结果保存到: {output_path}")
            
        except PermissionError as e:
            print(f"  文件访问权限错误: {e}")
            return None
        
        return all_stats
        
    except Exception as e:
        print(f"  回测过程中出现错误: {e}")
        return None

def generate_summary_report(all_stats, output_dir):
    """生成汇总报告"""
    if not all_stats:
        print("没有统计数据，无法生成汇总报告")
        return
    
    try:
        # 创建汇总文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        summary_filename = f"新策略汇总报告_{timestamp}.xlsx"
        summary_path = os.path.join(output_dir, summary_filename)
        
        # 创建Excel文件
        writer = pd.ExcelWriter(summary_path, engine='xlsxwriter')
        workbook = writer.book
        
        # 创建格式
        number_format = workbook.add_format({'num_format': '0.00'})
            integer_format = workbook.add_format({'num_format': '0'})  # 整数格式
            day_format = workbook.add_format({'num_format': '0.0'})  # 天数格式（1位小数）
        percent_format = workbook.add_format({'num_format': '0.00%'})
        
        # 汇总所有统计数据
        summary_data = []
        for file_stats in all_stats:
            for stats in file_stats:
                summary_data.append(stats)
        
        if summary_data:
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='详细数据', index=False)
            
            # 应用格式到详细数据表
            worksheet = writer.sheets["详细数据"]
            percent_format = workbook.add_format({'num_format': '0.00%'})
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
            
            # 为每个周期创建单独的详细数据表
            for time_frame in TIME_FRAMES:
                period_data = summary_df[summary_df['周期'] == time_frame]
                if not period_data.empty:
                    # 写入该周期的详细数据
                    period_data.to_excel(writer, sheet_name=f'{time_frame}统计', index=False)
                    
                    # 应用格式到周期详细数据表
                    worksheet = writer.sheets[f'{time_frame}统计']
                    for col_num, col_name in enumerate(period_data.columns):
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
            
            # 计算整体统计 - 包含四个周期和总体
            overall_stats_data = []
            
            # 添加四个周期的统计
            for time_frame in TIME_FRAMES:
                period_data = summary_df[summary_df['周期'] == time_frame]
                if not period_data.empty:
                    period_stats = {
                        '周期': time_frame,
                        '总股票数': len(period_data['股票代码'].unique()),
                        '总交易次数': period_data['交易次数'].sum(),
                        '总盈利交易次数': period_data['盈利交易次数'].sum(),
                        '平均胜率': period_data['胜率'].mean(),
                        '平均策略涨幅': period_data['策略涨幅'].mean(),
                        '平均策略年化涨幅': period_data['策略年化涨幅'].mean(),
                        '平均一直持有涨幅': period_data['一直持有涨幅'].mean(),
                        '平均一直持有年化涨幅': period_data['一直持有年化涨幅'].mean(),
                        '平均策略超额收益': period_data['策略超额收益'].mean(),
                        '平均策略超额年化收益': period_data['策略超额年化收益'].mean(),
                        '最大策略涨幅': period_data['策略涨幅'].max(),
                        '最小策略涨幅': period_data['策略涨幅'].min(),
                        '亏损股票数': len(period_data[period_data['策略涨幅'] < 0]),
                        '盈利股票平均策略涨幅': period_data[period_data['策略涨幅'] > 0]['策略涨幅'].mean() if len(period_data[period_data['策略涨幅'] > 0]) > 0 else 0,
                        '亏损股票平均亏损比': period_data[period_data['策略涨幅'] < 0]['策略涨幅'].mean() if len(period_data[period_data['策略涨幅'] < 0]) > 0 else 0,
                        '策略超额收益为正股票数': len(period_data[period_data['策略超额收益'] > 0]),
                        '策略超额收益为负股票数': len(period_data[period_data['策略超额收益'] <= 0]),
                        '最大策略超额收益': period_data['策略超额收益'].max(),
                        '最小策略超额收益': period_data['策略超额收益'].min()
                    }
                    overall_stats_data.append(period_stats)
            
            # 添加总体统计
            total_stats = {
                '周期': '总体',
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
                '策略超额收益为正股票数': len(summary_df[summary_df['策略超额收益'] > 0]),
                '策略超额收益为负股票数': len(summary_df[summary_df['策略超额收益'] <= 0]),
                '最大策略超额收益': summary_df['策略超额收益'].max(),
                '最小策略超额收益': summary_df['策略超额收益'].min()
            }
            overall_stats_data.append(total_stats)
            
            # 写入整体统计
            overall_df = pd.DataFrame(overall_stats_data)
            overall_df.to_excel(writer, sheet_name='整体统计', index=False)
            
            # 应用格式到整体统计表
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

def main():
    # 输入和输出目录
    input_dir = "analyzed_results"
    output_dir = "backtest_results_new_strategy"
    
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 获取所有Excel文件
    excel_files = glob.glob(os.path.join(input_dir, "*.xlsx"))
    
    if not excel_files:
        print(f"在 {input_dir} 目录中没有找到Excel文件")
        return
    
    print(f"找到 {len(excel_files)} 个文件进行回测")
    
    # 存储所有统计数据
    all_stats = []
    
    # 逐个处理文件
    for i, file_path in enumerate(excel_files, 1):
        print(f"\n[{i}/{len(excel_files)}] 回测新策略: {os.path.basename(file_path)}")
        
        try:
            file_stats = backtest_single_file(file_path, output_dir)
            if file_stats:
                all_stats.append(file_stats)
        except Exception as e:
            print(f"处理文件 {file_path} 时出现错误: {e}")
            continue
    
    # 生成汇总报告
    if all_stats:
        print(f"\n生成汇总报告...")
        generate_summary_report(all_stats, output_dir)
    
    print(f"\n回测完成！共处理 {len(all_stats)} 个文件")

if __name__ == "__main__":
    main()
