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

# 支持的周期类型（多周期共振策略使用小时线和日线）
TIME_FRAMES = ['小时线', '日线']

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

def find_signal_at_date(df, target_date):
    """在数据框中查找指定日期或之前最近的信号"""
    if df.empty:
        return None
    
    # 找到目标日期或之前最近的数据
    df_before = df[df['date'] <= target_date]
    if df_before.empty:
        return None
    
    # 返回最近的一条数据的信号
    return df_before.iloc[-1]['综合判断']

def backtest_strategy(df_hourly, df_daily):
    """执行多周期共振策略回测"""
    if df_hourly is None or len(df_hourly) < 10:
        return None, None, None
    if df_daily is None or len(df_daily) < 10:
        return None, None, None
    
    # 确保有必要的列
    required_columns = ['date', 'open', 'close', '综合判断']
    for df, name in [(df_hourly, '小时线'), (df_daily, '日线')]:
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"  {name}缺少必要列: {', '.join(missing_columns)}")
            return None, None, None
    
    # 准备数据
    df_hourly = df_hourly.copy()
    df_daily = df_daily.copy()
    
    # 1. 转换日期列
    df_hourly['date'] = pd.to_datetime(df_hourly['date'])
    df_daily['date'] = pd.to_datetime(df_daily['date'])
    
    # 2. 检查是否存在负数价格
    has_negative_prices = ((df_hourly['open'] <= 0).any() or (df_hourly['close'] <= 0).any() or
                          (df_daily['open'] <= 0).any() or (df_daily['close'] <= 0).any())
    
    # 3. 如果有负数价格，过滤配置的起始日期之前的数据
    if has_negative_prices:
        print(f"  检测到负数价格，过滤{BACKTEST_START_DATE}之前的数据")
        start_date = pd.Timestamp(BACKTEST_START_DATE)
        df_hourly = df_hourly[df_hourly['date'] >= start_date].copy()
        df_daily = df_daily[df_daily['date'] >= start_date].copy()
        if len(df_hourly) < 10 or len(df_daily) < 10:
            print("  过滤后数据不足10行，跳过回测")
            return None, None, None
    
    # 4. 排序数据
    df_hourly.sort_values('date', inplace=True)
    df_daily.sort_values('date', inplace=True)
    
    # 初始化变量
    cash = INITIAL_CAPITAL
    position = 0  # 持仓数量
    target_position_ratio = 0.0  # 目标仓位比例（0=空仓, 0.5=半仓, 1.0=全仓）
    trades = []
    equity_values = []
    buy_price = 0
    buy_date = None
    win_count = 0
    trade_count = 0
    total_hold_days = 0  # 总持有天数（自然天）
    
    # 记录开始和结束日期（使用小时线数据作为基准）
    start_date = df_hourly['date'].min()
    end_date = df_hourly['date'].max()
    
    # 获取回测开始和结束时的价格（用于计算买入并持有策略的收益，使用小时线数据）
    backtest_start_price = df_hourly.iloc[0]['close']
    backtest_end_price = df_hourly.iloc[-1]['close']
    backtest_price_change_ratio = ((backtest_end_price - backtest_start_price) / backtest_start_price) if backtest_start_price > 0 else 0
    
    # 计算买入并持有策略的收益（一直持有）
    buy_and_hold_return = backtest_price_change_ratio
    
    # 使用小时线数据作为主时间轴（更密集）
    for i in range(len(df_hourly)):
        row_hourly = df_hourly.iloc[i]
        date = row_hourly['date']
        close_price = row_hourly['close']
        open_price = row_hourly['open']
        
        # 获取小时线和日线的信号
        hourly_signal = row_hourly['综合判断']
        daily_signal = find_signal_at_date(df_daily, date)
        
        # 如果日线信号为空，跳过
        if daily_signal is None:
            # 记录当前资产净值
            current_equity = cash + position * close_price
            equity_values.append({
                'date': date,
                'equity': current_equity
            })
            continue
        
        # 计算当前资产净值
        current_equity = cash + position * close_price
        
        # 判断目标仓位
        hourly_buy = hourly_signal in BUY_SIGNALS
        daily_buy = daily_signal in BUY_SIGNALS
        hourly_sell = hourly_signal in SELL_SIGNALS
        daily_sell = daily_signal in SELL_SIGNALS
        
        # 卖出逻辑：任意一者出现卖出信号，空仓
        if hourly_sell or daily_sell:
            target_position_ratio = 0.0
            sell_reason = ""
            if hourly_sell and daily_sell:
                sell_reason = "小时线和日线都卖出信号"
            elif hourly_sell:
                sell_reason = "小时线卖出信号"
            else:
                sell_reason = "日线卖出信号"
        # 买入逻辑：根据信号决定仓位
        elif hourly_buy or daily_buy:
            if hourly_buy and daily_buy:
                target_position_ratio = 1.0  # 两者都买入信号，全仓
            else:
                target_position_ratio = 0.5  # 任意一者买入信号，半仓
        else:
            # 信号中性，保持当前仓位
            target_position_ratio = target_position_ratio
        
        # 计算目标持仓数量（基于当前价格）
        target_equity = current_equity * target_position_ratio
        target_position = target_equity / close_price if close_price > 0 else 0
        
        # 调整仓位
        position_diff = target_position - position
        
        # 买入
        if position_diff > 0.001:  # 需要买入（避免浮点数误差）
            buy_amount = position_diff * close_price
            if cash >= buy_amount:
                position += position_diff
                cash -= buy_amount
                buy_price = close_price  # 更新买入价（使用加权平均）
                if buy_date is None:
                    buy_date = date
                
                trades.append({
                    'date': date,
                    'action': '买入',
                    'price': close_price,
                    'position': position,
                    'equity': cash + position * close_price,
                    '小时线信号': hourly_signal,
                    '日线信号': daily_signal,
                    '目标仓位': f"{target_position_ratio*100:.0f}%",
                    '买入原因': "小时线和日线都买入信号" if (hourly_buy and daily_buy) else ("小时线买入信号" if hourly_buy else "日线买入信号")
                })
        
        # 卖出
        elif position_diff < -0.001:  # 需要卖出
            sell_amount = abs(position_diff) * close_price
            position += position_diff  # position_diff是负数
            cash += sell_amount
            
            # 如果全部卖出，重置买入价和买入日期
            if position < 0.001:
                # 计算盈亏
                if buy_price > 0:
                    profit = (close_price - buy_price) * (position + abs(position_diff))
                    win = profit > 0
                    win_count += 1 if win else 0
                    trade_count += 1
                    
                    # 计算持有天数（自然天）
                    if buy_date:
                        hold_days = (date - buy_date).days
                        total_hold_days += hold_days
                    
                    price_change_pct = ((close_price - buy_price) / buy_price * 100) if buy_price > 0 else 0
                    
                    trades.append({
                        'date': date,
                        'action': '卖出',
                        'price': close_price,
                        'position': 0,
                        'equity': cash,
                        '小时线信号': hourly_signal,
                        '日线信号': daily_signal,
                        '买入价': buy_price,
                        '卖出价': close_price,
                        '价格变化(%)': price_change_pct,
                        'profit': profit,
                        'win': win,
                        '卖出原因': sell_reason if 'sell_reason' in locals() else "仓位调整"
                    })
                
                buy_price = 0
                buy_date = None
            else:
                # 部分卖出，记录交易
                trades.append({
                    'date': date,
                    'action': '减仓',
                    'price': close_price,
                    'position': position,
                    'equity': cash + position * close_price,
                    '小时线信号': hourly_signal,
                    '日线信号': daily_signal,
                    '目标仓位': f"{target_position_ratio*100:.0f}%",
                    '卖出原因': sell_reason if 'sell_reason' in locals() else "仓位调整"
                })
        
        # 检查止盈止损（仅在持仓时）
        if position > 0.001 and buy_price > 0:
            price_change_pct = ((close_price - buy_price) / buy_price * 100) if buy_price > 0 else 0
            
            should_sell_all = False
            sell_reason_stop = ""
            
            # 根据配置检查止盈止损
            if ENABLE_PROFIT_TAKE and price_change_pct >= PROFIT_TAKE_PCT:
                should_sell_all = True
                sell_reason_stop = f"止盈卖出({price_change_pct:.2f}%)"
            elif ENABLE_STOP_LOSS and price_change_pct <= -STOP_LOSS_PCT:
                should_sell_all = True
                sell_reason_stop = f"止损卖出({price_change_pct:.2f}%)"
            
            # 执行止盈止损
            if should_sell_all:
                sell_amount = position * close_price
                cash += sell_amount
                
                # 计算盈亏
                profit = (close_price - buy_price) * position if buy_price > 0 else 0
                win = profit > 0
                win_count += 1 if win else 0
                trade_count += 1
                
                # 计算持有天数（自然天）
                if buy_date:
                    hold_days = (date - buy_date).days
                    total_hold_days += hold_days
                
                trades.append({
                    'date': date,
                    'action': '卖出',
                    'price': close_price,
                    'position': 0,
                    'equity': cash,
                    '小时线信号': hourly_signal,
                    '日线信号': daily_signal,
                    '买入价': buy_price,
                    '卖出价': close_price,
                    '价格变化(%)': price_change_pct,
                    'profit': profit,
                    'win': win,
                    '卖出原因': sell_reason_stop
                })
                
                position = 0
                target_position_ratio = 0.0
                buy_price = 0
                buy_date = None
        
        # 记录当前资产净值
        current_equity = cash + position * close_price
        equity_values.append({
            'date': date,
            'equity': current_equity
        })
    
    # 处理最后未卖出的持仓
    if position > 0.001:
        final_close_price = df_hourly.iloc[-1]['close']
        
        if final_close_price <= 0:
            print(f"  警告: 最后一天的收盘价无效({final_close_price})，使用买入价计算")
            final_close_price = buy_price if buy_price > 0 else INITIAL_CAPITAL
        
        final_equity = cash + position * final_close_price
        
        if final_equity <= 0:
            print(f"  警告: 计算出的最终资产净值为0或负数，使用持仓价值计算")
            final_equity = position * final_close_price if final_close_price > 0 else INITIAL_CAPITAL
        
        last_date = df_hourly.iloc[-1]['date']
        if equity_values and equity_values[-1]['date'] == last_date:
            equity_values[-1]['equity'] = final_equity
        else:
            equity_values.append({
                'date': last_date,
                'equity': final_equity
            })
        
        profit = (final_close_price - buy_price) * position if buy_price > 0 else 0
        win = profit > 0
        win_count += 1 if win else 0
        trade_count += 1
        
        # 计算最终持仓的持有天数（自然天）
        if buy_date:
            hold_days = (last_date - buy_date).days
            total_hold_days += hold_days
        
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
            '价格变化(%)': price_change_pct,
            'profit': profit,
            'win': win,
            '卖出原因': "回测结束"
        })
    else:
        final_equity = cash
        if final_equity < 0:
            print(f"  警告: 最终现金为负数({final_equity})，设置为0")
            final_equity = 0
        
        last_date = df_hourly.iloc[-1]['date']
        if equity_values:
            if equity_values[-1]['date'] == last_date:
                equity_values[-1]['equity'] = final_equity
            else:
                equity_values.append({
                    'date': last_date,
                    'equity': final_equity
                })
        else:
            equity_values.append({
                'date': last_date,
                'equity': final_equity
            })
    
    # 计算统计数据
    equity_df = pd.DataFrame(equity_values)
    max_equity = equity_df['equity'].max()
    min_equity = equity_df['equity'].min()
    win_rate = win_count / trade_count if trade_count > 0 else 0
    
    return_ratio = (final_equity / INITIAL_CAPITAL) - 1
    annualized_return = calculate_annualized_return(start_date, end_date, final_equity, INITIAL_CAPITAL)
    buy_and_hold_annualized = calculate_annualized_return(start_date, end_date, INITIAL_CAPITAL * (1 + buy_and_hold_return), INITIAL_CAPITAL)
    strategy_excess_return = return_ratio - buy_and_hold_return
    strategy_excess_annualized = annualized_return - buy_and_hold_annualized
    
    # 计算持有年化涨幅：策略涨幅 / (持有天数 / 365)
    hold_years = total_hold_days / 365.0 if total_hold_days > 0 else 0
    hold_annualized_return = (return_ratio / hold_years) if hold_years > 0 else 0
    
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
        
        # 读取小时线和日线数据
        try:
            df_hourly = pd.read_excel(file_path, sheet_name='小时线数据')
            df_daily = pd.read_excel(file_path, sheet_name='日线数据')
        except Exception as e:
            print(f"  读取数据失败: {e}")
            return None
        
        if df_hourly.empty or df_daily.empty:
            print(f"  小时线或日线数据为空，跳过")
            return None
        
        # 规范化列名
        df_hourly.columns = [safe_str(col).strip().replace(' ', '') for col in df_hourly.columns]
        df_daily.columns = [safe_str(col).strip().replace(' ', '') for col in df_daily.columns]
        
        # 清理数据
        df_hourly['date'] = pd.to_datetime(df_hourly['date'], errors='coerce')
        df_daily['date'] = pd.to_datetime(df_daily['date'], errors='coerce')
        df_hourly = df_hourly.dropna(subset=['date'])
        df_daily = df_daily.dropna(subset=['date'])
        df_hourly = df_hourly[(df_hourly['open'] > 0) & (df_hourly['close'] > 0)]
        df_daily = df_daily[(df_daily['open'] > 0) & (df_daily['close'] > 0)]
        
        if len(df_hourly) < 10 or len(df_daily) < 10:
            print(f"  数据清理后不足10行，跳过回测")
            return None
        
        stats, trades, equity_df = backtest_strategy(df_hourly, df_daily)
        
        if stats is None:
            print("  回测失败")
            return None
        
        stats['股票代码'] = file_name
        
        # 生成输出文件名
        output_filename = f"{file_name}_多周期共振策略_回测结果.xlsx"
        output_path = os.path.join(output_dir, output_filename)
        
        # 创建Excel文件
        try:
            writer = pd.ExcelWriter(output_path, engine='xlsxwriter')
            workbook = writer.book
            
            number_format = workbook.add_format({'num_format': '0.00'})
            integer_format = workbook.add_format({'num_format': '0'})  # 整数格式
            day_format = workbook.add_format({'num_format': '0.0'})  # 天数格式（1位小数）
            percent_format = workbook.add_format({'num_format': '0.00%'})
            
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
        summary_filename = f"多周期共振策略汇总报告_{timestamp}.xlsx"
        summary_path = os.path.join(output_dir, summary_filename)
        
        writer = pd.ExcelWriter(summary_path, engine='xlsxwriter')
        workbook = writer.book
        
        number_format = workbook.add_format({'num_format': '0.00'})
            integer_format = workbook.add_format({'num_format': '0'})  # 整数格式
            day_format = workbook.add_format({'num_format': '0.0'})  # 天数格式（1位小数）
        percent_format = workbook.add_format({'num_format': '0.00%'})
        
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
    output_dir = "backtest_results_multi_resonance"
    
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 获取所有Excel文件
    excel_files = glob.glob(os.path.join(input_dir, "*.xlsx"))
    
    if not excel_files:
        print(f"在 {input_dir} 目录中没有找到Excel文件")
        return
    
    print(f"找到 {len(excel_files)} 个文件进行多周期共振策略回测")
    print(f"策略说明：")
    print(f"  - 任意一者买入信号 → 半仓（50%）")
    print(f"  - 两者都买入信号 → 全仓（100%）")
    print(f"  - 任意一者卖出信号 → 空仓（0%）")
    print(f"  - 止盈止损：根据config.py配置")
    print("=" * 60)
    
    # 存储所有统计数据
    all_stats = []
    
    # 逐个处理文件
    for i, file_path in enumerate(excel_files, 1):
        print(f"\n[{i}/{len(excel_files)}] 回测多周期共振策略: {os.path.basename(file_path)}")
        
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

