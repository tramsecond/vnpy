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
from config import INITIAL_CAPITAL, BUY_SIGNALS, SELL_SIGNALS, BACKTEST_START_DATE

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
        return value.strftime('%Y-%m-%d')
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
    """执行回测策略"""
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
    backtest_price_change_pct = ((backtest_end_price - backtest_start_price) / backtest_start_price * 100) if backtest_start_price > 0 else 0
    
    # 计算买入并持有策略的收益（一直持有）
    buy_and_hold_return = backtest_price_change_pct / 100  # 转换为小数形式
    
    # 遍历每一行数据
    for i in range(len(df)):
        row = df.iloc[i]
        date = row['date']
        close_price = row['close']
        open_price = row['open']
        
        # 计算当前资产净值
        current_equity = cash + position * close_price
        equity_values.append({
            'date': date,
            'equity': current_equity
        })
        
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
        
        # 卖出信号 - 处理负价格问题
        elif signal in SELL_SIGNALS and position > 0 and i < len(df) - 1:
            # 使用下一天的开盘价卖出
            next_open = df.iloc[i+1]['open']
            # 如果价格小于等于0，跳过操作
            if next_open <= 0:
                continue
                
            sell_price = next_open
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
            
            trades.append({
                'date': date,
                'action': '卖出',
                'price': sell_price,
                'position': 0,
                'equity': current_equity,
                'signal': signal,
                '买入价': buy_price,
                '卖出价': sell_price,
                '价格变化(%)': price_change_pct,
                'profit': profit,
                'win': win
            })
            
            cash = position * sell_price
            position = 0
            buy_price = 0
            buy_date = None
    
    # 处理最后未卖出的持仓
    if position > 0:
        # 计算最后一天的资产净值
        final_close_price = df.iloc[-1]['close']
        final_equity = cash + position * final_close_price
        
        # 确保最后一天的资产净值被记录（如果循环中已经记录过，这里会覆盖为最终值）
        equity_values.append({
            'date': df.iloc[-1]['date'],
            'equity': final_equity
        })
        
        # 计算盈亏（买入价和当前价格的差值）
        profit = (final_close_price - buy_price) * position if buy_price > 0 else 0
        win = profit > 0
        win_count += 1 if win else 0
        trade_count += 1
        
        # 计算最终持仓的持有天数（自然天）
        if buy_date:
            hold_days = (df.iloc[-1]['date'] - buy_date).days
            total_hold_days += hold_days
        
        # 计算价格变化百分比
        price_change_pct = ((final_close_price - buy_price) / buy_price * 100) if buy_price > 0 else 0
        
        trades.append({
            'date': df.iloc[-1]['date'],
            'action': '未卖出',
            'price': final_close_price,
            'position': position,
            'equity': final_equity,
            'signal': "持仓结束",
            '买入价': buy_price,
            '当前价': final_close_price,
            '价格变化(%)': price_change_pct,
            'profit': profit,
            'win': win
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
        '回测期间涨跌幅(%)': backtest_price_change_pct
    }
    
    return trades, stats, equity_df

def backtest_single_file(file_path, output_dir):
    """回测单个文件"""
    file_name = os.path.basename(file_path)
    stock_code = os.path.splitext(file_name)[0]
    output_path = os.path.join(output_dir, f"{stock_code}_回测结果.xlsx")
    
    print(f"回测: {file_name}")
    
    # 存储所有周期的统计数据
    all_stats = []
    
    try:
        # 创建Excel写入器
        writer = pd.ExcelWriter(output_path, engine='xlsxwriter')
        
        # 获取工作簿和工作表对象用于格式化
        workbook = writer.book
        
        # 定义格式
        percent_format = workbook.add_format({'num_format': '0.00%'})
        number_format = workbook.add_format({'num_format': '0.00'})
            integer_format = workbook.add_format({'num_format': '0'})  # 整数格式
            day_format = workbook.add_format({'num_format': '0.0'})  # 天数格式（1位小数）
        
        # 处理所有周期数据
        for time_frame in TIME_FRAMES:
            sheet_name = f"{time_frame}数据"
            
            try:
                # 读取工作表
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                if df.empty:
                    print(f"  {sheet_name} 工作表为空，跳过")
                    continue
                
                # 规范化列名
                df.columns = [safe_str(col).strip().replace(' ', '') for col in df.columns]
                
                # 执行回测
                trades, stats, equity_df = backtest_strategy(df, time_frame)
                
                if trades is None or stats is None:
                    print(f"  {sheet_name} 回测失败")
                    continue
                
                # 保存交易记录
                trades_df = pd.DataFrame(trades)
                if not trades_df.empty:
                    trades_df.to_excel(writer, sheet_name=f"{time_frame}交易记录", index=False)
                    
                    # 分析交易记录中的价格信息
                    sell_trades = trades_df[trades_df['action'] == '卖出']
                    if not sell_trades.empty and '买入价' in sell_trades.columns and '卖出价' in sell_trades.columns:
                        avg_price_change = sell_trades['价格变化(%)'].mean()
                        max_price_change = sell_trades['价格变化(%)'].max()
                        min_price_change = sell_trades['价格变化(%)'].min()
                        print(f"  {time_frame}价格分析: 平均涨跌幅={avg_price_change:.2f}%, 最大涨幅={max_price_change:.2f}%, 最大跌幅={min_price_change:.2f}%")
                else:
                    print(f"  {time_frame}没有交易记录")
                
                # 保存资产净值曲线
                equity_df.to_excel(writer, sheet_name=f"{time_frame}资产净值", index=False)
                
                # 添加周期信息
                stats['周期'] = time_frame
                stats['股票代码'] = stock_code
                all_stats.append(stats)
                
                print(f"  {time_frame}回测完成: 最终资产={stats['最终资产净值']:.2f}, 涨幅={stats['策略涨幅']:.2%}")
                
            except Exception as e:
                print(f"  处理{sheet_name}时出错: {str(e)}")
        
        # 保存统计数据
        if all_stats:
            stats_df = pd.DataFrame(all_stats)
            stats_df.to_excel(writer, sheet_name="回测统计", index=False)
            
            # 应用格式到相关列
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
        
        # 保存Excel文件
        writer.close()
        print(f"回测结果已保存: {output_path}")
        
    except PermissionError:
        print(f"  错误: 无法保存文件 '{output_path}'，文件可能被其他程序占用")
        return []
    except Exception as e:
        print(f"  保存文件时出错: {str(e)}")
        return []
    
    return all_stats

def generate_summary_report(all_stats, output_dir):
    """生成总结报告"""
    if not all_stats:
        print("没有统计数据可生成总结报告")
        return
    
    # 合并所有统计数据
    summary_df = pd.DataFrame(all_stats)
    
    # 计算盈利股票数
    summary_df['盈利'] = summary_df['最终资产净值'] > INITIAL_CAPITAL
    profit_summary = summary_df.groupby('周期')['盈利'].agg(['sum', 'count'])
    profit_summary['亏损'] = profit_summary['count'] - profit_summary['sum']
    profit_summary.rename(columns={'sum': '盈利股票数', 'count': '总股票数'}, inplace=True)
    
    # 计算价格相关统计
    profitable_stocks = summary_df[summary_df['最终资产净值'] > INITIAL_CAPITAL]
    avg_profit_pct = profitable_stocks['策略涨幅'].mean() if len(profitable_stocks) > 0 else 0
    avg_loss_pct = summary_df[summary_df['最终资产净值'] <= INITIAL_CAPITAL]['策略涨幅'].mean() if len(summary_df[summary_df['最终资产净值'] <= INITIAL_CAPITAL]) > 0 else 0
    
    # 计算价格变化统计（买入并持有策略）
    if '回测期间涨跌幅(%)' in summary_df.columns:
        avg_price_change = summary_df['回测期间涨跌幅(%)'].mean()
        max_price_change = summary_df['回测期间涨跌幅(%)'].max()
        min_price_change = summary_df['回测期间涨跌幅(%)'].min()
        positive_price_changes = len(summary_df[summary_df['回测期间涨跌幅(%)'] > 0])
        negative_price_changes = len(summary_df[summary_df['回测期间涨跌幅(%)'] <= 0])
    else:
        avg_price_change = max_price_change = min_price_change = 0
        positive_price_changes = negative_price_changes = 0
    
    # 计算策略超额收益统计（策略涨幅 - 买入并持有涨幅）
    if '策略超额收益' in summary_df.columns:
        avg_excess_return = summary_df['策略超额收益'].mean()
        max_excess_return = summary_df['策略超额收益'].max()
        min_excess_return = summary_df['策略超额收益'].min()
        positive_excess_return = len(summary_df[summary_df['策略超额收益'] > 0])
        negative_excess_return = len(summary_df[summary_df['策略超额收益'] <= 0])
    else:
        avg_excess_return = max_excess_return = min_excess_return = 0
        positive_excess_return = negative_excess_return = 0
    
    # 保存总结报告
    output_path = os.path.join(output_dir, "回测总结报告.xlsx")
    try:
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            # 获取工作簿对象用于格式化
            workbook = writer.book
            percent_format = workbook.add_format({'num_format': '0.00%'})
            number_format = workbook.add_format({'num_format': '0.00'})
            integer_format = workbook.add_format({'num_format': '0'})  # 整数格式
            day_format = workbook.add_format({'num_format': '0.0'})  # 天数格式（1位小数）
            
            # 详细数据
            summary_df.to_excel(writer, sheet_name="详细数据", index=False)
            
            # 应用百分比格式到详细数据
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
            
            # 周期汇总
            period_summary = summary_df.groupby('周期').agg({
                '初始资金': 'first',
                '最终资产净值': ['mean', 'median', 'min', 'max'],
                '资产净值最大值': 'mean',
                '资产净值最小值': 'mean',
                '胜率': 'mean',
                '策略涨幅': 'mean',
                '策略年化涨幅': 'mean',
                '一直持有涨幅': 'mean',
                '一直持有年化涨幅': 'mean',
                '策略超额收益': 'mean',
                '策略超额年化收益': 'mean',
                '交易次数': 'sum',
                '盈利交易次数': 'sum'
            })
            period_summary.columns = ['_'.join(col).strip() for col in period_summary.columns.values]
            period_summary.reset_index(inplace=True)
            period_summary.to_excel(writer, sheet_name="周期汇总", index=False)
            
            # 盈利统计
            profit_summary.reset_index(inplace=True)
            profit_summary.to_excel(writer, sheet_name="盈利统计", index=False)
            
            # 价格表现统计
            price_stats = pd.DataFrame({
                '总股票数': [len(summary_df)],
                '盈利股票数': [len(profitable_stocks)],
                '亏损股票数': [len(summary_df) - len(profitable_stocks)],
                '盈利股票平均策略涨幅': [avg_profit_pct],
                '亏损股票平均亏损比': [avg_loss_pct],
                '总体平均策略涨幅': [summary_df['策略涨幅'].mean()],
                '总体平均策略年化涨幅': [summary_df['策略年化涨幅'].mean()],
                '总体平均一直持有涨幅': [summary_df['一直持有涨幅'].mean()],
                '总体平均一直持有年化涨幅': [summary_df['一直持有年化涨幅'].mean()],
                '总体平均策略超额收益': [summary_df['策略超额收益'].mean()],
                '总体平均策略超额年化收益': [summary_df['策略超额年化收益'].mean()],
                '平均价格涨跌幅(%)': [avg_price_change],
                '最大价格涨幅(%)': [max_price_change],
                '最大价格跌幅(%)': [min_price_change],
                '价格上涨股票数': [positive_price_changes],
                '价格下跌股票数': [negative_price_changes],
                '平均策略超额收益': [avg_excess_return],
                '最大策略超额收益': [max_excess_return],
                '最小策略超额收益': [min_excess_return],
                '策略超额收益为正股票数': [positive_excess_return],
                '策略超额收益为负股票数': [negative_excess_return]
            })
            price_stats.to_excel(writer, sheet_name="价格表现统计", index=False)
            
            # 应用百分比格式到价格表现统计
            worksheet = writer.sheets["价格表现统计"]
            for col_num, col_name in enumerate(price_stats.columns):
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
            
            # 价格汇总表
            if '回测开始价格' in summary_df.columns and '回测结束价格' in summary_df.columns:
                # 检查列是否存在，如果不存在则跳过
                available_columns = ['股票代码', '周期', '回测开始价格', '回测结束价格', '回测期间涨跌幅(%)']
                if '策略涨幅' in summary_df.columns:
                    available_columns.append('策略涨幅')
                if '策略年化涨幅' in summary_df.columns:
                    available_columns.append('策略年化涨幅')
                if '策略超额收益' in summary_df.columns:
                    available_columns.append('策略超额收益')
                
                price_summary = summary_df[available_columns].copy()
                price_summary = price_summary.sort_values('回测期间涨跌幅(%)', ascending=False)
                price_summary.to_excel(writer, sheet_name="价格汇总", index=False)
                
                # 应用百分比格式到价格汇总
                worksheet = writer.sheets["价格汇总"]
                for col_num, col_name in enumerate(price_summary.columns):
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
            
            # 最佳表现股票（按周期）
            if len(summary_df) > 0:
                for period in summary_df['周期'].unique():
                    period_data = summary_df[summary_df['周期'] == period]
                    if len(period_data) > 0:
                        # 使用实际存在的列名进行排序
                        sort_columns = []
                        if '策略涨幅' in period_data.columns:
                            sort_columns.append('策略涨幅')
                        if '策略年化涨幅' in period_data.columns:
                            sort_columns.append('策略年化涨幅')
                        
                        if sort_columns:
                            top_stocks = period_data.sort_values(
                                by=sort_columns, 
                                ascending=False
                            ).head(min(5, len(period_data)))
                        else:
                            # 如果没有找到排序列，使用最终资产净值
                            top_stocks = period_data.sort_values(
                                by='最终资产净值', 
                                ascending=False
                            ).head(min(5, len(period_data)))
                        
                        top_stocks.to_excel(writer, sheet_name=f"{period}最佳表现", index=False)
                        
                        # 应用百分比格式到最佳表现
                        worksheet = writer.sheets[f"{period}最佳表现"]
                        for col_num, col_name in enumerate(top_stocks.columns):
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
        
        print(f"总结报告已保存: {output_path}")
        
    except PermissionError:
        print(f"错误: 无法保存总结报告 '{output_path}'，文件可能被其他程序占用")
    except Exception as e:
        print(f"保存总结报告时出错: {str(e)}")

def main():
    # 输入和输出目录
    input_dir = "analyzed_results"
    output_dir = "backtest_results"
    
    # 检查目录
    if not os.path.exists(input_dir):
        print(f"错误: 输入目录 '{input_dir}' 不存在")
        return
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"创建输出目录: {output_dir}")
    
    # 获取所有Excel文件
    excel_files = glob.glob(os.path.join(input_dir, "*.xlsx"))
    if not excel_files:
        print("警告: 未找到任何Excel文件")
        return
    
    print(f"找到 {len(excel_files)} 个Excel文件，开始回测...")
    print(f"回测结果将保存到: {output_dir}")
    print("=" * 60)
    
    # 存储所有统计数据
    all_stats = []
    
    # 处理每个文件
    for i, file_path in enumerate(excel_files):
        print(f"\n[{i+1}/{len(excel_files)}] ", end="")
        file_stats = backtest_single_file(file_path, output_dir)
        if file_stats:
            all_stats.extend(file_stats)
    
    # 生成总结报告
    if all_stats:
        print("\n" + "=" * 60)
        print("生成总结报告...")
        generate_summary_report(all_stats, output_dir)
    
    print("\n" + "=" * 60)
    print(f"回测完成! 处理了 {len(excel_files)} 个文件")
    print(f"所有回测结果已保存到 '{output_dir}' 目录中")
    print("=" * 60)

if __name__ == "__main__":
    main()