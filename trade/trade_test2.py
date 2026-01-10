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
TIME_FRAMES = ['日线', '周线', '月线']

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

def find_previous_month_end(target_date, df_monthly):
    """查找指定日期之前最近的一个月结束日期"""
    target_date = pd.Timestamp(target_date)
    
    # 获取所有月结束日期
    month_ends = df_monthly['date'].tolist()
    
    # 找到最后一个小于等于目标日期的月份结束
    for date in reversed(month_ends):
        if date <= target_date:
            return date
    
    # 如果没有找到，返回最早的日期
    return min(month_ends) if month_ends else None

def find_previous_week_end(target_date, df_weekly):
    """查找指定日期之前最近的一个周结束日期"""
    target_date = pd.Timestamp(target_date)
    
    # 获取所有周结束日期
    week_ends = df_weekly['date'].tolist()
    
    # 找到最后一个小于等于目标日期的周结束
    for date in reversed(week_ends):
        if date <= target_date:
            return date
    
    # 如果没有找到，返回最早的日期
    return min(week_ends) if week_ends else None

def binary_search_signal(df, target_date):
    """二分查找获取给定日期的信号"""
    low, high = 0, len(df) - 1
    target_date = pd.to_datetime(target_date)
    
    # 特殊情况处理
    if len(df) == 0:
        return None
    if target_date < df.iloc[0]['date']:
        return df.iloc[0]['综合判断'] if len(df) > 0 else None
    if target_date > df.iloc[-1]['date']:
        return df.iloc[-1]['综合判断'] if len(df) > 0 else None
    
    while low <= high:
        mid = (low + high) // 2
        mid_date = df.iloc[mid]['date']
        
        if mid_date < target_date:
            low = mid + 1
        elif mid_date > target_date:
            high = mid - 1
        else:
            # 精确匹配
            return df.iloc[mid]['综合判断']
    
    # 找不到精确匹配，返回最近的小于等于日期的信号
    if high >= 0 and high < len(df):
        return df.iloc[high]['综合判断']
    
    return None

def backtest_strategy(df_daily, df_weekly, df_monthly):
    """执行逐层筛选策略的回测"""
    # 确保日期格式正确
    for df in [df_daily, df_weekly, df_monthly]:
        df['date'] = pd.to_datetime(df['date'])
    
    # 检查是否存在负数价格（开盘价或收盘价）
    has_negative_prices = False
    for df in [df_daily, df_weekly, df_monthly]:
        if (df['open'] <= 0).any() or (df['close'] <= 0).any():
            has_negative_prices = True
            break
    
    # 如果有负数价格，过滤配置的起始日期之前的数据
    if has_negative_prices:
        print(f"  检测到负数价格，过滤{BACKTEST_START_DATE}之前的数据")
        start_date = pd.Timestamp(BACKTEST_START_DATE)
        df_daily = df_daily[df_daily['date'] >= start_date].copy()
        df_weekly = df_weekly[df_weekly['date'] >= start_date].copy()
        df_monthly = df_monthly[df_monthly['date'] >= start_date].copy()
        if len(df_daily) < 10:
            print("  过滤后日线数据不足10行，跳过回测")
            return None, None, None
    
    # 按日期排序
    df_daily = df_daily.sort_values('date').reset_index(drop=True)
    df_weekly = df_weekly.sort_values('date').reset_index(drop=True)
    df_monthly = df_monthly.sort_values('date').reset_index(drop=True)
    
    # 初始化变量
    cash = INITIAL_CAPITAL
    position = 0
    trades = []
    equity_values = []
    buy_price = 0
    buy_date = None
    win_count = 0
    trade_count = 0
    total_hold_days = 0  # 总持股天数
    hold_periods = []    # 每次交易的持股天数
    
    # 记录开始和结束日期
    start_date = df_daily['date'].min()
    end_date = df_daily['date'].max()
    
    # 获取回测开始和结束时的价格
    backtest_start_price = df_daily.iloc[0]['close']  # 回测开始时的价格
    backtest_end_price = df_daily.iloc[-1]['close']   # 回测结束时的价格
    backtest_price_change_pct = ((backtest_end_price - backtest_start_price) / backtest_start_price * 100) if backtest_start_price > 0 else 0
    
    print(f"  回测日期范围: {start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")
    print(f"  回测开始价格: {backtest_start_price:.2f}, 结束价格: {backtest_end_price:.2f}, 涨跌幅: {backtest_price_change_pct:.2f}%")
    
    # 遍历日线数据
    for i in range(len(df_daily)):
        row = df_daily.iloc[i]
        date = row['date']
        close_price = row['close']
        
        # 计算当前资产净值
        current_equity = cash + position * close_price
        equity_values.append({
            'date': date,
            'equity': current_equity
        })
        
        # 获取日线信号
        daily_signal = row['综合判断'] if '综合判断' in row else None
        
        # 检查是否需要卖出
        sell_flag = False
        signal_source = "无信号"
        
        # 卖出逻辑：任意周期出现卖出信号
        if daily_signal in SELL_SIGNALS:
            sell_flag = True
            signal_source = "日线"
        else:
            # 对于周线，查找最近的周结束日期
            target_week_end = find_previous_week_end(date, df_weekly)
            if target_week_end:
                weekly_signal = binary_search_signal(df_weekly, target_week_end)
                if weekly_signal in SELL_SIGNALS:
                    sell_flag = True
                    signal_source = f"周线({target_week_end.strftime('%Y-%m-%d')})"
            else:
                weekly_signal = None
            
            # 对于月线，查找最近的月结束日期
            target_month_end = find_previous_month_end(date, df_monthly)
            if target_month_end:
                monthly_signal = binary_search_signal(df_monthly, target_month_end)
                if monthly_signal in SELL_SIGNALS:
                    sell_flag = True
                    signal_source = f"月线({target_month_end.strftime('%Y-%m-%d')})"
            else:
                monthly_signal = None
        
        # 卖出操作
        if sell_flag and position > 0 and i < len(df_daily) - 1:
            # 使用下一天的开盘价卖出
            next_open = df_daily.iloc[i+1]['open']
            # 如果价格小于等于0，跳过操作
            if next_open <= 0:
                continue
                
            sell_price = next_open
            # 计算盈亏（买入价和卖出价的差值）
            profit = (sell_price - buy_price) * position if buy_price > 0 else 0
            win = profit > 0
            win_count += 1 if win else 0
            trade_count += 1
            
            # 计算持股天数
            if buy_date:
                hold_days = (date - buy_date).days
                total_hold_days += hold_days
                hold_periods.append(hold_days)
            else:
                hold_days = 0
            
            # 计算价格变化百分比
            price_change_pct = ((sell_price - buy_price) / buy_price * 100) if buy_price > 0 else 0
            
            trades.append({
                'date': date,
                'action': '卖出',
                'price': sell_price,
                'position': 0,
                'equity': current_equity,
                '信号来源': signal_source,
                '买入价': buy_price,
                '卖出价': sell_price,
                '价格变化(%)': price_change_pct,
                '盈利': profit,
                '是否盈利': '是' if win else '否',
                '持股天数': hold_days
            })
            
            cash = position * sell_price
            position = 0
            buy_price = 0
            buy_date = None
            continue  # 卖出后不检查买入信号
        
        # 买入条件：日线买入信号
        if (not position) and daily_signal in BUY_SIGNALS and cash > 0 and i < len(df_daily) - 1:
            # 查找最近的周结束日期和目标月结束日期
            target_week_end = find_previous_week_end(date, df_weekly)
            target_month_end = find_previous_month_end(date, df_monthly)
            
            # 获取周线和月线信号
            weekly_signal = binary_search_signal(df_weekly, target_week_end) if target_week_end else None
            monthly_signal = binary_search_signal(df_monthly, target_month_end) if target_month_end else None
            
            # 周线买入信号验证 + 月线非卖出信号
            if (weekly_signal in BUY_SIGNALS) and (monthly_signal not in SELL_SIGNALS):
                # 使用下一天的开盘价买入
                next_open = df_daily.iloc[i+1]['open']
                # 如果价格小于等于0，跳过操作
                if next_open <= 0:
                    continue
                    
                buy_price = next_open
                position = cash / buy_price
                cash = 0
                buy_date = date  # 记录买入日期
                
                trades.append({
                    'date': date,
                    'action': '买入',
                    'price': buy_price,
                    'position': position,
                    'equity': current_equity,
                    '信号来源': f"日线 + 周线({target_week_end.strftime('%Y-%m-%d')}) + 月线({target_month_end.strftime('%Y-%m-%d')})",
                    '买入价': buy_price,
                    '持股天数': 0  # 买入时持股天数为0
                })
    
    # 处理最后未卖出的持仓
    if position > 0:
        # 计算最后一天的资产净值
        final_close_price = df_daily.iloc[-1]['close']
        final_equity = cash + position * final_close_price
        
        # 计算未卖出持仓的持股天数
        if buy_date:
            hold_days = (df_daily.iloc[-1]['date'] - buy_date).days
            total_hold_days += hold_days
            hold_periods.append(hold_days)
        else:
            hold_days = 0
        
        # 计算盈亏（买入价和当前价格的差值）
        profit = (final_close_price - buy_price) * position if buy_price > 0 else 0
        win = profit > 0
        win_count += 1 if win else 0
        trade_count += 1
        
        # 计算价格变化百分比
        price_change_pct = ((final_close_price - buy_price) / buy_price * 100) if buy_price > 0 else 0
        
        trades.append({
            'date': df_daily.iloc[-1]['date'],
            'action': '未卖出',
            'price': final_close_price,
            'position': position,
            'equity': final_equity,
            '信号来源': "持仓结束",
            '买入价': buy_price,
            '当前价': final_close_price,
            '价格变化(%)': price_change_pct,
            '盈利': profit,
            '是否盈利': '是' if win else '否',
            '持股天数': hold_days
        })
    else:
        final_equity = cash
    
    # 打印调试信息
    print(f"  交易次数: {trade_count}, 盈利次数: {win_count}, 最终资产: {final_equity:.2f}, 总持股天数: {total_hold_days}")
    
    # 计算统计数据
    equity_df = pd.DataFrame(equity_values)
    if not equity_df.empty:
        max_equity = equity_df['equity'].max()
        min_equity = equity_df['equity'].min()
    else:
        max_equity = INITIAL_CAPITAL
        min_equity = INITIAL_CAPITAL
        
    win_rate = win_count / trade_count if trade_count > 0 else 0
    
    # 计算涨幅（百分比形式：盈利50%显示为0.5）
    return_ratio = (final_equity / INITIAL_CAPITAL) - 1 if INITIAL_CAPITAL > 0 else 0
    annualized_return = calculate_annualized_return(start_date, end_date, final_equity, INITIAL_CAPITAL)
    
    # 计算平均持股天数
    avg_hold_days = np.mean(hold_periods) if hold_periods else 0
    
    # 计算日均收益率
    daily_return_percent = (return_ratio / avg_hold_days * 100) if avg_hold_days > 0 else 0
    
    # 汇总统计
    stats = {
        '初始资金': INITIAL_CAPITAL,
        '最终资产净值': final_equity,
        '资产净值最大值': max_equity,
        '资产净值最小值': min_equity,
        '胜率': win_rate,
        '涨幅': return_ratio,
        '年化涨幅': annualized_return,
        '交易次数': trade_count,
        '盈利交易次数': win_count,
        '开始日期': start_date,
        '结束日期': end_date,
        '总持股天数': total_hold_days,
        '平均持股天数': avg_hold_days,
        '日均收益率(%)': daily_return_percent,
        '回测开始价格': backtest_start_price,
        '回测结束价格': backtest_end_price,
        '回测期间涨跌幅(%)': backtest_price_change_pct
    }
    
    return trades, stats, equity_df

def backtest_single_file(file_path, output_dir):
    """回测单个文件"""
    file_name = os.path.basename(file_path)
    stock_code = os.path.splitext(file_name)[0]
    output_path = os.path.join(output_dir, f"{stock_code}_多周期策略_回测结果.xlsx")
    
    print(f"回测多周期策略: {file_name}")
    
    # 创建Excel写入器
    writer = pd.ExcelWriter(output_path, engine='xlsxwriter')
    
    # 获取工作簿对象用于格式化
    workbook = writer.book
    percent_format = workbook.add_format({'num_format': '0.00%'})
    number_format = workbook.add_format({'num_format': '0.00'})
            integer_format = workbook.add_format({'num_format': '0'})  # 整数格式
            day_format = workbook.add_format({'num_format': '0.0'})  # 天数格式（1位小数）
    
    # 存储所有统计数据
    all_stats = []
    
    try:
        # 读取三个周期的数据
        df_daily = pd.read_excel(file_path, sheet_name='日线数据')
        df_weekly = pd.read_excel(file_path, sheet_name='周线数据')
        df_monthly = pd.read_excel(file_path, sheet_name='月线数据')
        
        # 规范化列名
        for df in [df_daily, df_weekly, df_monthly]:
            df.columns = [safe_str(col).strip().replace(' ', '') for col in df.columns]
        
        # 检查必要列是否存在
        for df, tf in zip([df_daily, df_weekly, df_monthly], TIME_FRAMES):
            if 'date' not in df.columns:
                print(f"  错误: {tf}数据中缺少'date'列")
                return []
            if '综合判断' not in df.columns:
                print(f"  警告: {tf}数据中缺少'综合判断'列，将使用默认值")
                df['综合判断'] = None
        
        # 添加日期格式转换，确保日期列格式正确
        for df in [df_daily, df_weekly, df_monthly]:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
        
        # 执行逐层筛选策略回测
        trades, stats, equity_df = backtest_strategy(df_daily, df_weekly, df_monthly)
        
        if trades is None or stats is None:
            print(f"  {file_name} 回测失败")
            return []
        
        # 保存交易记录
        trades_df = pd.DataFrame(trades)
        if not trades_df.empty:
            trades_df.to_excel(writer, sheet_name="交易记录", index=False)
            
            # 应用格式到交易记录
            worksheet = writer.sheets["交易记录"]
            for col_num, col_name in enumerate(trades_df.columns):
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
            
            # 分析交易记录中的价格信息
            sell_trades = trades_df[trades_df['action'] == '卖出']
            if not sell_trades.empty and '买入价' in sell_trades.columns and '卖出价' in sell_trades.columns:
                avg_price_change = sell_trades['价格变化(%)'].mean()
                max_price_change = sell_trades['价格变化(%)'].max()
                min_price_change = sell_trades['价格变化(%)'].min()
                print(f"  价格分析: 平均涨跌幅={avg_price_change:.2f}%, 最大涨幅={max_price_change:.2f}%, 最大跌幅={min_price_change:.2f}%")
        else:
            print("  没有交易记录")
        
        # 保存资产净值曲线
        if not equity_df.empty:
            equity_df.to_excel(writer, sheet_name="资产净值", index=False)
            
            # 应用数字格式到资产净值
            worksheet = writer.sheets["资产净值"]
            for col_num, col_name in enumerate(equity_df.columns):
                if 'equity' in col_name or 'cash' in col_name or 'position' in col_name:
                    worksheet.set_column(col_num + 1, col_num + 1, 15, number_format)
        else:
            print("  没有资产净值数据")
        
        # 添加股票代码信息
        stats['股票代码'] = stock_code
        all_stats.append(stats)
        
        print(f"  多周期策略回测完成: 最终资产={stats['最终资产净值']:.2f}, 涨幅={stats['涨幅']:.2%}, 交易次数={stats['交易次数']}, 平均持股天数={stats['平均持股天数']:.1f}天")
            
    except Exception as e:
        print(f"  处理{file_name}时出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return []
    
    # 保存Excel文件
    writer.close()
    print(f"回测结果已保存: {output_path}")
    return all_stats

def generate_summary_report(all_stats, output_dir):
    """生成总结报告"""
    if not all_stats:
        print("没有统计数据可生成总结报告")
        return
    
    # 合并所有统计数据
    summary_df = pd.DataFrame(all_stats)
    
    # 保存总结报告
    output_path = os.path.join(output_dir, "多周期策略_回测总结报告.xlsx")
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        # 获取工作簿对象用于格式化
        workbook = writer.book
        percent_format = workbook.add_format({'num_format': '0.00%'})
        number_format = workbook.add_format({'num_format': '0.00'})
            integer_format = workbook.add_format({'num_format': '0'})  # 整数格式
            day_format = workbook.add_format({'num_format': '0.0'})  # 天数格式（1位小数）
        
        # 详细数据
        summary_df.to_excel(writer, sheet_name="详细数据", index=False)
        
        # 应用格式到详细数据
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
        total_trades = summary_df['交易次数'].sum()
        win_trades = summary_df['盈利交易次数'].sum()
        overall_win_rate = win_trades / total_trades if total_trades > 0 else 0
        avg_return = summary_df['涨幅'].mean()
        avg_annual_return = summary_df['年化涨幅'].mean()
        
        # 计算持股天数相关统计
        total_hold_days = summary_df['总持股天数'].sum()
        avg_hold_days = summary_df['平均持股天数'].mean()
        avg_daily_return = summary_df['日均收益率(%)'].mean()
        
        # 计算平均涨幅除以平均持股天数
        avg_return_per_hold_day = (avg_return / avg_hold_days) if avg_hold_days > 0 else 0
        
        # 计算价格相关统计
        profitable_stocks = summary_df[summary_df['最终资产净值'] > INITIAL_CAPITAL]
        avg_profit_pct = profitable_stocks['涨幅'].mean() if len(profitable_stocks) > 0 else 0
        avg_loss_pct = summary_df[summary_df['最终资产净值'] <= INITIAL_CAPITAL]['涨幅'].mean() if len(summary_df[summary_df['最终资产净值'] <= INITIAL_CAPITAL]) > 0 else 0
        
        # 计算价格变化统计
        if '回测期间涨跌幅(%)' in summary_df.columns:
            avg_price_change = summary_df['回测期间涨跌幅(%)'].mean()
            max_price_change = summary_df['回测期间涨跌幅(%)'].max()
            min_price_change = summary_df['回测期间涨跌幅(%)'].min()
            positive_price_changes = len(summary_df[summary_df['回测期间涨跌幅(%)'] > 0])
            negative_price_changes = len(summary_df[summary_df['回测期间涨跌幅(%)'] <= 0])
            
            # 计算盈利与期间涨跌幅的差值统计
            summary_df['盈利与涨跌幅差值(%)'] = summary_df['涨幅'] - summary_df['回测期间涨跌幅(%)']
            avg_profit_price_diff = summary_df['盈利与涨跌幅差值(%)'].mean()
            max_profit_price_diff = summary_df['盈利与涨跌幅差值(%)'].max()
            min_profit_price_diff = summary_df['盈利与涨跌幅差值(%)'].min()
            positive_profit_price_diff = len(summary_df[summary_df['盈利与涨跌幅差值(%)'] > 0])
            negative_profit_price_diff = len(summary_df[summary_df['盈利与涨跌幅差值(%)'] <= 0])
        else:
            avg_price_change = max_price_change = min_price_change = 0
            positive_price_changes = negative_price_changes = 0
            avg_profit_price_diff = max_profit_price_diff = min_profit_price_diff = 0
            positive_profit_price_diff = negative_profit_price_diff = 0
        
        overall_stats = pd.DataFrame({
            '总股票数': [len(summary_df)],
            '盈利股票数': [len(profitable_stocks)],
            '亏损股票数': [len(summary_df) - len(profitable_stocks)],
            '总交易次数': [total_trades],
            '盈利交易次数': [win_trades],
            '总体胜率': [overall_win_rate],
            '平均涨幅': [avg_return],
            '平均年化涨幅': [avg_annual_return],
            '盈利股票平均涨幅': [avg_profit_pct],
            '亏损股票平均亏损比': [avg_loss_pct],
            '总持股天数': [total_hold_days],
            '平均持股天数': [avg_hold_days],
            '平均日均收益率(%)': [avg_daily_return],
            '平均涨幅/持股天数': [avg_return_per_hold_day],
            '平均价格涨跌幅(%)': [avg_price_change],
            '最大价格涨幅(%)': [max_price_change],
            '最大价格跌幅(%)': [min_price_change],
            '价格上涨股票数': [positive_price_changes],
            '价格下跌股票数': [negative_price_changes],
            '平均盈利与涨跌幅差值(%)': [avg_profit_price_diff],
            '最大盈利与涨跌幅差值(%)': [max_profit_price_diff],
            '最小盈利与涨跌幅差值(%)': [min_profit_price_diff],
            '盈利超过涨跌幅股票数': [positive_profit_price_diff],
            '盈利低于涨跌幅股票数': [negative_profit_price_diff]
        })
        
        overall_stats.to_excel(writer, sheet_name="整体统计", index=False)
        
        # 应用格式到整体统计
        worksheet = writer.sheets["整体统计"]
        for col_num, col_name in enumerate(overall_stats.columns):
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
        
        # 个股表现排名
        if len(summary_df) > 0:
            # 先按涨幅降序排序，再按年化涨幅降序排序
            top_stocks = summary_df.sort_values(
                by=['涨幅', '年化涨幅'], 
                ascending=False
            ).head(min(10, len(summary_df)))
            
            top_stocks.to_excel(writer, sheet_name="最佳表现股票", index=False)
            
            # 应用格式到最佳表现股票
            worksheet = writer.sheets["最佳表现股票"]
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
            
            # 价格汇总表
            if '回测开始价格' in summary_df.columns and '回测结束价格' in summary_df.columns:
                price_summary = summary_df[['股票代码', '回测开始价格', '回测结束价格', '回测期间涨跌幅(%)', '涨幅', '年化涨幅', '盈利与涨跌幅差值(%)']].copy()
                price_summary = price_summary.sort_values('回测期间涨跌幅(%)', ascending=False)
                price_summary.to_excel(writer, sheet_name="价格汇总", index=False)
                
                # 应用格式到价格汇总
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
            
            # 最差表现股票
            worst_stocks = summary_df.sort_values(
                by=['涨幅', '年化涨幅'], 
                ascending=True
            ).head(min(10, len(summary_df)))
            
            worst_stocks.to_excel(writer, sheet_name="最差表现股票", index=False)
            
            # 应用格式到最差表现股票
            worksheet = writer.sheets["最差表现股票"]
            for col_num, col_name in enumerate(worst_stocks.columns):
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

def main():
    # 输入和输出目录
    input_dir = "analyzed_results"
    output_dir = "backtest_results_multi_period"
    
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
    
    print(f"找到 {len(excel_files)} 个Excel文件，开始多周期策略回测...")
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
    print(f"多周期策略回测完成! 处理了 {len(excel_files)} 个文件")
    print(f"所有回测结果已保存到 '{output_dir}' 目录中")
    print("=" * 60)

if __name__ == "__main__":
    main()