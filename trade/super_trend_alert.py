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

# 强制设置UTF-8编码环境
sys.stdout.reconfigure(encoding='utf-8') if hasattr(sys.stdout, 'reconfigure') else None
sys.stderr.reconfigure(encoding='utf-8') if hasattr(sys.stderr, 'reconfigure') else None

if sys.platform.startswith('win'):
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    os.environ['LANG'] = 'zh_CN.UTF-8'
    os.environ['LC_ALL'] = 'zh_CN.UTF-8'

# 支持的不同周期类型
TIME_FRAMES = ['小时线', '日线', '周线', '月线']

# 日志记录函数
def log_error(file_name, sheet_name, reason, exception=None):
    """记录错误信息到日志"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] 文件: {file_name} | 工作表: {sheet_name} | 原因: {reason}"
    if exception:
        log_entry += f" | 错误: {str(exception)}"
    
    print(log_entry)
    with open("super_trend_alert_errors.log", "a", encoding="utf-8") as log_file:
        log_file.write(log_entry + "\n")

# 安全字符串转换函数
def safe_str(value):
    """安全转换值为字符串"""
    if value is None:
        return ""
    if isinstance(value, (pd.Timestamp)):
        # 只保留日期
        return value.strftime('%Y-%m-%d')
    if isinstance(value, (np.int64, np.int32, np.float64)):
        return str(round(value, 3))
    return str(value)

def check_super_trend_signals(df, file_name, time_frame):
    """检查SuperTrend信号，返回最近十行的信号信息"""
    signals = []
    
    if df is None or len(df) < 10:
        return signals
    
    # 获取最后十行数据
    last_ten_rows = df.tail(10)
    
    for idx, (row_idx, row) in enumerate(last_ten_rows.iterrows()):
        if 'SuperTrend信号' not in row:
            continue
            
        signal = row['SuperTrend信号']
        date = row.get('date', '未知日期')
        close_price = row.get('close', 0)
        
        # 只记录买入信号和卖出信号
        if signal in ['买入信号', '卖出信号']:
            # 获取该行的全部数据
            full_row_data = {}
            for col in row.index:
                full_row_data[col] = row[col]
            
            signal_info = {
                'file_name': file_name,
                'time_frame': time_frame,
                'date': date,
                'signal': signal,
                'close_price': close_price,
                'row_position': f"倒数第{10-idx}行",
                'original_row': row_idx,
                'full_row_data': full_row_data  # 添加全部行数据
            }
            signals.append(signal_info)
    
    return signals

def process_excel_file(file_path):
    """处理单个Excel文件，检查SuperTrend信号"""
    file_name = os.path.basename(file_path)
    print(f"检查文件: {file_name}")
    
    all_signals = []
    
    # 处理所有周期数据（小时线、日线、周线、月线）
    for time_frame in TIME_FRAMES:
        sheet_name = f"{time_frame}数据"
        
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            df.columns = [safe_str(col).strip().replace(' ', '') for col in df.columns]
            
            if df.empty:
                log_error(file_name, sheet_name, "工作表为空")
                continue
                
            signals = check_super_trend_signals(df, file_name, time_frame)
            all_signals.extend(signals)
            
            if signals:
                print(f"  {time_frame}: 发现 {len(signals)} 个信号")
            else:
                print(f"  {time_frame}: 无信号")
                
        except Exception as e:
            log_error(file_name, sheet_name, "读取工作表失败", e)
    
    return all_signals

def create_alert_report(all_signals):
    """创建预警报告Excel文件"""
    if not all_signals:
        print("没有发现任何SuperTrend信号")
        return
    
    # 创建输出目录
    output_dir = "super_trend_alerts"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 生成文件名
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(output_dir, f"SuperTrend预警报告_{timestamp}.xlsx")
    
    try:
        workbook = xlsxwriter.Workbook(output_file, {
            'strings_to_urls': False,
            'strings_to_formulas': False,
            'constant_memory': True
        })
        
        # 分离买入信号和卖出信号
        buy_signals = [s for s in all_signals if s['signal'] == '买入信号']
        sell_signals = [s for s in all_signals if s['signal'] == '卖出信号']
        
        # 创建买入信号工作表
        if buy_signals:
            worksheet = workbook.add_worksheet('买入信号')
            
            # 获取所有可能的列名（从第一个信号的全行数据中获取）
            all_columns = set()
            for signal in buy_signals:
                all_columns.update(signal['full_row_data'].keys())
            
            # 定义基础列和动态列
            base_headers = ['文件名', '时间周期', '信号类型', '数据位置', '原始行号']
            dynamic_headers = sorted(list(all_columns))
            headers = base_headers + dynamic_headers
            
            # 写入表头
            for col_idx, header in enumerate(headers):
                worksheet.write(0, col_idx, header)
            
            # 写入数据
            for row_idx, signal in enumerate(buy_signals, start=1):
                # 写入基础信息
                worksheet.write(row_idx, 0, signal['file_name'])
                worksheet.write(row_idx, 1, signal['time_frame'])
                worksheet.write(row_idx, 2, signal['signal'])
                worksheet.write(row_idx, 3, signal['row_position'])
                worksheet.write(row_idx, 4, signal['original_row'])
                
                # 写入全行数据
                for col_idx, col_name in enumerate(dynamic_headers, start=len(base_headers)):
                    value = signal['full_row_data'].get(col_name, '')
                    worksheet.write(row_idx, col_idx, safe_str(value))
        
        # 创建卖出信号工作表
        if sell_signals:
            worksheet = workbook.add_worksheet('卖出信号')
            
            # 获取所有可能的列名（从第一个信号的全行数据中获取）
            all_columns = set()
            for signal in sell_signals:
                all_columns.update(signal['full_row_data'].keys())
            
            # 定义基础列和动态列
            base_headers = ['文件名', '时间周期', '信号类型', '数据位置', '原始行号']
            dynamic_headers = sorted(list(all_columns))
            headers = base_headers + dynamic_headers
            
            # 写入表头
            for col_idx, header in enumerate(headers):
                worksheet.write(0, col_idx, header)
            
            # 写入数据
            for row_idx, signal in enumerate(sell_signals, start=1):
                # 写入基础信息
                worksheet.write(row_idx, 0, signal['file_name'])
                worksheet.write(row_idx, 1, signal['time_frame'])
                worksheet.write(row_idx, 2, signal['signal'])
                worksheet.write(row_idx, 3, signal['row_position'])
                worksheet.write(row_idx, 4, signal['original_row'])
                
                # 写入全行数据
                for col_idx, col_name in enumerate(dynamic_headers, start=len(base_headers)):
                    value = signal['full_row_data'].get(col_name, '')
                    worksheet.write(row_idx, col_idx, safe_str(value))
        
        # 创建汇总工作表
        worksheet = workbook.add_worksheet('信号汇总')
        worksheet.write(0, 0, "SuperTrend预警信号汇总报告")
        worksheet.write(1, 0, f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        worksheet.write(2, 0, f"总信号数量: {len(all_signals)}")
        worksheet.write(3, 0, f"买入信号数量: {len(buy_signals)}")
        worksheet.write(4, 0, f"卖出信号数量: {len(sell_signals)}")
        
        # 按时间周期统计
        row_idx = 6
        worksheet.write(row_idx, 0, "按时间周期统计:")
        row_idx += 1
        
        for time_frame in TIME_FRAMES:
            frame_signals = [s for s in all_signals if s['time_frame'] == time_frame]
            frame_buy = [s for s in frame_signals if s['signal'] == '买入信号']
            frame_sell = [s for s in frame_signals if s['signal'] == '卖出信号']
            
            worksheet.write(row_idx, 0, f"{time_frame}: 总计{len(frame_signals)}个信号 (买入{len(frame_buy)}个, 卖出{len(frame_sell)}个)")
            row_idx += 1
        
        # 按文件统计
        row_idx += 1
        worksheet.write(row_idx, 0, "按文件统计:")
        row_idx += 1
        
        files = list(set([s['file_name'] for s in all_signals]))
        for file_name in files:
            file_signals = [s for s in all_signals if s['file_name'] == file_name]
            file_buy = [s for s in file_signals if s['signal'] == '买入信号']
            file_sell = [s for s in file_signals if s['signal'] == '卖出信号']
            
            worksheet.write(row_idx, 0, f"{file_name}: 总计{len(file_signals)}个信号 (买入{len(file_buy)}个, 卖出{len(file_sell)}个)")
            row_idx += 1
        
        workbook.close()
        print(f"预警报告已生成: {output_file}")
        return output_file
        
    except Exception as e:
        log_error("", "", "创建预警报告失败", e)
        return None

def main():
    """主函数：检查所有Excel文件的SuperTrend信号"""
    data_dir = "analyzed_results"
    
    if not os.path.exists(data_dir):
        print(f"错误: 数据目录 '{data_dir}' 不存在")
        print("请先运行趋势分析脚本生成分析结果")
        return
    
    excel_files = glob.glob(os.path.join(data_dir, "*.xlsx"))
    
    if not excel_files:
        print("警告: 未找到任何Excel文件")
        print(f"请确保趋势分析脚本已在 '{data_dir}' 目录中生成了Excel文件")
        return
    
    print(f"找到 {len(excel_files)} 个Excel文件，开始检查SuperTrend信号...")
    print("=" * 60)
    print("检查规则:")
    print("- 检查每个文件的小时线、日线、周线、月线四个周期")
    print("- 只记录最后十行数据中的买入信号和卖出信号")
    print("- 生成统一的预警报告文件，包含信号行的全部数据")
    print("=" * 60)
    
    open("super_trend_alert_errors.log", "w", encoding="utf-8").close()
    
    all_signals = []
    total_files = len(excel_files)
    
    for idx, file_path in enumerate(excel_files):
        print(f"\n[{idx+1}/{total_files}] ", end="")
        signals = process_excel_file(file_path)
        all_signals.extend(signals)
        
        if idx < total_files - 1:
            print("-" * 60)
    
    print("\n" + "=" * 60)
    print(f"检查完成! 共发现 {len(all_signals)} 个SuperTrend信号")
    
    if all_signals:
        output_file = create_alert_report(all_signals)
        if output_file:
            print(f"预警报告已保存: {output_file}")
        else:
            print("预警报告生成失败")
    else:
        print("未发现任何SuperTrend信号")
    
    print("\n详细错误信息请查看: super_trend_alert_errors.log")
    print("=" * 60)

if __name__ == "__main__":
    main()
