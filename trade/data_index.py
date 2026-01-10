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
import shutil

# 强制设置UTF-8编码环境
sys.stdout.reconfigure(encoding='utf-8') if hasattr(sys.stdout, 'reconfigure') else None
sys.stderr.reconfigure(encoding='utf-8') if hasattr(sys.stderr, 'reconfigure') else None

if sys.platform.startswith('win'):
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    os.environ['LANG'] = 'zh_CN.UTF-8'
    os.environ['LC_ALL'] = 'zh_CN.UTF-8'

# 技术信号列定义
TECH_SIGNAL_COLUMNS = [
    'MACD信号', 'KDJ状态', 'RSI状态', 'BOLL位置', 'MA信号', 
    '量能趋势', '趋势方向', '综合判断'
]

# 支持的不同周期类型
TIME_FRAMES = ['日线', '周线', '月线']

# ===== 新增：数值保留三位小数的函数 =====
def round_value(value, decimals=3):
    """保留指定小数位数，仅处理数值类型"""
    if isinstance(value, (int, float, np.number)):
        return round(value, decimals)
    return value

# 日志记录函数
def log_error(file_name, sheet_name, reason, exception=None):
    """记录错误信息到日志"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] 文件: {file_name} | 工作表: {sheet_name} | 原因: {reason}"
    if exception:
        log_entry += f" | 错误: {str(exception)}"
    
    # 打印到控制台
    print(log_entry)
    
    # 写入日志文件
    with open("index_analysis_errors.log", "a", encoding="utf-8") as log_file:
        log_file.write(log_entry + "\n")

# 安全字符串转换函数
def safe_str(value):
    """安全转换值为字符串，处理编码问题"""
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        try:
            # 尝试使用UTF-8解码
            return value.decode('utf-8', errors='replace')
        except:
            try:
                # 尝试使用GBK解码
                return value.decode('gbk', errors='replace')
            except:
                return "编码错误"
    if isinstance(value, (pd.Timestamp)):
        return value.strftime('%Y-%m-%d')
    if isinstance(value, (np.int64, np.int32, np.float64)):
        return str(round_value(value))  # 数值类型保留三位小数
    return str(value)

# 添加信号的函数 - 修复公式问题并保留小数位
def add_signals_to_dataframe(df):
    """为数据框添加技术分析信号列 - 修复公式问题并保留小数位"""
    if df is None or len(df) < 10:
        log_error("", "", f"数据不足（{len(df) if df is not None else 0}行），无法计算技术信号")
        return df
    
    try:
        # 创建副本避免修改原始数据
        df = df.copy()
        
        # ===== 检查必要列 =====
        required_columns = ['date', 'close', 'volume']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            log_error("", "", f"缺少必要列: {', '.join(missing_columns)}")
            for col in TECH_SIGNAL_COLUMNS:
                df[col] = '数据不足'
            return df
        
        # ===== 添加下个周期涨跌幅 =====
        # 添加下个周期收盘价列
        df['下个周期收盘价'] = df['close'].shift(-1)
        
        # 计算涨跌幅度（保留三位小数）
        df['涨跌幅度'] = (df['下个周期收盘价'] - df['close']).apply(round_value)
        df['涨跌幅百分比'] = (df['涨跌幅度'] / df['close'] * 100).apply(round_value)
        
        # 对于最后一行，没有下个周期数据，设为空
        df.loc[df.index[-1], '下个周期收盘价'] = None
        df.loc[df.index[-1], '涨跌幅度'] = None
        df.loc[df.index[-1], '涨跌幅百分比'] = None
        
        # ===== MACD信号 =====
        df['MACD信号'] = '数据不足'  # 默认值
        if 'DIF' in df.columns and 'DEA' in df.columns:
            # 金叉条件: DIF上穿DEA
            golden_cross = (df['DIF'] > df['DEA']) & (df['DIF'].shift(1) <= df['DEA'].shift(1))
            # 死叉条件: DIF下穿DEA
            death_cross = (df['DIF'] < df['DEA']) & (df['DIF'].shift(1) >= df['DEA'].shift(1))
            
            df.loc[golden_cross, 'MACD信号'] = '金叉(看多)'
            df.loc[death_cross, 'MACD信号'] = '死叉(看空)'
            df.loc[~(golden_cross | death_cross), 'MACD信号'] = '中性'
        
        # ===== KDJ状态 =====
        df['KDJ状态'] = '数据不足'
        if 'J' in df.columns:
            # 使用数值比较（引用值不做处理）
            df.loc[df['J'] > 80, 'KDJ状态'] = '超买(警惕)'
            df.loc[df['J'] < 20, 'KDJ状态'] = '超卖(机会)'
            df.loc[(df['J'] >= 20) & (df['J'] <= 80), 'KDJ状态'] = '中性'
        
        # ===== RSI状态 =====
        df['RSI状态'] = '数据不足'
        rsi_col = next((col for col in df.columns if col.startswith('RSI')), None)
        if rsi_col:
            # 使用数值比较（引用值不做处理）
            df.loc[df[rsi_col] > 70, 'RSI状态'] = '超买(警惕)'
            df.loc[df[rsi_col] < 30, 'RSI状态'] = '超卖(机会)'
            df.loc[(df[rsi_col] >= 30) & (df[rsi_col] <= 70), 'RSI状态'] = '中性'
        
        # ===== 布林带位置 =====
        df['BOLL位置'] = '数据不足'
        if 'BOLL_UPPER' in df.columns and 'BOLL_LOWER' in df.columns and 'BOLL_MIDDLE' in df.columns:
            # 使用数值比较（引用值不做处理）
            df.loc[df['close'] > df['BOLL_UPPER'], 'BOLL位置'] = '上轨上方(超买)'
            df.loc[df['close'] < df['BOLL_LOWER'], 'BOLL位置'] = '下轨下方(超卖)'
            df.loc[(df['close'] >= df['BOLL_LOWER']) & (df['close'] <= df['BOLL_UPPER']), 'BOLL位置'] = '中轨区间'
        
        # ===== 均线信号 =====
        df['MA信号'] = '数据不足'
        if 'MA5' in df.columns and 'MA10' in df.columns:
            # 使用数值比较（引用值不做处理）
            cross_up = (df['MA5'] > df['MA10']) & (df['MA5'].shift(1) <= df['MA10'].shift(1))
            cross_down = (df['MA5'] < df['MA10']) & (df['MA5'].shift(1) >= df['MA10'].shift(1))
            
            df.loc[cross_up, 'MA信号'] = '金叉(看多)'
            df.loc[cross_down, 'MA信号'] = '死叉(看空)'
            df.loc[~(cross_up | cross_down), 'MA信号'] = '中性'
        
        # ===== 量能趋势 =====
        df['量能趋势'] = '数据不足'
        if 'volume' in df.columns:
            # 计算5日均值（计算值保留三位小数）
            df['VOL_MA5'] = df['volume'].rolling(window=5, min_periods=1).mean().apply(round_value)
            
            # 量能趋势判断 - 直接比较
            conditions = [
                (df['volume'] > 1.5 * df['VOL_MA5']),
                (df['volume'] < 0.7 * df['VOL_MA5'])
            ]
            choices = ['放量', '缩量']
            df['量能趋势'] = np.select(conditions, choices, default='正常')
        
        # ===== 趋势方向 =====
        df['趋势方向'] = '数据不足'
        if 'MA20' in df.columns and 'MA60' in df.columns:
            # 直接使用数值比较（引用值不做处理）
            conditions = [
                (df['close'] > df['MA60']),
                (df['close'] > df['MA20']),
                (df['close'] < df['MA20']),
                (df['close'] < df['MA60'])
            ]
            choices = ['长期牛市', '短期强势', '短期弱势', '长期熊市']
            df['趋势方向'] = np.select(conditions, choices, default='震荡行情')
        
        # ===== 综合判断 =====
        df['综合判断'] = '数据不足'  # 默认值
        
        if len(df) > 0:
            # 计算看多信号数量
            bullish_signals = (
                (df['MACD信号'] == '金叉(看多)').astype(int) +
                (df['KDJ状态'] == '超卖(机会)').astype(int) +
                (df['RSI状态'] == '超卖(机会)').astype(int) +
                (df['BOLL位置'] == '下轨下方(超卖)').astype(int) +
                (df['MA信号'] == '金叉(看多)').astype(int) +
                (df['趋势方向'].str.contains('牛市|强势')).astype(int)
            )
            
            # 计算看空信号数量
            bearish_signals = (
                (df['MACD信号'] == '死叉(看空)').astype(int) +
                (df['KDJ状态'] == '超买(警惕)').astype(int) +
                (df['RSI状态'] == '超买(警惕)').astype(int) +
                (df['BOLL位置'] == '上轨上方(超买)').astype(int) +
                (df['MA信号'] == '死叉(看空)').astype(int) +
                (df['趋势方向'].str.contains('熊市|弱势')).astype(int)
            )
            
            # 根据多空信号比例综合判断
            signal_strength = bullish_signals - bearish_signals
            
            df.loc[signal_strength > 3, '综合判断'] = '强烈看多'
            df.loc[(signal_strength > 1) & (signal_strength <= 3), '综合判断'] = '看多'
            df.loc[(signal_strength >= -1) & (signal_strength <= 1), '综合判断'] = '中性'
            df.loc[(signal_strength < -1) & (signal_strength >= -3), '综合判断'] = '看空'
            df.loc[signal_strength < -3, '综合判断'] = '强烈看空'
            
            # 当出现危险信号时发出警报
            danger_signals = (
                (df['KDJ状态'] == '超买(警惕)') | 
                (df['RSI状态'] == '超买(警惕)') | 
                (df['BOLL位置'] == '上轨上方(超买)')
            )
            
            df.loc[danger_signals & (df['综合判断'] == '看多'), '综合判断'] = '看多但有风险'
            df.loc[danger_signals & (df['综合判断'] == '中性'), '综合判断'] = '谨慎观望'
        
        # 确保所有字符串列使用UTF-8编码
        for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].apply(safe_str)
        
        return df
    
    except Exception as e:
        log_error("", "", "添加信号时出错", e)
        return df

# 处理单个Excel文件 - 修复公式问题并保留小数位
def process_excel_file(file_path, output_dir):
    """处理单个Excel文件，在输出目录创建分析结果 - 修复公式问题并保留小数位"""
    file_name = os.path.basename(file_path)
    output_path = os.path.join(output_dir, file_name)
    start_time = time.time()
    print(f"处理: {file_name}")
    
    try:
        # 创建新的Excel文件 - 禁用公式和URL转换
        workbook = xlsxwriter.Workbook(output_path, {
            'strings_to_urls': False,
            'strings_to_formulas': False,
            'constant_memory': True
        })
        
        # 准备存储所有周期的信号数据
        all_signals = []
        
        # 处理所有周期数据（日线、周线、月线）
        for time_frame in TIME_FRAMES:
            sheet_name = f"{time_frame}数据"
            
            try:
                # 尝试读取特定工作表
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                # 规范化列名
                df.columns = [safe_str(col).strip().replace(' ', '') for col in df.columns]
                
                if df.empty:
                    log_error(file_name, sheet_name, "工作表为空")
                    continue
                    
                # 添加信号列
                df_with_signals = add_signals_to_dataframe(df)
                
                # 创建原始工作表
                worksheet = workbook.add_worksheet(sheet_name)
                
                # 写入列名
                for col_idx, col_name in enumerate(df_with_signals.columns):
                    worksheet.write(0, col_idx, safe_str(col_name))
                
                # 添加数据
                for row_idx, (_, row) in enumerate(df_with_signals.iterrows(), start=1):
                    for col_idx, value in enumerate(row):
                        # 安全处理所有值
                        safe_value = safe_str(value)
                        
                        # 写入值
                        if pd.isna(value) or value is None:
                            worksheet.write(row_idx, col_idx, "")
                        elif isinstance(value, (np.int64, np.int32, np.float64)):
                            # 数值类型保留三位小数
                            worksheet.write_number(row_idx, col_idx, round_value(value))
                        elif isinstance(value, (pd.Timestamp)):
                            worksheet.write(row_idx, col_idx, safe_value)
                        else:
                            # 防止Excel将字符串解释为公式
                            if safe_value.startswith('='):
                                safe_value = "'" + safe_value
                            worksheet.write(row_idx, col_idx, safe_value)
                
                # 生成数据总结
                stats_summary = []
                if not df_with_signals.empty:
                    # 计算关键统计数据
                    start_price = df_with_signals.iloc[0]['close']
                    end_price = df_with_signals.iloc[-1]['close']
                    price_change = round_value((end_price - start_price) / start_price * 100, 3)  # 保留三位小数
                    
                    high_point = df_with_signals['close'].max()
                    low_point = df_with_signals['close'].min()
                    
                    # 趋势天数统计
                    bullish_days = ((df_with_signals['综合判断'] == '看多') | (df_with_signals['综合判断'] == '强烈看多')).sum()
                    bearish_days = ((df_with_signals['综合判断'] == '看空') | (df_with_signals['综合判断'] == '强烈看空')).sum()
                    total_days = bullish_days + bearish_days
                    
                    bullish_percent = round_value((bullish_days / total_days * 100), 3) if total_days > 0 else 0
                    bearish_percent = round_value((bearish_days / total_days * 100), 3) if total_days > 0 else 0
                    
                    # 构建总结文本
                    stats_summary.append(f"[{time_frame}趋势总结]")
                    stats_summary.append(f"价格变动: {price_change:+.2f}% ({start_price:.2f} → {end_price:.2f})")
                    stats_summary.append(f"最高价: {round_value(high_point, 3)}")
                    stats_summary.append(f"最低价: {round_value(low_point, 3)}")
                    stats_summary.append(f"趋势方向: 看多 {bullish_percent:.1f}% | 看空 {bearish_percent:.1f}%")
                    
                    # 当前趋势状态
                    current_trend = df_with_signals.iloc[-1]['综合判断'] if '综合判断' in df_with_signals.columns else '未知'
                    stats_summary.append(f"当前趋势状态: {current_trend}")
                
                # 提取信号列和下个周期涨跌幅列
                signal_columns = ['date'] + [col for col in TECH_SIGNAL_COLUMNS if col in df_with_signals.columns]
                # 添加下个周期涨跌幅列
                signal_columns += ['涨跌幅度', '涨跌幅百分比']
                
                # 根据周期类型确定保留的记录数
                record_count = 30 if time_frame == '日线' else 12
                df_summary = df_with_signals[signal_columns].tail(record_count)
                
                # 存储信号数据
                all_signals.append({
                    'time_frame': time_frame,
                    'stats': stats_summary,
                    'df_signals': df_summary
                })
                
                print(f"成功处理{time_frame}数据，添加技术信号列和下个周期涨跌幅")
                
            except Exception as e:
                log_error(file_name, sheet_name, "读取工作表失败", e)
        
        # 创建信号总结工作表
        if all_signals:
            worksheet = workbook.add_worksheet('信号总结')
            
            # 写入文件头
            worksheet.write(0, 0, "指数技术信号综合分析报告")
            worksheet.write(1, 0, "生成时间: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            row_index = 2
            
            # 写入所有周期数据
            for signals in all_signals:
                time_frame = signals['time_frame']
                stats = signals['stats']
                df_signals = signals['df_signals']
                
                # 添加分隔行和时间周期标题
                worksheet.write(row_index, 0, f"=== {time_frame}趋势分析 ===")
                row_index += 1
                
                # 写入统计总结
                if stats:
                    for line in stats:
                        safe_line = safe_str(line)
                        # 防止Excel将字符串解释为公式
                        if safe_line.startswith('='):
                            safe_line = "'" + safe_line
                        worksheet.write(row_index, 0, safe_line)
                        row_index += 1
                
                row_index += 1
                
                # 写入信号表头
                header = ["日期"] + [col for col in TECH_SIGNAL_COLUMNS if col in df_signals.columns] + ["涨跌幅度", "涨跌幅百分比"]
                for col_idx, col_name in enumerate(header):
                    safe_col_name = safe_str(col_name)
                    # 防止Excel将字符串解释为公式
                    if safe_col_name.startswith('='):
                        safe_col_name = "'" + safe_col_name
                    worksheet.write(row_index, col_idx, safe_col_name)
                row_index += 1
                
                # 写入信号数据
                for _, row in df_signals.iterrows():
                    row_data = [row['date']] 
                    row_data += [row[col] for col in TECH_SIGNAL_COLUMNS if col in df_signals.columns]
                    row_data += [row['涨跌幅度'], row['涨跌幅百分比']]
                    
                    for col_idx, value in enumerate(row_data):
                        safe_value = safe_str(value)
                        # 防止Excel将字符串解释为公式
                        if safe_value.startswith('='):
                            safe_value = "'" + safe_value
                            
                        if pd.isna(value) or value is None:
                            worksheet.write(row_index, col_idx, "")
                        elif isinstance(value, (np.int64, np.int32, np.float64)):
                            # 数值类型保留三位小数
                            if isinstance(value, (np.float64)) and col_idx >= len(header) - 2:
                                worksheet.write_number(row_index, col_idx, round_value(value))
                            else:
                                worksheet.write(row_index, col_idx, safe_value)
                        elif isinstance(value, (pd.Timestamp)):
                            worksheet.write(row_index, col_idx, safe_value)
                        else:
                            worksheet.write(row_index, col_idx, safe_value)
                    row_index += 1
                
                # 添加空行分隔不同周期
                row_index += 2
        
        # 关闭工作簿
        workbook.close()
        
        process_time = time.time() - start_time
        print(f"完成: {file_name} → 已保存到输出目录 (用时: {process_time:.1f}秒)")
        return True
    
    except Exception as e:
        log_error(file_name, "", "处理文件失败", e)
        return False

# 主函数
def main():
    # 数据目录
    data_dir = "index_data"
    output_dir = "analyzed_results"
    
    # 检查目录是否存在
    if not os.path.exists(data_dir):
        print(f"错误: 数据目录 '{data_dir}' 不存在")
        print("请先运行指数数据准备脚本")
        return
    
    # 创建输出目录
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"创建输出目录: {output_dir}")
    
    # 获取所有Excel文件
    excel_files = glob.glob(os.path.join(data_dir, "*.xlsx"))
    
    if not excel_files:
        print("警告: 未找到任何Excel文件")
        print(f"请确保指数数据准备脚本已在 '{data_dir}' 目录中生成了Excel文件")
        return
    
    print(f"找到 {len(excel_files)} 个Excel文件，开始添加技术信号和下个周期涨跌幅...")
    print(f"分析后文件将保存到: {output_dir}")
    print("=" * 60)
    print(f"将分析以下周期: {', '.join(TIME_FRAMES)}")
    
    total_files = len(excel_files)
    success_count = 0
    skipped_files = []
    
    # 初始化错误日志
    open("index_analysis_errors.log", "w", encoding="utf-8").close()
    
    # 处理每个文件
    for idx, file_path in enumerate(excel_files):
        print(f"\n[{idx+1}/{total_files}] ", end="")
        result = process_excel_file(file_path, output_dir)
        if result:
            success_count += 1
        else:
            skipped_files.append(os.path.basename(file_path))
        
        if idx < total_files - 1:
            print("-" * 60)
    
    print("\n" + "=" * 60)
    print(f"处理完成! 成功处理 {success_count}/{total_files} 个文件")
    print(f"所有分析后文件已保存到 '{output_dir}' 目录中")
    
    if skipped_files:
        print("\n跳过文件报告:")
        print(f"共跳过 {len(skipped_files)} 个文件:")
        for file in skipped_files:
            print(f"- {file}")
            
        # 写入跳过文件日志
        with open("skipped_index_files.log", "w", encoding="utf-8") as log:
            log.write("\n".join(skipped_files))
    
    print("\n详细错误信息请查看: index_analysis_errors.log")
    print("跳过文件列表请查看: skipped_index_files.log")
    print("=" * 60)

if __name__ == "__main__":
    main()
