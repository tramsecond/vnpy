import os
import pandas as pd
import datetime
import numpy as np

def generate_all_cycles_report():
    """生成包含所有股票所有周期综合判断的完整报告"""
    DATA_DIR = "analyzed_results"
    REPORT_DIR = os.path.join(DATA_DIR, "全周期分析报告")
    os.makedirs(REPORT_DIR, exist_ok=True)
    
    # 准备时间戳
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 主报告数据结构
    all_reports = []
    
    print("开始全周期分析...")
    
    for filename in os.listdir(DATA_DIR):
        if not filename.endswith('_技术数据.xlsx'):
            continue
            
        file_path = os.path.join(DATA_DIR, filename)
        try:
            # 解析股票代码和名称
            parts = filename.split('_')
            stock_code = parts[0]
            stock_name = parts[1] if len(parts) > 1 else "未知"
            
            # 读取Excel文件
            excel_data = {}
            for period in ['日线数据', '周线数据', '月线数据']:
                try:
                    df = pd.read_excel(file_path, sheet_name=period)
                    if '综合判断' in df.columns:
                        last_judgment = str(df.iloc[-1]['综合判断']).strip()
                    else:
                        last_judgment = f"{period}表无综合判断列"
                except:
                    last_judgment = f"{period}表不存在"
                
                # 使用简化的表头
                period_key = period.replace('数据', '')
                excel_data[period_key] = last_judgment
            
            # 添加看多标记
            excel_data['日线看多'] = '看多' in excel_data.get('日线', '')
            excel_data['周线看多'] = '看多' in excel_data.get('周线', '')
            excel_data['月线看多'] = '看多' in excel_data.get('月线', '')
            
            # 计算看多总分 (月线优先，权重最高)
            excel_data['看多总分'] = (excel_data['月线看多'] * 4 + 
                                  excel_data['周线看多'] * 2 + 
                                  excel_data['日线看多'] * 1)
            
            # 添加到主报告
            all_reports.append({
                '股票代码': stock_code,
                '股票名称': stock_name,
                **excel_data
            })
            print(f"已处理: {stock_code}-{stock_name}")
            
        except Exception as e:
            print(f"处理文件 {filename} 时出错: {str(e)}")
    
    if not all_reports:
        print("未找到可分析的文件，程序结束")
        return
        
    print(f"\n分析完成! 共处理 {len(all_reports)} 个文件")
    
    # 创建DataFrame
    df = pd.DataFrame(all_reports)
    
    # 按看多数量和权重排序
    df = sort_by_multiple_cycles(df)
    
    # 生成报告文件路径
    excel_path = os.path.join(REPORT_DIR, f"全周期分析报告_{timestamp}.xlsx")
    text_path = os.path.join(REPORT_DIR, f"全周期分析摘要_{timestamp}.txt")
    
    # 生成Excel报告
    generate_excel_report(df, excel_path)
    
    # 生成文本摘要
    generate_text_report(df, text_path)
    
    print(f"\n报告生成完成!\nExcel报告: {excel_path}\n文本摘要: {text_path}")

def sort_by_multiple_cycles(df):
    """按照日周月看多优先级排序"""
    # 计算总看多周期数
    df['看多周期数'] = df['日线看多'].astype(int) + df['周线看多'].astype(int) + df['月线看多'].astype(int)
    
    # 设置排序优先级：看多周期数(降序) > 看多总分(降序) > 股票代码(升序)
    df = df.sort_values(by=['看多周期数', '看多总分', '股票代码'], ascending=[False, False, True])
    
    return df

def generate_excel_report(df, output_path):
    """生成Excel格式的完整报告"""
    # 复制一份用于Excel报告（去掉计算用的辅助列）
    excel_df = df[['股票代码', '股票名称', '日线', '周线', '月线']].copy()
    
    # 创建Excel写入器
    with pd.ExcelWriter(output_path) as writer:
        # 全周期分析表
        excel_df.to_excel(writer, sheet_name='全周期分析', index=False)
        
        # 分周期统计表
        period_stats = []
        for period in ['日线', '周线', '月线']:
            stats = pd.DataFrame({
                '判断类型': ['看多', '看空', '中性/其他'],
                '股票数量': [
                    len(df[df[f'{period}看多']]),
                    len(df[df[f'{period}'].str.contains('看空')]),
                    len(df) - len(df[df[f'{period}看多']]) - len(df[df[f'{period}'].str.contains('看空')])
                ]
            })
            stats.insert(0, '分析周期', period)
            period_stats.append(stats)
        
        pd.concat(period_stats).to_excel(writer, sheet_name='周期统计', index=False)
        
        # 多周期共振表
        resonance_df = df[
            df['日线看多'] & 
            df['周线看多'] & 
            df['月线看多']
        ][['股票代码', '股票名称', '日线', '周线', '月线']]
        resonance_df.to_excel(writer, sheet_name='三周期共振', index=False)
        
        # 周期组合分析表
        create_combination_sheet(df, writer)

def create_combination_sheet(df, writer):
    """创建周期组合分析表"""
    # 创建周期组合统计
    combinations = []
    
    # 所有可能的组合状态
    for daily in [0, 1]:
        for weekly in [0, 1]:
            for monthly in [0, 1]:
                count = len(df[
                    (df['日线看多'] == daily) &
                    (df['周线看多'] == weekly) &
                    (df['月线看多'] == monthly)
                ])
                
                combination = []
                if monthly: combination.append('月线')
                if weekly: combination.append('周线')
                if daily: combination.append('日线')
                
                status_name = "三周期看多" if daily and weekly and monthly else (
                    "双周期看多" if len(combination) == 2 else (
                    "单周期看多" if len(combination) == 1 else "无周期看多"
                ))
                
                combinations.append({
                    '日线看多': '是' if daily else '否',
                    '周线看多': '是' if weekly else '否',
                    '月线看多': '是' if monthly else '否',
                    '看多组合': ' + '.join(combination) if combination else '无',
                    '组合类型': status_name,
                    '股票数量': count
                })
    
    # 按组合优先级排序（三周期 > 双周期 > 单周期 > 无）
    combination_df = pd.DataFrame(combinations)
    combination_df['排序优先级'] = combination_df['组合类型'].map({
        '三周期看多': 0,
        '双周期看多': 1,
        '单周期看多': 2,
        '无周期看多': 3
    })
    combination_df = combination_df.sort_values('排序优先级').drop(columns='排序优先级')
    
    # 添加到Excel
    combination_df.to_excel(writer, sheet_name='周期组合分析', index=False)

def generate_text_report(df, output_path):
    """生成文本格式的报告摘要"""
    text = "全周期技术分析报告摘要\n\n"
    text += f"报告生成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    text += f"分析股票总数: {len(df)}\n\n"
    
    # 各周期看多情况统计
    text += "各周期看多情况统计:\n"
    text += "周期 | 看多股票 | 占比   | 最高分股票\n"
    text += "-" * 40 + "\n"
    
    top_stocks = {}
    
    for period in ['日线', '周线', '月线']:
        # 当前周期看多的股票
        period_bullish = df[df[f'{period}看多']]
        
        # 统计数量和占比
        count = len(period_bullish)
        percent = count / len(df)
        
        # 找出当前周期看多且总分最高的股票
        if not period_bullish.empty:
            top_stock = period_bullish.iloc[0]
            top_stocks[period] = f"{top_stock['股票代码']} {top_stock['股票名称']}"
        else:
            top_stocks[period] = "无"
        
        text += f"{period:4} | {count:6} | {percent:6.1%} | {top_stocks[period]}\n"
    
    # 多周期组合统计
    text += "\n多周期组合统计:\n"
    combinations = [
        ('三周期看多', '日线看多 & 周线看多 & 月线看多'),
        ('日周月三看多', '日线看多 & 周线看多 & 月线看多'),
        ('月周双看多', '月线看多 & 周线看多'),
        ('月日双看多', '月线看多 & 日线看多'),
        ('周日双看多', '周线看多 & 日线看多'),
        ('月线单看多', '月线看多'),
        ('周线单看多', '周线看多'),
        ('日线单看多', '日线看多')
    ]
    
    for name, condition in combinations:
        try:
            count = len(df.query(condition))
            text += f" - {name} ({condition}): {count}\n"
        except:
            count = 0
    
    # 写入文件
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(text)
    print(f"文本摘要已生成: {output_path}")

if __name__ == "__main__":
    generate_all_cycles_report()