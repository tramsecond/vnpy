#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
批量修复策略脚本中的数字格式问题

主要修复：
1. 整数字段（交易次数、总股票数等）应该使用整数格式，不是百分比或小数
2. 确保百分比、货币、普通数字的格式正确应用
"""

import re
import os

# 需要修复的文件列表
FILES_TO_FIX = [
    'trade_grid_trend_combined_profit_split_fixed.py',
    'trade_test_bbi_kdj.py',
    'trade_test_dual_ma.py',
    'trade_test1_multi_resonance.py',
    'trade_test1_new_strategy_no_profit.py',
    'trade_test1_new_strategy.py',
    'trade_test1.py',
    'trade_test2.py'
]

# 应该使用整数格式的字段关键词（不应有小数点）
INTEGER_KEYWORDS = [
    '交易次数', '盈利交易次数', '总交易次数', '总盈利交易次数',
    '总股票数', '盈利股票数', '亏损股票数',
    '趋势状态次数', '网格份数', '最终网格份数',
    '周期', '统计项'  # 这些是文本，但也不应该用数字格式
]

# 应该使用天数格式的字段（保留1位小数）
DAY_KEYWORDS = ['持有天数', '总持有天数', '平均持股天数']

def fix_file_formats(file_path):
    """修复单个文件的格式问题"""
    print(f"处理文件: {file_path}")
    
    if not os.path.exists(file_path):
        print(f"  文件不存在，跳过")
        return False
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        modifications = []
        
        # 1. 在格式定义部分添加整数格式（如果不存在）
        if "number_format = workbook.add_format({'num_format': '0.00'})" in content:
            # 检查是否已经有整数格式定义
            if "integer_format = workbook.add_format" not in content:
                # 在 number_format 定义后添加整数格式和天数格式
                content = content.replace(
                    "number_format = workbook.add_format({'num_format': '0.00'})",
                    "number_format = workbook.add_format({'num_format': '0.00'})\n            integer_format = workbook.add_format({'num_format': '0'})  # 整数格式\n            day_format = workbook.add_format({'num_format': '0.0'})  # 天数格式（1位小数）"
                )
                modifications.append("添加整数格式和天数格式定义")
        
        # 2. 修复列格式设置逻辑 - 替换所有格式判断代码块
        # 模式1: 匹配简单的if-else格式判断
        pattern1 = r"for col_num, col_name in enumerate\((\w+)\.columns\):\s*\n\s*if '涨跌幅' in col_name or '胜率' in col_name:[\s\S]*?else:\s*\n\s*worksheet\.set_column\(col_num \+ 1, col_num \+ 1, \d+, \w+\)"
        
        def replace_simple_format(match):
            df_name = match.group(1)
            # 检查缩进级别
            lines = match.group(0).split('\n')
            indent = len(lines[0]) - len(lines[0].lstrip())
            base_indent = ' ' * indent
            inner_indent = ' ' * (indent + 4)
            
            new_logic = f"""{base_indent}for col_num, col_name in enumerate({df_name}.columns):
{inner_indent}# 整数字段（交易次数、股票数等）
{inner_indent}if any(keyword in col_name for keyword in ['交易次数', '盈利交易次数', '总交易次数', '总盈利交易次数', '总股票数', '盈利股票数', '亏损股票数', '趋势状态次数', '网格份数', '最终网格份数']):
{inner_indent}    worksheet.set_column(col_num + 1, col_num + 1, 15, integer_format)
{inner_indent}# 天数字段（保留1位小数）
{inner_indent}elif any(keyword in col_name for keyword in ['持有天数', '总持有天数', '平均持股天数']):
{inner_indent}    worksheet.set_column(col_num + 1, col_num + 1, 15, day_format)
{inner_indent}# 百分比字段
{inner_indent}elif '涨跌幅' in col_name or '胜率' in col_name or '收益' in col_name or '利用率' in col_name or '占比' in col_name:
{inner_indent}    worksheet.set_column(col_num + 1, col_num + 1, 15, percent_format)
{inner_indent}# 货币字段
{inner_indent}elif any(keyword in col_name for keyword in ['成本', '市值', '资金', '资产', '价格', '金额', '盈亏', '现金']):
{inner_indent}    worksheet.set_column(col_num + 1, col_num + 1, 15, currency_format)
{inner_indent}# 默认数字格式
{inner_indent}else:
{inner_indent}    worksheet.set_column(col_num + 1, col_num + 1, 15, number_format)"""
            return new_logic
        
        # 应用简单格式替换
        new_content = re.sub(pattern1, replace_simple_format, content, flags=re.DOTALL)
        
        if new_content != content:
            content = new_content
            modifications.append("更新列格式设置逻辑")
        
        # 模式2: 匹配带有货币格式的if-elif-else格式判断
        pattern2 = r"for col_num, col_name in enumerate\((\w+)\.columns\):\s*\n\s*if '涨跌幅' in col_name or '胜率' in col_name:[\s\S]*?elif.*?currency_format\)[\s\S]*?else:\s*\n\s*worksheet\.set_column\(col_num \+ 1, col_num \+ 1, \d+, \w+\)"
        
        new_content2 = re.sub(pattern2, replace_simple_format, content, flags=re.DOTALL)
        
        if new_content2 != content:
            content = new_content2
            if "更新列格式设置逻辑" not in modifications:
                modifications.append("更新列格式设置逻辑")
        
        # 3. 确保所有的worksheet.set_column都使用15而不是12作为列宽（统一标准）
        content = re.sub(
            r'worksheet\.set_column\(col_num \+ 1, col_num \+ 1, 12,',
            'worksheet.set_column(col_num + 1, col_num + 1, 15,',
            content
        )
        
        if content != original_content:
            # 写回文件
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"  ✓ 已修复: {', '.join(modifications)}")
            return True
        else:
            print(f"  ○ 无需修改")
            return False
            
    except Exception as e:
        print(f"  ✗ 处理失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print("=" * 60)
    print("批量修复策略脚本的数字格式问题")
    print("=" * 60)
    print()
    
    fixed_count = 0
    failed_count = 0
    skipped_count = 0
    
    for file_name in FILES_TO_FIX:
        file_path = os.path.join(os.path.dirname(__file__), file_name)
        result = fix_file_formats(file_path)
        
        if result is True:
            fixed_count += 1
        elif result is False:
            skipped_count += 1
        else:
            failed_count += 1
    
    print()
    print("=" * 60)
    print(f"处理完成:")
    print(f"  已修复: {fixed_count} 个文件")
    print(f"  无需修改: {skipped_count} 个文件")
    print(f"  失败: {failed_count} 个文件")
    print("=" * 60)

if __name__ == "__main__":
    main()
