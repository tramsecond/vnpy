#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import pandas as pd
import numpy as np
import glob
import time
import shutil
from datetime import datetime, timedelta
import xlsxwriter
import traceback
import akshare as ak
import threading

# 导入配置文件
from config import START_DATE

# 强制设置UTF-8编码环境
sys.stdout.reconfigure(encoding='utf-8') if hasattr(sys.stdout, 'reconfigure') else None
sys.stderr.reconfigure(encoding='utf-8') if hasattr(sys.stderr, 'reconfigure') else None

if sys.platform.startswith('win'):
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    os.environ['LANG'] = 'zh_CN.UTF-8'
    os.environ['LC_ALL'] = 'zh_CN.UTF-8'

class DataUpdateManager:
    """智能数据更新管理器"""
    
    def __init__(self):
        self.today = datetime.now().date()
        self.yesterday = self.today - timedelta(days=1)
        self.last_trading_day = self._get_last_trading_day()
        
    def _get_last_trading_day(self):
        """获取最近的交易日"""
        # 简单实现：如果是周末，回退到周五
        current = self.today
        while current.weekday() > 4:  # 5=周六, 6=周日
            current = current - timedelta(days=1)
        return current
    
    def _is_trading_day(self, date):
        """判断是否为交易日（简单实现，实际应该考虑节假日）"""
        return date.weekday() < 5  # 周一到周五
    
    def _get_next_trading_day(self, start_date):
        """获取下一个交易日"""
        current = start_date + timedelta(days=1)
        while not self._is_trading_day(current):
            current = current + timedelta(days=1)
        return current
    
    def _get_trading_days_between(self, start_date, end_date):
        """获取两个日期之间的交易日数量"""
        current = start_date
        count = 0
        while current <= end_date:
            if self._is_trading_day(current):
                count += 1
            current += timedelta(days=1)
        return count
    
    def analyze_data_gaps(self, excel_file, symbol_code, symbol_name):
        """分析数据缺口，确定需要更新的数据范围"""
        print(f"🔍 分析 {symbol_name}({symbol_code}) 的数据缺口...")
        
        if not os.path.exists(excel_file):
            print(f"  📁 文件不存在，需要完整拉取数据")
            return {
                'need_full_update': True,
                'last_date': None,
                'missing_days': None,
                'update_start_date': START_DATE,
                'update_end_date': self.today.strftime('%Y-%m-%d')
            }
        
        try:
            # 读取现有数据
            with pd.ExcelFile(excel_file) as xls:
                if '日线数据' in xls.sheet_names:
                    df_daily = pd.read_excel(xls, '日线数据')
                else:
                    print(f"  ❌ 文件中没有日线数据工作表")
                    return None
                
                if '周线数据' in xls.sheet_names:
                    df_weekly = pd.read_excel(xls, '周线数据')
                else:
                    df_weekly = pd.DataFrame()
                    
                if '月线数据' in xls.sheet_names:
                    df_monthly = pd.read_excel(xls, '月线数据')
                else:
                    df_monthly = pd.DataFrame()
                    
            
            # 分析日线数据
            if df_daily.empty:
                print(f"  📊 日线数据为空，需要完整拉取")
                return {
                    'need_full_update': True,
                    'last_date': None,
                    'missing_days': None,
                    'update_start_date': START_DATE,
                    'update_end_date': self.today.strftime('%Y-%m-%d')
                }
            
            # 确保日期列存在且格式正确
            if 'date' not in df_daily.columns:
                print(f"  ❌ 日线数据缺少日期列")
                return None
            
            df_daily['date'] = pd.to_datetime(df_daily['date'])
            df_daily = df_daily.sort_values('date')
            
            # 获取最后一行数据的日期
            last_date = df_daily['date'].max().date()
            print(f"  📅 现有数据最后日期: {last_date.strftime('%Y-%m-%d')}")
            
            # 计算缺失的交易日数量
            if last_date >= self.last_trading_day:
                print(f"  ✅ 数据已是最新，无需更新")
                return {
                    'need_full_update': False,
                    'last_date': last_date,
                    'missing_days': 0,
                    'update_start_date': None,
                    'update_end_date': None
                }
            
            # 计算需要更新的日期范围
            update_start_date = self._get_next_trading_day(last_date)
            update_end_date = self.last_trading_day
            
            missing_days = self._get_trading_days_between(update_start_date, update_end_date)
            
            print(f"  📊 需要更新 {missing_days} 个交易日的数据")
            print(f"  📅 更新范围: {update_start_date.strftime('%Y-%m-%d')} 到 {update_end_date.strftime('%Y-%m-%d')}")
            
            return {
                'need_full_update': False,
                'last_date': last_date,
                'missing_days': missing_days,
                'update_start_date': update_start_date.strftime('%Y-%m-%d'),
                'update_end_date': update_end_date.strftime('%Y-%m-%d')
            }
            
        except Exception as e:
            print(f"  ❌ 分析数据缺口时出错: {e}")
            print(traceback.format_exc())
            return None
    
    def fetch_incremental_data(self, symbol_code, symbol_name, start_date, end_date, is_stock=True):
        """获取增量数据"""
        print(f"  📥 拉取增量数据: {start_date} 到 {end_date}")
        
        try:
            if is_stock:
                # 股票数据
                symbol_code_clean = symbol_code[2:] if symbol_code.startswith(('sh', 'sz')) else symbol_code
                
                # 尝试获取日线数据
                df_daily = ak.stock_zh_a_hist(
                    symbol=symbol_code_clean, 
                    period="daily", 
                    adjust="qfq", 
                    start_date=start_date, 
                    end_date=end_date
                )
                
            else:
                # 指数数据
                if symbol_code.startswith(('sh', 'sz')):
                    # 国内指数
                    df_daily = ak.stock_zh_index_daily_em(symbol=symbol_code)
                    # 过滤日期范围
                    df_daily['date'] = pd.to_datetime(df_daily['date'])
                    df_daily = df_daily[
                        (df_daily['date'] >= pd.to_datetime(start_date)) & 
                        (df_daily['date'] <= pd.to_datetime(end_date))
                    ]
                else:
                    # 海外指数
                    df_daily = pd.DataFrame()
            
            if df_daily is not None and not df_daily.empty:
                print(f"    ✅ 成功获取 {len(df_daily)} 条日线数据")
            else:
                print(f"    ❌ 获取日线数据失败")
                return None
            
            return df_daily
            
        except Exception as e:
            print(f"    ❌ 获取增量数据时出错: {e}")
            return None, None
    
    def merge_data(self, existing_df, new_df, time_frame='日线'):
        """合并现有数据和新数据"""
        if existing_df.empty:
            return new_df
        
        if new_df.empty:
            return existing_df
        
        print(f"    🔄 合并{time_frame}数据...")
        
        # 确保日期列格式一致
        existing_df['date'] = pd.to_datetime(existing_df['date'])
        new_df['date'] = pd.to_datetime(new_df['date'])
        
        # 找到需要替换的日期范围
        new_start_date = new_df['date'].min()
        new_end_date = new_df['date'].max()
        
        # 移除现有数据中与新数据重叠的部分
        existing_df_filtered = existing_df[
            ~((existing_df['date'] >= new_start_date) & (existing_df['date'] <= new_end_date))
        ]
        
        # 合并数据
        merged_df = pd.concat([existing_df_filtered, new_df], ignore_index=True)
        merged_df = merged_df.sort_values('date').reset_index(drop=True)
        
        print(f"    ✅ 合并完成: 原有 {len(existing_df)} 行 + 新增 {len(new_df)} 行 = 合并后 {len(merged_df)} 行")
        
        return merged_df
    
    def update_excel_file(self, excel_file, symbol_code, symbol_name, update_info):
        """更新Excel文件"""
        print(f"  💾 更新Excel文件: {excel_file}")
        
        try:
            # 创建备份
            backup_file = excel_file.replace('.xlsx', f'_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx')
            shutil.copy2(excel_file, backup_file)
            print(f"    📁 已创建备份: {backup_file}")
            
            # 读取现有数据
            with pd.ExcelFile(excel_file) as xls:
                df_daily = pd.read_excel(xls, '日线数据') if '日线数据' in xls.sheet_names else pd.DataFrame()
                df_weekly = pd.read_excel(xls, '周线数据') if '周线数据' in xls.sheet_names else pd.DataFrame()
                df_monthly = pd.read_excel(xls, '月线数据') if '月线数据' in xls.sheet_names else pd.DataFrame()
            
            # 获取增量数据
            df_new_daily = self.fetch_incremental_data(
                symbol_code, symbol_name, 
                update_info['update_start_date'], 
                update_info['update_end_date'],
                is_stock=symbol_code.startswith(('sh', 'sz'))
            )
            
            if df_new_daily is None:
                print(f"    ❌ 获取增量数据失败，跳过更新")
                return False
            
            # 合并数据
            df_daily_updated = self.merge_data(df_daily, df_new_daily, '日线')
            
            # 重新生成周线和月线数据
            if not df_daily_updated.empty:
                df_weekly_updated = self._generate_weekly_view(df_daily_updated)
                df_monthly_updated = self._generate_monthly_view(df_daily_updated)
            else:
                df_weekly_updated = df_weekly
                df_monthly_updated = df_monthly
            
            # 重新计算技术指标
            print(f"    🧮 重新计算技术指标...")
            df_daily_updated = self._calculate_all_indicators(df_daily_updated)
            df_weekly_updated = self._calculate_all_indicators(df_weekly_updated)
            df_monthly_updated = self._calculate_all_indicators(df_monthly_updated)
            
            # 保存更新后的数据
            self._save_to_excel(
                df_daily_updated, df_weekly_updated, df_monthly_updated,
                excel_file
            )
            
            print(f"    ✅ 文件更新完成")
            return True
            
        except Exception as e:
            print(f"    ❌ 更新Excel文件时出错: {e}")
            print(traceback.format_exc())
            return False
    
    def _generate_weekly_view(self, df):
        """生成周线视图"""
        if df.empty:
            return pd.DataFrame()
        
        df_copy = df.copy()
        df_copy['date'] = pd.to_datetime(df_copy['date'])
        df_copy = df_copy.set_index('date')
        
        weekly = df_copy.resample('W-FRI').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        })
        
        weekly = weekly.dropna().reset_index()
        return weekly
    
    def _generate_monthly_view(self, df):
        """生成月线视图"""
        if df.empty:
            return pd.DataFrame()
        
        df_copy = df.copy()
        df_copy['date'] = pd.to_datetime(df_copy['date'])
        df_copy = df_copy.set_index('date')
        
        monthly = df_copy.resample('ME').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        })
        
        monthly = monthly.dropna().reset_index()
        return monthly
    
    def _calculate_all_indicators(self, df):
        """计算所有技术指标"""
        if df.empty:
            return df
        
        try:
            # 导入技术指标计算函数
            from data_preparation import (
                calculate_ma, calculate_ema, calculate_macd, calculate_kdj,
                calculate_rsi, calculate_boll, calculate_trend_indicator_a,
                calculate_supertrend, calculate_qqe_mod
            )
            
            # 计算技术指标
            df = calculate_ma(df)
            df = calculate_ema(df)
            df = calculate_macd(df)
            df = calculate_kdj(df)
            df = calculate_rsi(df)
            df = calculate_boll(df)
            df = calculate_trend_indicator_a(df)
            df = calculate_supertrend(df)
            df = calculate_qqe_mod(df)
            
        except Exception as e:
            print(f"      ⚠️  计算技术指标时出错: {e}")
        
        return df
    
    def _save_to_excel(self, df_daily, df_weekly, df_monthly, filename):
        """保存数据到Excel文件"""
        try:
            with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
                if not df_daily.empty:
                    df_daily.to_excel(writer, sheet_name='日线数据', index=False)
                
                if not df_weekly.empty:
                    df_weekly.to_excel(writer, sheet_name='周线数据', index=False)
                
                if not df_monthly.empty:
                    df_monthly.to_excel(writer, sheet_name='月线数据', index=False)
            
            print(f"      💾 数据已保存到: {filename}")
            
        except Exception as e:
            print(f"      ❌ 保存Excel文件时出错: {str(e)}")
    
    def update_stock_data(self, stock_code, stock_name):
        """更新股票数据"""
        print(f"📈 更新股票数据: {stock_name}({stock_code})")
        
        # 确定文件路径
        excel_file = f"stock_data/{stock_code[2:]}_{stock_name}_技术数据.xlsx"
        
        # 分析数据缺口
        update_info = self.analyze_data_gaps(excel_file, stock_code, stock_name)
        
        if update_info is None:
            print(f"  ❌ 无法分析数据缺口，跳过更新")
            return False
        
        if not update_info['need_full_update'] and update_info['missing_days'] == 0:
            print(f"  ✅ 数据已是最新，无需更新")
            return True
        
        # 执行更新
        if update_info['need_full_update']:
            print(f"  🔄 需要完整更新，调用完整数据准备函数")
            # 这里可以调用原有的完整数据准备函数
            from data_preparation import prepare_stock_data
            return prepare_stock_data(stock_code, stock_name) is not None
        else:
            print(f"  🔄 执行增量更新")
            return self.update_excel_file(excel_file, stock_code, stock_name, update_info)
    
    def update_index_data(self, index_code, index_name, source_type='china'):
        """更新指数数据"""
        print(f"📊 更新指数数据: {index_name}({index_code})")
        
        # 确定文件路径
        excel_file = f"index_data/{index_name}_{index_code}_data.xlsx"
        
        # 分析数据缺口
        update_info = self.analyze_data_gaps(excel_file, index_code, index_name)
        
        if update_info is None:
            print(f"  ❌ 无法分析数据缺口，跳过更新")
            return False
        
        if not update_info['need_full_update'] and update_info['missing_days'] == 0:
            print(f"  ✅ 数据已是最新，无需更新")
            return True
        
        # 执行更新
        if update_info['need_full_update']:
            print(f"  🔄 需要完整更新，调用完整数据准备函数")
            # 这里可以调用原有的完整数据准备函数
            from data_index_preparation_improved import fetch_index_data
            return fetch_index_data(index_code, index_name, source_type)
        else:
            print(f"  🔄 执行增量更新")
            return self.update_excel_file(excel_file, index_code, index_name, update_info)
    
    def batch_update_stocks(self, stock_list, batch_size=10):
        """批量更新股票数据"""
        print(f"🚀 开始批量更新股票数据 ({len(stock_list)} 只股票)")
        
        total_batches = (len(stock_list) + batch_size - 1) // batch_size
        success_count = 0
        
        for batch_idx in range(total_batches):
            batch_start = batch_idx * batch_size
            batch_end = min((batch_idx + 1) * batch_size, len(stock_list))
            
            print(f"\n📦 批次 {batch_idx + 1}/{total_batches}: 处理第 {batch_start + 1}-{batch_end} 只股票")
            
            for idx in range(batch_start, batch_end):
                stock_code, stock_name = stock_list[idx]
                print(f"\n📈 [{idx+1}/{len(stock_list)}] 更新股票: {stock_name}({stock_code})")
                
                try:
                    if self.update_stock_data(stock_code, stock_name):
                        success_count += 1
                        print(f"  ✅ 股票 {stock_name} 更新成功")
                    else:
                        print(f"  ❌ 股票 {stock_name} 更新失败")
                except Exception as e:
                    print(f"  ❌ 更新股票 {stock_name} 时出错: {e}")
                
                # 每只股票之间休息1秒
                if idx + 1 < batch_end:
                    time.sleep(1)
            
            # 批次间休息5秒
            if batch_idx + 1 < total_batches:
                print(f"⏸️  批次间休息5秒...")
                time.sleep(5)
        
        print(f"\n✅ 批量更新完成! 成功更新: {success_count}/{len(stock_list)} 只股票")
        return success_count
    
    def batch_update_indices(self, index_list, batch_size=5):
        """批量更新指数数据"""
        print(f"🚀 开始批量更新指数数据 ({len(index_list)} 个指数)")
        
        total_batches = (len(index_list) + batch_size - 1) // batch_size
        success_count = 0
        
        for batch_idx in range(total_batches):
            batch_start = batch_idx * batch_size
            batch_end = min((batch_idx + 1) * batch_size, len(index_list))
            
            print(f"\n📦 批次 {batch_idx + 1}/{total_batches}: 处理第 {batch_start + 1}-{batch_end} 个指数")
            
            for idx in range(batch_start, batch_end):
                index_info = index_list[idx]
                print(f"\n📊 [{idx+1}/{len(index_list)}] 更新指数: {index_info['name']}({index_info['code']})")
                
                try:
                    if self.update_index_data(index_info['code'], index_info['name'], index_info['source']):
                        success_count += 1
                        print(f"  ✅ 指数 {index_info['name']} 更新成功")
                    else:
                        print(f"  ❌ 指数 {index_info['name']} 更新失败")
                except Exception as e:
                    print(f"  ❌ 更新指数 {index_info['name']} 时出错: {e}")
                
                # 每个指数之间休息2秒
                if idx + 1 < batch_end:
                    time.sleep(2)
            
            # 批次间休息3秒
            if batch_idx + 1 < total_batches:
                print(f"⏸️  批次间休息3秒...")
                time.sleep(3)
        
        print(f"\n✅ 批量更新完成! 成功更新: {success_count}/{len(index_list)} 个指数")
        return success_count

def main():
    """主函数：智能数据更新"""
    print("=" * 80)
    print("🚀 智能数据更新系统启动")
    print(f"🕐 当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📅 最后交易日: {datetime.now().date()}")
    print("=" * 80)
    
    # 创建更新管理器
    update_manager = DataUpdateManager()
    
    # 获取股票列表
    from data_preparation import get_hs300_stocks
    stock_list = get_hs300_stocks()
    
    # 获取指数列表
    from data_index_preparation_improved import load_indices_from_csv
    index_list = load_indices_from_csv()
    
    print(f"📋 加载完成: {len(stock_list)} 只股票 + {len(index_list)} 个指数")
    
    # 选择更新模式
    print("\n🎯 请选择更新模式:")
    print("1. 更新所有股票和指数")
    print("2. 只更新股票")
    print("3. 只更新指数")
    print("4. 更新指定股票")
    print("5. 更新指定指数")
    
    choice = input("请输入选择 (1-5): ").strip()
    
    if choice == '1':
        # 更新所有数据
        print("\n🚀 开始更新所有数据...")
        update_manager.batch_update_stocks(stock_list)
        update_manager.batch_update_indices(index_list)
        
    elif choice == '2':
        # 只更新股票
        print("\n🚀 开始更新股票数据...")
        update_manager.batch_update_stocks(stock_list)
        
    elif choice == '3':
        # 只更新指数
        print("\n🚀 开始更新指数数据...")
        update_manager.batch_update_indices(index_list)
        
    elif choice == '4':
        # 更新指定股票
        stock_code = input("请输入股票代码 (如: sh600519): ").strip()
        stock_name = input("请输入股票名称: ").strip()
        update_manager.update_stock_data(stock_code, stock_name)
        
    elif choice == '5':
        # 更新指定指数
        index_code = input("请输入指数代码 (如: 000300): ").strip()
        index_name = input("请输入指数名称: ").strip()
        source_type = input("请输入数据源 (china/hk/us): ").strip()
        update_manager.update_index_data(index_code, index_name, source_type)
        
    else:
        print("❌ 无效选择")
        return
    
    print("\n" + "=" * 80)
    print("🎉 数据更新完成!")
    print(f"🕐 完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

if __name__ == "__main__":
    main()
