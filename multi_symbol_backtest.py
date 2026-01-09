"""
多标的CTA策略回测脚本
一个策略同时对多个标的进行回测，并汇总结果
"""
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

# 设置输出编码
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from vnpy_ctastrategy.backtesting import BacktestingEngine
from vnpy_ctastrategy.strategies.atr_rsi_strategy import AtrRsiStrategy
from vnpy.trader.constant import Interval


# 配置：要回测的标的列表
SYMBOLS = [
    ("000001", "SSE", "上证指数"),
    ("399001", "SZSE", "深证成指"),
    ("399006", "SZSE", "创业板指"),
    ("000688", "SSE", "科创50"),
    ("899050", "BSE", "北证50"),
]

# 回测参数
INTERVAL = Interval.DAILY
START_DATE = datetime(2020, 1, 2)
END_DATE = datetime(2026, 1, 7)

# 交易成本参数（股票指数）
RATE = 0.0003  # 手续费率（万三）
SLIPPAGE = 0  # 交易滑点
SIZE = 1  # 合约乘数（股票为1）
PRICETICK = 0.01  # 价格跳动（股票为0.01）
CAPITAL = 1_000_000  # 回测资金（100万）

# 策略参数
STRATEGY_SETTING = {}


def run_single_backtest(vt_symbol: str, symbol_name: str) -> pd.DataFrame:
    """对单个标的运行回测"""
    print(f"\n{'='*70}")
    print(f"开始回测: {symbol_name} ({vt_symbol})")
    print(f"{'='*70}")
    
    # 创建回测引擎
    engine = BacktestingEngine()
    
    # 设置参数
    engine.set_parameters(
        vt_symbol=vt_symbol,
        interval=INTERVAL,
        start=START_DATE,
        end=END_DATE,
        rate=RATE,
        slippage=SLIPPAGE,
        size=SIZE,
        pricetick=PRICETICK,
        capital=CAPITAL
    )
    
    # 添加策略
    engine.add_strategy(AtrRsiStrategy, STRATEGY_SETTING)
    
    # 加载数据
    print("加载历史数据...")
    engine.load_data()
    
    # 运行回测
    print("运行回测...")
    engine.run_backtesting()
    
    # 计算结果
    df = engine.calculate_result()
    
    # 计算统计指标
    engine.calculate_statistics(df)
    
    print(f"✓ {symbol_name} 回测完成")
    
    return df


def combine_results(results: dict) -> pd.DataFrame:
    """合并多个标的的回测结果"""
    print(f"\n{'='*70}")
    print("合并回测结果...")
    print(f"{'='*70}")
    
    # 合并所有DataFrame
    combined_df = None
    
    for vt_symbol, df in results.items():
        if combined_df is None:
            combined_df = df.copy()
        else:
            # 按日期对齐并相加
            combined_df = combined_df.add(df, fill_value=0)
    
    # 去除NaN值
    combined_df = combined_df.dropna()
    
    return combined_df


def show_portfolio_results(df: pd.DataFrame):
    """显示组合回测结果"""
    engine = BacktestingEngine()
    # 计算统计指标（会打印到控制台）
    engine.calculate_statistics(df)
    # 显示图表（会弹出浏览器窗口显示Plotly交互式图表）
    engine.show_chart(df)


def main():
    """主函数"""
    print("="*70)
    print("多标的CTA策略回测")
    print("="*70)
    print(f"策略: AtrRsiStrategy")
    print(f"周期: {INTERVAL.value}")
    print(f"日期范围: {START_DATE.strftime('%Y-%m-%d')} 至 {END_DATE.strftime('%Y-%m-%d')}")
    print(f"标的数量: {len(SYMBOLS)}")
    print("="*70)
    
    # 存储每个标的的回测结果
    results = {}
    
    # 对每个标的进行回测
    for symbol, exchange, name in SYMBOLS:
        vt_symbol = f"{symbol}.{exchange}"
        try:
            df = run_single_backtest(vt_symbol, name)
            results[vt_symbol] = df
        except Exception as e:
            print(f"✗ {name} ({vt_symbol}) 回测失败: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    if not results:
        print("\n所有标的回测都失败了！")
        return
    
    # 合并结果
    combined_df = combine_results(results)
    
    # 显示组合结果
    print(f"\n{'='*70}")
    print("组合回测结果统计")
    print(f"{'='*70}")
    show_portfolio_results(combined_df)
    
    print(f"\n{'='*70}")
    print("多标的回测完成！")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()

