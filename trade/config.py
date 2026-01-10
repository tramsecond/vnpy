# -*- coding: utf-8 -*-
"""
配置文件 - 统一管理所有脚本的配置参数
"""

# 数据获取起始日期
START_DATE = "2020-01-01"

# 回测起始日期（用于过滤负数价格数据）
BACKTEST_START_DATE = "2020-01-01"

# 小时线数据获取起始日期（最近N年，默认2年）
HOURLY_DATA_YEARS = 2  # 获取最近2年的小时线数据

# 初始资金
INITIAL_CAPITAL = 100000

# 数据源配置
DATA_SOURCE = "china"  # china, yfinance

# 技术指标参数
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

KDJ_K = 9
KDJ_D = 3
KDJ_J = 3

RSI_PERIOD = 14

BOLL_PERIOD = 20
BOLL_STD = 2

MA_PERIODS = [5, 10, 20, 30, 60]

# 信号配置
BUY_SIGNALS = ['强烈买入', '买入', '看多',  '买入信号', '看多信号', '上升趋势']
SELL_SIGNALS = ['强烈卖出', '卖出', '看空', '卖出信号', '看空信号', '下降趋势']

# 止盈止损配置（通用配置，适用于所有策略）
ENABLE_PROFIT_TAKE = True   # 是否启用止盈
PROFIT_TAKE_PCT = 10        # 止盈百分比（如10表示上涨10%时卖出）
ENABLE_STOP_LOSS = True     # 是否启用止损
STOP_LOSS_PCT = 5           # 止损百分比（如5表示下跌5%时卖出）

# 双均线策略配置
DUAL_MA_STRATEGY = {
    "SHORT_MA_PERIOD": 5,        # 短期均线周期（如MA5）
    "LONG_MA_PERIOD": 20,         # 长期均线周期（如MA20）
    "ENABLE_PROFIT_TAKE": True,   # 是否启用止盈（可单独配置）
    "PROFIT_TAKE_PCT": 10,        # 止盈百分比（如10表示上涨10%时卖出）
    "ENABLE_STOP_LOSS": True,     # 是否启用止损（可单独配置）
    "STOP_LOSS_PCT": 5,           # 止损百分比（如5表示下跌5%时卖出）
}

# KDJ战法（BBIKDJSelector）策略配置
# 参考原版配置：D:\tradez\StockTradebyZ\configs.json
BBI_KDJ_STRATEGY = {
    # 原版策略参数（与原版configs.json对齐）
    "j_threshold": 15,            # J值阈值
    "bbi_min_window": 20,         # BBI最小窗口
    "max_window": 120,            # 最大窗口
    "price_range_pct": 1,         # 价格波动约束（小数形式：1表示100%，0.01表示1%，与原版对齐）
    "bbi_q_threshold": 0.2,       # BBI分位数阈值
    "j_q_threshold": 0.10,        # J值分位数阈值
    
    # 回测相关配置（原版配置中没有，用于回测脚本）
    "ENABLE_PRICE_RANGE": True,   # 是否启用价格波动约束（已启用）
    "ENABLE_PROFIT_TAKE": True,   # 是否启用止盈
    "PROFIT_TAKE_PCT": 10,        # 止盈百分比（如10表示上涨10%时卖出）
    "ENABLE_STOP_LOSS": True,     # 是否启用止损
    "STOP_LOSS_PCT": 5,           # 止损百分比（如5表示下跌5%时卖出）
    
    # 知行约束配置（原版使用固定的知行线计算，这里保留用于向后兼容）
    "SHORT_MA_PERIOD": 5,         # 短期均线（用于知行约束，但实际使用原版的知行线计算）
    "LONG_MA_PERIOD": 20,         # 长期均线（用于知行约束，但实际使用原版的知行线计算）
}

# 网格策略配置
GRID_STRATEGY = {
    "GRID_SIZE_PCT": 1.5,           # 网格大小百分比（涨跌幅）
    "GRID_COUNT": 10,               # 网格份数
    "GRID_AMOUNT_PER_UNIT": 10000,  # 每份金额（元）
    "MIN_HOLD_UNITS": 0,            # 最小持仓份数（不卖出）
    "MAX_HOLD_UNITS": 10,           # 最大持仓份数（不买入）
    "REQUIRED_PROFIT_PCT": 1.5      # 每份卖出必须盈利百分比
}

# 马丁格尔策略配置
MARTINGALE_STRATEGY = {
    "INITIAL_BET_AMOUNT": 10000,    # 初始投注金额
    "MULTIPLIER": 2.0,              # 倍投倍数（亏损后加倍）
    "MAX_CONSECUTIVE_LOSSES": 5,    # 最大连续亏损次数
    "PROFIT_TARGET_PCT": 5,         # 止盈百分比
    "STOP_LOSS_PCT": 10            # 止损百分比
}

# 输出目录配置
ANALYZED_RESULTS_DIR = "analyzed_results"
BACKTEST_RESULTS_DIR = "backtest_results_multi_period"
STOCK_DATA_DIR = "stock_data"
INDEX_DATA_DIR = "index_data"

# 文件编码
FILE_ENCODING = "utf-8"

# 重试配置
MAX_RETRIES = 3
BASE_DELAY = 1
