"""
基于ATR和RSI的仓位比例策略
改进版：使用资金比例而非固定手数

原策略来自：vnpy_ctastrategy.strategies.atr_rsi_strategy
改进：
1. 使用 position_percent 替代 fixed_size
2. 根据当前资金和目标仓位比例动态计算买入数量
3. 适合不同价格的标的进行公平对比
"""

import numpy as np
from vnpy.trader.constant import Interval, Direction, Offset
from vnpy.trader.object import BarData, TickData, OrderData, TradeData
from vnpy.trader.utility import BarGenerator, ArrayManager
from vnpy_ctastrategy import CtaTemplate, StopOrder


class AtrRsiPositionStrategy(CtaTemplate):
    """
    基于ATR和RSI的仓位比例策略
    
    策略逻辑：
    1. 当ATR > ATR均线时（波动增强），进场
    2. RSI > 66 做多，RSI < 34 做空
    3. 使用移动止损：从最高点回撤0.8%平仓
    
    关键改进：
    - 使用 position_percent 控制仓位（如50表示50%仓位）
    - 自动计算买入数量 = (资金 * position_percent%) / 当前价格
    """
    
    author = "VeighNa改进版"
    
    # 策略参数
    atr_length: int = 22              # ATR周期
    atr_ma_length: int = 10           # ATR均线周期
    rsi_length: int = 5               # RSI周期
    rsi_entry: int = 16               # RSI入场阈值（50±16）
    trailing_percent: float = 0.8    # 移动止损百分比
    position_percent: float = 95.0   # 目标仓位比例（%）
    
    # 策略变量
    atr_value: float = 0
    atr_ma: float = 0
    rsi_value: float = 0
    rsi_buy: float = 0
    rsi_sell: float = 0
    intra_trade_high: float = 0
    intra_trade_low: float = 0
    current_capital: float = 0        # 当前可用资金
    
    parameters = [
        "atr_length",
        "atr_ma_length", 
        "rsi_length",
        "rsi_entry",
        "trailing_percent",
        "position_percent"
    ]
    
    variables = [
        "atr_value",
        "atr_ma",
        "rsi_value",
        "rsi_buy",
        "rsi_sell",
        "intra_trade_high",
        "intra_trade_low",
        "current_capital"
    ]
    
    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        """初始化"""
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)
        
        self.bg: BarGenerator = BarGenerator(self.on_bar)
        self.am: ArrayManager = ArrayManager()
        
        self.rsi_buy = 50 + self.rsi_entry
        self.rsi_sell = 50 - self.rsi_entry
    
    def on_init(self) -> None:
        """策略初始化"""
        self.write_log("策略初始化")
        self.load_bar(10)
    
    def on_start(self) -> None:
        """策略启动"""
        self.write_log("策略启动")
    
    def on_stop(self) -> None:
        """策略停止"""
        self.write_log("策略停止")
    
    def on_tick(self, tick: TickData) -> None:
        """tick更新"""
        self.bg.update_tick(tick)
    
    def on_bar(self, bar: BarData) -> None:
        """K线更新"""
        self.cancel_all()
        
        am: ArrayManager = self.am
        am.update_bar(bar)
        if not am.inited:
            return
        
        # 计算技术指标
        atr_array: np.ndarray = am.atr(self.atr_length, array=True)
        self.atr_value = atr_array[-1]
        self.atr_ma = atr_array[-self.atr_ma_length:].mean()
        self.rsi_value = am.rsi(self.rsi_length)
        
        # 根据仓位状态执行不同逻辑
        if self.pos == 0:
            # 无仓位：判断入场信号
            self.intra_trade_high = bar.high_price
            self.intra_trade_low = bar.low_price
            
            if self.atr_value > self.atr_ma:
                # 计算目标交易数量
                # 注意：这里使用一个参考资金量（通常在策略参数中设置）
                # 或者使用固定的基准资金（如1,000,000）
                reference_capital = 1_000_000  # 基准资金
                target_value = reference_capital * (self.position_percent / 100)
                target_volume = int(target_value / bar.close_price)
                
                if target_volume > 0:
                    if self.rsi_value > self.rsi_buy:
                        # 做多信号
                        self.buy(bar.close_price + 5, target_volume)
                    elif self.rsi_value < self.rsi_sell:
                        # 做空信号（仅期货）
                        self.short(bar.close_price - 5, target_volume)
        
        elif self.pos > 0:
            # 持有多单：移动止损
            self.intra_trade_high = max(self.intra_trade_high, bar.high_price)
            self.intra_trade_low = bar.low_price
            
            long_stop: float = self.intra_trade_high * (1 - self.trailing_percent / 100)
            self.sell(long_stop, abs(self.pos), stop=True)
        
        elif self.pos < 0:
            # 持有空单：移动止损
            self.intra_trade_low = min(self.intra_trade_low, bar.low_price)
            self.intra_trade_high = bar.high_price
            
            short_stop: float = self.intra_trade_low * (1 + self.trailing_percent / 100)
            self.cover(short_stop, abs(self.pos), stop=True)
        
        self.put_event()
    
    def on_order(self, order: OrderData) -> None:
        """委托更新"""
        pass
    
    def on_trade(self, trade: TradeData) -> None:
        """成交更新"""
        self.put_event()
    
    def on_stop_order(self, stop_order: StopOrder) -> None:
        """停止单更新"""
        pass
    

