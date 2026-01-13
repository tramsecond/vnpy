"""
网格+趋势组合策略
Grid + Trend Combined Strategy

策略逻辑：
- 默认状态：执行网格策略（每次买入固定金额，盈利目标卖出）
- 趋势买入信号：冻结网格持仓，使用剩余资金全仓买入趋势
- 趋势卖出信号：清空趋势持仓，恢复网格交易

技术指标：
- SuperTrend: 趋势跟踪指标
- QQE MOD: 动量指标
- Trend A-V2: 平滑Heikin Ashi云图

综合判断规则：
- 买入信号：三个指标都看涨
- 卖出信号：三个指标都看跌
- 持有/谨慎观望：SuperTrend延续信号
- 看多/看空：至少一个指标看涨/看跌
"""

import numpy as np
from typing import List
from vnpy.trader.constant import Interval, Direction, Offset
from vnpy.trader.object import BarData, TickData, OrderData, TradeData
from vnpy.trader.utility import BarGenerator, ArrayManager
from vnpy_ctastrategy import CtaTemplate, StopOrder


class GridTrendStrategy(CtaTemplate):
    """网格+趋势组合策略"""
    
    author = "用户自定义"
    
    # 策略参数（不使用类型注解以提高兼容性）
    # SuperTrend参数
    supertrend_length = 10
    supertrend_multiplier = 3.0
    
    # QQE MOD参数
    qqe_rsi_length = 6
    qqe_rsi_smoothing = 5
    qqe_factor = 3.0
    qqe_threshold = 3.0
    qqe_bollinger_length = 50
    qqe_bollinger_mult = 0.35
    
    # Trend A-V2参数
    trend_a_period = 9
    
    # 网格策略参数
    grid_size_pct = 1.5  # 网格间距百分比
    grid_amount_pct = 5.0  # 每份网格占初始资金的百分比（%）
    min_hold_units = 1  # 最小持仓份数
    max_hold_units = 18  # 最多18份 = 90%资金
    required_profit_pct = 1.5  # 目标盈利百分比
    trend_position_pct = 10.0  # 趋势买入时使用的资金百分比（10%），总计100%
    
    # 策略变量
    supertrend_value = 0.0
    supertrend_direction = 0  # 1=上升, -1=下降
    qqe_signal = 0  # 1=看多, -1=看空, 0=中性
    trend_a_direction = 0  # 1=上升, -1=下降
    
    综合判断 = "中性"
    
    # 网格变量
    grid_units = 0  # 当前网格份数
    grid_buy_prices = []  # 网格买入价格记录
    grid_reference_price = 0.0  # 网格参考价格
    initial_grid_bought = False  # 是否已买入初始底仓
    
    # 趋势变量
    trend_active = False  # 是否处于趋势状态
    trend_position = 0.0  # 趋势持仓数量
    trend_buy_price = 0.0  # 趋势买入价格
    
    # 统计变量
    total_trades = 0  # 总交易次数
    winning_trades = 0  # 盈利交易次数
    grid_profit = 0.0  # 网格累计收益
    trend_profit = 0.0  # 趋势累计收益
    trend_trade_count = 0  # 趋势交易次数
    grid_trade_count = 0  # 网格交易次数
    
    # 资金管理变量（模仿原始策略的现金跟踪）
    initial_capital = 0.0  # 初始资金
    grid_amount_per_unit = 0.0  # 每份网格的金额
    cash = 0.0  # 剩余现金
    
    parameters = [
        "supertrend_length",
        "supertrend_multiplier",
        "qqe_rsi_length",
        "qqe_rsi_smoothing",
        "qqe_factor",
        "qqe_threshold",
        "qqe_bollinger_length",
        "qqe_bollinger_mult",
        "trend_a_period",
        "grid_size_pct",
        "grid_amount_pct",
        "min_hold_units",
        "max_hold_units",
        "required_profit_pct",
        "trend_position_pct"
    ]
    
    variables = [
        "supertrend_value",
        "supertrend_direction",
        "qqe_signal",
        "trend_a_direction",
        "综合判断",
        "grid_units",
        "trend_active",
        "trend_position",
        "total_trades",
        "winning_trades",
        "grid_profit",
        "trend_profit",
        "cash"
    ]
    
    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        """初始化"""
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)
        
        self.bg = BarGenerator(self.on_bar)
        self.am = ArrayManager(max(200, self.qqe_bollinger_length * 2))
        
        # 初始化网格买入价格列表
        self.grid_buy_prices = []
        
        # 记录初始资金（在回测开始时设置）
        self.initial_capital = 0.0
        self.grid_amount_per_unit = 0.0  # 每份网格的实际金额
    
    def on_init(self):
        """策略初始化"""
        self.write_log("策略初始化")
        self.load_bar(10)
    
    def on_start(self):
        """策略启动"""
        self.write_log("策略启动")
        
        # 获取初始资金（从回测引擎或实盘账户）
        # 在回测中，capital在backtesting_engine中设置
        # 这里我们暂时使用一个默认值，实际会从回测参数中获取
        # 在真正运行时，backtester会根据回测设置的资金来计算
        # 为了让策略能够正确计算，我们在第一个bar时获取当前权益作为初始资金
        pass
    
    def on_stop(self):
        """策略停止"""
        self.write_log("策略停止")
        self.output_statistics()
    
    def on_tick(self, tick: TickData):
        """Tick数据更新"""
        self.bg.update_tick(tick)
    
    def on_bar(self, bar: BarData):
        """K线数据更新"""
        # ⚠️ 注意：原始策略不使用cancel_all()，因为是直接执行买卖，不挂单
        # 如果使用cancel_all()会导致趋势卖出订单被撤销
        # self.cancel_all()  # 已移除
        
        am: ArrayManager = self.am
        am.update_bar(bar)
        
        if not am.inited:
            return
        
        # 计算技术指标
        self.calculate_indicators()
        
        # 生成综合判断
        self.generate_combined_signal()
        
        # 执行交易逻辑
        self.execute_trading_logic(bar)
        
        self.put_event()
    
    def calculate_indicators(self):
        """计算所有技术指标"""
        am = self.am
        
        # 1. 计算SuperTrend
        self.calculate_supertrend()
        
        # 2. 计算QQE MOD
        self.calculate_qqe_mod()
        
        # 3. 计算Trend A-V2
        self.calculate_trend_a()
    
    def calculate_supertrend(self):
        """计算SuperTrend指标"""
        am = self.am
        
        # 计算ATR
        atr = am.atr(self.supertrend_length, array=True)
        
        # 计算HL2
        hl2 = (am.high_array + am.low_array) / 2
        
        # 计算基础上下轨
        basic_upper = hl2 + (self.supertrend_multiplier * atr)
        basic_lower = hl2 - (self.supertrend_multiplier * atr)
        
        # 计算最终上下轨（简化版，只取最后几个值）
        n = min(100, am.size)
        upper_band = np.zeros(n)
        lower_band = np.zeros(n)
        supertrend = np.zeros(n)
        direction = np.zeros(n, dtype=int)
        
        upper_band[0] = basic_upper[-n]
        lower_band[0] = basic_lower[-n]
        direction[0] = -1
        supertrend[0] = upper_band[0]
        
        for i in range(1, n):
            idx = -n + i
            
            # 上轨
            if basic_upper[idx] < upper_band[i-1] or am.close_array[idx-1] > upper_band[i-1]:
                upper_band[i] = basic_upper[idx]
            else:
                upper_band[i] = upper_band[i-1]
            
            # 下轨
            if basic_lower[idx] > lower_band[i-1] or am.close_array[idx-1] < lower_band[i-1]:
                lower_band[i] = basic_lower[idx]
            else:
                lower_band[i] = lower_band[i-1]
            
            # SuperTrend和方向
            if supertrend[i-1] == upper_band[i-1]:
                if am.close_array[idx] > upper_band[i]:
                    direction[i] = 1
                    supertrend[i] = lower_band[i]
                else:
                    direction[i] = -1
                    supertrend[i] = upper_band[i]
            else:
                if am.close_array[idx] < lower_band[i]:
                    direction[i] = -1
                    supertrend[i] = upper_band[i]
                else:
                    direction[i] = 1
                    supertrend[i] = lower_band[i]
        
        self.supertrend_value = supertrend[-1]
        self.supertrend_direction = int(direction[-1])
    
    def calculate_qqe_mod(self):
        """计算QQE MOD指标"""
        am = self.am
        
        # 计算RSI
        rsi = am.rsi(self.qqe_rsi_length, array=True)
        
        # 平滑RSI
        smoothed_rsi = self._ema(rsi, self.qqe_rsi_smoothing)
        
        # 计算ATR-like指标
        atr_rsi = np.abs(np.diff(smoothed_rsi))
        atr_rsi = np.insert(atr_rsi, 0, 0)
        
        wilders_length = self.qqe_rsi_length * 2 - 1
        smoothed_atr_rsi = self._ema(atr_rsi, wilders_length)
        dynamic_atr_rsi = smoothed_atr_rsi * self.qqe_factor
        
        # 计算QQE bands（简化版）
        n = min(100, len(smoothed_rsi))
        long_band = smoothed_rsi[-n:] - dynamic_atr_rsi[-n:]
        short_band = smoothed_rsi[-n:] + dynamic_atr_rsi[-n:]
        
        # 计算信号（简化版）
        # 计算布林带
        qqe_primary_centered = smoothed_rsi - 50
        bollinger_basis = self._sma(qqe_primary_centered, self.qqe_bollinger_length)
        bollinger_std = self._std(qqe_primary_centered, self.qqe_bollinger_length)
        bollinger_upper = bollinger_basis + self.qqe_bollinger_mult * bollinger_std
        bollinger_lower = bollinger_basis - self.qqe_bollinger_mult * bollinger_std
        
        # 生成信号
        current_rsi = smoothed_rsi[-1]
        if (current_rsi - 50 > self.qqe_threshold) and ((current_rsi - 50) > bollinger_upper[-1]):
            self.qqe_signal = 1  # 看多
        elif (current_rsi - 50 < -self.qqe_threshold) and ((current_rsi - 50) < bollinger_lower[-1]):
            self.qqe_signal = -1  # 看空
        else:
            self.qqe_signal = 0  # 中性
    
    def calculate_trend_a(self):
        """计算Trend A-V2指标"""
        am = self.am
        
        # 计算Heikin Ashi
        ha_close = (am.open_array + am.high_array + am.low_array + am.close_array) / 4
        
        n = am.size
        ha_open = np.zeros(n)
        ha_open[0] = (am.open_array[0] + am.close_array[0]) / 2
        
        for i in range(1, n):
            ha_open[i] = (ha_open[i-1] + ha_close[i-1]) / 2
        
        ha_high = np.maximum(am.high_array, np.maximum(ha_open, ha_close))
        ha_low = np.minimum(am.low_array, np.minimum(ha_open, ha_close))
        
        # 使用EMA平滑
        trend_a_open = self._ema(ha_open, self.trend_a_period)
        trend_a_close = self._ema(ha_close, self.trend_a_period)
        trend_a_high = self._ema(ha_high, self.trend_a_period)
        trend_a_low = self._ema(ha_low, self.trend_a_period)
        
        # 计算趋势强度
        trend_a_strength = 100 * (trend_a_close - trend_a_open) / (trend_a_high - trend_a_low + 1e-9)
        
        # 趋势方向
        if trend_a_strength[-1] > 0:
            self.trend_a_direction = 1
        else:
            self.trend_a_direction = -1
    
    def generate_combined_signal(self):
        """生成综合判断信号"""
        # 将SuperTrend方向转换为信号名称
        if self.supertrend_direction == 1:
            supertrend_signal = "买入" if not hasattr(self, 'last_supertrend_signal') or self.last_supertrend_signal != "买入" else "持有"
        else:
            supertrend_signal = "卖出" if not hasattr(self, 'last_supertrend_signal') or self.last_supertrend_signal != "卖出" else "谨慎观望"
        
        self.last_supertrend_signal = supertrend_signal
        
        # QQE信号
        qqe_signal_name = "看多" if self.qqe_signal == 1 else ("看空" if self.qqe_signal == -1 else "中性")
        
        # Trend A信号
        trend_a_signal = "上升" if self.trend_a_direction == 1 else "下降"
        
        # 综合判断逻辑（与data_trend.py一致）
        if supertrend_signal in ["持有", "谨慎观望"]:
            self.综合判断 = supertrend_signal
        elif supertrend_signal == "买入" and qqe_signal_name == "看多" and trend_a_signal == "上升":
            self.综合判断 = "买入信号"
        elif supertrend_signal == "卖出" and qqe_signal_name == "看空" and trend_a_signal == "下降":
            self.综合判断 = "卖出信号"
        elif supertrend_signal == "买入" or qqe_signal_name == "看多" or trend_a_signal == "上升":
            self.综合判断 = "看多信号"
        elif supertrend_signal == "卖出" or qqe_signal_name == "看空" or trend_a_signal == "下降":
            self.综合判断 = "看空信号"
        else:
            self.综合判断 = "中性"
    
    def execute_trading_logic(self, bar: BarData):
        """执行交易逻辑"""
        close_price = bar.close_price
        
        # 初始化资金计算（第一次执行时）
        if self.initial_capital == 0.0:
            # 在回测中，我们使用一个参考资金来计算比例
            # 这个值应该与回测设置的初始资金一致
            # 为了保证策略的收益率不受初始资金影响，我们使用固定参考资金
            self.initial_capital = 100000.0  # 参考资金100,000元
            self.grid_amount_per_unit = self.initial_capital * (self.grid_amount_pct / 100.0)
            self.cash = self.initial_capital  # ✅ 初始化现金
            self.write_log(f"初始化资金计算: 参考资金={self.initial_capital}, 每份网格金额={self.grid_amount_per_unit:.2f}, 初始现金={self.cash:.2f}")
        
        # 检查趋势信号
        trend_buy_signal = self.综合判断 in ["买入信号"]
        trend_sell_signal = self.综合判断 in ["卖出信号"]
        
        # 计算当前持仓（网格持仓 + 趋势持仓）
        grid_position = self.grid_units * (self.grid_amount_per_unit / (self.grid_buy_prices[0] if self.grid_buy_prices else close_price))
        total_position = grid_position + self.trend_position
        
        # 趋势状态处理
        if self.trend_active and trend_sell_signal:
            # 卖出趋势持仓
            if self.trend_position > 0:
                sell_position = self.trend_position
                profit = (close_price - self.trend_buy_price) * sell_position
                sell_amount = sell_position * close_price
                self.sell(close_price, sell_position)
                
                # ✅ 卖出后，现金增加
                self.cash += sell_amount
                
                # 统计追踪
                self.trend_profit += profit
                self.total_trades += 1
                self.trend_trade_count += 1
                if profit > 0:
                    self.winning_trades += 1
                
                # ✅ 确保完全清零
                self.trend_position = 0
                self.trend_buy_price = 0
                self.trend_active = False
                self.grid_reference_price = close_price
                self.write_log(f"趋势卖出信号，卖出{sell_position:.2f}手，卖出金额={sell_amount:.2f}，盈亏: {profit:.2f}，现金={self.cash:.2f}，恢复网格策略")
            else:
                # 异常情况：趋势活跃但没有持仓
                self.trend_active = False
                self.write_log(f"⚠️ 趋势状态异常：活跃但无持仓，重置状态")
            return
        
        # 非趋势状态，检查趋势买入信号
        if not self.trend_active and trend_buy_signal:
            # ✅ 趋势买入：使用 **所有剩余现金**（与原始策略一致）
            if self.cash > 0:
                trend_volume = self.cash / close_price
                self.buy(close_price, trend_volume)
                self.trend_position = trend_volume
                self.trend_buy_price = close_price
                self.trend_active = True
                actual_amount = self.cash
                self.cash = 0  # ✅ 现金全部用于趋势买入
                self.write_log(f"趋势买入信号，使用所有剩余现金{actual_amount:.2f}，买入{trend_volume:.2f}手 @ {close_price:.2f}")
            else:
                self.write_log(f"⚠️ 趋势买入信号，但无剩余现金，跳过")
            return
        
        # 趋势状态下不执行网格交易
        if self.trend_active:
            return
        
        # === 网格策略逻辑 ===
        
        # 1. 初始底仓
        if not self.initial_grid_bought and self.grid_units == 0:
            # ✅ 检查现金是否足够
            if self.cash < self.grid_amount_per_unit:
                self.write_log(f"⚠️ 初始底仓买入失败：现金不足({self.cash:.2f} < {self.grid_amount_per_unit:.2f})")
                return
            
            buy_volume = self.grid_amount_per_unit / close_price
            self.buy(close_price, buy_volume)
            self.grid_units += 1
            self.grid_buy_prices.append(close_price)
            self.grid_reference_price = close_price
            self.initial_grid_bought = True
            actual_amount = buy_volume * close_price
            self.cash -= self.grid_amount_per_unit
            self.write_log(f"网格买入初始底仓: {buy_volume:.2f}手 @ {close_price:.2f}, 金额={actual_amount:.2f}, 剩余现金={self.cash:.2f}")
            return
        
        # 2. 网格买入逻辑（只在未达到最大份数时买入）
        if self.grid_units < self.max_hold_units and self.grid_reference_price > 0:
            # 更新参考价格（如果价格创新高）
            if close_price > self.grid_reference_price:
                self.grid_reference_price = close_price
            
            # 计算目标买入价格
            target_buy_price = self.grid_reference_price * (1 - self.grid_size_pct / 100)
            
            # 检查买入条件
            if close_price <= target_buy_price:
                # ✅ 检查现金是否足够
                if self.cash < self.grid_amount_per_unit:
                    self.write_log(f"⚠️ 网格买入信号，但现金不足({self.cash:.2f} < {self.grid_amount_per_unit:.2f})，跳过")
                    return
                
                buy_volume = self.grid_amount_per_unit / close_price
                self.buy(close_price, buy_volume)
                self.grid_units += 1
                self.grid_buy_prices.append(close_price)
                self.grid_reference_price = close_price
                actual_amount = buy_volume * close_price
                self.cash -= self.grid_amount_per_unit
                self.write_log(f"网格买入: {buy_volume:.2f}手 @ {close_price:.2f}, 份数: {self.grid_units}/{self.max_hold_units}, 金额={actual_amount:.2f}, 剩余现金={self.cash:.2f}")
        elif self.grid_units >= self.max_hold_units:
            # 已达到最大份数，不再买入（静默，不输出日志以避免刷屏）
            pass
        
        # 3. 网格卖出逻辑
        if self.grid_units > self.min_hold_units and self.grid_buy_prices:
            prices_to_remove = []
            
            for buy_price in self.grid_buy_prices:
                target_sell_price = buy_price * (1 + self.required_profit_pct / 100)
                
                if close_price >= target_sell_price:
                    sell_volume = self.grid_amount_per_unit / buy_price
                    sell_amount = sell_volume * close_price
                    profit = (close_price - buy_price) * sell_volume
                    self.sell(close_price, sell_volume)
                    
                    # ✅ 卖出后，现金增加
                    self.cash += sell_amount
                    
                    # 统计追踪
                    self.grid_profit += profit
                    self.total_trades += 1
                    self.grid_trade_count += 1
                    if profit > 0:
                        self.winning_trades += 1
                    
                    self.grid_units -= 1
                    prices_to_remove.append(buy_price)
                    profit_pct = (close_price - buy_price) / buy_price * 100
                    self.write_log(f"网格卖出: {sell_volume:.2f}手 @ {close_price:.2f}, 盈利: {profit_pct:.2f}%, 收益: {profit:.2f}, 份数: {self.grid_units}, 现金={self.cash:.2f}")
                    break  # 每次只卖出一份
            
            # 移除已卖出的价格记录
            for price in prices_to_remove:
                if price in self.grid_buy_prices:
                    self.grid_buy_prices.remove(price)
    
    def on_order(self, order: OrderData):
        """委托回报"""
        pass
    
    def on_trade(self, trade: TradeData):
        """成交回报"""
        self.put_event()
    
    def on_stop_order(self, stop_order: StopOrder):
        """停止单回报"""
        pass
    
    # === 辅助函数 ===
    
    def _ema(self, data: np.ndarray, period: int) -> np.ndarray:
        """计算EMA"""
        alpha = 2.0 / (period + 1)
        ema = np.zeros_like(data)
        ema[0] = data[0]
        
        for i in range(1, len(data)):
            ema[i] = alpha * data[i] + (1 - alpha) * ema[i-1]
        
        return ema
    
    def _sma(self, data: np.ndarray, period: int) -> np.ndarray:
        """计算SMA"""
        if len(data) < period:
            return data
        
        result = np.zeros_like(data)
        result[:period-1] = data[:period-1]
        
        for i in range(period-1, len(data)):
            result[i] = np.mean(data[i-period+1:i+1])
        
        return result
    
    def _std(self, data: np.ndarray, period: int) -> np.ndarray:
        """计算标准差"""
        if len(data) < period:
            return np.zeros_like(data)
        
        result = np.zeros_like(data)
        result[:period-1] = 0
        
        for i in range(period-1, len(data)):
            result[i] = np.std(data[i-period+1:i+1])
        
        return result
    
    def output_statistics(self):
        """输出详细统计信息"""
        self.write_log("=" * 60)
        self.write_log("策略统计报告")
        self.write_log("=" * 60)
        
        # 基础统计
        win_rate = (self.winning_trades / self.total_trades * 100) if self.total_trades > 0 else 0
        self.write_log(f"总交易次数: {self.total_trades}")
        self.write_log(f"盈利交易次数: {self.winning_trades}")
        self.write_log(f"胜率: {win_rate:.2f}%")
        
        # 网格统计
        self.write_log(f"\n网格策略统计:")
        self.write_log(f"  网格交易次数: {self.grid_trade_count}")
        self.write_log(f"  网格累计收益: {self.grid_profit:.2f}")
        self.write_log(f"  当前网格份数: {self.grid_units}")
        
        # 趋势统计
        self.write_log(f"\n趋势策略统计:")
        self.write_log(f"  趋势交易次数: {self.trend_trade_count}")
        self.write_log(f"  趋势累计收益: {self.trend_profit:.2f}")
        self.write_log(f"  当前趋势状态: {'活跃' if self.trend_active else '非活跃'}")
        
        # 总收益
        total_profit = self.grid_profit + self.trend_profit
        self.write_log(f"\n总收益: {total_profit:.2f}")
        if total_profit != 0:
            self.write_log(f"  网格贡献: {(self.grid_profit/total_profit*100):.2f}%")
            self.write_log(f"  趋势贡献: {(self.trend_profit/total_profit*100):.2f}%")
        else:
            self.write_log(f"  网格贡献: N/A")
            self.write_log(f"  趋势贡献: N/A")
        
        self.write_log("=" * 60)

