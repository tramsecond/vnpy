# -*- coding: utf-8 -*-
"""
改进的回测引擎 - 参考VeighNa架构设计
支持多种策略、参数优化、性能分析和可视化
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import json
from pathlib import Path


class OrderType(Enum):
    """订单类型"""
    MARKET = "市价单"
    LIMIT = "限价单"


class OrderStatus(Enum):
    """订单状态"""
    SUBMITTED = "已提交"
    FILLED = "已成交"
    CANCELLED = "已撤销"


class PositionSide(Enum):
    """持仓方向"""
    LONG = "多头"
    SHORT = "空头"
    NONE = "空仓"


@dataclass
class BarData:
    """K线数据"""
    symbol: str
    datetime: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    turnover: float = 0.0
    open_interest: float = 0.0
    
    # 技术指标字段（可选）
    indicators: Dict = field(default_factory=dict)


@dataclass
class OrderData:
    """订单数据"""
    order_id: str
    symbol: str
    order_type: OrderType
    direction: str  # "买入" or "卖出"
    price: float
    volume: int
    traded: int = 0
    status: OrderStatus = OrderStatus.SUBMITTED
    datetime: datetime = None


@dataclass
class TradeData:
    """成交数据"""
    trade_id: str
    order_id: str
    symbol: str
    direction: str
    price: float
    volume: int
    datetime: datetime
    commission: float = 0.0


@dataclass
class PositionData:
    """持仓数据"""
    symbol: str
    side: PositionSide
    volume: int
    avg_price: float
    current_price: float
    pnl: float = 0.0
    pnl_pct: float = 0.0


class BacktestEngine:
    """
    回测引擎基类
    参考VeighNa的CtaBacktester设计，但简化并适配Excel数据源
    """
    
    def __init__(
        self,
        strategy_class,
        symbol: str,
        data_path: str,
        start_date: str,
        end_date: str,
        initial_capital: float = 100000,
        commission_rate: float = 0.0003,
        slippage: float = 0.0,
        size_multiplier: int = 1,
        price_tick: float = 0.01
    ):
        """
        初始化回测引擎
        
        Parameters:
        -----------
        strategy_class : class
            策略类（需继承StrategyTemplate）
        symbol : str
            交易标的代码
        data_path : str
            数据文件路径（Excel格式）
        start_date : str
            回测开始日期 "YYYY-MM-DD"
        end_date : str
            回测结束日期 "YYYY-MM-DD"
        initial_capital : float
            初始资金
        commission_rate : float
            手续费率（双边，如0.0003表示万三）
        slippage : float
            滑点（固定值）
        size_multiplier : int
            合约乘数（股票为1）
        price_tick : float
            最小价格变动单位
        """
        self.strategy_class = strategy_class
        self.symbol = symbol
        self.data_path = data_path
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.initial_capital = initial_capital
        self.commission_rate = commission_rate
        self.slippage = slippage
        self.size_multiplier = size_multiplier
        self.price_tick = price_tick
        
        # 回测状态
        self.capital = initial_capital
        self.position = PositionData(
            symbol=symbol,
            side=PositionSide.NONE,
            volume=0,
            avg_price=0.0,
            current_price=0.0
        )
        
        # 记录数据
        self.orders: List[OrderData] = []
        self.trades: List[TradeData] = []
        self.daily_results: List[Dict] = []
        
        # 策略实例
        self.strategy = None
        
        # 数据
        self.bars: List[BarData] = []
        self.current_bar: BarData = None
        self.bar_index: int = 0
        
        # ID生成器
        self.order_count = 0
        self.trade_count = 0
    
    def load_data(self, sheet_name: str = "日线") -> bool:
        """
        从Excel加载数据
        
        Parameters:
        -----------
        sheet_name : str
            工作表名称（日线/周线/月线/小时线）
        """
        try:
            df = pd.read_excel(self.data_path, sheet_name=sheet_name)
            
            # 确保日期列存在
            if '日期' not in df.columns:
                print(f"错误：Excel文件中没有'日期'列")
                return False
            
            # 转换日期格式
            df['日期'] = pd.to_datetime(df['日期'])
            
            # 过滤日期范围
            df = df[(df['日期'] >= self.start_date) & (df['日期'] <= self.end_date)]
            
            if len(df) == 0:
                print(f"警告：日期范围内没有数据")
                return False
            
            # 转换为BarData对象
            for _, row in df.iterrows():
                bar = BarData(
                    symbol=self.symbol,
                    datetime=row['日期'],
                    open=row.get('开盘', row.get('收盘', 0)),
                    high=row.get('最高', row.get('收盘', 0)),
                    low=row.get('最低', row.get('收盘', 0)),
                    close=row['收盘'],
                    volume=row.get('成交量', 0),
                    turnover=row.get('成交额', 0)
                )
                
                # 添加所有技术指标到indicators字典
                indicators = {}
                for col in df.columns:
                    if col not in ['日期', '开盘', '最高', '最低', '收盘', '成交量', '成交额']:
                        indicators[col] = row[col]
                bar.indicators = indicators
                
                self.bars.append(bar)
            
            print(f"成功加载 {len(self.bars)} 根K线数据")
            return True
            
        except Exception as e:
            print(f"加载数据失败: {e}")
            return False
    
    def send_order(
        self,
        direction: str,
        price: float,
        volume: int,
        order_type: OrderType = OrderType.MARKET
    ) -> str:
        """
        发送订单
        
        Parameters:
        -----------
        direction : str
            "买入" or "卖出"
        price : float
            价格（市价单时忽略）
        volume : int
            数量
        order_type : OrderType
            订单类型
        
        Returns:
        --------
        order_id : str
            订单ID
        """
        self.order_count += 1
        order_id = f"ORDER_{self.order_count}"
        
        order = OrderData(
            order_id=order_id,
            symbol=self.symbol,
            order_type=order_type,
            direction=direction,
            price=price,
            volume=volume,
            datetime=self.current_bar.datetime
        )
        
        self.orders.append(order)
        
        # 立即撮合成交（简化处理）
        self._match_order(order)
        
        return order_id
    
    def _match_order(self, order: OrderData):
        """撮合订单"""
        # 计算成交价格（考虑滑点）
        if order.direction == "买入":
            trade_price = self.current_bar.close + self.slippage
        else:
            trade_price = self.current_bar.close - self.slippage
        
        # 创建成交记录
        self.trade_count += 1
        trade = TradeData(
            trade_id=f"TRADE_{self.trade_count}",
            order_id=order.order_id,
            symbol=self.symbol,
            direction=order.direction,
            price=trade_price,
            volume=order.volume,
            datetime=self.current_bar.datetime,
            commission=trade_price * order.volume * self.commission_rate
        )
        
        self.trades.append(trade)
        
        # 更新订单状态
        order.status = OrderStatus.FILLED
        order.traded = order.volume
        
        # 更新持仓
        self._update_position(trade)
    
    def _update_position(self, trade: TradeData):
        """更新持仓"""
        if trade.direction == "买入":
            if self.position.side == PositionSide.NONE:
                # 开仓
                self.position.side = PositionSide.LONG
                self.position.volume = trade.volume
                self.position.avg_price = trade.price
                self.capital -= trade.price * trade.volume + trade.commission
            elif self.position.side == PositionSide.LONG:
                # 加仓
                total_cost = self.position.avg_price * self.position.volume + trade.price * trade.volume
                self.position.volume += trade.volume
                self.position.avg_price = total_cost / self.position.volume
                self.capital -= trade.price * trade.volume + trade.commission
        
        elif trade.direction == "卖出":
            if self.position.side == PositionSide.LONG:
                if trade.volume >= self.position.volume:
                    # 平仓
                    pnl = (trade.price - self.position.avg_price) * self.position.volume - trade.commission
                    self.capital += trade.price * self.position.volume - trade.commission
                    self.position.side = PositionSide.NONE
                    self.position.volume = 0
                    self.position.avg_price = 0.0
                else:
                    # 减仓
                    pnl = (trade.price - self.position.avg_price) * trade.volume - trade.commission
                    self.capital += trade.price * trade.volume - trade.commission
                    self.position.volume -= trade.volume
    
    def run_backtest(self, strategy_params: Dict = None) -> Dict:
        """
        运行回测
        
        Parameters:
        -----------
        strategy_params : Dict
            策略参数字典
        
        Returns:
        --------
        results : Dict
            回测结果统计
        """
        if not self.bars:
            print("错误：未加载数据，请先调用load_data()")
            return {}
        
        # 初始化策略
        self.strategy = self.strategy_class(
            engine=self,
            params=strategy_params or {}
        )
        self.strategy.on_init()
        
        print(f"\n开始回测: {self.symbol}")
        print(f"策略: {self.strategy_class.__name__}")
        print(f"数据范围: {self.bars[0].datetime} 至 {self.bars[-1].datetime}")
        print(f"K线数量: {len(self.bars)}")
        print(f"初始资金: {self.initial_capital:,.2f}\n")
        
        # 遍历K线
        for i, bar in enumerate(self.bars):
            self.bar_index = i
            self.current_bar = bar
            
            # 更新持仓价格和盈亏
            if self.position.side == PositionSide.LONG:
                self.position.current_price = bar.close
                self.position.pnl = (bar.close - self.position.avg_price) * self.position.volume
                self.position.pnl_pct = (bar.close - self.position.avg_price) / self.position.avg_price * 100
            
            # 调用策略的on_bar
            self.strategy.on_bar(bar)
            
            # 记录每日结果
            self._record_daily_result(bar)
        
        # 计算回测统计
        results = self._calculate_statistics()
        
        # 调用策略的on_stop
        self.strategy.on_stop()
        
        return results
    
    def _record_daily_result(self, bar: BarData):
        """记录每日结果"""
        # 计算总资产
        position_value = 0
        if self.position.side == PositionSide.LONG:
            position_value = self.position.volume * bar.close
        
        total_value = self.capital + position_value
        
        # 记录
        daily_result = {
            'date': bar.datetime,
            'close': bar.close,
            'capital': self.capital,
            'position_value': position_value,
            'total_value': total_value,
            'position_volume': self.position.volume if self.position.side == PositionSide.LONG else 0,
            'position_pnl': self.position.pnl if self.position.side == PositionSide.LONG else 0,
            'return_pct': (total_value - self.initial_capital) / self.initial_capital * 100
        }
        
        self.daily_results.append(daily_result)
    
    def _calculate_statistics(self) -> Dict:
        """计算回测统计指标"""
        if not self.daily_results:
            return {}
        
        df_daily = pd.DataFrame(self.daily_results)
        
        # 基础指标
        final_value = df_daily['total_value'].iloc[-1]
        total_return = (final_value - self.initial_capital) / self.initial_capital * 100
        
        # 交易统计
        winning_trades = [t for t in self.trades if t.direction == "卖出"]
        if winning_trades:
            # 计算每笔交易的盈亏（简化：假设按买入卖出配对）
            buy_trades = [t for t in self.trades if t.direction == "买入"]
            sell_trades = [t for t in self.trades if t.direction == "卖出"]
            
            trade_pnls = []
            for i in range(min(len(buy_trades), len(sell_trades))):
                pnl = (sell_trades[i].price - buy_trades[i].price) * sell_trades[i].volume
                pnl -= buy_trades[i].commission + sell_trades[i].commission
                trade_pnls.append(pnl)
            
            winning_count = len([p for p in trade_pnls if p > 0])
            losing_count = len([p for p in trade_pnls if p <= 0])
            win_rate = winning_count / len(trade_pnls) * 100 if trade_pnls else 0
        else:
            winning_count = 0
            losing_count = 0
            win_rate = 0
            trade_pnls = []
        
        # 最大回撤
        df_daily['peak'] = df_daily['total_value'].cummax()
        df_daily['drawdown'] = (df_daily['total_value'] - df_daily['peak']) / df_daily['peak'] * 100
        max_drawdown = df_daily['drawdown'].min()
        
        # 年化收益率
        days = (df_daily['date'].iloc[-1] - df_daily['date'].iloc[0]).days
        years = days / 365.0
        annualized_return = ((final_value / self.initial_capital) ** (1 / years) - 1) * 100 if years > 0 else 0
        
        # 夏普比率（简化计算，假设无风险利率为3%）
        df_daily['daily_return'] = df_daily['total_value'].pct_change()
        daily_return_std = df_daily['daily_return'].std()
        avg_daily_return = df_daily['daily_return'].mean()
        risk_free_rate = 0.03 / 252  # 年化3%转换为日收益率
        sharpe_ratio = (avg_daily_return - risk_free_rate) / daily_return_std * np.sqrt(252) if daily_return_std > 0 else 0
        
        # 统计结果
        stats = {
            '策略名称': self.strategy_class.__name__,
            '标的代码': self.symbol,
            '回测周期': f"{df_daily['date'].iloc[0].strftime('%Y-%m-%d')} 至 {df_daily['date'].iloc[-1].strftime('%Y-%m-%d')}",
            '初始资金': self.initial_capital,
            '最终资金': final_value,
            '总收益': final_value - self.initial_capital,
            '总收益率(%)': round(total_return, 2),
            '年化收益率(%)': round(annualized_return, 2),
            '最大回撤(%)': round(max_drawdown, 2),
            '夏普比率': round(sharpe_ratio, 2),
            '交易次数': len(self.trades) // 2,  # 买卖算一次完整交易
            '盈利次数': winning_count,
            '亏损次数': losing_count,
            '胜率(%)': round(win_rate, 2),
            '日收益数据': df_daily
        }
        
        return stats
    
    def optimize_parameters(
        self,
        param_ranges: Dict[str, List],
        target_metric: str = '总收益率(%)',
        optimize_mode: str = 'max'
    ) -> List[Dict]:
        """
        参数优化
        
        Parameters:
        -----------
        param_ranges : Dict[str, List]
            参数范围字典，如 {'fast_ma': [5, 10, 20], 'slow_ma': [30, 60]}
        target_metric : str
            优化目标指标
        optimize_mode : str
            优化模式 'max' 或 'min'
        
        Returns:
        --------
        results : List[Dict]
            所有参数组合的回测结果
        """
        import itertools
        
        print(f"\n开始参数优化...")
        print(f"优化目标: {target_metric} ({optimize_mode})")
        
        # 生成所有参数组合
        param_names = list(param_ranges.keys())
        param_values = list(param_ranges.values())
        param_combinations = list(itertools.product(*param_values))
        
        print(f"参数组合数量: {len(param_combinations)}\n")
        
        results = []
        for i, params in enumerate(param_combinations, 1):
            param_dict = dict(zip(param_names, params))
            
            # 重置引擎状态
            self._reset_engine()
            
            # 运行回测
            stats = self.run_backtest(param_dict)
            
            if stats:
                stats['参数'] = param_dict
                results.append(stats)
                
                print(f"[{i}/{len(param_combinations)}] 参数: {param_dict} => {target_metric}: {stats.get(target_metric, 'N/A')}")
        
        # 排序结果
        if optimize_mode == 'max':
            results.sort(key=lambda x: x.get(target_metric, float('-inf')), reverse=True)
        else:
            results.sort(key=lambda x: x.get(target_metric, float('inf')))
        
        print(f"\n优化完成！最佳参数: {results[0]['参数']}")
        print(f"最佳{target_metric}: {results[0][target_metric]}")
        
        return results
    
    def _reset_engine(self):
        """重置引擎状态"""
        self.capital = self.initial_capital
        self.position = PositionData(
            symbol=self.symbol,
            side=PositionSide.NONE,
            volume=0,
            avg_price=0.0,
            current_price=0.0
        )
        self.orders = []
        self.trades = []
        self.daily_results = []
        self.order_count = 0
        self.trade_count = 0
        self.bar_index = 0


class StrategyTemplate:
    """
    策略模板基类
    所有策略需要继承此类并实现相应方法
    """
    
    def __init__(self, engine: BacktestEngine, params: Dict):
        """
        初始化策略
        
        Parameters:
        -----------
        engine : BacktestEngine
            回测引擎实例
        params : Dict
            策略参数字典
        """
        self.engine = engine
        self.params = params
        
        # 持仓标志
        self.pos = 0  # 0: 空仓, >0: 持仓数量
    
    def on_init(self):
        """
        策略初始化回调
        可用于加载历史数据、初始化指标等
        """
        pass
    
    def on_bar(self, bar: BarData):
        """
        K线数据回调
        
        Parameters:
        -----------
        bar : BarData
            当前K线数据
        """
        pass
    
    def on_stop(self):
        """
        策略停止回调
        可用于清理资源、保存结果等
        """
        pass
    
    def buy(self, price: float, volume: int):
        """买入"""
        if self.pos == 0:  # 只在空仓时买入
            self.engine.send_order("买入", price, volume)
            self.pos = volume
    
    def sell(self, price: float, volume: int):
        """卖出"""
        if self.pos > 0:  # 只在持仓时卖出
            self.engine.send_order("卖出", price, min(volume, self.pos))
            self.pos = 0
    
    def get_indicator(self, name: str, default=None):
        """获取当前K线的技术指标值"""
        if self.engine.current_bar:
            return self.engine.current_bar.indicators.get(name, default)
        return default


# 工具函数
def save_backtest_results(results: Dict, output_path: str):
    """保存回测结果到Excel"""
    import xlsxwriter
    
    workbook = xlsxwriter.Workbook(output_path)
    
    # 格式定义
    header_format = workbook.add_format({
        'bold': True,
        'bg_color': '#4472C4',
        'font_color': 'white',
        'border': 1
    })
    
    number_format = workbook.add_format({'num_format': '#,##0.00'})
    percent_format = workbook.add_format({'num_format': '0.00%'})
    date_format = workbook.add_format({'num_format': 'yyyy-mm-dd'})
    
    # 1. 统计摘要
    ws_summary = workbook.add_worksheet('统计摘要')
    ws_summary.set_column('A:A', 20)
    ws_summary.set_column('B:B', 20)
    
    row = 0
    ws_summary.write(row, 0, '指标', header_format)
    ws_summary.write(row, 1, '数值', header_format)
    
    for key, value in results.items():
        if key != '日收益数据':
            row += 1
            ws_summary.write(row, 0, key)
            if isinstance(value, (int, float)):
                ws_summary.write(row, 1, value, number_format)
            else:
                ws_summary.write(row, 1, str(value))
    
    # 2. 每日收益曲线
    if '日收益数据' in results:
        df_daily = results['日收益数据']
        ws_daily = workbook.add_worksheet('每日收益')
        
        # 写入表头
        for col, column_name in enumerate(df_daily.columns):
            ws_daily.write(0, col, column_name, header_format)
        
        # 写入数据
        for row_idx, row_data in df_daily.iterrows():
            for col_idx, value in enumerate(row_data):
                if col_idx == 0:  # 日期列
                    ws_daily.write(row_idx + 1, col_idx, value, date_format)
                else:
                    ws_daily.write(row_idx + 1, col_idx, value, number_format)
    
    workbook.close()
    print(f"\n回测结果已保存至: {output_path}")


if __name__ == "__main__":
    print("回测引擎模块 - 使用示例请参考 backtest_examples.py")
