"""
严格遵循单标的回测逻辑的多标的回测工具
完全使用BacktesterEngine的标准流程，确保结果与单标的回测完全一致
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from copy import copy

from PySide6 import QtWidgets, QtCore, QtGui

# 导入pyqtgraph用于图表显示（与单标的回测使用相同的图表组件）
import pyqtgraph as pg

from vnpy.trader.ui import create_qapp
from vnpy.trader.constant import Interval, Exchange
from vnpy.trader.engine import MainEngine
from vnpy.event import EventEngine

# 导入BacktesterEngine（单标的回测引擎）和图表组件
from vnpy_ctabacktester.engine import BacktesterEngine
from vnpy_ctabacktester.ui.widget import BacktesterChart
from vnpy_ctastrategy.backtesting import BacktestingMode

# 可用的标的列表
AVAILABLE_SYMBOLS = [
    ("000001", Exchange.SSE, "上证指数"),
    ("399001", Exchange.SZSE, "深证成指"),
    ("399006", Exchange.SZSE, "创业板指"),
    ("000688", Exchange.SSE, "科创50"),
    ("899050", Exchange.BSE, "北证50"),
    ("HSI", Exchange.GLOBAL, "恒生指数"),
    ("HSTECH", Exchange.GLOBAL, "恒生科技"),
    ("GSPC", Exchange.GLOBAL, "标普500"),
    ("IXIC", Exchange.GLOBAL, "纳斯达克"),
    ("DJI", Exchange.GLOBAL, "道琼斯"),
]

# 默认参数（与单标的回测保持一致）
# 默认日期：当前日期往前3年
_end_dt = datetime.now()
_start_dt = _end_dt - timedelta(days=3 * 365)
DEFAULT_START = _start_dt
DEFAULT_END = _end_dt
DEFAULT_RATE = 0.000025  # 与单标的回测一致
DEFAULT_SLIPPAGE = 0.2   # 与单标的回测一致
DEFAULT_SIZE = 300       # 与单标的回测一致
DEFAULT_PRICETICK = 0.2  # 与单标的回测一致
DEFAULT_CAPITAL = 1_000_000


class SingleBacktestResultWidget(QtWidgets.QWidget):
    """单个标的的回测结果页签 - 使用与单标的回测相同的图表组件"""
    
    def __init__(self, vt_symbol: str, symbol_name: str, statistics: dict, df=None, parent=None):
        super().__init__(parent)
        self.vt_symbol = vt_symbol
        self.symbol_name = symbol_name
        self.statistics = statistics
        self.df = df
        
        self.init_ui()
        self.display_results()
    
    def init_ui(self):
        """初始化UI - 使用与单标的回测相同的布局"""
        # 左侧：统计信息（使用表格，更紧凑）
        self.statistics_monitor = self.create_statistics_table()
        
        left_vbox = QtWidgets.QVBoxLayout()
        title = QtWidgets.QLabel(f"{self.symbol_name} ({self.vt_symbol})")
        title.setStyleSheet("font-size: 14px; font-weight: bold;")
        left_vbox.addWidget(title)
        left_vbox.addWidget(self.statistics_monitor)
        
        left_widget = QtWidgets.QWidget()
        left_widget.setLayout(left_vbox)
        left_widget.setFixedWidth(220)  # 固定左侧宽度为220px，给图表更多空间
        
        # 右侧：使用与单标的回测相同的BacktesterChart
        self.chart = BacktesterChart()
        
        right_vbox = QtWidgets.QHBoxLayout()
        right_vbox.addWidget(self.chart)
        
        right_widget = QtWidgets.QWidget()
        right_widget.setLayout(right_vbox)
        
        # 主布局：左右分布（与单标的回测相同）
        hbox = QtWidgets.QHBoxLayout()
        hbox.addWidget(left_widget)
        hbox.addWidget(right_widget)
        
        self.setLayout(hbox)
    
    def create_statistics_table(self):
        """创建统计信息表格"""
        table = QtWidgets.QTableWidget()
        table.setColumnCount(2)
        table.setHorizontalHeaderLabels(["指标", "数值"])
        table.horizontalHeader().setStretchLastSection(True)
        table.horizontalHeader().setMinimumSectionSize(60)  # 第一列最小60px
        table.verticalHeader().setVisible(False)
        table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        table.setShowGrid(False)  # 隐藏网格线，更简洁
        table.setAlternatingRowColors(True)  # 交替行颜色
        return table
    
    def display_results(self):
        """显示回测结果"""
        # 显示统计信息到表格
        self.set_statistics_data(self.statistics)
        
        # 显示图表（与单标的回测完全相同）
        if self.df is not None:
            self.chart.set_data(self.df)
    
    def set_statistics_data(self, statistics: dict):
        """设置统计数据到表格"""
        # 定义要显示的统计指标
        display_items = [
            ("首个交易日", "start_date", ""),
            ("最后交易日", "end_date", ""),
            ("总交易日", "total_days", ""),
            ("盈利交易日", "profit_days", ""),
            ("亏损交易日", "loss_days", ""),
            ("", "", ""),  # 分隔行
            ("起始资金", "capital", ",.2f"),
            ("结束资金", "end_balance", ",.2f"),
            ("总收益率", "total_return", ".2f%"),
            ("年化收益", "annual_return", ".2f%"),
            ("最大回撤", "max_drawdown", ",.2f"),
            ("百分比最大回撤", "max_ddpercent", ".2f%"),
            ("", "", ""),  # 分隔行
            ("总盈亏", "total_net_pnl", ",.2f"),
            ("总手续费", "total_commission", ",.2f"),
            ("总滑点", "total_slippage", ",.2f"),
            ("总成交金额", "total_turnover", ",.2f"),
            ("总成交笔数", "total_trade_count", ",.0f"),
            ("", "", ""),  # 分隔行
            ("日均盈亏", "daily_net_pnl", ",.2f"),
            ("日均收益率", "daily_return", ".4f%"),
            ("收益标准差", "return_std", ".4f%"),
            ("Sharpe Ratio", "sharpe_ratio", ".2f"),
            ("收益回撤比", "return_drawdown_ratio", ".2f"),
        ]
        
        self.statistics_monitor.setRowCount(len(display_items))
        
        for row, (label, key, fmt) in enumerate(display_items):
            # 第一列：指标名称
            self.statistics_monitor.setItem(row, 0, QtWidgets.QTableWidgetItem(label))
            
            # 第二列：数值
            if key and key in statistics:
                value = statistics[key]
                if fmt:
                    if "%" in fmt:
                        text = f"{value:{fmt.replace('%', '')}}"
                    else:
                        text = f"{value:{fmt}}"
                else:
                    text = str(value)
                self.statistics_monitor.setItem(row, 1, QtWidgets.QTableWidgetItem(text))
            else:
                # 空行用于分隔
                self.statistics_monitor.setItem(row, 1, QtWidgets.QTableWidgetItem(""))
        
        # 调整列宽
        self.statistics_monitor.resizeColumnsToContents()


class SummaryResultWidget(QtWidgets.QWidget):
    """汇总统计页签"""
    
    def __init__(self, results: Dict[str, Tuple[str, dict]], parent=None):
        super().__init__(parent)
        self.results = results
        self.init_ui()
        self.display_summary()
    
    def init_ui(self):
        """初始化UI"""
        layout = QtWidgets.QVBoxLayout()
        
        title = QtWidgets.QLabel("汇总统计（平均值）")
        title.setStyleSheet("font-size: 16px; font-weight: bold; padding: 10px;")
        layout.addWidget(title)
        
        self.summary_table = QtWidgets.QTableWidget()
        layout.addWidget(self.summary_table)
        
        self.setLayout(layout)
    
    def display_summary(self):
        """显示汇总数据"""
        all_stats = []
        for vt_symbol, (name, stats) in self.results.items():
            stats_copy = stats.copy()
            stats_copy['vt_symbol'] = vt_symbol
            stats_copy['name'] = name
            all_stats.append(stats_copy)
        
        self.display_summary_table(all_stats)
    
    def display_summary_table(self, all_stats: List[dict]):
        """显示汇总统计表格"""
        if not all_stats:
            return
        
        # 计算平均值（只计算数值类型的字段）
        avg_stats = {}
        keys_to_avg = [
            'total_return', 'annual_return', 'max_ddpercent',
            'sharpe_ratio', 'return_drawdown_ratio', 'daily_return',
            'return_std', 'total_net_pnl', 'total_commission',
            'total_slippage', 'total_turnover', 'total_trade_count',
            'profit_days', 'loss_days', 'total_days'
        ]
        
        for key in keys_to_avg:
            # 确保只收集数值类型
            values = []
            for s in all_stats:
                if key in s:
                    val = s.get(key, 0)
                    # 只添加数值类型（int或float）
                    if isinstance(val, (int, float)):
                        values.append(val)
            
            if values:
                avg_stats[key] = sum(values) / len(values)
        
        # 设置表格 - 使用更多列来显示对比
        num_symbols = len(all_stats)
        self.summary_table.setColumnCount(num_symbols + 2)  # 指标名 + 平均值 + 各个标的
        
        headers = ["指标", "平均值"] + [s.get('name', '') for s in all_stats]
        self.summary_table.setHorizontalHeaderLabels(headers)
        
        # 定义要显示的指标及其显示顺序
        display_stats = [
            ('total_return', '总收益率', '%'),
            ('annual_return', '年化收益', '%'),
            ('max_ddpercent', '最大回撤', '%'),
            ('sharpe_ratio', 'Sharpe比率', ''),
            ('return_drawdown_ratio', '收益回撤比', ''),
            ('total_days', '总交易日', ''),
            ('profit_days', '盈利交易日', ''),
            ('loss_days', '亏损交易日', ''),
            ('total_net_pnl', '总盈亏', '元'),
            ('total_commission', '总手续费', '元'),
            ('total_slippage', '总滑点', '元'),
            ('total_turnover', '总成交额', '元'),
            ('total_trade_count', '总成交笔数', '笔'),
            ('daily_return', '日均收益率', '%'),
            ('return_std', '收益标准差', '%'),
        ]
        
        self.summary_table.setRowCount(len(display_stats))
        
        for row, (key, label, unit) in enumerate(display_stats):
            # 第一列：指标名称
            self.summary_table.setItem(row, 0, QtWidgets.QTableWidgetItem(label))
            
            # 第二列：平均值
            if key in avg_stats:
                avg_val = avg_stats[key]
                if unit == '%':
                    text = f"{avg_val:.2f}%"
                elif unit == '元':
                    text = f"{avg_val:,.2f}"
                elif unit == '笔':
                    text = f"{avg_val:.0f}"
                else:
                    text = f"{avg_val:.2f}"
                
                item = QtWidgets.QTableWidgetItem(text)
                item.setFont(QtGui.QFont("Consolas", 10, QtGui.QFont.Bold))
                self.summary_table.setItem(row, 1, item)
            
            # 其他列：各个标的的数值
            for col, stats in enumerate(all_stats, start=2):
                if key in stats:
                    val = stats[key]
                    if isinstance(val, (int, float)):
                        if unit == '%':
                            text = f"{val:.2f}%"
                        elif unit == '元':
                            text = f"{val:,.2f}"
                        elif unit == '笔':
                            text = f"{val:.0f}"
                        else:
                            text = f"{val:.2f}"
                        
                        self.summary_table.setItem(row, col, QtWidgets.QTableWidgetItem(text))
        
        # 调整列宽
        self.summary_table.resizeColumnsToContents()
        
        # 设置表头样式
        header = self.summary_table.horizontalHeader()
        header.setDefaultAlignment(QtCore.Qt.AlignCenter)
        
        # 设置单元格对齐
        for row in range(self.summary_table.rowCount()):
            for col in range(self.summary_table.columnCount()):
                item = self.summary_table.item(row, col)
                if item:
                    if col == 0:  # 指标名称左对齐
                        item.setTextAlignment(QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter)
                    else:  # 数值右对齐
                        item.setTextAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
    
    def get_stat_label(self, key: str) -> str:
        """获取统计指标的中文标签"""
        labels = {
            'total_return': '总收益率',
            'annual_return': '年化收益',
            'max_ddpercent': '最大回撤(%)',
            'sharpe_ratio': 'Sharpe比率',
            'return_drawdown_ratio': '收益回撤比',
            'daily_return': '日均收益率',
            'return_std': '收益标准差',
            'total_net_pnl': '总盈亏',
            'total_commission': '总手续费',
        }
        return labels.get(key, key)


class StrategySettingDialog(QtWidgets.QDialog):
    """策略参数设置对话框"""
    
    def __init__(self, class_name: str, setting: dict, parent=None):
        super().__init__(parent)
        self.class_name = class_name
        self.setting = copy(setting)
        self.widgets = {}
        
        self.init_ui()
    
    def init_ui(self):
        """初始化UI"""
        self.setWindowTitle(f"{self.class_name} 参数设置")
        self.setMinimumWidth(400)
        
        layout = QtWidgets.QVBoxLayout()
        
        # 参数表单
        form_layout = QtWidgets.QFormLayout()
        
        for key, value in self.setting.items():
            if isinstance(value, bool):
                widget = QtWidgets.QCheckBox()
                widget.setChecked(value)
            elif isinstance(value, int):
                widget = QtWidgets.QSpinBox()
                widget.setRange(-999999, 999999)
                widget.setValue(value)
            elif isinstance(value, float):
                widget = QtWidgets.QDoubleSpinBox()
                widget.setRange(-999999.0, 999999.0)
                widget.setDecimals(6)
                widget.setValue(value)
            else:
                widget = QtWidgets.QLineEdit(str(value))
            
            form_layout.addRow(key, widget)
            self.widgets[key] = widget
        
        layout.addLayout(form_layout)
        
        # 按钮
        button_layout = QtWidgets.QHBoxLayout()
        ok_button = QtWidgets.QPushButton("确定")
        ok_button.clicked.connect(self.accept)
        cancel_button = QtWidgets.QPushButton("取消")
        cancel_button.clicked.connect(self.reject)
        button_layout.addWidget(ok_button)
        button_layout.addWidget(cancel_button)
        layout.addLayout(button_layout)
        
        self.setLayout(layout)
    
    def get_setting(self) -> dict:
        """获取设置"""
        result = {}
        for key, widget in self.widgets.items():
            if isinstance(widget, QtWidgets.QCheckBox):
                result[key] = widget.isChecked()
            elif isinstance(widget, QtWidgets.QSpinBox):
                result[key] = widget.value()
            elif isinstance(widget, QtWidgets.QDoubleSpinBox):
                result[key] = widget.value()
            else:
                result[key] = widget.text()
        return result


class MultiBacktestStrictWidget(QtWidgets.QWidget):
    """
    严格的多标的回测Widget
    完全使用BacktesterEngine的标准流程，确保与单标的回测结果一致
    """
    
    def __init__(self, main_engine: MainEngine = None, event_engine: EventEngine = None, parent=None):
        super().__init__(parent)
        
        # 创建BacktesterEngine实例
        if main_engine is None or event_engine is None:
            self.event_engine = EventEngine()
            self.main_engine = MainEngine(self.event_engine)
            # 添加CtaBacktesterApp
            from vnpy_ctabacktester import CtaBacktesterApp
            self.main_engine.add_app(CtaBacktesterApp)
        else:
            self.main_engine = main_engine
            self.event_engine = event_engine
        
        # 获取BacktesterEngine
        from vnpy_ctabacktester import APP_NAME
        self.backtester_engine: BacktesterEngine = self.main_engine.get_engine(APP_NAME)
        self.backtester_engine.init_engine()
        
        # 手动加载用户策略（确保从当前目录加载）
        self.load_user_strategies()
        
        # 获取策略类列表
        self.class_names: List[str] = self.backtester_engine.get_strategy_class_names()
        self.settings: Dict[str, dict] = {}
        for class_name in self.class_names:
            self.settings[class_name] = self.backtester_engine.get_default_setting(class_name)
        
        self.results: Dict[str, Tuple[str, dict]] = {}
        
        self.init_ui()
    
    def init_ui(self):
        """初始化UI - 使用左右布局，模仿单标的回测"""
        self.setWindowTitle("多标的CTA策略回测（严格模式）")
        
        # ========== 左侧：配置区域 ==========
        left_vbox = QtWidgets.QVBoxLayout()
        
        # 策略选择
        left_vbox.addWidget(QtWidgets.QLabel("策略:"))
        self.strategy_combo = QtWidgets.QComboBox()
        self.strategy_combo.addItems(self.class_names)
        left_vbox.addWidget(self.strategy_combo)
        
        # 策略参数按钮
        self.param_button = QtWidgets.QPushButton("策略参数")
        self.param_button.clicked.connect(self.show_strategy_setting)
        left_vbox.addWidget(self.param_button)
        
        # 标的选择 - 从数据库动态加载
        left_vbox.addWidget(QtWidgets.QLabel("选择标的:"))
        self.symbol_list = QtWidgets.QListWidget()
        self.symbol_list.setSelectionMode(QtWidgets.QAbstractItemView.MultiSelection)
        
        # 加载数据库中的所有标的
        self.load_symbols_from_database()
        
        left_vbox.addWidget(self.symbol_list)
        
        # 全选/反选按钮
        symbol_button_layout = QtWidgets.QHBoxLayout()
        select_all_button = QtWidgets.QPushButton("全选")
        select_all_button.clicked.connect(self.select_all_symbols)
        symbol_button_layout.addWidget(select_all_button)
        
        deselect_all_button = QtWidgets.QPushButton("全不选")
        deselect_all_button.clicked.connect(self.deselect_all_symbols)
        symbol_button_layout.addWidget(deselect_all_button)
        
        invert_button = QtWidgets.QPushButton("反选")
        invert_button.clicked.connect(self.invert_selection)
        symbol_button_layout.addWidget(invert_button)
        
        left_vbox.addLayout(symbol_button_layout)
        
        # 周期
        left_vbox.addWidget(QtWidgets.QLabel("周期:"))
        self.interval_combo = QtWidgets.QComboBox()
        self.interval_combo.addItem("日线", Interval.DAILY.value)
        self.interval_combo.addItem("小时", Interval.HOUR.value)
        self.interval_combo.addItem("分钟", Interval.MINUTE.value)
        left_vbox.addWidget(self.interval_combo)
        
        # 日期
        left_vbox.addWidget(QtWidgets.QLabel("开始日期:"))
        self.start_date = QtWidgets.QDateEdit()
        self.start_date.setDate(DEFAULT_START.date())
        self.start_date.setCalendarPopup(True)
        left_vbox.addWidget(self.start_date)
        
        left_vbox.addWidget(QtWidgets.QLabel("结束日期:"))
        self.end_date = QtWidgets.QDateEdit()
        self.end_date.setDate(DEFAULT_END.date())
        self.end_date.setCalendarPopup(True)
        left_vbox.addWidget(self.end_date)
        
        # 资金
        left_vbox.addWidget(QtWidgets.QLabel("回测资金:"))
        self.capital_spin = QtWidgets.QDoubleSpinBox()
        self.capital_spin.setMaximum(100000000)
        self.capital_spin.setValue(DEFAULT_CAPITAL)
        self.capital_spin.setDecimals(0)
        left_vbox.addWidget(self.capital_spin)
        
        # 手续费率
        left_vbox.addWidget(QtWidgets.QLabel("手续费率:"))
        self.rate_spin = QtWidgets.QDoubleSpinBox()
        self.rate_spin.setMaximum(1.0)
        self.rate_spin.setValue(DEFAULT_RATE)
        self.rate_spin.setDecimals(6)
        self.rate_spin.setSingleStep(0.000001)
        left_vbox.addWidget(self.rate_spin)
        
        # 滑点
        left_vbox.addWidget(QtWidgets.QLabel("滑点:"))
        self.slippage_spin = QtWidgets.QDoubleSpinBox()
        self.slippage_spin.setMaximum(1000)
        self.slippage_spin.setValue(DEFAULT_SLIPPAGE)
        self.slippage_spin.setDecimals(1)
        left_vbox.addWidget(self.slippage_spin)
        
        # 合约乘数
        left_vbox.addWidget(QtWidgets.QLabel("合约乘数:"))
        self.size_spin = QtWidgets.QSpinBox()
        self.size_spin.setMaximum(1000000)
        self.size_spin.setValue(int(DEFAULT_SIZE))
        left_vbox.addWidget(self.size_spin)
        
        # 价格跳动
        left_vbox.addWidget(QtWidgets.QLabel("价格跳动:"))
        self.pricetick_spin = QtWidgets.QDoubleSpinBox()
        self.pricetick_spin.setMaximum(1000)
        self.pricetick_spin.setValue(DEFAULT_PRICETICK)
        self.pricetick_spin.setDecimals(1)
        left_vbox.addWidget(self.pricetick_spin)
        
        # 开始回测按钮
        self.start_button = QtWidgets.QPushButton("开始回测")
        self.start_button.clicked.connect(self.start_backtest)
        left_vbox.addWidget(self.start_button)
        
        left_vbox.addStretch()
        
        # 创建左侧widget - 限制宽度给图表更多空间
        left_widget = QtWidgets.QWidget()
        left_widget.setLayout(left_vbox)
        left_widget.setFixedWidth(200)  # 固定宽度200px
        
        # ========== 右侧：结果页签 ==========
        self.result_tabs = QtWidgets.QTabWidget()
        
        # ========== 主布局：左右分布 ==========
        hbox = QtWidgets.QHBoxLayout()
        hbox.addWidget(left_widget)
        hbox.addWidget(self.result_tabs)
        
        self.setLayout(hbox)
    
    def load_user_strategies(self):
        """手动加载用户自定义策略"""
        try:
            from pathlib import Path
            strategies_path = Path(__file__).parent / "strategies"
            if strategies_path.exists():
                self.backtester_engine.load_strategy_class_from_folder(
                    strategies_path, 
                    "strategies"
                )
        except Exception as e:
            pass  # 静默失败，不影响系统内置策略
    
    def load_symbols_from_database(self):
        """从数据库加载所有可用的标的"""
        try:
            from vnpy.trader.database import get_database, BarOverview
            
            database = get_database()
            
            # 获取所有标的概览
            overviews = database.get_bar_overview()
            
            if not overviews:
                # 如果数据库为空，使用默认列表
                self.load_default_symbols()
                return
            
            # 按交易所和代码分组，去重
            symbol_dict = {}
            for overview in overviews:
                key = (overview.symbol, overview.exchange)
                if key not in symbol_dict:
                    symbol_dict[key] = overview
            
            # 按交易所和代码排序
            sorted_symbols = sorted(symbol_dict.values(), 
                                   key=lambda x: (x.exchange.value, x.symbol))
            
            # 添加到列表
            count = 0
            for overview in sorted_symbols:
                symbol = overview.symbol
                exchange = overview.exchange
                vt_symbol = f"{symbol}.{exchange.value}"
                
                # 尝试获取中文名称（如果有的话）
                name = self.get_symbol_name(symbol, exchange)
                
                display_text = f"{name} ({vt_symbol})" if name else vt_symbol
                
                item = QtWidgets.QListWidgetItem(display_text)
                item.setData(QtCore.Qt.UserRole, (symbol, exchange, name or symbol))
                self.symbol_list.addItem(item)
                count += 1
            
            # 显示加载的标的数量（在窗口标题中）
            self.setWindowTitle(f"多标的CTA策略回测（严格模式） - 已加载{count}个标的")
                
        except Exception as e:
            # 如果加载失败，使用默认列表
            import traceback
            traceback.print_exc()
            self.load_default_symbols()
    
    def load_default_symbols(self):
        """加载默认标的列表"""
        for symbol, exchange, name in AVAILABLE_SYMBOLS:
            vt_symbol = f"{symbol}.{exchange.value}"
            item = QtWidgets.QListWidgetItem(f"{name} ({vt_symbol})")
            item.setData(QtCore.Qt.UserRole, (symbol, exchange, name))
            self.symbol_list.addItem(item)
    
    def get_symbol_name(self, symbol: str, exchange: Exchange) -> str:
        """获取标的中文名称"""
        # 常见指数的中文名称映射
        name_map = {
            ("000001", Exchange.SSE): "上证指数",
            ("399001", Exchange.SZSE): "深证成指",
            ("399006", Exchange.SZSE): "创业板指",
            ("000688", Exchange.SSE): "科创50",
            ("899050", Exchange.BSE): "北证50",
            ("HSI", Exchange.GLOBAL): "恒生指数",
            ("HSTECH", Exchange.GLOBAL): "恒生科技",
            ("GSPC", Exchange.GLOBAL): "标普500",
            ("IXIC", Exchange.GLOBAL): "纳斯达克",
            ("DJI", Exchange.GLOBAL): "道琼斯",
        }
        
        return name_map.get((symbol, exchange), "")
    
    def select_all_symbols(self):
        """全选所有标的"""
        for i in range(self.symbol_list.count()):
            item = self.symbol_list.item(i)
            item.setSelected(True)
    
    def deselect_all_symbols(self):
        """取消选择所有标的"""
        for i in range(self.symbol_list.count()):
            item = self.symbol_list.item(i)
            item.setSelected(False)
    
    def invert_selection(self):
        """反选标的"""
        for i in range(self.symbol_list.count()):
            item = self.symbol_list.item(i)
            item.setSelected(not item.isSelected())
    
    def show_strategy_setting(self):
        """显示策略参数设置对话框"""
        class_name = self.strategy_combo.currentText()
        if not class_name:
            return
        
        old_setting = self.settings[class_name]
        dialog = StrategySettingDialog(class_name, old_setting, self)
        
        if dialog.exec() == QtWidgets.QDialog.DialogCode.Accepted:
            new_setting = dialog.get_setting()
            self.settings[class_name] = new_setting
            QtWidgets.QMessageBox.information(self, "成功", "策略参数已更新")
    
    def start_backtest(self):
        """开始回测"""
        # 获取选中的标的
        selected_items = self.symbol_list.selectedItems()
        if not selected_items:
            QtWidgets.QMessageBox.warning(self, "警告", "请至少选择一个标的！")
            return
        
        # 获取策略
        class_name = self.strategy_combo.currentText()
        if not class_name:
            QtWidgets.QMessageBox.warning(self, "警告", "请选择策略！")
            return
        
        # 获取配置
        interval = self.interval_combo.currentData()
        start = datetime.combine(self.start_date.date().toPython(), datetime.min.time())
        end = datetime.combine(self.end_date.date().toPython(), datetime.max.time())
        rate = self.rate_spin.value()
        slippage = self.slippage_spin.value()
        size = self.size_spin.value()
        pricetick = self.pricetick_spin.value()
        capital = int(self.capital_spin.value())
        setting = self.settings[class_name]
        
        # 清空之前的结果
        self.result_tabs.clear()
        self.results.clear()
        
        # 记录失败的标的
        failed_symbols = []
        skipped_symbols = []
        
        # 显示进度
        progress = QtWidgets.QProgressDialog("正在回测...", "取消", 0, len(selected_items), self)
        progress.setWindowModality(QtCore.Qt.WindowModal)
        progress.show()
        
        # 对每个标的进行回测
        for i, item in enumerate(selected_items):
            if progress.wasCanceled():
                break
            
            symbol, exchange, name = item.data(QtCore.Qt.UserRole)
            vt_symbol = f"{symbol}.{exchange.value}"
            
            progress.setLabelText(f"正在回测: {name} ({vt_symbol}) [{i+1}/{len(selected_items)}]")
            progress.setValue(i)
            QtWidgets.QApplication.processEvents()
            
            try:
                # 根据标的类型自动设置合约乘数
                actual_size = self.get_appropriate_size(symbol, exchange, int(size))
                
                # 使用BacktesterEngine运行回测（完全相同的单标的回测流程）
                self.backtester_engine.run_backtesting(
                    class_name=class_name,
                    vt_symbol=vt_symbol,
                    interval=interval,
                    start=start,
                    end=end,
                    rate=rate,
                    slippage=slippage,
                    size=actual_size,
                    pricetick=pricetick,
                    capital=capital,
                    setting=setting
                )
                
                # 获取结果（与单标的回测完全相同）
                statistics = self.backtester_engine.get_result_statistics()
                
                if statistics:
                    # 添加capital到statistics（确保是数字类型，而不是字符串）
                    statistics['capital'] = float(capital)
                    
                    # 获取DataFrame用于图表显示（与单标的回测完全相同）
                    df = self.backtester_engine.get_result_df()
                    
                    # 创建结果页签
                    result_widget = SingleBacktestResultWidget(vt_symbol, name, statistics, df)
                    self.result_tabs.addTab(result_widget, name)
                    
                    # 保存结果
                    self.results[vt_symbol] = (name, statistics)
                else:
                    # 记录未返回结果的标的
                    skipped_symbols.append(f"{name} ({vt_symbol})")
                
            except Exception as e:
                # 记录失败的标的
                failed_symbols.append(f"{name} ({vt_symbol}): {str(e)}")
                import traceback
                traceback.print_exc()
        
        progress.setValue(len(selected_items))
        
        # 如果有结果，添加汇总页签
        if self.results:
            summary_widget = SummaryResultWidget(self.results)
            self.result_tabs.addTab(summary_widget, "汇总统计")
        
        # 构建完成消息
        success_count = len(self.results)
        total_count = len(selected_items)
        
        message_parts = [f"回测完成！成功: {success_count}/{total_count}"]
        
        if skipped_symbols:
            message_parts.append(f"\n\n未返回结果 ({len(skipped_symbols)}个):")
            for s in skipped_symbols[:10]:  # 最多显示10个
                message_parts.append(f"  • {s}")
            if len(skipped_symbols) > 10:
                message_parts.append(f"  ... 还有 {len(skipped_symbols)-10} 个")
        
        if failed_symbols:
            message_parts.append(f"\n\n回测失败 ({len(failed_symbols)}个):")
            for f in failed_symbols[:10]:  # 最多显示10个
                message_parts.append(f"  • {f}")
            if len(failed_symbols) > 10:
                message_parts.append(f"  ... 还有 {len(failed_symbols)-10} 个")
        
        message = "\n".join(message_parts)
        
        # 根据结果选择消息类型
        if success_count == total_count:
            QtWidgets.QMessageBox.information(self, "完成", message)
        elif success_count > 0:
            QtWidgets.QMessageBox.warning(self, "部分完成", message)
        else:
            QtWidgets.QMessageBox.critical(self, "失败", message)


def main():
    """主函数"""
    app = create_qapp("多标的回测工具（严格模式）")
    
    widget = MultiBacktestStrictWidget()
    widget.resize(1400, 900)
    widget.show()
    
    app.exec()


if __name__ == "__main__":
    main()

