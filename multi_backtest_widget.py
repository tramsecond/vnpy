"""
多标的回测Widget - 可嵌入到VeighNa主界面
"""
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# 设置输出编码
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from PySide6 import QtWidgets, QtCore, QtGui
try:
    from PySide6 import QtWebEngineWidgets
    HAS_WEBENGINE = True
except ImportError:
    HAS_WEBENGINE = False
    import webbrowser
    import tempfile

from vnpy.trader.constant import Interval, Exchange
from vnpy.trader.engine import MainEngine
from vnpy.event import EventEngine
from vnpy_ctastrategy.backtesting import BacktestingEngine
from vnpy_ctastrategy.template import CtaTemplate, TargetPosTemplate
import importlib
from pathlib import Path
from glob import glob
from types import ModuleType

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

# 默认参数
DEFAULT_INTERVAL = Interval.DAILY
DEFAULT_START = datetime(2020, 1, 2)
DEFAULT_END = datetime(2026, 1, 7)
DEFAULT_RATE = 0.0003
DEFAULT_SLIPPAGE = 0
DEFAULT_SIZE = 1
DEFAULT_PRICETICK = 0.01
DEFAULT_CAPITAL = 1_000_000


class BacktestResultTab(QtWidgets.QWidget):
    """单个标的的回测结果页签"""
    
    def __init__(self, vt_symbol: str, symbol_name: str, df: pd.DataFrame, parent=None):
        super().__init__(parent)
        self.vt_symbol = vt_symbol
        self.symbol_name = symbol_name
        self.df = df
        
        self.init_ui()
        self.display_results()
    
    def init_ui(self):
        """初始化UI"""
        layout = QtWidgets.QVBoxLayout()
        
        # 标题
        title = QtWidgets.QLabel(f"{self.symbol_name} ({self.vt_symbol}) 回测结果")
        title.setStyleSheet("font-size: 16px; font-weight: bold; padding: 10px;")
        layout.addWidget(title)
        
        # 分割器：左侧统计，右侧图表
        splitter = QtWidgets.QSplitter(QtCore.Qt.Horizontal)
        
        # 左侧：统计指标
        stats_widget = QtWidgets.QWidget()
        stats_layout = QtWidgets.QVBoxLayout()
        self.stats_text = QtWidgets.QTextEdit()
        self.stats_text.setReadOnly(True)
        self.stats_text.setFont(QtGui.QFont("Consolas", 10))
        stats_layout.addWidget(self.stats_text)
        stats_widget.setLayout(stats_layout)
        
        # 右侧：图表
        if HAS_WEBENGINE:
            self.chart_view = QtWebEngineWidgets.QWebEngineView()
            splitter.addWidget(stats_widget)
            splitter.addWidget(self.chart_view)
            splitter.setStretchFactor(0, 1)
            splitter.setStretchFactor(1, 2)
        else:
            self.chart_view = None
            layout.addWidget(stats_widget)
            stats_widget.setMaximumWidth(400)
            splitter = None
        
        if splitter:
            layout.addWidget(splitter)
        else:
            layout.addWidget(stats_widget)
        
        self.setLayout(layout)
    
    def display_results(self):
        """显示回测结果"""
        try:
            engine = BacktestingEngine()
            stats = engine.calculate_statistics(self.df, output=False)
            stats_text = self.format_statistics(stats)
            self.stats_text.setPlainText(stats_text)
            
            html = self.create_chart_html()
            if self.chart_view:
                self.chart_view.setHtml(html)
            elif HAS_WEBENGINE is False:
                temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding='utf-8')
                temp_file.write(html)
                temp_file.close()
                webbrowser.open(f'file:///{temp_file.name}')
        except Exception as e:
            import traceback
            error_msg = f"显示结果失败:\n{str(e)}\n\n{traceback.format_exc()}"
            self.stats_text.setPlainText(error_msg)
    
    def format_statistics(self, stats: dict) -> str:
        """格式化统计指标"""
        lines = [
            f"{'='*50}",
            f"{self.symbol_name} ({self.vt_symbol}) 回测统计",
            f"{'='*50}",
            "",
            "【日期信息】",
            f"  首个交易日：    {stats.get('start_date', 'N/A')}",
            f"  最后交易日：    {stats.get('end_date', 'N/A')}",
            f"  总交易日：      {stats.get('total_days', 0):,}",
            f"  盈利交易日：    {stats.get('profit_days', 0):,}",
            f"  亏损交易日：    {stats.get('loss_days', 0):,}",
            "",
            "【资金盈亏】",
            f"  起始资金：      {stats.get('start_balance', 0):,.2f}",
            f"  结束资金：      {stats.get('end_balance', 0):,.2f}",
            f"  总收益率：      {stats.get('total_return', 0):.2f}%",
            f"  年化收益：      {stats.get('annual_return', 0):.2f}%",
            f"  最大回撤:       {stats.get('max_dd', 0):,.2f}",
            f"  百分比最大回撤: {stats.get('max_ddpercent', 0):.2f}%",
            f"  总盈亏：        {stats.get('total_net_pnl', 0):,.2f}",
            "",
            "【交易成本】",
            f"  总手续费：      {stats.get('total_commission', 0):,.2f}",
            f"  总滑点：        {stats.get('total_slippage', 0):,.2f}",
            f"  总成交金额：    {stats.get('total_turnover', 0):,.2f}",
            f"  总成交笔数：    {stats.get('total_trade_count', 0):,.0f}",
            "",
            "【日均数据】",
            f"  日均盈亏：      {stats.get('daily_net_pnl', 0):,.2f}",
            f"  日均手续费：    {stats.get('daily_commission', 0):,.2f}",
            f"  日均滑点：      {stats.get('daily_slippage', 0):,.2f}",
            f"  日均成交金额：  {stats.get('daily_turnover', 0):,.2f}",
            f"  日均成交笔数：  {stats.get('daily_trade_count', 0):.2f}",
            f"  日均收益率：    {stats.get('daily_return', 0):.4f}%",
            f"  收益标准差：    {stats.get('return_std', 0):.4f}%",
            "",
            "【绩效评价】",
            f"  Sharpe Ratio：  {stats.get('sharpe_ratio', 0):.2f}",
            f"  收益回撤比：    {stats.get('return_drawdown_ratio', 0):.2f}",
        ]
        return "\n".join(lines)
    
    def create_chart_html(self) -> str:
        """创建Plotly图表的HTML"""
        fig = make_subplots(
            rows=4,
            cols=1,
            subplot_titles=["账户净值", "净值回撤", "每日盈亏", "盈亏分布"],
            vertical_spacing=0.06,
            row_heights=[0.3, 0.2, 0.3, 0.2]
        )
        
        # 账户净值
        fig.add_trace(
            go.Scatter(
                x=self.df.index,
                y=self.df['balance'],
                mode='lines',
                name='账户净值',
                line=dict(color='blue', width=2)
            ),
            row=1, col=1
        )
        
        # 净值回撤
        fig.add_trace(
            go.Scatter(
                x=self.df.index,
                y=self.df['drawdown'],
                mode='lines',
                name='净值回撤',
                fill='tozeroy',
                fillcolor='rgba(255,0,0,0.3)',
                line=dict(color='red', width=1)
            ),
            row=2, col=1
        )
        
        # 每日盈亏
        colors = ['green' if x > 0 else 'red' for x in self.df['net_pnl']]
        fig.add_trace(
            go.Bar(
                x=self.df.index,
                y=self.df['net_pnl'],
                name='每日盈亏',
                marker_color=colors
            ),
            row=3, col=1
        )
        
        # 盈亏分布
        fig.add_trace(
            go.Histogram(
                x=self.df['net_pnl'],
                nbinsx=50,
                name='盈亏分布',
                marker_color='lightblue'
            ),
            row=4, col=1
        )
        
        fig.update_layout(
            height=1000,
            title_text=f"{self.symbol_name} ({self.vt_symbol}) 回测结果",
            showlegend=False
        )
        
        return fig.to_html(include_plotlyjs='cdn')


class SummaryTab(QtWidgets.QWidget):
    """汇总统计页签"""
    
    def __init__(self, results: Dict[str, Tuple[str, pd.DataFrame]], parent=None):
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
        
        splitter = QtWidgets.QSplitter(QtCore.Qt.Horizontal)
        
        # 左侧：汇总表格
        table_widget = QtWidgets.QWidget()
        table_layout = QtWidgets.QVBoxLayout()
        self.summary_table = QtWidgets.QTableWidget()
        table_layout.addWidget(self.summary_table)
        table_widget.setLayout(table_layout)
        
        # 右侧：对比图表
        if HAS_WEBENGINE:
            self.chart_view = QtWebEngineWidgets.QWebEngineView()
            splitter.addWidget(table_widget)
            splitter.addWidget(self.chart_view)
            splitter.setStretchFactor(0, 1)
            splitter.setStretchFactor(1, 2)
        else:
            self.chart_view = None
            layout.addWidget(table_widget)
            table_widget.setMaximumWidth(500)
            splitter = None
        
        if splitter:
            layout.addWidget(splitter)
        else:
            layout.addWidget(table_widget)
        
        self.setLayout(layout)
    
    def display_summary(self):
        """显示汇总数据"""
        all_stats = []
        for vt_symbol, (name, df) in self.results.items():
            try:
                engine = BacktestingEngine()
                stats = engine.calculate_statistics(df, output=False)
                stats['vt_symbol'] = vt_symbol
                stats['name'] = name
                all_stats.append(stats)
            except Exception as e:
                print(f"计算 {name} 统计指标失败: {e}")
                continue
        
        self.display_summary_table(all_stats)
        
        html = self.create_comparison_chart(all_stats)
        if self.chart_view:
            self.chart_view.setHtml(html)
        elif HAS_WEBENGINE is False:
            temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding='utf-8')
            temp_file.write(html)
            temp_file.close()
            webbrowser.open(f'file:///{temp_file.name}')
    
    def display_summary_table(self, all_stats: List[dict]):
        """显示汇总统计表格"""
        if not all_stats:
            return
        
        avg_stats = {}
        keys_to_avg = [
            'total_return', 'annual_return', 'max_ddpercent',
            'sharpe_ratio', 'return_drawdown_ratio', 'daily_return',
            'return_std', 'total_net_pnl', 'total_commission'
        ]
        
        for key in keys_to_avg:
            values = [s.get(key, 0) for s in all_stats if key in s]
            if values:
                avg_stats[key] = sum(values) / len(values)
        
        self.summary_table.setColumnCount(2)
        self.summary_table.setRowCount(len(avg_stats) + len(all_stats) + 2)
        self.summary_table.setHorizontalHeaderLabels(["指标", "数值"])
        
        row = 0
        self.summary_table.setItem(row, 0, QtWidgets.QTableWidgetItem("【平均值】"))
        self.summary_table.setItem(row, 1, QtWidgets.QTableWidgetItem(""))
        row += 1
        
        for key, value in avg_stats.items():
            label = self.get_stat_label(key)
            self.summary_table.setItem(row, 0, QtWidgets.QTableWidgetItem(f"  {label}"))
            if 'return' in key or 'ratio' in key or 'percent' in key:
                self.summary_table.setItem(row, 1, QtWidgets.QTableWidgetItem(f"{value:.2f}%"))
            else:
                self.summary_table.setItem(row, 1, QtWidgets.QTableWidgetItem(f"{value:,.2f}"))
            row += 1
        
        row += 1
        self.summary_table.setItem(row, 0, QtWidgets.QTableWidgetItem("【各标的表现】"))
        self.summary_table.setItem(row, 1, QtWidgets.QTableWidgetItem(""))
        row += 1
        
        for stats in all_stats:
            name = stats.get('name', '')
            total_return = stats.get('total_return', 0)
            sharpe = stats.get('sharpe_ratio', 0)
            max_dd = stats.get('max_ddpercent', 0)
            
            self.summary_table.setItem(row, 0, QtWidgets.QTableWidgetItem(f"  {name}"))
            self.summary_table.setItem(row, 1, QtWidgets.QTableWidgetItem(
                f"收益:{total_return:.2f}% | Sharpe:{sharpe:.2f} | 回撤:{max_dd:.2f}%"
            ))
            row += 1
        
        self.summary_table.resizeColumnsToContents()
    
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
    
    def create_comparison_chart(self, all_stats: List[dict]) -> str:
        """创建对比图表"""
        fig = make_subplots(
            rows=2,
            cols=2,
            subplot_titles=["总收益率对比", "Sharpe比率对比", "最大回撤对比", "年化收益对比"],
            specs=[[{"type": "bar"}, {"type": "bar"}],
                   [{"type": "bar"}, {"type": "bar"}]]
        )
        
        names = [s.get('name', '') for s in all_stats]
        total_returns = [s.get('total_return', 0) for s in all_stats]
        sharpe_ratios = [s.get('sharpe_ratio', 0) for s in all_stats]
        max_dds = [s.get('max_ddpercent', 0) for s in all_stats]
        annual_returns = [s.get('annual_return', 0) for s in all_stats]
        
        fig.add_trace(
            go.Bar(x=names, y=total_returns, name='总收益率(%)', marker_color='lightblue'),
            row=1, col=1
        )
        fig.add_trace(
            go.Bar(x=names, y=sharpe_ratios, name='Sharpe比率', marker_color='lightgreen'),
            row=1, col=2
        )
        fig.add_trace(
            go.Bar(x=names, y=max_dds, name='最大回撤(%)', marker_color='lightcoral'),
            row=2, col=1
        )
        fig.add_trace(
            go.Bar(x=names, y=annual_returns, name='年化收益(%)', marker_color='lightyellow'),
            row=2, col=2
        )
        
        fig.update_layout(
            height=800,
            title_text="多标的回测对比",
            showlegend=False
        )
        
        return fig.to_html(include_plotlyjs='cdn')


class MultiBacktestWidget(QtWidgets.QWidget):
    """
    多标的回测Widget - 可以作为独立窗口或在主界面中嵌入
    """
    
    def __init__(self, parent=None, main_engine: MainEngine = None, event_engine: EventEngine = None):
        super().__init__(parent)
        self.main_engine = main_engine
        self.event_engine = event_engine
        self.strategy_classes: Dict[str, type] = {}  # 存储策略类名和类对象的映射
        self.results: Dict[str, Tuple[str, pd.DataFrame]] = {}
        self.load_strategy_classes()  # 加载所有策略类
        self.init_ui()
    
    def load_strategy_classes(self):
        """加载所有CTA策略类"""
        try:
            # 方法1：如果可以使用BacktesterEngine，直接获取策略列表
            if self.main_engine:
                try:
                    from vnpy_ctabacktester import APP_NAME
                    backtester_engine = self.main_engine.get_engine(APP_NAME)
                    if backtester_engine:
                        class_names = backtester_engine.get_strategy_class_names()
                        for class_name in class_names:
                            strategy_class = backtester_engine.classes.get(class_name)
                            if strategy_class:
                                self.strategy_classes[class_name] = strategy_class
                        print(f"[信息] 从BacktesterEngine加载了 {len(self.strategy_classes)} 个策略")
                        return
                except Exception as e:
                    print(f"[警告] 无法从BacktesterEngine加载策略: {e}")
            
            # 方法2：直接从策略目录加载
            self.load_strategy_classes_from_folder()
            
        except Exception as e:
            print(f"[错误] 加载策略类失败: {e}")
            import traceback
            traceback.print_exc()
            # 如果加载失败，至少添加一个默认策略
            try:
                from vnpy_ctastrategy.strategies.atr_rsi_strategy import AtrRsiStrategy
                self.strategy_classes["AtrRsiStrategy"] = AtrRsiStrategy
            except:
                pass
    
    def load_strategy_classes_from_folder(self):
        """从策略文件夹加载策略类"""
        try:
            import vnpy_ctastrategy
            from vnpy.trader.utility import TRADER_DIR
            
            # 1. 从vnpy_ctastrategy.strategies加载内置策略
            app_path = Path(vnpy_ctastrategy.__file__).parent
            path1 = app_path.joinpath("strategies")
            self.load_strategy_class_from_folder(path1, "vnpy_ctastrategy.strategies")
            
            # 2. 从用户strategies目录加载策略
            path2 = TRADER_DIR.joinpath("strategies")
            if path2.exists():
                self.load_strategy_class_from_folder(path2, "strategies")
            
            # 3. 从当前目录的strategies文件夹加载
            path3 = Path.cwd().joinpath("strategies")
            if path3.exists():
                self.load_strategy_class_from_folder(path3, "strategies")
            
            print(f"[信息] 加载了 {len(self.strategy_classes)} 个策略: {list(self.strategy_classes.keys())}")
            
        except Exception as e:
            print(f"[错误] 从文件夹加载策略失败: {e}")
            import traceback
            traceback.print_exc()
    
    def load_strategy_class_from_folder(self, path: Path, module_name: str = ""):
        """从指定文件夹加载策略类"""
        for suffix in ["py", "pyd", "so"]:
            pathname = str(path.joinpath(f"*.{suffix}"))
            for filepath in glob(pathname):
                filename = Path(filepath).stem
                if filename.startswith("__"):
                    continue
                
                if module_name:
                    full_module_name = f"{module_name}.{filename}"
                else:
                    full_module_name = filename
                
                self.load_strategy_class_from_module(full_module_name)
    
    def load_strategy_class_from_module(self, module_name: str):
        """从模块加载策略类"""
        try:
            module = importlib.import_module(module_name)
            importlib.reload(module)  # 重载模块，确保修改生效
            
            for name in dir(module):
                value = getattr(module, name)
                if (
                    isinstance(value, type)
                    and issubclass(value, CtaTemplate)
                    and value not in {CtaTemplate, TargetPosTemplate}
                ):
                    self.strategy_classes[value.__name__] = value
                    
        except Exception as e:
            # 静默失败，避免输出过多错误信息
            pass
    
    def init_ui(self):
        """初始化UI"""
        self.setWindowTitle("多标的CTA策略回测")
        
        layout = QtWidgets.QVBoxLayout()
        
        # 配置区域
        config_group = QtWidgets.QGroupBox("回测配置")
        config_layout = QtWidgets.QGridLayout()
        
        # 标的选择
        config_layout.addWidget(QtWidgets.QLabel("选择标的:"), 0, 0)
        self.symbol_list = QtWidgets.QListWidget()
        self.symbol_list.setSelectionMode(QtWidgets.QAbstractItemView.MultiSelection)
        for symbol, exchange, name in AVAILABLE_SYMBOLS:
            vt_symbol = f"{symbol}.{exchange.value}"
            item = QtWidgets.QListWidgetItem(f"{name} ({vt_symbol})")
            item.setData(QtCore.Qt.UserRole, (symbol, exchange, name))
            self.symbol_list.addItem(item)
        config_layout.addWidget(self.symbol_list, 0, 1, 3, 1)
        
        # 策略选择
        config_layout.addWidget(QtWidgets.QLabel("策略:"), 0, 2)
        self.strategy_combo = QtWidgets.QComboBox()
        
        # 添加所有加载的策略类
        if self.strategy_classes:
            strategy_names = sorted(self.strategy_classes.keys())
            for name in strategy_names:
                self.strategy_combo.addItem(name, self.strategy_classes[name])
            print(f"[信息] 已加载 {len(strategy_names)} 个策略到下拉框")
        else:
            # 如果没有加载到策略，至少添加一个默认策略
            try:
                from vnpy_ctastrategy.strategies.atr_rsi_strategy import AtrRsiStrategy
                self.strategy_combo.addItem("AtrRsiStrategy", AtrRsiStrategy)
                self.strategy_classes["AtrRsiStrategy"] = AtrRsiStrategy
                print("[警告] 无法加载策略列表，使用默认策略 AtrRsiStrategy")
            except Exception as e:
                print(f"[错误] 无法加载默认策略: {e}")
                self.strategy_combo.addItem("AtrRsiStrategy")  # 至少添加名称
        
        config_layout.addWidget(self.strategy_combo, 0, 3)
        
        # 周期选择
        config_layout.addWidget(QtWidgets.QLabel("周期:"), 1, 2)
        self.interval_combo = QtWidgets.QComboBox()
        self.interval_combo.addItem("日线 (d)", Interval.DAILY)
        self.interval_combo.addItem("小时 (h)", Interval.HOUR)
        self.interval_combo.addItem("分钟 (m)", Interval.MINUTE)
        config_layout.addWidget(self.interval_combo, 1, 3)
        
        # 日期范围
        config_layout.addWidget(QtWidgets.QLabel("开始日期:"), 2, 2)
        self.start_date = QtWidgets.QDateEdit()
        self.start_date.setDate(DEFAULT_START.date())
        self.start_date.setCalendarPopup(True)
        config_layout.addWidget(self.start_date, 2, 3)
        
        config_layout.addWidget(QtWidgets.QLabel("结束日期:"), 3, 2)
        self.end_date = QtWidgets.QDateEdit()
        self.end_date.setDate(DEFAULT_END.date())
        self.end_date.setCalendarPopup(True)
        config_layout.addWidget(self.end_date, 3, 3)
        
        # 资金和手续费
        config_layout.addWidget(QtWidgets.QLabel("回测资金:"), 4, 2)
        self.capital_spin = QtWidgets.QDoubleSpinBox()
        self.capital_spin.setMaximum(100000000)
        self.capital_spin.setValue(DEFAULT_CAPITAL)
        self.capital_spin.setDecimals(0)
        config_layout.addWidget(self.capital_spin, 4, 3)
        
        config_layout.addWidget(QtWidgets.QLabel("手续费率:"), 5, 2)
        self.rate_spin = QtWidgets.QDoubleSpinBox()
        self.rate_spin.setMaximum(1.0)
        self.rate_spin.setValue(DEFAULT_RATE)
        self.rate_spin.setDecimals(6)
        config_layout.addWidget(self.rate_spin, 5, 3)
        
        config_group.setLayout(config_layout)
        layout.addWidget(config_group)
        
        # 按钮
        button_layout = QtWidgets.QHBoxLayout()
        self.start_button = QtWidgets.QPushButton("开始回测")
        self.start_button.clicked.connect(self.start_backtest)
        self.start_button.setStyleSheet("font-size: 14px; padding: 5px 20px;")
        button_layout.addWidget(self.start_button)
        button_layout.addStretch()
        layout.addLayout(button_layout)
        
        # 结果页签
        self.result_tabs = QtWidgets.QTabWidget()
        layout.addWidget(self.result_tabs)
        
        self.setLayout(layout)
    
    def start_backtest(self):
        """开始回测"""
        selected_items = self.symbol_list.selectedItems()
        if not selected_items:
            QtWidgets.QMessageBox.warning(self, "警告", "请至少选择一个标的！")
            return
        
        interval = self.interval_combo.currentData()
        start_date = datetime.combine(self.start_date.date().toPython(), datetime.min.time())
        end_date = datetime.combine(self.end_date.date().toPython(), datetime.max.time())
        capital = self.capital_spin.value()
        rate = self.rate_spin.value()
        
        self.result_tabs.clear()
        self.results.clear()
        
        progress = QtWidgets.QProgressDialog("正在回测...", "取消", 0, len(selected_items), self)
        progress.setWindowModality(QtCore.Qt.WindowModal)
        progress.show()
        
        for i, item in enumerate(selected_items):
            if progress.wasCanceled():
                break
            
            symbol, exchange, name = item.data(QtCore.Qt.UserRole)
            vt_symbol = f"{symbol}.{exchange.value}"
            
            progress.setLabelText(f"正在回测: {name} ({vt_symbol})")
            progress.setValue(i)
            QtWidgets.QApplication.processEvents()
            
            try:
                df = self.run_single_backtest(
                    vt_symbol, interval, start_date, end_date, rate, capital
                )
                
                result_widget = BacktestResultTab(vt_symbol, name, df)
                self.result_tabs.addTab(result_widget, name)
                
                self.results[vt_symbol] = (name, df)
                
            except Exception as e:
                QtWidgets.QMessageBox.critical(
                    self, "错误", 
                    f"{name} ({vt_symbol}) 回测失败:\n{str(e)}"
                )
                import traceback
                traceback.print_exc()
        
        progress.setValue(len(selected_items))
        
        if self.results:
            summary_widget = SummaryTab(self.results)
            self.result_tabs.addTab(summary_widget, "汇总统计")
        
        QtWidgets.QMessageBox.information(self, "完成", "回测完成！")
    
    def run_single_backtest(
        self, vt_symbol: str, interval: Interval,
        start: datetime, end: datetime, rate: float, capital: float
    ) -> pd.DataFrame:
        """运行单个标的的回测"""
        # 获取选中的策略类
        strategy_class = self.strategy_combo.currentData()
        
        # 如果currentData返回None，尝试从策略名称获取
        if strategy_class is None:
            strategy_name = self.strategy_combo.currentText()
            strategy_class = self.strategy_classes.get(strategy_name)
            
            # 如果还是没有，尝试动态导入
            if strategy_class is None:
                try:
                    # 尝试从vnpy_ctastrategy.strategies导入
                    module_name = f"vnpy_ctastrategy.strategies.{strategy_name.lower().replace('strategy', '_strategy')}"
                    if not module_name.endswith('_strategy'):
                        module_name = f"{module_name}_strategy"
                    module = importlib.import_module(module_name)
                    strategy_class = getattr(module, strategy_name, None)
                except:
                    try:
                        # 如果失败，尝试直接导入已知策略
                        if strategy_name == "AtrRsiStrategy":
                            from vnpy_ctastrategy.strategies.atr_rsi_strategy import AtrRsiStrategy
                            strategy_class = AtrRsiStrategy
                        elif strategy_name == "BollChannelStrategy":
                            from vnpy_ctastrategy.strategies.boll_channel_strategy import BollChannelStrategy
                            strategy_class = BollChannelStrategy
                        elif strategy_name == "DoubleMaStrategy":
                            from vnpy_ctastrategy.strategies.double_ma_strategy import DoubleMaStrategy
                            strategy_class = DoubleMaStrategy
                        elif strategy_name == "DualThrustStrategy":
                            from vnpy_ctastrategy.strategies.dual_thrust_strategy import DualThrustStrategy
                            strategy_class = DualThrustStrategy
                        elif strategy_name == "KingKeltnerStrategy":
                            from vnpy_ctastrategy.strategies.king_keltner_strategy import KingKeltnerStrategy
                            strategy_class = KingKeltnerStrategy
                        elif strategy_name == "TurtleSignalStrategy":
                            from vnpy_ctastrategy.strategies.turtle_signal_strategy import TurtleSignalStrategy
                            strategy_class = TurtleSignalStrategy
                        elif strategy_name == "MultiTimeframeStrategy":
                            from vnpy_ctastrategy.strategies.multi_timeframe_strategy import MultiTimeframeStrategy
                            strategy_class = MultiTimeframeStrategy
                        elif strategy_name == "MultiSignalStrategy":
                            from vnpy_ctastrategy.strategies.multi_signal_strategy import MultiSignalStrategy
                            strategy_class = MultiSignalStrategy
                        else:
                            # 默认使用AtrRsiStrategy
                            from vnpy_ctastrategy.strategies.atr_rsi_strategy import AtrRsiStrategy
                            strategy_class = AtrRsiStrategy
                            print(f"[警告] 无法加载策略 {strategy_name}，使用默认策略 AtrRsiStrategy")
                    except Exception as e:
                        print(f"[错误] 无法加载策略类: {e}")
                        # 最后备选：使用AtrRsiStrategy
                        from vnpy_ctastrategy.strategies.atr_rsi_strategy import AtrRsiStrategy
                        strategy_class = AtrRsiStrategy
        
        if strategy_class is None:
            raise ValueError(f"无法加载策略类: {self.strategy_combo.currentText()}")
        
        engine = BacktestingEngine()
        engine.set_parameters(
            vt_symbol=vt_symbol,
            interval=interval,
            start=start,
            end=end,
            rate=rate,
            slippage=DEFAULT_SLIPPAGE,
            size=DEFAULT_SIZE,
            pricetick=DEFAULT_PRICETICK,
            capital=capital
        )
        
        # 获取策略的默认参数
        try:
            if hasattr(strategy_class, 'get_class_parameters'):
                default_setting = strategy_class.get_class_parameters()
            else:
                default_setting = {}
        except Exception as e:
            print(f"[警告] 无法获取策略默认参数: {e}，使用空参数")
            default_setting = {}
        
        # 使用选中的策略类及其默认参数
        engine.add_strategy(strategy_class, default_setting)
        engine.load_data()
        engine.run_backtesting()
        df = engine.calculate_result()
        
        return df

