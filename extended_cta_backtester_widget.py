"""
扩展的CTA回测Widget - 将单标的回测和多标的回测整合在一个界面中
使用QTabWidget将两种回测方式分开
"""
import sys
from functools import partial

from PySide6 import QtWidgets, QtCore, QtGui
from vnpy.trader.engine import MainEngine
from vnpy.event import EventEngine

# 导入原有的CTA回测Widget
try:
    from vnpy_ctabacktester.ui.widget import BacktesterManager
    HAS_CTA_BACKTESTER = True
except ImportError:
    HAS_CTA_BACKTESTER = False

# 导入多标的回测Widget（严格模式）
try:
    from multi_backtest_strict import MultiBacktestStrictWidget
    HAS_MULTI_BACKTEST = True
except ImportError:
    HAS_MULTI_BACKTEST = False


class ExtendedCtaBacktesterWidget(QtWidgets.QWidget):
    """
    扩展的CTA回测Widget
    使用标签页将单标的回测和多标的回测整合在一起
    """
    
    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        super().__init__()
        self.main_engine = main_engine
        self.event_engine = event_engine
        
        self.init_ui()
    
    def init_ui(self):
        """初始化UI"""
        self.setWindowTitle("CTA回测")
        
        # 创建标签页
        self.tabs = QtWidgets.QTabWidget()
        
        # 标签页1: 单标的回测（原有的CTA回测界面）
        if HAS_CTA_BACKTESTER:
            try:
                single_backtest_widget = BacktesterManager(self.main_engine, self.event_engine)
                self.tabs.addTab(single_backtest_widget, "单标的回测")
            except Exception as e:
                error_label = QtWidgets.QLabel(f"CTA回测模块加载失败:\n{str(e)}")
                error_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
                self.tabs.addTab(error_label, "单标的回测")
        else:
            error_label = QtWidgets.QLabel("CTA回测模块未加载")
            error_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
            self.tabs.addTab(error_label, "单标的回测")
        
        # 标签页2: 多标的回测（严格模式）
        if HAS_MULTI_BACKTEST:
            try:
                # 使用严格模式的多标的回测，完全遵循单标的回测逻辑
                multi_backtest_widget = MultiBacktestStrictWidget(
                    main_engine=self.main_engine,
                    event_engine=self.event_engine,
                    parent=self
                )
                self.tabs.addTab(multi_backtest_widget, "多标的回测")
            except Exception as e:
                error_label = QtWidgets.QLabel(f"多标的回测模块加载失败:\n{str(e)}")
                error_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
                self.tabs.addTab(error_label, "多标的回测")
                import traceback
                traceback.print_exc()
        else:
            error_label = QtWidgets.QLabel("多标的回测模块未加载\n请确保 multi_backtest_strict.py 文件存在")
            error_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
            self.tabs.addTab(error_label, "多标的回测")
        
        # 设置布局
        layout = QtWidgets.QVBoxLayout()
        layout.addWidget(self.tabs)
        layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(layout)

