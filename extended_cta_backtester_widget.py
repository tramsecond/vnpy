"""
扩展的CTA回测Widget - 将单标的回测和多标的回测整合在一个界面中
使用QTabWidget将两种回测方式分开
"""
import sys
from functools import partial

# 设置输出编码
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from PySide6 import QtWidgets, QtCore, QtGui
from vnpy.trader.engine import MainEngine
from vnpy.event import EventEngine

# 导入原有的CTA回测Widget
try:
    from vnpy_ctabacktester.ui.widget import BacktesterManager
    HAS_CTA_BACKTESTER = True
except ImportError:
    HAS_CTA_BACKTESTER = False
    print("警告: 无法导入CTA回测模块")

# 导入多标的回测Widget
try:
    from multi_backtest_widget import MultiBacktestWidget
    HAS_MULTI_BACKTEST = True
except ImportError:
    HAS_MULTI_BACKTEST = False
    print("警告: 无法导入多标的回测模块")


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
        
        # 标签页2: 多标的回测
        if HAS_MULTI_BACKTEST:
            try:
                # 传递 main_engine 和 event_engine，以便加载所有策略
                multi_backtest_widget = MultiBacktestWidget(
                    parent=self,
                    main_engine=self.main_engine,
                    event_engine=self.event_engine
                )
                self.tabs.addTab(multi_backtest_widget, "多标的回测")
            except Exception as e:
                error_label = QtWidgets.QLabel(f"多标的回测模块加载失败:\n{str(e)}")
                error_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
                self.tabs.addTab(error_label, "多标的回测")
                import traceback
                traceback.print_exc()
        else:
            error_label = QtWidgets.QLabel("多标的回测模块未加载\n请确保 multi_backtest_widget.py 文件存在")
            error_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
            self.tabs.addTab(error_label, "多标的回测")
        
        # 设置布局
        layout = QtWidgets.QVBoxLayout()
        layout.addWidget(self.tabs)
        layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(layout)

