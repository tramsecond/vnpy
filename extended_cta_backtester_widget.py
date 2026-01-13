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
    
    def load_user_strategies_for_single_backtest(self, backtest_widget):
        """为单标的回测加载用户自定义策略"""
        try:
            from pathlib import Path
            
            # 获取backtester_engine
            backtester_engine = backtest_widget.backtester_engine
            
            # 从脚本所在目录加载strategies文件夹
            strategies_path = Path(__file__).parent / "strategies"
            if strategies_path.exists():
                backtester_engine.load_strategy_class_from_folder(
                    strategies_path, 
                    "strategies"
                )
                
                # 获取所有策略类名
                class_names = backtester_engine.get_strategy_class_names()
                class_names.sort()
                
                # 为新加载的策略初始化默认设置
                for class_name in class_names:
                    if class_name not in backtest_widget.settings:
                        setting = backtester_engine.get_default_setting(class_name)
                        backtest_widget.settings[class_name] = setting
                
                # 重新加载策略列表到UI
                backtest_widget.class_combo.clear()
                backtest_widget.class_combo.addItems(class_names)
                
        except Exception as e:
            import traceback
            traceback.print_exc()
    
    def init_ui(self):
        """初始化UI"""
        self.setWindowTitle("CTA回测")
        
        # 创建标签页
        self.tabs = QtWidgets.QTabWidget()
        
        # 标签页1: 单标的回测（原有的CTA回测界面）
        if HAS_CTA_BACKTESTER:
            try:
                single_backtest_widget = BacktesterManager(self.main_engine, self.event_engine)
                
                # 手动加载用户自定义策略
                self.load_user_strategies_for_single_backtest(single_backtest_widget)
                
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

