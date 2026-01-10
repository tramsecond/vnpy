"""
VeighNa Trader 启动脚本（包含多标的回测功能）
"""
import sys
from pathlib import Path
from datetime import datetime
from functools import partial

# 设置输出编码
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp
from PySide6 import QtWidgets

# 导入多标的回测Widget
from multi_backtest_widget import MultiBacktestWidget

# 导入交易接口（如果已安装）
try:
    from vnpy_ctp import CtpGateway
    HAS_CTP = True
except ImportError:
    HAS_CTP = False

# 导入功能模块（如果已安装）
try:
    from vnpy_ctastrategy import CtaStrategyApp
    HAS_CTA_STRATEGY = True
except ImportError:
    HAS_CTA_STRATEGY = False

try:
    from vnpy_ctabacktester import CtaBacktesterApp
    HAS_CTA_BACKTESTER = True
except ImportError:
    HAS_CTA_BACKTESTER = False

try:
    from vnpy_datamanager import DataManagerApp
    HAS_DATA_MANAGER = True
except ImportError:
    HAS_DATA_MANAGER = False


class MainWindowWithMultiBacktest(MainWindow):
    """扩展的主窗口，添加多标的回测功能"""
    
    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        super().__init__(main_engine, event_engine)
        self.multi_backtest_widget = None
        self.add_multi_backtest_menu()
    
    def add_multi_backtest_menu(self):
        """添加多标的回测菜单"""
        # 获取功能菜单
        menu_bar = self.menuBar()
        app_menu = None
        
        # 查找功能菜单
        for action in menu_bar.actions():
            if action.text() == "功能":
                app_menu = action.menu()
                break
        
        if app_menu:
            # 在功能菜单中添加分隔线
            app_menu.addSeparator()
            
            # 添加多标的回测菜单项
            multi_backtest_action = QtWidgets.QAction("多标的回测", self)
            multi_backtest_action.triggered.connect(self.open_multi_backtest)
            app_menu.addAction(multi_backtest_action)
    
    def open_multi_backtest(self):
        """打开多标的回测窗口"""
        if self.multi_backtest_widget is None:
            self.multi_backtest_widget = MultiBacktestWidget(self)
            self.multi_backtest_widget.setWindowFlags(
                QtWidgets.Qt.Window | 
                QtWidgets.Qt.WindowMinMaxButtonsHint | 
                QtWidgets.Qt.WindowCloseButtonHint
            )
        
        self.multi_backtest_widget.show()
        self.multi_backtest_widget.raise_()
        self.multi_backtest_widget.activateWindow()


def main():
    """启动 VeighNa Trader"""
    qapp = create_qapp()
    
    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    
    # 添加交易接口
    if HAS_CTP:
        main_engine.add_gateway(CtpGateway)
    
    # 添加功能模块
    if HAS_CTA_STRATEGY:
        main_engine.add_app(CtaStrategyApp)
    
    if HAS_CTA_BACKTESTER:
        main_engine.add_app(CtaBacktesterApp)
    
    if HAS_DATA_MANAGER:
        main_engine.add_app(DataManagerApp)
    
    # 使用扩展的主窗口
    main_window = MainWindowWithMultiBacktest(main_engine, event_engine)
    main_window.showMaximized()
    
    qapp.exec()


if __name__ == "__main__":
    main()

