"""
VeighNa Trader 启动脚本
包含已安装的功能模块和多标的回测功能
"""
import sys
from functools import partial
from pathlib import Path

# 设置输出编码
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp
from PySide6 import QtWidgets, QtCore, QtGui

# 导入多标的回测Widget
try:
    from multi_backtest_widget import MultiBacktestWidget
    HAS_MULTI_BACKTEST = True
except ImportError:
    HAS_MULTI_BACKTEST = False
    print("警告: 无法导入多标的回测模块，请确保 multi_backtest_widget.py 在同一目录下")

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
        self.extended_backtester_widget = None
    
    def open_widget(self, widget_class, name: str) -> None:
        """
        重写open_widget方法，如果是CTA回测则替换为扩展版本（包含多标的回测标签页）
        """
        # 检查是否是CTA回测Widget - 通过name判断（通常为'CtaBacktester'）
        # 或者检查widget_class的名称
        try:
            widget_class_name = getattr(widget_class, '__name__', '')
            is_cta_backtester = (
                name == 'CtaBacktester' or 
                'BacktesterManager' in widget_class_name or
                'cta_backtester' in name.lower()
            )
            
            if is_cta_backtester:
                # 使用扩展的CTA回测Widget（包含标签页）
                if self.extended_backtester_widget is None:
                    try:
                        from extended_cta_backtester_widget import ExtendedCtaBacktesterWidget
                        self.extended_backtester_widget = ExtendedCtaBacktesterWidget(
                            self.main_engine, self.event_engine
                        )
                        self.widgets[name] = self.extended_backtester_widget
                        print(f"[信息] 已加载扩展CTA回测Widget（包含多标的回测功能）")
                    except Exception as e:
                        print(f"[错误] 加载扩展CTA回测Widget失败: {e}")
                        import traceback
                        traceback.print_exc()
                        # 如果加载失败，使用原有的Widget
                        widget = self.widgets.get(name, None)
                        if not widget:
                            widget = widget_class(self.main_engine, self.event_engine)
                            self.widgets[name] = widget
                        if isinstance(widget, QtWidgets.QDialog):
                            widget.exec()
                        else:
                            widget.show()
                        return
                
                widget = self.extended_backtester_widget
            else:
                # 其他Widget使用原有逻辑
                widget = self.widgets.get(name, None)
                if not widget:
                    widget = widget_class(self.main_engine, self.event_engine)
                    self.widgets[name] = widget
        except Exception as e:
            # 如果出现异常，使用原有逻辑
            print(f"[错误] 检查CTA回测Widget时出错: {e}")
            import traceback
            traceback.print_exc()
            widget = self.widgets.get(name, None)
            if not widget:
                widget = widget_class(self.main_engine, self.event_engine)
                self.widgets[name] = widget
        
        # 显示Widget
        if isinstance(widget, QtWidgets.QDialog):
            widget.exec()
        else:
            widget.show()


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
    
    # 使用扩展的主窗口（包含多标的回测功能）
    main_window = MainWindowWithMultiBacktest(main_engine, event_engine)
    main_window.showMaximized()
    
    qapp.exec()


if __name__ == "__main__":
    main()

