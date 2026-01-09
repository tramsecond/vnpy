"""
VeighNa Trader 启动脚本
包含已安装的功能模块
"""
from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp

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
    
    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()
    
    qapp.exec()


if __name__ == "__main__":
    main()

