@echo off
echo 清理Python缓存...
echo.

REM 清理当前目录的Python缓存
if exist "__pycache__" (
    echo 删除 __pycache__ 目录...
    rd /s /q "__pycache__"
)

REM 清理multi_backtest_widget的缓存
if exist "multi_backtest_widget.pyc" (
    echo 删除 multi_backtest_widget.pyc...
    del /f /q "multi_backtest_widget.pyc"
)

REM 清理extended_cta_backtester_widget的缓存
if exist "extended_cta_backtester_widget.pyc" (
    echo 删除 extended_cta_backtester_widget.pyc...
    del /f /q "extended_cta_backtester_widget.pyc"
)

echo.
echo 缓存清理完成！
echo.
echo 正在启动VeighNa Trader...
echo.
python run.py

pause

