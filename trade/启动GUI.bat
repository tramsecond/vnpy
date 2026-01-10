@echo off
chcp 65001 >nul
title 股票技术分析交易系统 - GUI启动器

echo.
echo ========================================
echo    股票技术分析交易系统 - GUI启动器
echo ========================================
echo.

:: 检查Python是否安装
python --version >nul 2>&1
if errorlevel 1 (
    echo [错误] 未检测到Python，请先安装Python
    echo 请访问 https://www.python.org/downloads/ 下载并安装Python
    pause
    exit /b 1
)

:: 检查必要的Python包
echo [信息] 检查Python环境...
python -c "import tkinter" >nul 2>&1
if errorlevel 1 (
    echo [错误] 缺少tkinter模块，请安装完整的Python
    pause
    exit /b 1
)

python -c "import pandas" >nul 2>&1
if errorlevel 1 (
    echo [警告] 缺少pandas模块，正在安装...
    pip install pandas
)

python -c "import akshare" >nul 2>&1
if errorlevel 1 (
    echo [警告] 缺少akshare模块，正在安装...
    pip install akshare
)

python -c "import yfinance" >nul 2>&1
if errorlevel 1 (
    echo [警告] 缺少yfinance模块，正在安装...
    pip install yfinance
)

:: 检查GUI文件是否存在
if not exist "gui_main.py" (
    echo [错误] 未找到gui_main.py文件
    echo 请确保批处理文件与gui_main.py在同一目录下
    pause
    exit /b 1
)

echo [信息] 环境检查完成，启动GUI界面...
echo.

:: 启动GUI
python gui_main.py

:: 如果GUI异常退出，显示错误信息
if errorlevel 1 (
    echo.
    echo [错误] GUI程序异常退出
    echo 请检查Python环境和依赖包是否正确安装
    pause
)

echo.
echo [信息] 程序已退出
pause
