@echo off
chcp 65001 >nul
echo ========================================
echo 智能数据更新系统 - 每日更新
echo ========================================
echo.

REM 检查Python是否安装
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ 错误：未找到Python，请先安装Python
    pause
    exit /b 1
)

echo ✅ Python环境检查通过
echo.

REM 检查必要文件是否存在
if not exist "data_update_manager.py" (
    echo ❌ 错误：找不到 data_update_manager.py 文件
    pause
    exit /b 1
)

if not exist "daily_update.py" (
    echo ❌ 错误：找不到 daily_update.py 文件
    pause
    exit /b 1
)

echo ✅ 必要文件检查通过
echo.

REM 显示当前时间
echo 🕐 当前时间：%date% %time%
echo.

REM 询问是否继续
echo 是否开始每日数据更新？
echo 1. 是，开始更新
echo 2. 否，退出
echo.
set /p choice="请输入选择 (1 或 2): "

if "%choice%"=="1" (
    echo.
    echo 🚀 开始执行每日数据更新...
    echo.
    
    REM 运行更新脚本
    python daily_update.py
    
    echo.
    echo ✅ 更新完成！
    echo.
    echo 按任意键退出...
    pause >nul
) else (
    echo.
    echo 👋 已取消更新，退出程序
    echo.
    pause
)
