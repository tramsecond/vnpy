@echo off
chcp 65001 >nul
echo ========================================
echo 关闭Cursor Worktree功能
echo ========================================
echo.

echo [1/3] 检查Worktree目录...
if exist "C:\Users\dd\.cursor\worktrees\vnpy" (
    echo ✓ 找到Worktree目录
    echo.
    echo [2/3] 删除Worktree目录...
    rmdir /s /q "C:\Users\dd\.cursor\worktrees\vnpy"
    echo ✓ Worktree目录已删除
) else (
    echo ✗ Worktree目录不存在
)

echo.
echo [3/3] 清理Worktree配置...
if exist ".cursor\worktrees.json" (
    del /q ".cursor\worktrees.json"
    echo ✓ 配置文件已删除
) else (
    echo ✗ 配置文件不存在
)

echo.
echo ========================================
echo ✅ 清理完成！
echo ========================================
echo.
echo 建议：
echo 1. 关闭Cursor编辑器
echo 2. 重新打开项目：D:\vnpy\vnpy
echo 3. 确认文件路径不再包含"worktrees"
echo.
pause



