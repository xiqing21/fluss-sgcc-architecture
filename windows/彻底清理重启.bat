@echo off
chcp 65001 >nul
title SGCC Fluss 彻底清理重启工具

echo.
echo =========================================
echo 🧹 SGCC Fluss 彻底清理重启工具
echo =========================================
echo.

echo ⚠️  警告：此操作将彻底清理所有Fluss数据！
echo 💡 目的：解决 LogStorageException 问题
echo.

set /p confirm=确认要彻底清理并重启环境？(Y/N): 
if /i "%confirm%" neq "Y" (
    echo 已取消操作
    pause
    exit /b 0
)

echo.
echo 🚀 开始彻底清理重启过程...
echo.

:: 获取脚本所在目录
set "SCRIPT_DIR=%~dp0"
set "PS_SCRIPT=%SCRIPT_DIR%彻底清理重启脚本.ps1"

:: 检查PowerShell脚本是否存在
if not exist "%PS_SCRIPT%" (
    echo ❌ PowerShell脚本不存在: %PS_SCRIPT%
    pause
    exit /b 1
)

:: 切换到项目根目录
cd /d "%SCRIPT_DIR%.."

:: 运行彻底清理脚本
powershell -Command "& {Set-ExecutionPolicy -ExecutionPolicy Bypass -Scope Process -Force; & '%PS_SCRIPT%'}"

if %errorLevel% == 0 (
    echo.
    echo ✅ 彻底清理重启完成
    echo 💡 现在可以运行测试脚本了
) else (
    echo.
    echo ❌ 彻底清理重启过程中出现错误
    echo 📋 请查看上方的错误信息进行故障排除
)

echo.
pause 