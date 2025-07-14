@echo off
chcp 65001 >nul
title SGCC Fluss 全链路验证测试 - Windows版

echo.
echo ========================================
echo 🚀 SGCC Fluss 全链路验证测试启动器
echo ========================================
echo.

:: 检查是否以管理员身份运行
net session >nul 2>&1
if %errorLevel% == 0 (
    echo ✅ 已获得管理员权限
) else (
    echo ⚠️  检测到未以管理员身份运行
    echo 📝 建议以管理员身份运行以避免权限问题
    echo.
    set /p choice=是否继续？(Y/N): 
    if /i "%choice%" neq "Y" (
        echo 已取消执行
        pause
        exit /b 1
    )
)

echo.
echo 🔍 正在检查运行环境...

:: 检查Docker
docker --version >nul 2>&1
if %errorLevel% == 0 (
    echo ✅ Docker 已安装
) else (
    echo ❌ Docker 未安装或未启动
    echo 📋 请先安装并启动 Docker Desktop
    pause
    exit /b 1
)

:: 检查docker-compose
docker-compose --version >nul 2>&1
if %errorLevel% == 0 (
    echo ✅ Docker Compose 已安装
) else (
    echo ❌ Docker Compose 未安装
    echo 📋 请确保 Docker Desktop 完整安装
    pause
    exit /b 1
)

:: 检查PowerShell
powershell -Command "Write-Host 'PowerShell 可用'" >nul 2>&1
if %errorLevel% == 0 (
    echo ✅ PowerShell 可用
) else (
    echo ❌ PowerShell 不可用
    pause
    exit /b 1
)

echo.
echo 🚀 环境检查完毕，准备启动测试脚本...
echo.
echo 📋 测试将包括以下步骤：
echo    1. 启动Docker Compose环境
echo    2. 验证数据同步状态
echo    3. 运行业务场景测试
echo    4. 执行增删改测试
echo    5. 生成性能报告
echo.

set /p confirm=确认开始测试？(Y/N): 
if /i "%confirm%" neq "Y" (
    echo 已取消测试
    pause
    exit /b 0
)

echo.
echo 🎯 正在启动PowerShell测试脚本...
echo.

:: 获取脚本所在目录
set "SCRIPT_DIR=%~dp0"
set "PS_SCRIPT=%SCRIPT_DIR%一键启动全链路验证测试.ps1"

:: 检查PowerShell脚本是否存在
if not exist "%PS_SCRIPT%" (
    echo ❌ PowerShell脚本不存在: %PS_SCRIPT%
    pause
    exit /b 1
)

:: 切换到项目根目录（批处理文件所在目录的上级目录）
cd /d "%SCRIPT_DIR%.."

:: 设置PowerShell执行策略并运行脚本
powershell -Command "& {Set-ExecutionPolicy -ExecutionPolicy Bypass -Scope Process -Force; & '%PS_SCRIPT%'}"

if %errorLevel% == 0 (
    echo.
    echo ✅ 测试脚本执行完毕
) else (
    echo.
    echo ❌ 测试脚本执行过程中出现错误
    echo 📋 请查看上方的错误信息进行故障排除
)

echo.
echo 📄 测试报告已保存在 test-reports 目录下
echo.
pause 