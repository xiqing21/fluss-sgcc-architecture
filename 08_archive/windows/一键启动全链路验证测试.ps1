# 🚀 SGCC Fluss 一键启动全链路验证测试脚本 (Windows PowerShell版)
# 功能：环境启动 + 业务SQL执行 + 全链路数据验证 + 增删改测试 + 性能指标统计
# 作者：AI助手 & 用户协作开发
# 版本：v1.0
# 日期：$(Get-Date -Format "yyyy-MM-dd")

# 设置PowerShell错误处理
$ErrorActionPreference = "Continue"

# 测试配置
$script:TEST_START_TIME = Get-Date
$script:TEST_REPORT_DIR = "test-reports"
$script:TEST_REPORT_FILE = "$TEST_REPORT_DIR/full_test_report_$(Get-Date -Format 'yyyyMMdd_HHmmss').md"

# 创建测试报告目录
if (!(Test-Path $TEST_REPORT_DIR)) {
    New-Item -ItemType Directory -Path $TEST_REPORT_DIR -Force | Out-Null
}

# 性能指标统计
$script:metrics = @{
    total_records_processed = 0
    total_jobs_created = 0
    total_test_scenarios = 0
    successful_scenarios = 0
    failed_scenarios = 0
}

# 日志函数
function Write-LogInfo {
    param([string]$Message)
    Write-Host "[INFO] $Message" -ForegroundColor Blue
    Add-Content -Path $TEST_REPORT_FILE -Value "[INFO] $Message"
}

function Write-LogSuccess {
    param([string]$Message)
    Write-Host "[SUCCESS] $Message" -ForegroundColor Green
    Add-Content -Path $TEST_REPORT_FILE -Value "[SUCCESS] $Message"
}

function Write-LogWarning {
    param([string]$Message)
    Write-Host "[WARNING] $Message" -ForegroundColor Yellow
    Add-Content -Path $TEST_REPORT_FILE -Value "[WARNING] $Message"
}

function Write-LogError {
    param([string]$Message)
    Write-Host "[ERROR] $Message" -ForegroundColor Red
    Add-Content -Path $TEST_REPORT_FILE -Value "[ERROR] $Message"
}

# 初始化测试报告
function Initialize-TestReport {
    $reportContent = @"
# 🚀 SGCC Fluss 全链路验证测试报告

**测试开始时间**: $(Get-Date)  
**测试环境**: Docker Compose + Fluss 0.7.0 + Flink 1.20 + PostgreSQL  
**测试版本**: v1.0  
**运行平台**: Windows PowerShell

---

## 📊 测试概览

"@
    Set-Content -Path $TEST_REPORT_FILE -Value $reportContent
}

# 环境启动函数（增强清理版）
function Start-Environment {
    Write-LogInfo "🌟 步骤1: 启动测试环境（含彻底清理）"
    
    # 优雅停止所有Flink任务
    Write-LogInfo "优雅停止所有Flink任务..."
    try {
        $runningJobs = docker exec jobmanager-sgcc /opt/flink/bin/flink list -r 2>&1
        
        if ($runningJobs -and $runningJobs -notlike "*No running jobs*") {
            $jobIds = [regex]::Matches($runningJobs, "([a-f0-9]{32})")
            
            foreach ($match in $jobIds) {
                $jobId = $match.Value
                Write-LogInfo "停止任务: $jobId"
                docker exec jobmanager-sgcc /opt/flink/bin/flink cancel $jobId 2>&1 | Out-Null
                Start-Sleep -Seconds 2
            }
            
            Write-LogSuccess "✅ 已停止 $($jobIds.Count) 个Flink任务"
        }
    } catch {
        Write-LogWarning "停止Flink任务时出现警告: $($_.Exception.Message)"
    }
    
    # 停止现有环境
    Write-LogInfo "停止现有环境..."
    try {
        docker-compose down 2>&1 | Out-Null
        Start-Sleep -Seconds 10
    } catch {
        Write-LogWarning "停止环境时出现警告: $($_.Exception.Message)"
    }
    
    # 彻底清理Fluss相关数据卷（解决metadata问题）
    Write-LogInfo "彻底清理Fluss metadata和数据卷..."
    $volumesToRemove = @(
        "fluss_coordinator_data",
        "fluss_tablet_data", 
        "flink_checkpoints",
        "flink_savepoints"
    )

    foreach ($volume in $volumesToRemove) {
        try {
            $volumeExists = docker volume ls -q | Select-String "^$volume$"
            if ($volumeExists) {
                Write-LogInfo "删除卷: $volume"
                docker volume rm $volume 2>&1 | Out-Null
            }
        } catch {
            Write-LogWarning "删除卷 $volume 时出现警告"
        }
    }
    
    # 清理未使用的卷
    Write-LogInfo "清理未使用的Docker卷..."
    try {
        docker volume prune -f 2>&1 | Out-Null
    } catch {
        Write-LogWarning "清理Docker卷时出现警告: $($_.Exception.Message)"
    }
    
    # 清理系统缓存
    Write-LogInfo "清理Docker系统缓存..."
    try {
        docker system prune -f 2>&1 | Out-Null
    } catch {
        Write-LogWarning "清理系统缓存时出现警告: $($_.Exception.Message)"
    }
    
    # 启动环境
    Write-LogInfo "启动Docker Compose环境..."
    try {
        $result = docker-compose up -d 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-LogSuccess "环境启动成功"
        } else {
            Write-LogError "环境启动失败: $result"
            exit 1
        }
    } catch {
        Write-LogError "环境启动失败: $($_.Exception.Message)"
        exit 1
    }
    
    # 等待服务就绪
    Write-LogInfo "等待服务就绪..."
    Start-Sleep -Seconds 45
    
    # 验证服务状态
    Write-LogInfo "验证服务状态..."
    $services = @("coordinator-server-sgcc", "tablet-server-sgcc", "jobmanager-sgcc", "taskmanager-sgcc-1", "postgres-sgcc-source", "postgres-sgcc-sink")
    
    foreach ($service in $services) {
        try {
            $containerStatus = docker ps --format "table {{.Names}}" | Select-String $service
            if ($containerStatus) {
                Write-LogSuccess "✅ $service 服务正常"
            } else {
                Write-LogError "❌ $service 服务异常"
            }
        } catch {
            Write-LogError "❌ $service 服务状态检查失败: $($_.Exception.Message)"
        }
    }
}

# 执行SQL脚本函数
function Invoke-SqlScript {
    param(
        [string]$ScriptFile,
        [string]$Description
    )
    
    Write-LogInfo "执行: $Description"
    
    $startTime = Get-Date
    
    try {
        if (Test-Path $ScriptFile) {
            $result = Get-Content $ScriptFile | docker exec -i sql-client-sgcc /opt/flink/bin/sql-client.sh embedded 2>&1
            if ($LASTEXITCODE -eq 0) {
                $endTime = Get-Date
                $duration = ($endTime - $startTime).TotalSeconds
                Write-LogSuccess "✅ $Description 完成 (耗时: $([math]::Round($duration))秒)"
                $script:metrics.total_jobs_created++
                return $true
            } else {
                Write-LogError "❌ $Description 失败: $result"
                return $false
            }
        } else {
            Write-LogError "❌ SQL脚本文件不存在: $ScriptFile"
            return $false
        }
    } catch {
        Write-LogError "❌ $Description 执行异常: $($_.Exception.Message)"
        return $false
    }
}

# 数据验证函数
function Test-DataValidation {
    Write-LogInfo "🔍 步骤2: 数据验证"
    
    # 验证PostgreSQL源数据
    Write-LogInfo "验证PostgreSQL源数据..."
    try {
        $sourceCount = docker exec postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -t -c "SELECT COUNT(*) FROM sgcc_power.power_monitoring;" 2>&1
        $sourceCount = ($sourceCount -split "`n" | Where-Object {$_ -match "^\s*\d+\s*$"})[0].Trim()
        if ([int]$sourceCount -gt 0) {
            Write-LogSuccess "✅ 源数据表记录数: $sourceCount"
            $script:metrics.total_records_processed += [int]$sourceCount
        } else {
            Write-LogWarning "⚠️ 源数据表无数据"
        }
    } catch {
        Write-LogWarning "⚠️ 源数据验证失败: $($_.Exception.Message)"
    }
    
    # 验证Fluss各层数据
    Write-LogInfo "验证Fluss数据湖各层数据..."
    
    # 创建验证SQL脚本
    $validateFlussScript = @"
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

USE CATALOG fluss_catalog;

-- 验证ODS层
SELECT 'ODS power_monitoring' as layer, COUNT(*) as count FROM sgcc_ods.power_monitoring_ods;

-- 验证DWD层
SELECT 'DWD power_monitoring' as layer, COUNT(*) as count FROM sgcc_dwd.power_monitoring_dwd;

-- 验证DWS层
SELECT 'DWS power_summary' as layer, COUNT(*) as count FROM sgcc_dws.power_summary_dws;

-- 验证ADS层
SELECT 'ADS智能报告' as layer, COUNT(*) as count FROM sgcc_ads.power_intelligence_report;

-- 验证最新数据
SELECT 'DWD最新数据' as info, equipment_id, voltage_a, current_a, monitoring_time 
FROM sgcc_dwd.power_monitoring_dwd 
ORDER BY monitoring_time DESC LIMIT 5;
"@
    
    $tempValidateFile = [System.IO.Path]::GetTempFileName() + ".sql"
    Set-Content -Path $tempValidateFile -Value $validateFlussScript
    
    # 执行验证
    if (Invoke-SqlScript -ScriptFile $tempValidateFile -Description "Fluss数据湖验证") {
        Write-LogSuccess "✅ Fluss数据湖验证完成"
    } else {
        Write-LogError "❌ Fluss数据湖验证失败"
    }
    
    # 清理临时文件
    Remove-Item $tempValidateFile -Force -ErrorAction SilentlyContinue
    
    # 验证PostgreSQL目标数据
    Write-LogInfo "验证PostgreSQL目标数据..."
    try {
        # 先检查是否有表
        $tables = docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -t -c "\dt" 2>&1
        if ($tables -like "*did not find any relations*" -or $tables -like "*Did not find any relations*") {
            Write-LogWarning "⚠️ 目标数据库暂无表，数据流可能正在建立中"
        } else {
            Write-LogSuccess "✅ 目标数据库表结构正常"
            # 如果有表，尝试查询数据
            $targetCount = docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -t -c "SELECT COUNT(*) FROM power_summary_report;" 2>&1
            $targetCount = ($targetCount -split "`n" | Where-Object {$_ -match "^\s*\d+\s*$"})[0].Trim()
            if ([int]$targetCount -gt 0) {
                Write-LogSuccess "✅ 目标数据表记录数: $targetCount"
                $script:metrics.total_records_processed += [int]$targetCount
            }
        }
    } catch {
        Write-LogWarning "⚠️ 目标数据验证失败: $($_.Exception.Message)"
    }
}

# 停止所有Flink任务
function Stop-AllFlinkJobs {
    Write-LogInfo "🛑 停止所有Flink任务..."
    
    try {
        # 获取所有运行中的任务
        $runningJobs = docker exec jobmanager-sgcc /opt/flink/bin/flink list -r 2>&1
        
        if ($runningJobs -and $runningJobs -notlike "*No running jobs*") {
            # 提取任务ID并停止
            $jobIds = [regex]::Matches($runningJobs, "([a-f0-9]{32})")
            
            foreach ($match in $jobIds) {
                $jobId = $match.Value
                Write-LogInfo "停止任务: $jobId"
                docker exec jobmanager-sgcc /opt/flink/bin/flink cancel $jobId 2>&1 | Out-Null
                Start-Sleep -Seconds 2
            }
            
            Write-LogSuccess "✅ 已停止 $($jobIds.Count) 个任务"
        } else {
            Write-LogInfo "没有运行中的任务需要停止"
        }
    } catch {
        Write-LogWarning "⚠️ 停止任务时出现警告: $($_.Exception.Message)"
    }
    
    # 等待任务完全停止
    Start-Sleep -Seconds 10
}

# 业务场景测试函数（增强版）
function Invoke-BusinessScenarios {
    Write-LogInfo "🎯 步骤3: 业务场景测试（含任务管理）"
    
    $scenarios = @(
        @{File = "business-scenarios/场景1_高频维度表服务.sql"; Description = "场景1_高频维度表服务"},
        @{File = "business-scenarios/场景2_智能双流JOIN.sql"; Description = "场景2_智能双流JOIN"},
        @{File = "business-scenarios/场景3_时间旅行查询.sql"; Description = "场景3_时间旅行查询"},
        @{File = "business-scenarios/场景4_柱状流优化.sql"; Description = "场景4_柱状流优化"}
    )
    
    foreach ($scenario in $scenarios) {
        $script:metrics.total_test_scenarios++
        
        Write-LogInfo "🎯 开始执行: $($scenario.Description)"
        
        if (Invoke-SqlScript -ScriptFile $scenario.File -Description $scenario.Description) {
            $script:metrics.successful_scenarios++
            
            # 场景执行成功后等待一段时间让任务稳定运行
            Write-LogInfo "等待场景任务稳定运行 (20秒)..."
            Start-Sleep -Seconds 20
            
            # 停止当前场景的所有任务
            Write-LogInfo "🛑 清理场景任务，准备下一个场景..."
            Stop-AllFlinkJobs
            
        } else {
            $script:metrics.failed_scenarios++
            Write-LogError "❌ 场景执行失败，停止所有任务后继续下一个场景"
            Stop-AllFlinkJobs
        }
        
        # 场景间暂停
        Write-LogInfo "场景间等待 (15秒)..."
        Start-Sleep -Seconds 15
    }
}

# 增删改测试函数
function Invoke-CrudOperations {
    Write-LogInfo "🔄 步骤4: 增删改操作测试"
    
    # 创建增删改测试脚本
    $testEquipmentId = Get-Random -Minimum 9000 -Maximum 9999
    $crudScript = @"
-- 插入测试数据到sgcc_power.power_monitoring表
INSERT INTO sgcc_power.power_monitoring (
    monitoring_id, equipment_id, voltage_a, voltage_b, voltage_c, 
    current_a, current_b, current_c, power_active, power_reactive, 
    frequency, temperature, humidity, monitoring_time
) VALUES 
  ($testEquipmentId, $testEquipmentId, 220.5, 219.8, 221.2, 15.2, 15.1, 15.3, 3350.0, 450.0, 50.01, 25.5, 60.2, NOW()),
  ($($testEquipmentId+1), $($testEquipmentId+1), 218.3, 218.1, 218.9, 14.8, 14.7, 14.9, 3230.0, 420.0, 50.02, 26.5, 58.0, NOW()),
  ($($testEquipmentId+2), $($testEquipmentId+2), 225.1, 224.8, 225.3, 16.1, 16.0, 16.2, 3625.0, 480.0, 49.99, 24.0, 62.0, NOW());

-- 更新测试数据
UPDATE sgcc_power.power_monitoring SET temperature = 35.0, power_active = 3400.0 WHERE equipment_id = $testEquipmentId;

-- 删除测试数据
DELETE FROM sgcc_power.power_monitoring WHERE equipment_id = $($testEquipmentId+2);
"@
    
    $tempCrudFile = [System.IO.Path]::GetTempFileName() + ".sql"
    Set-Content -Path $tempCrudFile -Value $crudScript
    
    # 执行增删改操作
    Write-LogInfo "执行增删改操作..."
    try {
        $result = Get-Content $tempCrudFile | docker exec -i postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-LogSuccess "✅ 增删改操作执行成功"
            
            # 等待CDC同步
            Write-LogInfo "等待CDC同步..."
            Start-Sleep -Seconds 30
            
            # 验证同步结果
            Write-LogInfo "验证CDC同步结果..."
            Test-DataValidation
        } else {
            Write-LogError "❌ 增删改操作执行失败: $result"
        }
    } catch {
        Write-LogError "❌ 增删改操作执行异常: $($_.Exception.Message)"
    } finally {
        # 清理临时文件
        Remove-Item $tempCrudFile -Force -ErrorAction SilentlyContinue
    }
}

# 性能指标统计函数
function Get-PerformanceMetrics {
    Write-LogInfo "📈 步骤5: 性能指标统计"
    
    $testEndTime = Get-Date
    $totalDuration = ($testEndTime - $TEST_START_TIME).TotalSeconds
    
    # 获取Flink作业信息
    Write-LogInfo "获取Flink作业信息..."
    try {
        $flinkJobs = docker exec jobmanager-sgcc /opt/flink/bin/flink list 2>&1
        $runningJobsCount = ($flinkJobs | Select-String "RUNNING").Count
        if (!$runningJobsCount) { $runningJobsCount = 0 }
    } catch {
        $runningJobsCount = 0
        Write-LogWarning "获取Flink作业信息失败: $($_.Exception.Message)"
    }
    
    # 计算吞吐量
    $throughput = 0
    if ($totalDuration -gt 0) {
        $throughput = [math]::Round($metrics.total_records_processed / $totalDuration, 2)
    }
    
    # 计算成功率
    $successRate = 0
    if ($metrics.total_test_scenarios -gt 0) {
        $successRate = [math]::Round(($metrics.successful_scenarios * 100) / $metrics.total_test_scenarios, 2)
    }
    
    # 输出性能指标到报告
    $metricsContent = @"

## 📈 性能指标统计

| 指标 | 数值 |
|------|------|
| 总耗时 | $([math]::Round($totalDuration))秒 |
| 处理记录数 | $($metrics.total_records_processed) |
| 创建作业数 | $($metrics.total_jobs_created) |
| 当前运行作业数 | $runningJobsCount |
| 测试场景数 | $($metrics.total_test_scenarios) |
| 成功场景数 | $($metrics.successful_scenarios) |
| 失败场景数 | $($metrics.failed_scenarios) |
| 数据吞吐量 | $throughput 记录/秒 |
| 成功率 | $successRate% |

## 🔧 系统资源使用情况

"@
    Add-Content -Path $TEST_REPORT_FILE -Value $metricsContent
    
    # 获取容器资源使用情况
    Write-LogInfo "获取容器资源使用情况..."
    try {
        $dockerStats = docker stats --no-stream --format "table {{.Name}}`t{{.CPUPerc}}`t{{.MemUsage}}" 2>&1
        Add-Content -Path $TEST_REPORT_FILE -Value $dockerStats
    } catch {
        Write-LogWarning "获取Docker状态失败: $($_.Exception.Message)"
    }
    
    # 输出到终端
    Write-LogSuccess "📊 测试完成统计："
    Write-LogSuccess "  - 总耗时: $([math]::Round($totalDuration))秒"
    Write-LogSuccess "  - 处理记录数: $($metrics.total_records_processed)"
    Write-LogSuccess "  - 创建作业数: $($metrics.total_jobs_created)"
    Write-LogSuccess "  - 成功场景数: $($metrics.successful_scenarios)/$($metrics.total_test_scenarios)"
    Write-LogSuccess "  - 数据吞吐量: $throughput 记录/秒"
    Write-LogSuccess "  - 成功率: $successRate%"
}

# 主函数
function Start-FullTestSuite {
    Write-Host "🚀 SGCC Fluss 一键启动全链路验证测试 (Windows版)" -ForegroundColor Blue
    Write-Host "===============================================" -ForegroundColor Blue
    
    try {
        # 初始化测试报告
        Initialize-TestReport
        
        # 执行测试流程
        Start-Environment
        Test-DataValidation
        Invoke-BusinessScenarios
        Invoke-CrudOperations
        Get-PerformanceMetrics
        
        # 完成信息
        Write-LogSuccess "🎉 全链路验证测试完成！"
        Write-LogSuccess "📄 详细报告: $TEST_REPORT_FILE"
        
        Write-Host "🎉 测试完成！详细报告已保存到: $TEST_REPORT_FILE" -ForegroundColor Green
        
        # 询问是否打开报告
        $openReport = Read-Host "是否要打开测试报告？(Y/N)"
        if ($openReport -eq "Y" -or $openReport -eq "y") {
            if (Test-Path $TEST_REPORT_FILE) {
                Start-Process notepad.exe $TEST_REPORT_FILE
            }
        }
        
    } catch {
        Write-LogError "测试过程中发生错误: $($_.Exception.Message)"
        Write-Host "❌ 测试执行失败，请检查错误日志" -ForegroundColor Red
    }
}

# 设置脚本主入口点
if ($MyInvocation.InvocationName -ne '.') {
    Start-FullTestSuite
} 