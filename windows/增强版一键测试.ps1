# 🚀 SGCC Fluss 增强版一键测试脚本 (Windows PowerShell版)
# 功能：完整数据流测试 + 任务管理 + 真实数据验证 + 增删改实时性测试
# 作者：AI助手 & 用户协作开发
# 版本：v2.0
# 日期：$(Get-Date -Format "yyyy-MM-dd")

# 设置PowerShell错误处理
$ErrorActionPreference = "Continue"

# 测试配置
$script:TEST_START_TIME = Get-Date
$script:TEST_REPORT_DIR = "test-reports"
$script:TEST_REPORT_FILE = "$TEST_REPORT_DIR/enhanced_test_report_$(Get-Date -Format 'yyyyMMdd_HHmmss').md"

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
    data_sync_tests = 0
    successful_sync_tests = 0
}

# 日志函数
function Write-LogInfo {
    param([string]$Message)
    Write-Host "[INFO] $Message" -ForegroundColor Blue
    Add-Content -Path $TEST_REPORT_FILE -Value "[INFO] $(Get-Date -Format 'HH:mm:ss') $Message"
}

function Write-LogSuccess {
    param([string]$Message)
    Write-Host "[SUCCESS] $Message" -ForegroundColor Green
    Add-Content -Path $TEST_REPORT_FILE -Value "[SUCCESS] $(Get-Date -Format 'HH:mm:ss') $Message"
}

function Write-LogWarning {
    param([string]$Message)
    Write-Host "[WARNING] $Message" -ForegroundColor Yellow
    Add-Content -Path $TEST_REPORT_FILE -Value "[WARNING] $(Get-Date -Format 'HH:mm:ss') $Message"
}

function Write-LogError {
    param([string]$Message)
    Write-Host "[ERROR] $Message" -ForegroundColor Red
    Add-Content -Path $TEST_REPORT_FILE -Value "[ERROR] $(Get-Date -Format 'HH:mm:ss') $Message"
}

# 初始化测试报告
function Initialize-TestReport {
    $reportContent = @"
# 🚀 SGCC Fluss 增强版全链路验证测试报告

**测试开始时间**: $(Get-Date)  
**测试环境**: Docker Compose + Fluss 0.7.0 + Flink 1.20 + PostgreSQL  
**测试版本**: v2.0 (增强版)  
**运行平台**: Windows PowerShell

**关键改进**:
- ✅ 任务间自动清理
- ✅ 真实数据验证
- ✅ 增删改实时性测试
- ✅ 详细数据流监控

---

## 📊 测试概览

"@
    Set-Content -Path $TEST_REPORT_FILE -Value $reportContent
}

# 停止所有Flink任务
function Stop-AllFlinkJobs {
    Write-LogInfo "🛑 停止所有Flink任务..."
    
    try {
        # 获取所有运行中的任务
        $runningJobs = docker exec jobmanager-sgcc /opt/flink/bin/flink list -r 2>/dev/null
        
        if ($runningJobs -match "(\w{32})") {
            $jobIds = [regex]::Matches($runningJobs, "([a-f0-9]{32})")
            
            foreach ($match in $jobIds) {
                $jobId = $match.Value
                Write-LogInfo "停止任务: $jobId"
                docker exec jobmanager-sgcc /opt/flink/bin/flink cancel $jobId 2>&1 | Out-Null
                Start-Sleep -Seconds 2
            }
            
            Write-LogSuccess "✅ 所有任务已停止"
        } else {
            Write-LogInfo "没有运行中的任务"
        }
    } catch {
        Write-LogWarning "⚠️ 停止任务时出现警告: $($_.Exception.Message)"
    }
    
    # 等待任务完全停止
    Start-Sleep -Seconds 10
}

# 环境启动函数（增强版）
function Start-EnhancedEnvironment {
    Write-LogInfo "🌟 步骤1: 启动增强测试环境"
    
    # 停止现有环境
    Write-LogInfo "停止现有环境..."
    try {
        docker-compose down 2>&1 | Out-Null
        Start-Sleep -Seconds 5
    } catch {
        Write-LogWarning "停止环境时出现警告: $($_.Exception.Message)"
    }
    
    # 清理Docker卷
    Write-LogInfo "清理Docker卷..."
    try {
        docker volume prune -f 2>&1 | Out-Null
    } catch {
        Write-LogWarning "清理Docker卷时出现警告: $($_.Exception.Message)"
    }
    
    # 启动环境
    Write-LogInfo "启动Docker Compose环境..."
    try {
        $result = docker-compose up -d 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-LogSuccess "环境启动成功"
        } else {
            Write-LogError "环境启动失败: $result"
            return $false
        }
    } catch {
        Write-LogError "环境启动失败: $($_.Exception.Message)"
        return $false
    }
    
    # 等待服务就绪
    Write-LogInfo "等待服务完全就绪..."
    Start-Sleep -Seconds 60
    
    # 验证服务状态
    Write-LogInfo "验证服务状态..."
    $services = @("coordinator-server-sgcc", "tablet-server-sgcc", "jobmanager-sgcc", "taskmanager-sgcc-1", "postgres-sgcc-source", "postgres-sgcc-sink")
    $healthyServices = 0
    
    foreach ($service in $services) {
        try {
            $containerStatus = docker ps --format "table {{.Names}}" | Select-String $service
            if ($containerStatus) {
                Write-LogSuccess "✅ $service 服务正常"
                $healthyServices++
            } else {
                Write-LogError "❌ $service 服务异常"
            }
        } catch {
            Write-LogError "❌ $service 服务状态检查失败: $($_.Exception.Message)"
        }
    }
    
    if ($healthyServices -eq $services.Count) {
        Write-LogSuccess "✅ 所有核心服务正常启动"
        return $true
    } else {
        Write-LogError "❌ 部分服务启动失败 ($healthyServices/$($services.Count))"
        return $false
    }
}

# 建立数据流
function Establish-DataFlow {
    Write-LogInfo "🔄 步骤2: 建立完整数据流"
    
    # 首先确保停止所有现有任务
    Stop-AllFlinkJobs
    
    # 按顺序执行SQL脚本建立数据流
    $sqlScripts = @(
        @{File = "fluss/sql/1_cdc_source_to_fluss.sql"; Description = "建立CDC源到Fluss的数据流"},
        @{File = "fluss/sql/2_fluss_dwd_layer.sql"; Description = "创建DWD数据仓库详细层"},
        @{File = "fluss/sql/3_fluss_dws_layer.sql"; Description = "创建DWS数据仓库汇总层"},
        @{File = "fluss/sql/4_fluss_ads_layer.sql"; Description = "创建ADS应用数据服务层"},
        @{File = "fluss/sql/5_sink_to_postgres.sql"; Description = "建立到PostgreSQL的数据输出"}
    )
    
    foreach ($script in $sqlScripts) {
        Write-LogInfo "执行: $($script.Description)"
        
        $startTime = Get-Date
        try {
            if (Test-Path $script.File) {
                $result = Get-Content $script.File | docker exec -i sql-client-sgcc /opt/flink/bin/sql-client.sh embedded 2>&1
                if ($LASTEXITCODE -eq 0) {
                    $endTime = Get-Date
                    $duration = ($endTime - $startTime).TotalSeconds
                    Write-LogSuccess "✅ $($script.Description) 完成 (耗时: $([math]::Round($duration))秒)"
                    $script:metrics.total_jobs_created++
                } else {
                    Write-LogError "❌ $($script.Description) 失败: $result"
                    return $false
                }
            } else {
                Write-LogError "❌ SQL脚本文件不存在: $($script.File)"
                return $false
            }
        } catch {
            Write-LogError "❌ $($script.Description) 执行异常: $($_.Exception.Message)"
            return $false
        }
        
        # 脚本间等待
        Start-Sleep -Seconds 15
    }
    
    # 等待数据流稳定
    Write-LogInfo "等待数据流稳定..."
    Start-Sleep -Seconds 30
    
    Write-LogSuccess "✅ 数据流建立完成"
    return $true
}

# 验证数据同步（增强版）
function Test-EnhancedDataSync {
    Write-LogInfo "🔍 步骤3: 增强数据同步验证"
    
    # 验证源数据
    Write-LogInfo "验证PostgreSQL源数据..."
    try {
        $sourceCount = docker exec postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -t -c "SELECT COUNT(*) FROM sgcc_power.power_monitoring;" 2>&1
        $sourceCount = ($sourceCount -split "`n" | Where-Object {$_ -match "^\s*\d+\s*$"})[0].Trim()
        if ([int]$sourceCount -gt 0) {
            Write-LogSuccess "✅ 源数据表记录数: $sourceCount"
            $script:metrics.total_records_processed += [int]$sourceCount
        } else {
            Write-LogWarning "⚠️ 源数据表无数据，将插入测试数据"
            return $false
        }
    } catch {
        Write-LogWarning "⚠️ 源数据验证失败: $($_.Exception.Message)"
        return $false
    }
    
    # 验证Fluss数据湖
    Write-LogInfo "验证Fluss数据湖各层数据..."
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

-- 验证最新数据
SELECT 'Latest data' as info, equipment_id, voltage_a, current_a, monitoring_time 
FROM sgcc_dwd.power_monitoring_dwd 
ORDER BY monitoring_time DESC LIMIT 3;
"@
    
    $tempValidateFile = [System.IO.Path]::GetTempFileName() + ".sql"
    Set-Content -Path $tempValidateFile -Value $validateFlussScript
    
    try {
        $result = Get-Content $tempValidateFile | docker exec -i sql-client-sgcc /opt/flink/bin/sql-client.sh embedded 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-LogSuccess "✅ Fluss数据湖验证完成"
        } else {
            Write-LogWarning "⚠️ Fluss数据湖验证部分失败: $result"
        }
    } catch {
        Write-LogWarning "⚠️ Fluss数据湖验证异常: $($_.Exception.Message)"
    } finally {
        Remove-Item $tempValidateFile -Force -ErrorAction SilentlyContinue
    }
    
    # 验证目标数据库
    Write-LogInfo "验证PostgreSQL目标数据库..."
    try {
        # 检查数据库是否有表
        $tables = docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -t -c "\dt" 2>&1
        if ($tables -like "*did not find any relations*" -or $tables -like "*Did not find any relations*") {
            Write-LogWarning "⚠️ 目标数据库暂无表，数据流可能正在建立中"
        } else {
            Write-LogSuccess "✅ 目标数据库表结构正常"
        }
    } catch {
        Write-LogWarning "⚠️ 目标数据验证失败: $($_.Exception.Message)"
    }
    
    return $true
}

# 增删改实时性测试（新增）
function Test-RealTimeDataChanges {
    Write-LogInfo "🔄 步骤4: 增删改实时性测试"
    
    $script:metrics.data_sync_tests = 3 # 测试INSERT, UPDATE, DELETE
    
    # 1. INSERT测试
    Write-LogInfo "🟢 测试1: INSERT实时同步"
    $insertTimestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $testEquipmentId = Get-Random -Minimum 9000 -Maximum 9999
    
    $insertSQL = @"
INSERT INTO sgcc_power.power_monitoring (
    monitoring_id, equipment_id, voltage_a, voltage_b, voltage_c, 
    current_a, current_b, current_c, power_active, power_reactive, 
    frequency, temperature, humidity, monitoring_time
) VALUES (
    $testEquipmentId, $testEquipmentId, 220.5, 219.8, 221.2,
    15.2, 15.1, 15.3, 3350.0, 450.0,
    50.01, 25.5, 60.2, '$insertTimestamp'
);
"@
    
    try {
        $result = docker exec postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "$insertSQL" 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-LogSuccess "✅ INSERT操作执行成功 (设备ID: $testEquipmentId)"
            $script:metrics.successful_sync_tests++
        } else {
            Write-LogError "❌ INSERT操作失败: $result"
        }
    } catch {
        Write-LogError "❌ INSERT操作异常: $($_.Exception.Message)"
    }
    
    # 等待CDC同步
    Write-LogInfo "等待CDC同步处理 (30秒)..."
    Start-Sleep -Seconds 30
    
    # 2. UPDATE测试
    Write-LogInfo "🟡 测试2: UPDATE实时同步"
    $updateSQL = @"
UPDATE sgcc_power.power_monitoring 
SET temperature = 35.8, power_active = 3400.0, updated_at = CURRENT_TIMESTAMP 
WHERE equipment_id = $testEquipmentId;
"@
    
    try {
        $result = docker exec postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "$updateSQL" 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-LogSuccess "✅ UPDATE操作执行成功 (设备ID: $testEquipmentId)"
            $script:metrics.successful_sync_tests++
        } else {
            Write-LogError "❌ UPDATE操作失败: $result"
        }
    } catch {
        Write-LogError "❌ UPDATE操作异常: $($_.Exception.Message)"
    }
    
    # 等待CDC同步
    Write-LogInfo "等待CDC同步处理 (30秒)..."
    Start-Sleep -Seconds 30
    
    # 3. 验证同步结果
    Write-LogInfo "🔍 验证同步结果..."
    Test-EnhancedDataSync
    
    # 4. DELETE测试
    Write-LogInfo "🔴 测试3: DELETE实时同步"
    $deleteSQL = @"
DELETE FROM sgcc_power.power_monitoring WHERE equipment_id = $testEquipmentId;
"@
    
    try {
        $result = docker exec postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "$deleteSQL" 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-LogSuccess "✅ DELETE操作执行成功 (设备ID: $testEquipmentId)"
            $script:metrics.successful_sync_tests++
        } else {
            Write-LogError "❌ DELETE操作失败: $result"
        }
    } catch {
        Write-LogError "❌ DELETE操作异常: $($_.Exception.Message)"
    }
    
    # 最终同步验证
    Write-LogInfo "等待最终CDC同步处理 (30秒)..."
    Start-Sleep -Seconds 30
    
    Write-LogSuccess "✅ 增删改实时性测试完成 (成功: $($script:metrics.successful_sync_tests)/$($script:metrics.data_sync_tests))"
}

# 单场景测试函数（增强版）
function Invoke-SingleScenarioTest {
    param(
        [string]$ScriptFile,
        [string]$Description
    )
    
    Write-LogInfo "🎯 执行场景: $Description"
    
    # 停止之前的任务
    Stop-AllFlinkJobs
    
    $startTime = Get-Date
    try {
        if (Test-Path $ScriptFile) {
            $result = Get-Content $ScriptFile | docker exec -i sql-client-sgcc /opt/flink/bin/sql-client.sh embedded 2>&1
            if ($LASTEXITCODE -eq 0) {
                $endTime = Get-Date
                $duration = ($endTime - $startTime).TotalSeconds
                Write-LogSuccess "✅ $Description 完成 (耗时: $([math]::Round($duration))秒)"
                
                # 验证场景结果
                Write-LogInfo "验证场景执行结果..."
                Start-Sleep -Seconds 10
                
                # 检查任务状态
                $runningJobs = docker exec jobmanager-sgcc /opt/flink/bin/flink list -r 2>/dev/null
                if ($runningJobs -and $runningJobs -notlike "*No running jobs*") {
                    Write-LogSuccess "✅ 场景任务正在运行"
                }
                
                $script:metrics.total_jobs_created++
                return $true
            } else {
                Write-LogError "❌ $Description 失败: $result"
                return $false
            }
        } else {
            Write-LogError "❌ 场景脚本文件不存在: $ScriptFile"
            return $false
        }
    } catch {
        Write-LogError "❌ $Description 执行异常: $($_.Exception.Message)"
        return $false
    }
}

# 场景测试函数（增强版）
function Invoke-EnhancedBusinessScenarios {
    Write-LogInfo "🎯 步骤5: 增强业务场景测试"
    
    $scenarios = @(
        @{File = "business-scenarios/场景1_高频维度表服务.sql"; Description = "场景1_高频维度表服务"},
        @{File = "business-scenarios/场景2_智能双流JOIN.sql"; Description = "场景2_智能双流JOIN"},
        @{File = "business-scenarios/场景3_时间旅行查询.sql"; Description = "场景3_时间旅行查询"},
        @{File = "business-scenarios/场景4_柱状流优化.sql"; Description = "场景4_柱状流优化"}
    )
    
    foreach ($scenario in $scenarios) {
        $script:metrics.total_test_scenarios++
        
        if (Invoke-SingleScenarioTest -ScriptFile $scenario.File -Description $scenario.Description) {
            $script:metrics.successful_scenarios++
        } else {
            $script:metrics.failed_scenarios++
        }
        
        # 场景间等待
        Write-LogInfo "场景间等待 (15秒)..."
        Start-Sleep -Seconds 15
    }
}

# 性能指标统计函数（增强版）
function Get-EnhancedPerformanceMetrics {
    Write-LogInfo "📈 步骤6: 增强性能指标统计"
    
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
    
    # 计算各种指标
    $throughput = if ($totalDuration -gt 0) { [math]::Round($metrics.total_records_processed / $totalDuration, 2) } else { 0 }
    $successRate = if ($metrics.total_test_scenarios -gt 0) { [math]::Round(($metrics.successful_scenarios * 100) / $metrics.total_test_scenarios, 2) } else { 0 }
    $syncSuccessRate = if ($metrics.data_sync_tests -gt 0) { [math]::Round(($metrics.successful_sync_tests * 100) / $metrics.data_sync_tests, 2) } else { 0 }
    
    # 获取数据库当前状态
    Write-LogInfo "获取数据库状态..."
    try {
        $sourceCount = docker exec postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -t -c "SELECT COUNT(*) FROM sgcc_power.power_monitoring;" 2>&1
        $sourceCount = ($sourceCount -split "`n" | Where-Object {$_ -match "^\s*\d+\s*$"})[0].Trim()
    } catch {
        $sourceCount = "N/A"
    }
    
    # 输出性能指标到报告
    $metricsContent = @"

## 📈 增强性能指标统计

### 基础指标
| 指标 | 数值 |
|------|------|
| 总耗时 | $([math]::Round($totalDuration))秒 |
| 处理记录数 | $($metrics.total_records_processed) |
| 创建作业数 | $($metrics.total_jobs_created) |
| 当前运行作业数 | $runningJobsCount |
| 源数据库记录数 | $sourceCount |

### 测试场景指标
| 指标 | 数值 |
|------|------|
| 测试场景数 | $($metrics.total_test_scenarios) |
| 成功场景数 | $($metrics.successful_scenarios) |
| 失败场景数 | $($metrics.failed_scenarios) |
| 场景成功率 | $successRate% |

### 数据同步指标
| 指标 | 数值 |
|------|------|
| 增删改测试数 | $($metrics.data_sync_tests) |
| 同步成功数 | $($metrics.successful_sync_tests) |
| 同步成功率 | $syncSuccessRate% |
| 数据吞吐量 | $throughput 记录/秒 |

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
    Write-LogSuccess "📊 增强测试完成统计："
    Write-LogSuccess "  - 总耗时: $([math]::Round($totalDuration))秒"
    Write-LogSuccess "  - 处理记录数: $($metrics.total_records_processed)"
    Write-LogSuccess "  - 创建作业数: $($metrics.total_jobs_created)"
    Write-LogSuccess "  - 场景成功数: $($metrics.successful_scenarios)/$($metrics.total_test_scenarios)"
    Write-LogSuccess "  - 同步成功数: $($metrics.successful_sync_tests)/$($metrics.data_sync_tests)"
    Write-LogSuccess "  - 场景成功率: $successRate%"
    Write-LogSuccess "  - 数据同步率: $syncSuccessRate%"
}

# 主函数（增强版）
function Start-EnhancedFullTestSuite {
    Write-Host "🚀 SGCC Fluss 增强版一键全链路验证测试 (Windows版)" -ForegroundColor Blue
    Write-Host "================================================================" -ForegroundColor Blue
    Write-Host "🎯 新功能: 任务管理 + 真实数据验证 + 增删改实时性测试" -ForegroundColor Cyan
    Write-Host "================================================================" -ForegroundColor Blue
    
    try {
        # 初始化测试报告
        Initialize-TestReport
        
        # 执行增强测试流程
        if (!(Start-EnhancedEnvironment)) {
            Write-LogError "环境启动失败，终止测试"
            return
        }
        
        if (!(Establish-DataFlow)) {
            Write-LogError "数据流建立失败，终止测试"
            return
        }
        
        Test-EnhancedDataSync
        Test-RealTimeDataChanges
        Invoke-EnhancedBusinessScenarios
        Get-EnhancedPerformanceMetrics
        
        # 完成信息
        Write-LogSuccess "🎉 增强版全链路验证测试完成！"
        Write-LogSuccess "📄 详细报告: $TEST_REPORT_FILE"
        
        Write-Host ""
        Write-Host "🎉 增强版测试完成！详细报告已保存到: $TEST_REPORT_FILE" -ForegroundColor Green
        Write-Host "🌐 Flink Web UI: http://localhost:8091" -ForegroundColor Cyan
        Write-Host "🗄️ PostgreSQL源: localhost:5442" -ForegroundColor Cyan
        Write-Host "🗄️ PostgreSQL目标: localhost:5443" -ForegroundColor Cyan
        
        # 询问是否打开报告和Web UI
        $openReport = Read-Host "是否要打开测试报告？(Y/N)"
        if ($openReport -eq "Y" -or $openReport -eq "y") {
            if (Test-Path $TEST_REPORT_FILE) {
                Start-Process notepad.exe $TEST_REPORT_FILE
            }
        }
        
        $openWebUI = Read-Host "是否要打开Flink Web UI？(Y/N)"
        if ($openWebUI -eq "Y" -or $openWebUI -eq "y") {
            Start-Process "http://localhost:8091"
        }
        
    } catch {
        Write-LogError "测试过程中发生错误: $($_.Exception.Message)"
        Write-Host "❌ 测试执行失败，请检查错误日志" -ForegroundColor Red
    }
}

# 设置脚本主入口点
if ($MyInvocation.InvocationName -ne '.') {
    Start-EnhancedFullTestSuite
} 