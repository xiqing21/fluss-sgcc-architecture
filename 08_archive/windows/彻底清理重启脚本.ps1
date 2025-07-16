# 🧹 SGCC Fluss 彻底清理重启脚本 (Windows PowerShell版)
# 功能：彻底清理所有metadata + 优雅停止 + 重新初始化
# 目的：解决 LogStorageException: Table schema not found in zookeeper metadata 问题
# 版本：v1.0

$ErrorActionPreference = "Continue"

Write-Host "🧹 开始彻底清理Fluss环境..." -ForegroundColor Yellow

# 1. 优雅停止所有Flink任务
Write-Host "🛑 步骤1: 优雅停止所有Flink任务" -ForegroundColor Blue
try {
    $runningJobs = docker exec jobmanager-sgcc /opt/flink/bin/flink list -r 2>&1
    
    if ($runningJobs -and $runningJobs -notlike "*No running jobs*") {
        $jobIds = [regex]::Matches($runningJobs, "([a-f0-9]{32})")
        
        foreach ($match in $jobIds) {
            $jobId = $match.Value
            Write-Host "  停止任务: $jobId" -ForegroundColor Cyan
            docker exec jobmanager-sgcc /opt/flink/bin/flink cancel $jobId 2>&1 | Out-Null
            Start-Sleep -Seconds 3
        }
        
        Write-Host "✅ 已停止 $($jobIds.Count) 个Flink任务" -ForegroundColor Green
    } else {
        Write-Host "ℹ️ 没有运行中的Flink任务" -ForegroundColor Gray
    }
} catch {
    Write-Host "⚠️ 停止Flink任务时出现警告: $($_.Exception.Message)" -ForegroundColor Yellow
}

# 2. 停止Docker Compose环境
Write-Host "🛑 步骤2: 停止Docker Compose环境" -ForegroundColor Blue
docker-compose down 2>&1 | Out-Null
Start-Sleep -Seconds 10

# 3. 彻底清理所有相关卷和数据
Write-Host "🗑️ 步骤3: 彻底清理所有数据卷" -ForegroundColor Blue

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
            Write-Host "  删除卷: $volume" -ForegroundColor Cyan
            docker volume rm $volume 2>&1 | Out-Null
        }
    } catch {
        Write-Host "  ⚠️ 删除卷 $volume 时出现警告" -ForegroundColor Yellow
    }
}

# 4. 清理所有未使用的卷
Write-Host "🧹 步骤4: 清理所有未使用的卷" -ForegroundColor Blue
docker volume prune -f 2>&1 | Out-Null

# 5. 清理Docker网络
Write-Host "🌐 步骤5: 清理Docker网络" -ForegroundColor Blue
try {
    docker network rm fluss-sgcc-architecture_fluss-sgcc-network 2>&1 | Out-Null
} catch {
    Write-Host "  ℹ️ 网络可能已经不存在" -ForegroundColor Gray
}
docker network prune -f 2>&1 | Out-Null

# 6. 清理Docker系统缓存
Write-Host "🗄️ 步骤6: 清理Docker系统缓存" -ForegroundColor Blue
docker system prune -f 2>&1 | Out-Null

# 7. 等待系统完全清理
Write-Host "⏱️ 步骤7: 等待系统完全清理" -ForegroundColor Blue
Start-Sleep -Seconds 15

# 8. 重新启动环境
Write-Host "🚀 步骤8: 重新启动环境" -ForegroundColor Blue
try {
    $result = docker-compose up -d 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ 环境启动成功" -ForegroundColor Green
    } else {
        Write-Host "❌ 环境启动失败: $result" -ForegroundColor Red
        exit 1
    }
} catch {
    Write-Host "❌ 环境启动异常: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

# 9. 等待服务完全就绪
Write-Host "⏳ 步骤9: 等待服务完全就绪 (120秒)" -ForegroundColor Blue
Start-Sleep -Seconds 120

# 10. 验证服务状态
Write-Host "🔍 步骤10: 验证服务状态" -ForegroundColor Blue
$services = @("coordinator-server-sgcc", "tablet-server-sgcc", "jobmanager-sgcc", "taskmanager-sgcc-1", "postgres-sgcc-source", "postgres-sgcc-sink", "zookeeper-sgcc")
$healthyServices = 0

foreach ($service in $services) {
    try {
        $containerStatus = docker ps --format "table {{.Names}}\t{{.Status}}" | Select-String $service
        if ($containerStatus -and $containerStatus -like "*Up*") {
            Write-Host "  ✅ $service 服务正常" -ForegroundColor Green
            $healthyServices++
        } else {
            Write-Host "  ❌ $service 服务异常" -ForegroundColor Red
        }
    } catch {
        Write-Host "  ❌ $service 服务状态检查失败" -ForegroundColor Red
    }
}

if ($healthyServices -eq $services.Count) {
    Write-Host "🎉 所有服务正常启动！环境已彻底清理并重新初始化" -ForegroundColor Green
    Write-Host "💡 现在可以运行测试脚本了" -ForegroundColor Cyan
    Write-Host "🌐 Flink Web UI: http://localhost:8091" -ForegroundColor Cyan
} else {
    Write-Host "⚠️ 部分服务启动失败 ($healthyServices/$($services.Count))" -ForegroundColor Yellow
    Write-Host "💡 请检查Docker日志: docker-compose logs" -ForegroundColor Cyan
}

Write-Host ""
Write-Host "🧹 彻底清理重启完成！" -ForegroundColor Green 