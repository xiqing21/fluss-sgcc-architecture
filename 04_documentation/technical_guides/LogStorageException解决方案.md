# 🔧 Fluss LogStorageException 完整解决方案

## 📋 问题描述

在使用Fluss时，经常遇到以下错误：

```
Caused by: com.alibaba.fluss.exception.LogStorageException: 
Failed to load table 'fluss.ads_fault_analysis_report': 
Table schema not found in zookeeper metadata.
```

## 🔍 根本原因

这个问题的根本原因是：

1. **Metadata不一致**: Fluss的表schema在ZooKeeper中的metadata与实际状态不一致
2. **并发访问冲突**: 多个Flink任务同时访问同一个Fluss表时产生的竞争条件
3. **不完整的清理**: 简单的`docker-compose down`无法完全清理Fluss的metadata状态
4. **任务残留**: 之前失败的Flink任务可能仍然持有表的引用

## 🛠️ 解决方案

### Windows环境

#### 方法1: 一键解决（推荐）
```batch
# 双击运行批处理文件
windows\彻底清理重启.bat
```

#### 方法2: PowerShell脚本
```powershell
# 运行彻底清理脚本
.\windows\彻底清理重启脚本.ps1
```

### Linux/macOS环境

#### 创建彻底清理脚本
```bash
# 创建清理脚本
cat > scripts/彻底清理重启.sh << 'EOF'
#!/bin/bash

echo "🧹 开始彻底清理Fluss环境..."

# 1. 优雅停止所有Flink任务
echo "🛑 停止所有Flink任务..."
running_jobs=$(docker exec jobmanager-sgcc /opt/flink/bin/flink list -r 2>/dev/null)

if [[ "$running_jobs" != *"No running jobs"* ]] && [[ -n "$running_jobs" ]]; then
    job_ids=$(echo "$running_jobs" | grep -oE '[a-f0-9]{32}')
    
    for job_id in $job_ids; do
        echo "  停止任务: $job_id"
        docker exec jobmanager-sgcc /opt/flink/bin/flink cancel "$job_id" >/dev/null 2>&1
        sleep 2
    done
    
    echo "✅ 已停止Flink任务"
fi

# 2. 停止Docker Compose环境
echo "🛑 停止Docker Compose环境..."
docker-compose down >/dev/null 2>&1
sleep 10

# 3. 彻底清理相关数据卷
echo "🗑️ 彻底清理数据卷..."
volumes_to_remove=(
    "fluss_coordinator_data"
    "fluss_tablet_data"
    "flink_checkpoints"
    "flink_savepoints"
)

for volume in "${volumes_to_remove[@]}"; do
    if docker volume ls -q | grep -q "^${volume}$"; then
        echo "  删除卷: $volume"
        docker volume rm "$volume" >/dev/null 2>&1
    fi
done

# 4. 清理未使用的卷和缓存
echo "🧹 清理Docker缓存..."
docker volume prune -f >/dev/null 2>&1
docker system prune -f >/dev/null 2>&1

# 5. 重新启动环境
echo "🚀 重新启动环境..."
if docker-compose up -d; then
    echo "✅ 环境启动成功"
else
    echo "❌ 环境启动失败"
    exit 1
fi

# 6. 等待服务就绪
echo "⏳ 等待服务就绪 (120秒)..."
sleep 120

echo "🎉 彻底清理重启完成！"
EOF

# 赋予执行权限
chmod +x scripts/彻底清理重启.sh

# 运行脚本
./scripts/彻底清理重启.sh
```

## 🔄 预防措施

### 1. 在测试脚本中添加任务管理

确保每个场景测试后都停止相关任务：

```powershell
# PowerShell版本
function Stop-AllFlinkJobs {
    $runningJobs = docker exec jobmanager-sgcc /opt/flink/bin/flink list -r 2>&1
    
    if ($runningJobs -and $runningJobs -notlike "*No running jobs*") {
        $jobIds = [regex]::Matches($runningJobs, "([a-f0-9]{32})")
        
        foreach ($match in $jobIds) {
            $jobId = $match.Value
            docker exec jobmanager-sgcc /opt/flink/bin/flink cancel $jobId 2>&1 | Out-Null
            Start-Sleep -Seconds 2
        }
    }
}
```

```bash
# Bash版本
stop_all_flink_jobs() {
    local running_jobs=$(docker exec jobmanager-sgcc /opt/flink/bin/flink list -r 2>/dev/null)
    
    if [[ "$running_jobs" != *"No running jobs"* ]] && [[ -n "$running_jobs" ]]; then
        local job_ids=$(echo "$running_jobs" | grep -oE '[a-f0-9]{32}')
        
        for job_id in $job_ids; do
            docker exec jobmanager-sgcc /opt/flink/bin/flink cancel "$job_id" >/dev/null 2>&1
            sleep 2
        done
    fi
}
```

### 2. 在业务场景间添加清理

```sql
-- 在每个业务场景开始前添加
USE CATALOG default_catalog;
DROP TABLE IF EXISTS temp_table_name;
```

### 3. 优雅的环境停止

```bash
# 停止环境前先停止所有任务
./scripts/stop_all_jobs.sh
docker-compose down
```

## 📊 验证解决方案

运行彻底清理脚本后，验证环境状态：

```bash
# 检查容器状态
docker ps

# 检查Flink任务
docker exec jobmanager-sgcc /opt/flink/bin/flink list

# 检查Fluss服务
docker logs coordinator-server-sgcc --tail 20
docker logs tablet-server-sgcc --tail 20

# 访问Flink Web UI
# http://localhost:8091
```

## 🎯 最佳实践

1. **每次测试前运行彻底清理脚本**
2. **场景测试间添加任务停止**
3. **避免多个任务同时访问同一个Fluss表**
4. **定期清理Docker缓存和未使用的卷**
5. **监控ZooKeeper日志确保metadata一致性**

## 📞 支持

如果问题依然存在：

1. 检查ZooKeeper日志：`docker logs zookeeper-sgcc`
2. 检查Fluss Coordinator日志：`docker logs coordinator-server-sgcc`
3. 检查Fluss Tablet日志：`docker logs tablet-server-sgcc`
4. 提供完整的错误日志和环境信息

---

**最后更新**: 2025年7月14日  
**版本**: v1.0  
**适用环境**: Fluss 0.7.0 + Flink 1.20 