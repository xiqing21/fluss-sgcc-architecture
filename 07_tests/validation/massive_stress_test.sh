#!/bin/bash

echo "🔥 =================================="
echo "   Fluss大规模压力测试开始"
echo "   目标：模拟50万设备实时数据流"
echo "=================================="

# 第1步：环境检查
echo "[INFO] 检查测试环境..."
if ! docker ps | grep -q "sql-client-sgcc"; then
    echo "[ERROR] SQL客户端容器未运行"
    exit 1
fi
echo "[SUCCESS] 环境检查通过"

# 第2步：创建大规模测试表
echo "[INFO] 创建大规模测试表..."
timeout 45 docker exec -i sql-client-sgcc ./sql-client <<'EOSQL'
SET 'sql-client.execution.result-mode' = 'tableau';
CREATE CATALOG fluss_catalog WITH ('type' = 'fluss', 'bootstrap.servers' = 'coordinator-server-sgcc:9123');
USE CATALOG fluss_catalog;
USE fluss;

-- 清理旧表
DROP TABLE IF EXISTS massive_device_monitoring;
DROP TABLE IF EXISTS device_master_table;

-- 创建设备主表（模拟50万设备）
CREATE TABLE device_master_table (
    device_id STRING,
    device_name STRING,
    device_type STRING,
    region STRING,
    substation STRING,
    voltage_level STRING,
    capacity_mw DOUBLE,
    install_date STRING,
    status STRING,
    PRIMARY KEY (device_id) NOT ENFORCED
) WITH (
    'bucket.num' = '32'
);

-- 创建大规模监控数据流表
CREATE TABLE massive_device_monitoring (
    monitor_id STRING,
    device_id STRING,
    timestamp_ms BIGINT,
    voltage DOUBLE,
    current_amp DOUBLE,
    power_mw DOUBLE,
    temperature DOUBLE,
    frequency DOUBLE,
    load_rate DOUBLE,
    alert_level STRING,
    data_source STRING,
    PRIMARY KEY (monitor_id) NOT ENFORCED
) WITH (
    'bucket.num' = '64'
);

SHOW TABLES;
QUIT;
EOSQL

echo "[SUCCESS] 测试表创建完成"
sleep 2

echo "[INFO] 🌊 开始生成大规模设备数据..."
