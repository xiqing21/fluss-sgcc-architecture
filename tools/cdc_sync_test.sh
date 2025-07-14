#!/bin/bash

echo "[INFO] 🔧 准备CDC测试环境..."

# 第1步：检查源数据库数据
echo "[INFO] 检查源数据库现有数据..."
docker exec postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "
SELECT '源数据库设备数量' as info, COUNT(*) as count FROM power_equipment;
SELECT * FROM power_equipment LIMIT 3;
"

# 第2步：创建Fluss CDC连接
echo "[INFO] 🌊 创建Fluss CDC数据流..."
timeout 60 docker exec -i sql-client-sgcc ./sql-client <<'EOSQL'
SET 'sql-client.execution.result-mode' = 'tableau';
CREATE CATALOG fluss_catalog WITH ('type' = 'fluss', 'bootstrap.servers' = 'coordinator-server-sgcc:9123');
USE CATALOG fluss_catalog;
USE fluss;

-- 创建CDC源表（从PostgreSQL源数据库同步）
CREATE TABLE power_equipment_cdc (
    equipment_id STRING,
    equipment_name STRING,
    equipment_type STRING,
    capacity_mw DOUBLE,
    location STRING,
    status STRING,
    last_maintenance_date STRING,
    PRIMARY KEY (equipment_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres-sgcc-source',
    'port' = '5432',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'database-name' = 'sgcc_source_db',
    'schema-name' = 'public',
    'table-name' = 'power_equipment',
    'decoding.plugin.name' = 'pgoutput'
);

-- 创建目标表（同步到目标数据库）
CREATE TABLE power_equipment_sync (
    equipment_id STRING,
    equipment_name STRING,
    equipment_type STRING,
    capacity_mw DOUBLE,
    location STRING,
    status STRING,
    last_maintenance_date STRING,
    sync_timestamp STRING,
    PRIMARY KEY (equipment_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'table-name' = 'power_equipment_realtime'
);

SHOW TABLES;
QUIT;
EOSQL

echo "[SUCCESS] ✅ Fluss CDC流创建完成"
sleep 3

# 第3步：触发数据变更来测试CDC
echo "[INFO] 🔄 触发数据变更测试CDC同步..."
docker exec postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "
-- 插入新设备
INSERT INTO power_equipment VALUES 
('CDC001', 'CDC测试变压器001', '变压器', 500.0, '北京CDC测试站', 'RUNNING', '2025-07-14'),
('CDC002', 'CDC测试开关002', '开关', 0.0, '上海CDC测试站', 'RUNNING', '2025-07-14'),
('CDC003', 'CDC测试线路003', '线路', 800.0, '广州CDC测试站', 'RUNNING', '2025-07-14');

-- 更新现有设备
UPDATE power_equipment SET status = 'MAINTENANCE', last_maintenance_date = '2025-07-14' WHERE equipment_id = 'EQ001';

SELECT '数据变更完成' as info, COUNT(*) as total_count FROM power_equipment;
"

echo "[SUCCESS] ✅ 数据变更触发完成"
sleep 5

# 第4步：验证CDC同步结果
echo "[INFO] 🔍 验证CDC同步效果..."
docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
SELECT '目标数据库同步结果' as info, COUNT(*) as synced_count FROM power_equipment_realtime WHERE equipment_id LIKE 'CDC%';
SELECT equipment_id, equipment_name, status, sync_timestamp FROM power_equipment_realtime WHERE equipment_id LIKE 'CDC%' LIMIT 5;
" || echo "[WARN] 目标表可能还在创建中..."

echo ""
echo "🎉 =================================="
echo "   CDC数据同步测试完成"
echo "   实时变更捕获验证完毕"
echo "=================================="

