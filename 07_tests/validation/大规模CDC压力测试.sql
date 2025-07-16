-- 🔥 大规模CDC压力测试（10万设备数据）
SET 'sql-client.execution.result-mode' = 'tableau';

-- 1. 创建CDC源表（连接10万设备数据）
CREATE TEMPORARY TABLE massive_power_equipment_cdc (
    equipment_id STRING,
    equipment_name STRING,
    equipment_type STRING,
    capacity_mw DECIMAL(10,2),
    location STRING,
    status STRING,
    last_maintenance_date DATE,
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
    'decoding.plugin.name' = 'pgoutput',
    'slot.name' = 'massive_power_equipment_slot'
);

-- 2. 创建Fluss catalog
CREATE CATALOG fluss_catalog WITH ('type' = 'fluss', 'bootstrap.servers' = 'coordinator-server-sgcc:9123');
USE CATALOG fluss_catalog;

-- 3. 创建Fluss数据库
CREATE DATABASE IF NOT EXISTS fluss;
USE fluss;

-- 4. 创建大规模Fluss表（高并发设计）
CREATE TABLE IF NOT EXISTS massive_power_equipment_fluss (
    equipment_id STRING,
    equipment_name STRING,
    equipment_type STRING,
    capacity_mw DECIMAL(10,2),
    location STRING,
    status STRING,
    last_maintenance_date DATE,
    sync_timestamp TIMESTAMP(3),
    PRIMARY KEY (equipment_id) NOT ENFORCED
) WITH (
    'bucket.num' = '64'
);

-- 5. 切换回默认catalog
USE CATALOG default_catalog;

-- 6. 🚀 启动大规模CDC同步作业
INSERT INTO fluss_catalog.fluss.massive_power_equipment_fluss
SELECT 
    equipment_id,
    equipment_name,
    equipment_type,
    capacity_mw,
    location,
    status,
    last_maintenance_date,
    CURRENT_TIMESTAMP as sync_timestamp
FROM massive_power_equipment_cdc;
