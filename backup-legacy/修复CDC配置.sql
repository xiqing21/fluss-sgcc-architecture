-- 🔴 修复CDC配置 - 使用pgoutput解码器
-- 必须设置TABLEAU模式
SET 'sql-client.execution.result-mode' = 'tableau';

-- ========================================
-- 先停止并删除现有的CDC作业
-- ========================================
-- 首先创建catalog
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

USE CATALOG default_catalog;

-- 删除现有的CDC表（如果存在）
DROP TABLE IF EXISTS sgcc_power_equipment_cdc_source;

-- ========================================
-- 创建修复的CDC源表（使用pgoutput解码器）
-- ========================================
CREATE TABLE sgcc_power_equipment_cdc_source (
    equipment_id BIGINT,
    equipment_name STRING,
    equipment_type STRING,
    location STRING,
    voltage_level STRING,
    capacity_mw DECIMAL(10,2),
    manufacturer STRING,
    installation_date DATE,
    last_maintenance_date DATE,
    status STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (equipment_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres-sgcc-source',
    'port' = '5432',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'database-name' = 'sgcc_source_db',
    'schema-name' = 'sgcc_power',
    'table-name' = 'power_equipment',
    'slot.name' = 'power_equipment_pgoutput_slot',
    'decoding.plugin.name' = 'pgoutput'  -- 🔴 关键修复：使用pgoutput解码器
);

-- ========================================
-- 重新启动数据流作业
-- ========================================
USE CATALOG fluss_catalog;
USE fluss;

-- 清空目标表（如果需要）
-- DELETE FROM power_equipment_fluss_target;

-- 启动修复后的数据流作业
INSERT INTO power_equipment_fluss_target
SELECT 
    equipment_id,
    equipment_name,
    equipment_type,
    location,
    voltage_level,
    capacity_mw,
    manufacturer,
    installation_date,
    last_maintenance_date,
    status,
    created_at,
    updated_at
FROM default_catalog.default_database.sgcc_power_equipment_cdc_source;

QUIT; 