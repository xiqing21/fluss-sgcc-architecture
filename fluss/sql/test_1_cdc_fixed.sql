-- 测试修复版本 - 移除元数据列的CDC源到Fluss脚本

SET 'execution.checkpointing.interval' = '60s';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
SET 'table.exec.source.idle-timeout' = '30s';

-- 创建Fluss Catalog
CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- CDC源表测试
CREATE TABLE IF NOT EXISTS sgcc_power_equipment_source (
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
    'slot.name' = 'sgcc_equipment_slot',
    'decoding.plugin.name' = 'pgoutput'
);

-- 使用Fluss Catalog
USE CATALOG fluss_catalog;

-- Fluss表测试（移除元数据列）
CREATE TABLE IF NOT EXISTS fluss_ods_power_equipment (
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
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123',
    'table-name' = 'ods_power_equipment',
    'bucket' = '4',
    'value.format' = 'json',
    'fluss.table.datalake.enabled' = 'true',
    'fluss.table.log.retention' = '7d',
    'fluss.table.compaction.enabled' = 'true'
);
