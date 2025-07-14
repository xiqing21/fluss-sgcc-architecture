-- ========================================
-- 国网电力监控系统 - Fluss架构
-- 第一步：从PostgreSQL CDC源同步数据到Fluss
-- ========================================

-- 设置执行环境
SET 'execution.checkpointing.interval' = '60s';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
SET 'table.exec.source.idle-timeout' = '30s';

-- 创建Fluss Catalog
CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- ========================================
-- 创建PostgreSQL CDC源表 (在默认catalog中)
-- ========================================

-- 电力设备CDC源表
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

-- 实时监控数据CDC源表
CREATE TABLE IF NOT EXISTS sgcc_power_monitoring_source (
    monitoring_id BIGINT,
    equipment_id BIGINT,
    voltage_a DECIMAL(8,2),
    voltage_b DECIMAL(8,2),
    voltage_c DECIMAL(8,2),
    current_a DECIMAL(8,2),
    current_b DECIMAL(8,2),
    current_c DECIMAL(8,2),
    power_active DECIMAL(10,2),
    power_reactive DECIMAL(10,2),
    frequency DECIMAL(6,3),
    temperature DECIMAL(6,2),
    humidity DECIMAL(5,2),
    monitoring_time TIMESTAMP(3),
    created_at TIMESTAMP(3),
    PRIMARY KEY (monitoring_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres-sgcc-source',
    'port' = '5432',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'database-name' = 'sgcc_source_db',
    'schema-name' = 'sgcc_power',
    'table-name' = 'power_monitoring',
    'slot.name' = 'sgcc_monitoring_slot',
    'decoding.plugin.name' = 'pgoutput'
);

-- 设备告警CDC源表
CREATE TABLE IF NOT EXISTS sgcc_equipment_alarms_source (
    alarm_id BIGINT,
    equipment_id BIGINT,
    alarm_type STRING,
    alarm_level INT,
    alarm_message STRING,
    alarm_code STRING,
    is_resolved BOOLEAN,
    occurred_at TIMESTAMP(3),
    resolved_at TIMESTAMP(3),
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (alarm_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres-sgcc-source',
    'port' = '5432',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'database-name' = 'sgcc_source_db',
    'schema-name' = 'sgcc_power',
    'table-name' = 'equipment_alarms',
    'slot.name' = 'sgcc_alarms_slot',
    'decoding.plugin.name' = 'pgoutput'
);

-- 客户用电信息CDC源表
CREATE TABLE IF NOT EXISTS sgcc_customer_usage_source (
    usage_id BIGINT,
    customer_id BIGINT,
    customer_name STRING,
    customer_type STRING,
    meter_reading DECIMAL(10,2),
    billing_period DATE,
    usage_amount DECIMAL(10,2),
    peak_demand DECIMAL(8,2),
    tariff_type STRING,
    billing_amount DECIMAL(10,2),
    reading_time TIMESTAMP(3),
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (usage_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres-sgcc-source',
    'port' = '5432',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'database-name' = 'sgcc_source_db',
    'schema-name' = 'sgcc_power',
    'table-name' = 'customer_usage',
    'slot.name' = 'sgcc_usage_slot',
    'decoding.plugin.name' = 'pgoutput'
);

-- ========================================
-- 创建Fluss目标表（ODS层）
-- ========================================

-- 使用Fluss Catalog创建Fluss表
USE CATALOG fluss_catalog;

-- Fluss电力设备表
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
    process_time TIMESTAMP(3) METADATA FROM 'timestamp',
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

-- Fluss实时监控数据表
CREATE TABLE IF NOT EXISTS fluss_ods_power_monitoring (
    monitoring_id BIGINT,
    equipment_id BIGINT,
    voltage_a DECIMAL(8,2),
    voltage_b DECIMAL(8,2),
    voltage_c DECIMAL(8,2),
    current_a DECIMAL(8,2),
    current_b DECIMAL(8,2),
    current_c DECIMAL(8,2),
    power_active DECIMAL(10,2),
    power_reactive DECIMAL(10,2),
    frequency DECIMAL(6,3),
    temperature DECIMAL(6,2),
    humidity DECIMAL(5,2),
    monitoring_time TIMESTAMP(3),
    created_at TIMESTAMP(3),
    process_time TIMESTAMP(3) METADATA FROM 'timestamp',
    PRIMARY KEY (monitoring_id) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123',
    'table-name' = 'ods_power_monitoring',
    'bucket' = '8',
    'value.format' = 'json',
    'fluss.table.datalake.enabled' = 'true',
    'fluss.table.log.retention' = '3d',
    'fluss.table.compaction.enabled' = 'false'
);

-- Fluss设备告警表
CREATE TABLE IF NOT EXISTS fluss_ods_equipment_alarms (
    alarm_id BIGINT,
    equipment_id BIGINT,
    alarm_type STRING,
    alarm_level INT,
    alarm_message STRING,
    alarm_code STRING,
    is_resolved BOOLEAN,
    occurred_at TIMESTAMP(3),
    resolved_at TIMESTAMP(3),
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    process_time TIMESTAMP(3) METADATA FROM 'timestamp',
    PRIMARY KEY (alarm_id) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123',
    'table-name' = 'ods_equipment_alarms',
    'bucket' = '4',
    'value.format' = 'json',
    'fluss.table.datalake.enabled' = 'true',
    'fluss.table.log.retention' = '30d',
    'fluss.table.compaction.enabled' = 'true'
);

-- Fluss客户用电信息表
CREATE TABLE IF NOT EXISTS fluss_ods_customer_usage (
    usage_id BIGINT,
    customer_id BIGINT,
    customer_name STRING,
    customer_type STRING,
    meter_reading DECIMAL(10,2),
    billing_period DATE,
    usage_amount DECIMAL(10,2),
    peak_demand DECIMAL(8,2),
    tariff_type STRING,
    billing_amount DECIMAL(10,2),
    reading_time TIMESTAMP(3),
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    process_time TIMESTAMP(3) METADATA FROM 'timestamp',
    PRIMARY KEY (usage_id) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123',
    'table-name' = 'ods_customer_usage',
    'bucket' = '4',
    'value.format' = 'json',
    'fluss.table.datalake.enabled' = 'true',
    'fluss.table.log.retention' = '90d',
    'fluss.table.compaction.enabled' = 'true'
);

-- ========================================
-- 数据同步任务
-- ========================================

-- 同步电力设备数据
INSERT INTO fluss_ods_power_equipment
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
FROM sgcc_power_equipment_source;

-- 同步实时监控数据
INSERT INTO fluss_ods_power_monitoring
SELECT 
    monitoring_id,
    equipment_id,
    voltage_a,
    voltage_b,
    voltage_c,
    current_a,
    current_b,
    current_c,
    power_active,
    power_reactive,
    frequency,
    temperature,
    humidity,
    monitoring_time,
    created_at
FROM sgcc_power_monitoring_source;

-- 同步设备告警数据
INSERT INTO fluss_ods_equipment_alarms
SELECT 
    alarm_id,
    equipment_id,
    alarm_type,
    alarm_level,
    alarm_message,
    alarm_code,
    is_resolved,
    occurred_at,
    resolved_at,
    created_at,
    updated_at
FROM sgcc_equipment_alarms_source;

-- 同步客户用电信息
INSERT INTO fluss_ods_customer_usage
SELECT 
    usage_id,
    customer_id,
    customer_name,
    customer_type,
    meter_reading,
    billing_period,
    usage_amount,
    peak_demand,
    tariff_type,
    billing_amount,
    reading_time,
    created_at,
    updated_at
FROM sgcc_customer_usage_source;

-- ========================================
-- 数据质量监控
-- ========================================

-- 创建数据质量检查表
CREATE TABLE IF NOT EXISTS fluss_dq_monitoring (
    table_name STRING,
    record_count BIGINT,
    error_count BIGINT,
    check_time TIMESTAMP(3),
    status STRING,
    PRIMARY KEY (table_name, check_time) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123',
    'table-name' = 'dq_monitoring',
    'bucket' = '1',
    'value.format' = 'json',
    'fluss.table.log.retention' = '7d'
);

-- ========================================
-- 启动CDC数据流作业
-- ========================================

-- 启动电力设备CDC数据流
INSERT INTO fluss_ods_power_equipment
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
FROM default_catalog.default_database.sgcc_power_equipment_source;

-- 启动实时监控数据CDC数据流
INSERT INTO fluss_ods_power_monitoring
SELECT 
    monitoring_id,
    equipment_id,
    voltage_a,
    voltage_b,
    voltage_c,
    current_a,
    current_b,
    current_c,
    power_active,
    power_reactive,
    frequency,
    temperature,
    humidity,
    monitoring_time
FROM default_catalog.default_database.sgcc_power_monitoring_source;

-- 启动设备告警CDC数据流
INSERT INTO fluss_ods_equipment_alarms
SELECT 
    alarm_id,
    equipment_id,
    alarm_type,
    alarm_level,
    alarm_message,
    alarm_code,
    is_resolved,
    occurred_at,
    resolved_at,
    created_at,
    updated_at
FROM default_catalog.default_database.sgcc_equipment_alarms_source;

-- 启动客户用电数据CDC数据流
INSERT INTO fluss_ods_customer_usage
SELECT 
    usage_id,
    equipment_id,
    customer_id,
    usage_date,
    usage_kwh,
    peak_demand_kw,
    off_peak_usage_kwh,
    billing_amount,
    created_at,
    updated_at
FROM default_catalog.default_database.sgcc_customer_usage_source; 