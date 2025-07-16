-- 简化版CDC数据流脚本
-- 直接从PostgreSQL源同步到PostgreSQL sink

-- 创建PostgreSQL CDC源表 - 电力调度数据
CREATE TABLE IF NOT EXISTS postgres_dispatch_source (
    dispatch_id STRING,
    grid_region STRING,
    dispatch_time TIMESTAMP(3),
    load_demand_mw DOUBLE,
    supply_capacity_mw DOUBLE,
    emergency_level STRING,
    load_balance_status STRING,
    grid_frequency_hz DOUBLE,
    voltage_stability DOUBLE,
    PRIMARY KEY (dispatch_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres-sgcc-source',
    'port' = '5432',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'database-name' = 'sgcc_source_db',
    'schema-name' = 'public',
    'table-name' = 'power_dispatch_data',
    'slot.name' = 'dispatch_cdc_slot',
    'decoding.plugin.name' = 'pgoutput'
);

-- 创建PostgreSQL CDC源表 - 设备维度数据
CREATE TABLE IF NOT EXISTS postgres_device_source (
    device_id STRING,
    device_name STRING,
    device_type STRING,
    location STRING,
    capacity_mw DOUBLE,
    installation_date TIMESTAMP(3),
    manufacturer STRING,
    model_name STRING,
    efficiency_rate DOUBLE,
    maintenance_status STRING,
    real_time_voltage DOUBLE,
    real_time_current DOUBLE,
    real_time_temperature DOUBLE,
    PRIMARY KEY (device_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres-sgcc-source',
    'port' = '5432',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'database-name' = 'sgcc_source_db',
    'schema-name' = 'public',
    'table-name' = 'device_dimension_data',
    'slot.name' = 'device_cdc_slot',
    'decoding.plugin.name' = 'pgoutput'
);

-- 创建PostgreSQL Sink表 - 设备状态汇总
CREATE TABLE IF NOT EXISTS postgres_device_status_sink (
    summary_id STRING,
    device_id STRING,
    device_type STRING,
    location STRING,
    status STRING,
    load_factor DOUBLE,
    efficiency DOUBLE,
    temperature DOUBLE,
    update_time TIMESTAMP(3),
    PRIMARY KEY (summary_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'device_status_summary',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s'
);

-- 创建PostgreSQL Sink表 - 电网监控指标
CREATE TABLE IF NOT EXISTS postgres_grid_metrics_sink (
    metric_id STRING,
    grid_region STRING,
    total_devices INTEGER,
    online_devices INTEGER,
    offline_devices INTEGER,
    maintenance_devices INTEGER,
    avg_efficiency DOUBLE,
    avg_load_factor DOUBLE,
    avg_temperature DOUBLE,
    alert_count INTEGER,
    update_time TIMESTAMP(3),
    PRIMARY KEY (metric_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'grid_monitoring_metrics',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s'
);

-- 启动设备状态汇总数据流
INSERT INTO postgres_device_status_sink
SELECT 
    CONCAT('DEVICE_STATUS_', device_id, '_', UNIX_TIMESTAMP()) as summary_id,
    device_id,
    device_type,
    location,
    CASE 
        WHEN maintenance_status = 'NORMAL' THEN 'NORMAL'
        WHEN maintenance_status = 'WARNING' THEN 'WARNING'
        WHEN maintenance_status = 'MAINTENANCE' THEN 'MAINTENANCE'
        ELSE 'CRITICAL'
    END as status,
    (real_time_current / capacity_mw) * 100 as load_factor,
    efficiency_rate * 100 as efficiency,
    real_time_temperature as temperature,
    installation_date as update_time
FROM postgres_device_source;

-- 启动电网监控指标数据流
INSERT INTO postgres_grid_metrics_sink
SELECT 
    CONCAT('GRID_METRICS_', SUBSTRING(location, 1, 2), '_', UNIX_TIMESTAMP()) as metric_id,
    SUBSTRING(location, 1, 2) as grid_region,
    COUNT(*) as total_devices,
    COUNT(CASE WHEN maintenance_status = 'NORMAL' THEN 1 END) as online_devices,
    COUNT(CASE WHEN maintenance_status = 'CRITICAL' THEN 1 END) as offline_devices,
    COUNT(CASE WHEN maintenance_status = 'MAINTENANCE' THEN 1 END) as maintenance_devices,
    AVG(efficiency_rate * 100) as avg_efficiency,
    AVG((real_time_current / capacity_mw) * 100) as avg_load_factor,
    AVG(real_time_temperature) as avg_temperature,
    COUNT(CASE WHEN real_time_temperature > 70 THEN 1 END) as alert_count,
    MAX(installation_date) as update_time
FROM postgres_device_source
GROUP BY SUBSTRING(location, 1, 2); 