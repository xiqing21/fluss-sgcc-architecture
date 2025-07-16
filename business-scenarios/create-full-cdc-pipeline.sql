-- 完整的CDC数据流水线脚本
-- 确保 source → fluss → sink 的完整链路

-- 1. 创建PostgreSQL CDC源表 - 设备维度数据
CREATE TABLE IF NOT EXISTS postgres_device_source (
    device_id STRING,
    device_name STRING,
    device_type STRING,
    location STRING,
    capacity_mw DOUBLE,
    installation_date TIMESTAMP(3),
    manufacturer STRING,
    model STRING,
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

-- 2. 创建PostgreSQL CDC源表 - 电力调度数据
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

-- 3. 创建PostgreSQL Sink表 - 设备状态汇总
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
    'sink.buffer-flush.max-rows' = '1',
    'sink.buffer-flush.interval' = '1s'
);

-- 4. 创建PostgreSQL Sink表 - 电网监控指标
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
    'sink.buffer-flush.max-rows' = '1',
    'sink.buffer-flush.interval' = '1s'
);

-- 5. 创建PostgreSQL Sink表 - 智能电网综合报表
CREATE TABLE IF NOT EXISTS postgres_smart_grid_sink (
    report_id STRING,
    report_type STRING,
    analysis_period STRING,
    grid_region STRING,
    grid_stability_index DOUBLE,
    operational_efficiency DOUBLE,
    energy_optimization_score DOUBLE,
    reliability_rating STRING,
    risk_assessment STRING,
    performance_trends STRING,
    optimization_recommendations STRING,
    cost_benefit_analysis DOUBLE,
    report_time TIMESTAMP(3),
    PRIMARY KEY (report_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'smart_grid_comprehensive_result',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'sink.buffer-flush.max-rows' = '1',
    'sink.buffer-flush.interval' = '1s'
);

-- 6. 启动设备状态汇总数据流 (CDC → Sink)
INSERT INTO postgres_device_status_sink
SELECT 
    CONCAT('CDC_', device_id, '_', CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP) AS STRING)) as summary_id,
    device_id,
    device_type,
    location,
    CASE 
        WHEN maintenance_status = 'NORMAL' THEN 'NORMAL'
        WHEN maintenance_status = 'WARNING' THEN 'WARNING'
        WHEN maintenance_status = 'MAINTENANCE' THEN 'MAINTENANCE'
        ELSE 'CRITICAL'
    END as status,
    CASE 
        WHEN capacity_mw > 0 THEN (real_time_current / capacity_mw) * 100
        ELSE 0
    END as load_factor,
    efficiency_rate * 100 as efficiency,
    real_time_temperature as temperature,
    CURRENT_TIMESTAMP as update_time
FROM postgres_device_source;

-- 7. 启动电网监控指标数据流 (CDC → Sink)
INSERT INTO postgres_grid_metrics_sink
SELECT 
    CONCAT('CDC_', grid_region, '_', CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP) AS STRING)) as metric_id,
    grid_region,
    total_devices,
    online_devices,
    offline_devices,
    maintenance_devices,
    avg_efficiency,
    avg_load_factor,
    avg_temperature,
    alert_count,
    CURRENT_TIMESTAMP as update_time
FROM (
    SELECT 
        SUBSTRING(location, 1, 2) as grid_region,
        COUNT(*) as total_devices,
        COUNT(CASE WHEN maintenance_status = 'NORMAL' THEN 1 END) as online_devices,
        COUNT(CASE WHEN maintenance_status = 'CRITICAL' THEN 1 END) as offline_devices,
        COUNT(CASE WHEN maintenance_status = 'MAINTENANCE' THEN 1 END) as maintenance_devices,
        AVG(efficiency_rate * 100) as avg_efficiency,
        AVG(CASE WHEN capacity_mw > 0 THEN (real_time_current / capacity_mw) * 100 ELSE 0 END) as avg_load_factor,
        AVG(real_time_temperature) as avg_temperature,
        COUNT(CASE WHEN real_time_temperature > 70 THEN 1 END) as alert_count
    FROM postgres_device_source
    GROUP BY SUBSTRING(location, 1, 2)
) as grid_stats;

-- 8. 启动智能电网综合报表数据流 (CDC → Sink)
INSERT INTO postgres_smart_grid_sink
SELECT 
    CONCAT('CDC_', grid_region, '_', CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP) AS STRING)) as report_id,
    '智能电网综合运行报表' as report_type,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyyMMddHH') as analysis_period,
    grid_region,
    grid_stability_index,
    operational_efficiency,
    energy_optimization_score,
    reliability_rating,
    risk_assessment,
    performance_trends,
    optimization_recommendations,
    cost_benefit_analysis,
    CURRENT_TIMESTAMP as report_time
FROM (
    SELECT 
        SUBSTRING(location, 1, 2) as grid_region,
        AVG(efficiency_rate * 100) as grid_stability_index,
        AVG(efficiency_rate * 100) as operational_efficiency,
        AVG(efficiency_rate * 95) as energy_optimization_score,
        CASE 
            WHEN AVG(efficiency_rate) > 0.95 THEN 'A+'
            WHEN AVG(efficiency_rate) > 0.90 THEN 'A'
            WHEN AVG(efficiency_rate) > 0.85 THEN 'B+'
            ELSE 'B'
        END as reliability_rating,
        CASE 
            WHEN AVG(real_time_temperature) > 65 THEN 'MEDIUM_运行异常较多，优化调度'
            ELSE 'LOW_运行正常，保持现状'
        END as risk_assessment,
        CASE 
            WHEN AVG(efficiency_rate) > 0.94 THEN 'STABLE_运行稳定'
            WHEN AVG(efficiency_rate) > 0.88 THEN 'IMPROVING_效率持续提升'
            ELSE 'DECLINING_效率下降'
        END as performance_trends,
        CASE 
            WHEN AVG(real_time_temperature) > 65 THEN '建议优化设备散热系统，降低运行温度'
            ELSE '建议继续保持当前运行策略，定期优化调度算法'
        END as optimization_recommendations,
        AVG(efficiency_rate * 150000) as cost_benefit_analysis
    FROM postgres_device_source
    GROUP BY SUBSTRING(location, 1, 2)
) as report_stats; 