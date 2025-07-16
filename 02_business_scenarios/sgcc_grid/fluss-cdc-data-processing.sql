-- ===============================================
-- 🔋 Fluss CDC数据处理 - 为大屏生成汇总数据
-- 补充用户的综合业务场景测试.sql，生成Dashboard所需的汇总表
-- 数据流：PostgreSQL源 → CDC → Fluss → 本脚本处理 → PostgreSQL sink
-- ===============================================

SET 'sql-client.execution.result-mode' = 'tableau';

-- 创建Fluss Catalog连接
CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

USE CATALOG fluss_catalog;
USE fluss;

-- 回到default catalog创建PostgreSQL sink表
USE CATALOG default_catalog;

-- 创建设备状态汇总表的sink连接
CREATE TABLE IF NOT EXISTS postgres_device_status_summary (
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
    'password' = 'sgcc_pass_2024'
);

-- 创建电网监控指标表的sink连接
CREATE TABLE IF NOT EXISTS postgres_grid_monitoring_metrics (
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
    'password' = 'sgcc_pass_2024'
);

-- ===============================================
-- 从Fluss DWD层生成设备状态汇总数据
-- ===============================================

-- 🚀 生成设备状态汇总数据 - 用于大屏实时设备监控表
INSERT INTO postgres_device_status_summary
SELECT 
    CONCAT('DEVICE_SUMMARY_', device_id, '_', CAST(DATE_FORMAT(dispatch_time, 'yyyyMMddHHmmss') AS STRING)) as summary_id,
    device_id,
    device_type,
    device_location as location,
    CASE 
        WHEN operational_status = 'OPTIMAL' THEN 'NORMAL'
        WHEN operational_status = 'GOOD' THEN 'NORMAL'
        WHEN operational_status = 'ACCEPTABLE' THEN 'WARNING'
        WHEN operational_status = 'NEEDS_ATTENTION' THEN 'CRITICAL'
        WHEN risk_level = 'HIGH_RISK' THEN 'CRITICAL'
        WHEN risk_level = 'MEDIUM_RISK' THEN 'WARNING'
        ELSE 'MAINTENANCE'
    END as status,
    supply_demand_balance as load_factor,
    device_health_score as efficiency,
    -- 模拟温度数据（从设备健康评分推算）
    CASE 
        WHEN device_health_score > 90 THEN 25.0 + (100 - device_health_score) * 2
        WHEN device_health_score > 80 THEN 35.0 + (90 - device_health_score) * 3
        WHEN device_health_score > 70 THEN 45.0 + (80 - device_health_score) * 4
        ELSE 65.0 + (70 - device_health_score) * 2
    END as temperature,
    dispatch_time as update_time
FROM fluss_catalog.fluss.dwd_smart_grid_detail
WHERE dispatch_time >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR;

-- ===============================================
-- 从Fluss DWS层生成电网监控指标数据
-- ===============================================

-- 🚀 生成电网监控指标数据 - 用于大屏统计指标
INSERT INTO postgres_grid_monitoring_metrics
SELECT 
    CONCAT('GRID_METRICS_', grid_region, '_', time_window) as metric_id,
    grid_region,
    total_devices as total_devices,
    CAST(total_devices * frequency_stability_rate / 100 AS INTEGER) as online_devices,
    CAST(total_devices * (100 - frequency_stability_rate) / 100 AS INTEGER) as offline_devices,
    high_risk_incidents as maintenance_devices,
    avg_device_health as avg_efficiency,
    avg_supply_demand_balance as avg_load_factor,
    -- 模拟平均温度（从设备健康评分推算）
    CASE 
        WHEN avg_device_health > 90 THEN 30.0
        WHEN avg_device_health > 80 THEN 40.0
        WHEN avg_device_health > 70 THEN 50.0
        ELSE 60.0
    END as avg_temperature,
    emergency_responses as alert_count,
    CURRENT_TIMESTAMP as update_time
FROM fluss_catalog.fluss.dws_grid_operation_summary
WHERE time_window >= DATE_FORMAT(CURRENT_TIMESTAMP - INTERVAL '2' HOUR, 'yyyyMMddHH');

-- ===============================================
-- 创建定时任务刷新汇总数据
-- ===============================================

-- 每分钟刷新设备状态汇总
CREATE TEMPORARY VIEW refresh_device_status AS
SELECT 
    CONCAT('DEVICE_SUMMARY_', device_id, '_', CAST(DATE_FORMAT(dispatch_time, 'yyyyMMddHHmmss') AS STRING)) as summary_id,
    device_id,
    device_type,
    device_location as location,
    CASE 
        WHEN operational_status = 'OPTIMAL' THEN 'NORMAL'
        WHEN operational_status = 'GOOD' THEN 'NORMAL'
        WHEN operational_status = 'ACCEPTABLE' THEN 'WARNING'
        WHEN operational_status = 'NEEDS_ATTENTION' THEN 'CRITICAL'
        WHEN risk_level = 'HIGH_RISK' THEN 'CRITICAL'
        WHEN risk_level = 'MEDIUM_RISK' THEN 'WARNING'
        ELSE 'MAINTENANCE'
    END as status,
    supply_demand_balance as load_factor,
    device_health_score as efficiency,
    CASE 
        WHEN device_health_score > 90 THEN 25.0 + (100 - device_health_score) * 2
        WHEN device_health_score > 80 THEN 35.0 + (90 - device_health_score) * 3
        WHEN device_health_score > 70 THEN 45.0 + (80 - device_health_score) * 4
        ELSE 65.0 + (70 - device_health_score) * 2
    END as temperature,
    dispatch_time as update_time
FROM fluss_catalog.fluss.dwd_smart_grid_detail
WHERE dispatch_time >= CURRENT_TIMESTAMP - INTERVAL '30' MINUTE;

-- 每5分钟刷新电网监控指标
CREATE TEMPORARY VIEW refresh_grid_metrics AS
SELECT 
    CONCAT('GRID_METRICS_', grid_region, '_', time_window) as metric_id,
    grid_region,
    total_devices as total_devices,
    CAST(total_devices * frequency_stability_rate / 100 AS INTEGER) as online_devices,
    CAST(total_devices * (100 - frequency_stability_rate) / 100 AS INTEGER) as offline_devices,
    high_risk_incidents as maintenance_devices,
    avg_device_health as avg_efficiency,
    avg_supply_demand_balance as avg_load_factor,
    CASE 
        WHEN avg_device_health > 90 THEN 30.0
        WHEN avg_device_health > 80 THEN 40.0
        WHEN avg_device_health > 70 THEN 50.0
        ELSE 60.0
    END as avg_temperature,
    emergency_responses as alert_count,
    CURRENT_TIMESTAMP as update_time
FROM fluss_catalog.fluss.dws_grid_operation_summary
WHERE time_window >= DATE_FORMAT(CURRENT_TIMESTAMP - INTERVAL '1' HOUR, 'yyyyMMddHH');

-- ===============================================
-- 数据质量监控查询
-- ===============================================

-- 查看数据流状态
SELECT 
    'PostgreSQL源->Fluss CDC数据流' as data_flow,
    'ODS电力调度' as layer,
    COUNT(*) as record_count
FROM fluss_catalog.fluss.ods_power_dispatch_raw
WHERE event_time >= CURRENT_TIMESTAMP - INTERVAL '10' MINUTE
UNION ALL
SELECT 
    'PostgreSQL源->Fluss CDC数据流' as data_flow,
    'ODS设备维度' as layer,
    COUNT(*) as record_count
FROM fluss_catalog.fluss.ods_device_dimension_raw
WHERE event_time >= CURRENT_TIMESTAMP - INTERVAL '10' MINUTE
UNION ALL
SELECT 
    'Fluss数仓分层处理' as data_flow,
    'DWD智能电网明细' as layer,
    COUNT(*) as record_count
FROM fluss_catalog.fluss.dwd_smart_grid_detail
WHERE dispatch_time >= CURRENT_TIMESTAMP - INTERVAL '10' MINUTE
UNION ALL
SELECT 
    'Fluss数仓分层处理' as data_flow,
    'DWS电网运行汇总' as layer,
    COUNT(*) as record_count
FROM fluss_catalog.fluss.dws_grid_operation_summary
WHERE time_window >= DATE_FORMAT(CURRENT_TIMESTAMP - INTERVAL '1' HOUR, 'yyyyMMddHH')
UNION ALL
SELECT 
    'Fluss数仓分层处理' as data_flow,
    'ADS综合分析报表' as layer,
    COUNT(*) as record_count
FROM fluss_catalog.fluss.ads_smart_grid_comprehensive_report
WHERE report_time >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR;

-- 查看PostgreSQL sink数据状态
SELECT 
    'Fluss->PostgreSQL sink数据流' as data_flow,
    'Sink综合分析结果' as layer,
    COUNT(*) as record_count
FROM postgres_smart_grid_comprehensive_result
WHERE report_time >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR
UNION ALL
SELECT 
    'Fluss->PostgreSQL sink数据流' as data_flow,
    'Sink设备状态汇总' as layer,
    COUNT(*) as record_count
FROM postgres_device_status_summary
WHERE update_time >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR
UNION ALL
SELECT 
    'Fluss->PostgreSQL sink数据流' as data_flow,
    'Sink电网监控指标' as layer,
    COUNT(*) as record_count
FROM postgres_grid_monitoring_metrics
WHERE update_time >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR;

-- ===============================================
-- 性能监控查询
-- ===============================================

-- 延迟监控 - 检查端到端数据延迟
SELECT 
    'CDC数据延迟监控' as monitor_type,
    AVG(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - update_time))) as avg_delay_seconds,
    MAX(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - update_time))) as max_delay_seconds,
    MIN(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - update_time))) as min_delay_seconds
FROM postgres_device_status_summary
WHERE update_time >= CURRENT_TIMESTAMP - INTERVAL '5' MINUTE;

-- 吞吐量监控 - 检查每分钟处理的记录数
SELECT 
    'CDC数据吞吐量监控' as monitor_type,
    DATE_TRUNC('minute', update_time) as time_minute,
    COUNT(*) as records_per_minute
FROM postgres_device_status_summary
WHERE update_time >= CURRENT_TIMESTAMP - INTERVAL '10' MINUTE
GROUP BY DATE_TRUNC('minute', update_time)
ORDER BY time_minute DESC;

-- 数据完整性监控 - 检查各层数据一致性
SELECT 
    '数据完整性监控' as monitor_type,
    (SELECT COUNT(*) FROM fluss_catalog.fluss.ods_power_dispatch_raw WHERE event_time >= CURRENT_TIMESTAMP - INTERVAL '5' MINUTE) as ods_dispatch_count,
    (SELECT COUNT(*) FROM fluss_catalog.fluss.ods_device_dimension_raw WHERE event_time >= CURRENT_TIMESTAMP - INTERVAL '5' MINUTE) as ods_device_count,
    (SELECT COUNT(*) FROM fluss_catalog.fluss.dwd_smart_grid_detail WHERE dispatch_time >= CURRENT_TIMESTAMP - INTERVAL '5' MINUTE) as dwd_detail_count,
    (SELECT COUNT(*) FROM postgres_device_status_summary WHERE update_time >= CURRENT_TIMESTAMP - INTERVAL '5' MINUTE) as sink_summary_count; 