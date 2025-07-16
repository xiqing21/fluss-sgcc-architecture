-- ===============================================
-- 🔴 场景3：时间旅行查询 + 数仓分层 + PostgreSQL回流
-- 🔥 Fluss vs Kafka 架构升级对比：
-- 1. ✅ 时间旅行查询：Fluss原生支持FOR SYSTEM_TIME AS OF，Kafka无此功能
-- 2. ✅ 版本化存储：Fluss自动管理多版本，Kafka需要复杂配置
-- 3. ✅ 历史回溯：可查询任意时间点数据，Kafka只能顺序消费
-- 4. ✅ 故障分析：基于历史版本的根因分析，Kafka需要外部时序数据库
-- 数据流：PostgreSQL CDC → Fluss时序数仓（支持时间旅行） → PostgreSQL故障分析
-- ===============================================

SET 'sql-client.execution.result-mode' = 'tableau';

-- ===============================================
-- 1. PostgreSQL CDC历史数据源（时间旅行查询场景）
-- ===============================================

-- 🚀 设备历史运行CDC流：支持时间旅行查询的历史数据
CREATE TABLE device_historical_stream (
    device_id STRING,
    record_time TIMESTAMP(3),
    voltage DOUBLE,
    current_val DOUBLE,  -- 避免保留字冲突
    temperature DOUBLE,
    power_output DOUBLE,
    efficiency DOUBLE,
    load_percentage DOUBLE,
    operational_mode STRING,
    error_codes STRING,
    WATERMARK FOR record_time AS record_time - INTERVAL '1' SECOND
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres-sgcc-source',
    'port' = '5432',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'database-name' = 'sgcc_source_db',
    'schema-name' = 'public',
    'table-name' = 'device_historical_data',
    'slot.name' = 'device_historical_slot',
    'decoding.plugin.name' = 'pgoutput'
);

-- 📝 备用：DataGen历史数据源（暂时注释）
/*
CREATE TEMPORARY TABLE device_historical_stream_backup (
    device_id STRING,
    record_time TIMESTAMP(3),
    voltage DOUBLE,
    current_val DOUBLE,
    temperature DOUBLE,
    power_output DOUBLE,
    efficiency DOUBLE,
    load_percentage DOUBLE,
    operational_mode STRING,
    error_codes STRING,
    WATERMARK FOR record_time AS record_time - INTERVAL '1' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '150',  -- 降低测试数据量
    'fields.device_id.kind' = 'sequence',
    'fields.device_id.start' = '100000',
    'fields.device_id.end' = '100020',
    'fields.voltage.min' = '210.0',
    'fields.voltage.max' = '250.0',
    'fields.current_val.min' = '40.0',
    'fields.current_val.max' = '220.0',
    'fields.temperature.min' = '15.0',
    'fields.temperature.max' = '90.0',
    'fields.power_output.min' = '50.0',
    'fields.power_output.max' = '600.0',
    'fields.efficiency.min' = '0.75',
    'fields.efficiency.max' = '0.99',
    'fields.load_percentage.min' = '30.0',
    'fields.load_percentage.max' = '100.0',
    'fields.operational_mode.length' = '8',
    'fields.error_codes.length' = '10'
);
*/

-- ===============================================
-- 2. 创建Fluss Catalog和时序数仓分层表
-- ===============================================

CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

USE CATALOG fluss_catalog;
CREATE DATABASE IF NOT EXISTS fluss;
USE fluss;

-- ODS层：设备历史运行原始数据（支持时间旅行）
CREATE TABLE ods_device_historical_raw (
    device_id STRING,
    record_time TIMESTAMP(3),
    voltage DOUBLE,
    `current` DOUBLE,
    temperature DOUBLE,
    power_output DOUBLE,
    efficiency DOUBLE,
    load_percentage DOUBLE,
    operational_mode STRING,
    error_codes STRING,
    PRIMARY KEY (device_id, record_time) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- DWD层：设备运行明细（时序分析）
CREATE TABLE dwd_device_timeseries_detail (
    ts_id STRING,
    device_id STRING,
    record_time TIMESTAMP(3),
    device_name STRING,
    device_type STRING,
    location STRING,
    voltage DOUBLE,
    `current` DOUBLE,
    temperature DOUBLE,
    power_output DOUBLE,
    efficiency DOUBLE,
    load_percentage DOUBLE,
    operational_mode_desc STRING,
    performance_index DOUBLE,
    health_score DOUBLE,
    anomaly_flags STRING,
    PRIMARY KEY (ts_id) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- DWS层：时间窗口汇总（支持时间旅行分析）
CREATE TABLE dws_device_timeseries_summary (
    summary_id STRING,
    device_id STRING,
    time_window TIMESTAMP(3),
    window_type STRING,
    avg_voltage DOUBLE,
    avg_current DOUBLE,
    avg_temperature DOUBLE,
    avg_efficiency DOUBLE,
    max_temperature DOUBLE,
    min_efficiency DOUBLE,
    anomaly_count BIGINT,
    performance_trend STRING,
    PRIMARY KEY (summary_id) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- ADS层：故障分析报表
CREATE TABLE ads_fault_analysis_report (
    analysis_id STRING PRIMARY KEY NOT ENFORCED,
    device_id STRING,
    analysis_type STRING,
    fault_time_period STRING,
    pre_fault_performance DOUBLE,
    fault_indicators STRING,
    root_cause_analysis STRING,
    performance_degradation DOUBLE,
    recovery_suggestions STRING,
    maintenance_priority STRING,
    analysis_time TIMESTAMP(3)
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- ===============================================
-- 3. 回到Default Catalog创建PostgreSQL Sink
-- ===============================================

USE CATALOG default_catalog;

-- PostgreSQL故障分析结果表
CREATE TABLE postgres_fault_analysis_result (
    analysis_id STRING,
    device_id STRING,
    analysis_type STRING,
    fault_time_period STRING,
    pre_fault_performance DOUBLE,
    fault_indicators STRING,
    root_cause_analysis STRING,
    performance_degradation DOUBLE,
    recovery_suggestions STRING,
    maintenance_priority STRING,
    analysis_time TIMESTAMP(3),
    PRIMARY KEY (analysis_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'fault_analysis_result',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024'
);

-- ===============================================
-- 4. ODS层：历史数据采集
-- ===============================================

-- 🚀 ODS层：采集设备历史运行数据
INSERT INTO fluss_catalog.fluss.ods_device_historical_raw
SELECT 
    device_id,
    record_time,
    voltage,
    current_val as `current`,  -- 修复字段映射
    temperature,
    power_output,
    efficiency,
    load_percentage,
    operational_mode,
    error_codes
FROM device_historical_stream;

-- ===============================================
-- 5. DWD层：时序数据明细化
-- ===============================================

-- 🚀 DWD层：设备时序数据处理
INSERT INTO fluss_catalog.fluss.dwd_device_timeseries_detail
SELECT 
    CONCAT(device_id, '_', CAST(EXTRACT(EPOCH FROM record_time) AS STRING)) as ts_id,
    device_id,
    record_time,
    CONCAT('智能设备_', device_id) as device_name,
    CASE 
        WHEN CAST(device_id AS INT) % 3 = 0 THEN '变压器'
        WHEN CAST(device_id AS INT) % 3 = 1 THEN '发电机'
        ELSE '配电设备'
    END as device_type,
    CASE 
        WHEN CAST(device_id AS INT) % 5 = 0 THEN '北京'
        WHEN CAST(device_id AS INT) % 5 = 1 THEN '上海'
        WHEN CAST(device_id AS INT) % 5 = 2 THEN '广州'
        WHEN CAST(device_id AS INT) % 5 = 3 THEN '深圳'
        ELSE '成都'
    END as location,
    voltage,
    `current`,
    temperature,
    power_output,
    efficiency,
    load_percentage,
    CASE 
        WHEN operational_mode = 'A' THEN 'NORMAL'
        WHEN operational_mode = 'B' THEN 'PEAK'
        WHEN operational_mode = 'C' THEN 'MAINTENANCE'
        ELSE 'EMERGENCY'
    END as operational_mode_desc,
    -- 计算性能指数
    CASE 
        WHEN efficiency > 0.95 AND temperature < 50 THEN 100.0
        WHEN efficiency > 0.90 AND temperature < 60 THEN 90.0
        WHEN efficiency > 0.85 AND temperature < 70 THEN 80.0
        WHEN efficiency > 0.80 AND temperature < 80 THEN 70.0
        ELSE 60.0
    END as performance_index,
    -- 计算健康评分
    (efficiency * 0.4 + (100 - temperature) * 0.3 + load_percentage * 0.3) as health_score,
    -- 异常标记
    CASE 
        WHEN temperature > 80 OR efficiency < 0.80 THEN 'HIGH_ANOMALY'
        WHEN temperature > 70 OR efficiency < 0.85 THEN 'MEDIUM_ANOMALY'
        WHEN temperature > 60 OR efficiency < 0.90 THEN 'LOW_ANOMALY'
        ELSE 'NORMAL'
    END as anomaly_flags
FROM fluss_catalog.fluss.ods_device_historical_raw;

-- ===============================================
-- 6. DWS层：时间窗口汇总
-- ===============================================

-- 🚀 DWS层：按小时聚合设备时序数据
INSERT INTO fluss_catalog.fluss.dws_device_timeseries_summary
SELECT 
    CONCAT(device_id, '_', CAST(DATE_FORMAT(record_time, 'yyyyMMddHH') AS STRING)) as summary_id,
    device_id,
    CAST(DATE_FORMAT(record_time, 'yyyy-MM-dd HH:00:00') AS TIMESTAMP(3)) as time_window,
    'HOURLY' as window_type,
    AVG(voltage) as avg_voltage,
    AVG(`current`) as avg_current,
    AVG(temperature) as avg_temperature,
    AVG(efficiency) as avg_efficiency,
    MAX(temperature) as max_temperature,
    MIN(efficiency) as min_efficiency,
    CAST(SUM(CASE WHEN anomaly_flags <> 'NORMAL' THEN 1 ELSE 0 END) AS BIGINT) as anomaly_count,
    CASE 
        WHEN AVG(efficiency) > 0.95 THEN 'IMPROVING'
        WHEN AVG(efficiency) > 0.90 THEN 'STABLE'
        WHEN AVG(efficiency) > 0.85 THEN 'DECLINING'
        ELSE 'CRITICAL'
    END as performance_trend
FROM fluss_catalog.fluss.dwd_device_timeseries_detail
GROUP BY device_id, DATE_FORMAT(record_time, 'yyyyMMddHH'), DATE_FORMAT(record_time, 'yyyy-MM-dd HH:00:00');

-- ===============================================
-- 7. ADS层：故障分析报表生成
-- ===============================================

-- 🚀 ADS层：生成设备故障分析报表
INSERT INTO fluss_catalog.fluss.ads_fault_analysis_report
SELECT 
    CONCAT('FAULT_ANALYSIS_', device_id, '_', CAST(DATE_FORMAT(time_window, 'yyyyMMddHH') AS STRING)) as analysis_id,
    device_id,
    '设备故障根因分析' as analysis_type,
    DATE_FORMAT(time_window, 'yyyy-MM-dd HH:00:00') as fault_time_period,
    avg_efficiency * 100 as pre_fault_performance,
    CASE 
        WHEN max_temperature > 80 AND min_efficiency < 0.80 THEN '高温+低效率双重故障'
        WHEN max_temperature > 80 THEN '设备过热故障'
        WHEN min_efficiency < 0.80 THEN '效率异常故障'
        WHEN anomaly_count > 10 THEN '频繁异常故障'
        ELSE '性能正常'
    END as fault_indicators,
    CASE 
        WHEN max_temperature > 80 AND min_efficiency < 0.80 THEN '散热系统故障且负载过重，建议立即停机检修'
        WHEN max_temperature > 80 THEN '散热系统异常，检查冷却装置和通风系统'
        WHEN min_efficiency < 0.80 THEN '设备老化或负载不匹配，需要校准或更换部件'
        WHEN anomaly_count > 10 THEN '监控系统异常或设备运行环境不稳定'
        ELSE '设备运行正常，继续监控'
    END as root_cause_analysis,
    CASE 
        WHEN performance_trend = 'CRITICAL' THEN 40.0
        WHEN performance_trend = 'DECLINING' THEN 20.0
        WHEN performance_trend = 'STABLE' THEN 5.0
        ELSE 0.0
    END as performance_degradation,
    CASE 
        WHEN performance_trend = 'CRITICAL' THEN '立即安排紧急维护，更换关键部件'
        WHEN performance_trend = 'DECLINING' THEN '计划预防性维护，优化运行参数'
        WHEN performance_trend = 'STABLE' THEN '保持现有维护计划'
        ELSE '可延长维护周期'
    END as recovery_suggestions,
    CASE 
        WHEN performance_trend = 'CRITICAL' THEN 'P0_URGENT'
        WHEN performance_trend = 'DECLINING' THEN 'P1_HIGH'
        WHEN performance_trend = 'STABLE' THEN 'P2_MEDIUM'
        ELSE 'P3_LOW'
    END as maintenance_priority,
    CURRENT_TIMESTAMP as analysis_time
FROM fluss_catalog.fluss.dws_device_timeseries_summary
WHERE anomaly_count > 0 OR performance_trend IN ('DECLINING', 'CRITICAL');

-- ===============================================
-- 8. 数据回流PostgreSQL
-- ===============================================

-- 🚀 最终回流：Fluss ADS层 → PostgreSQL
INSERT INTO postgres_fault_analysis_result
SELECT 
    analysis_id,
    device_id,
    analysis_type,
    fault_time_period,
    pre_fault_performance,
    fault_indicators,
    root_cause_analysis,
    performance_degradation,
    recovery_suggestions,
    maintenance_priority,
    analysis_time
FROM fluss_catalog.fluss.ads_fault_analysis_report;

-- ===============================================
-- 9. 时间旅行查询测试
-- ===============================================

/*
-- 查看PostgreSQL中的故障分析结果
SELECT * FROM postgres_fault_analysis_result ORDER BY analysis_time DESC LIMIT 10;

-- 时间旅行查询：查看特定时间点的设备状态
SELECT 
    device_id,
    record_time,
    voltage,
            `current`,
    temperature,
    efficiency,
    performance_index
FROM fluss_catalog.fluss.dwd_device_timeseries_detail 
FOR SYSTEM_TIME AS OF TIMESTAMP '2025-01-13 18:30:00'
WHERE device_id = '100001'
ORDER BY record_time DESC
LIMIT 10;

-- 对比故障前后的关键指标（时间旅行）
WITH pre_fault_data AS (
    SELECT 
        AVG(voltage) as avg_voltage_pre,
        AVG(`current`) as avg_current_pre,
        AVG(temperature) as avg_temperature_pre,
        AVG(efficiency) as avg_efficiency_pre
    FROM fluss_catalog.fluss.ods_device_historical_raw 
    FOR SYSTEM_TIME AS OF TIMESTAMP '2025-01-13 17:25:00'
    WHERE device_id = '100001'
),
post_fault_data AS (
    SELECT 
        AVG(voltage) as avg_voltage_post,
        AVG(`current`) as avg_current_post,
        AVG(temperature) as avg_temperature_post,
        AVG(efficiency) as avg_efficiency_post
    FROM fluss_catalog.fluss.ods_device_historical_raw 
    FOR SYSTEM_TIME AS OF TIMESTAMP '2025-01-13 17:35:00'
    WHERE device_id = '100001'
)
SELECT 
    'DEVICE_100001' as device_id,
    pre.avg_voltage_pre,
    post.avg_voltage_post,
    pre.avg_temperature_pre,
    post.avg_temperature_post,
    pre.avg_efficiency_pre,
    post.avg_efficiency_post
FROM pre_fault_data pre, post_fault_data post;

-- ===============================================
-- 🎯 增删改查监控测试 + 验证逻辑
-- ===============================================

-- 📊 【监控 1】时间旅行查询初始状态
SELECT '=== 🎯 场景3：时间旅行查询监控 ===' as monitor_title;

-- 查看各层数据量
SELECT '历史原始数据' as layer, COUNT(*) as record_count FROM fluss_catalog.fluss.ods_device_historical_raw
UNION ALL
SELECT '时序明细数据' as layer, COUNT(*) as record_count FROM fluss_catalog.fluss.dwd_device_timeseries_detail
UNION ALL
SELECT '时序汇总数据' as layer, COUNT(*) as record_count FROM fluss_catalog.fluss.dws_device_timeseries_summary
UNION ALL
SELECT '故障分析报表' as layer, COUNT(*) as record_count FROM fluss_catalog.fluss.ads_fault_analysis_report;

-- 📊 【监控 2】时序数据质量检查
SELECT '=== 📊 时序数据质量监控 ===' as monitor_title;

-- 检查时序数据分布
SELECT 
    device_type,
    COUNT(*) as record_count,
    AVG(efficiency) as avg_efficiency,
    AVG(health_score) as avg_health,
    MIN(record_time) as earliest_time,
    MAX(record_time) as latest_time
FROM fluss_catalog.fluss.dwd_device_timeseries_detail
GROUP BY device_type
ORDER BY record_count DESC;

-- 🔥 【测试 1】增加操作 - 插入测试时序数据
SELECT '=== 🔥 增加操作测试 ===' as test_title;

-- 创建PostgreSQL历史数据源连接（对应device_historical_stream CDC源）
CREATE TABLE postgres_source_device_historical_data (
    record_id STRING,
    device_id STRING,
    voltage DOUBLE,
    current_val DOUBLE,
    temperature DOUBLE,
    power_output DOUBLE,
    efficiency DOUBLE,
    status STRING,
    record_time TIMESTAMP(3),
    PRIMARY KEY (record_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-source:5432/sgcc_source_db',
    'table-name' = 'device_historical_data',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024'
);

-- 向PostgreSQL源插入历史测试数据（会被device_historical_stream CDC捕获）
INSERT INTO postgres_source_device_historical_data VALUES
('HIST_TEST001', 'TEST001', 235.5, 150.0, 45.2, 350.8, 0.96, 'ACTIVE', CURRENT_TIMESTAMP - INTERVAL '2' HOUR),
('HIST_TEST002', 'TEST002', 228.3, 125.5, 38.7, 280.3, 0.94, 'WARNING', CURRENT_TIMESTAMP - INTERVAL '1' HOUR),
('HIST_TEST003', 'TEST003', 240.1, 180.2, 52.1, 420.5, 0.92, 'NORMAL', CURRENT_TIMESTAMP);

-- 验证插入结果
SELECT 'ODS历史数据新增验证' as verification, COUNT(*) as new_records 
FROM fluss_catalog.fluss.ods_device_historical_raw 
WHERE record_id LIKE 'HIST_TEST%';

SELECT 'DWD时序数据新增验证' as verification, COUNT(*) as new_records 
FROM fluss_catalog.fluss.dwd_device_timeseries_detail 
WHERE device_id LIKE 'TEST%';

-- 🔄 【测试 2】更新操作测试
SELECT '=== 🔄 更新操作测试 ===' as test_title;

-- 更新前状态查询
SELECT 'UPDATE前时序状态' as status, device_id, efficiency, health_score
FROM fluss_catalog.fluss.dwd_device_timeseries_detail 
WHERE device_id = 'TEST001';

-- 在PostgreSQL源执行历史数据更新（会被device_historical_stream CDC捕获）
UPDATE postgres_source_device_historical_data 
SET efficiency = 0.99, status = 'OPTIMAL'
WHERE record_id = 'HIST_TEST001';

-- 验证更新通过CDC同步到Fluss
SELECT 'UPDATE后历史数据验证' as status, record_id, efficiency, status
FROM fluss_catalog.fluss.ods_device_historical_raw 
WHERE record_id = 'HIST_TEST001';

-- 历史数据更新验证
SELECT 'UPDATE历史数据验证' as status, record_id, efficiency, status
FROM fluss_catalog.fluss.ods_device_historical_raw 
WHERE record_id = 'HIST_TEST001';

-- ❌ 【测试 3】删除操作测试
SELECT '=== ❌ 删除操作测试 ===' as test_title;

-- 删除前统计
SELECT 'DELETE前历史数据统计' as phase, COUNT(*) as total_count 
FROM fluss_catalog.fluss.ods_device_historical_raw;

-- 在PostgreSQL源执行历史数据删除（会被device_historical_stream CDC捕获）
DELETE FROM postgres_source_device_historical_data 
WHERE record_id = 'HIST_TEST003';

-- 删除后验证
SELECT 'DELETE历史数据验证(应为0)' as verification, COUNT(*) as should_be_zero 
FROM fluss_catalog.fluss.ods_device_historical_raw 
WHERE record_id = 'HIST_TEST003';

SELECT 'DELETE时序数据验证(应为0)' as verification, COUNT(*) as should_be_zero 
FROM fluss_catalog.fluss.dwd_device_timeseries_detail 
WHERE device_id = 'TEST002';

-- 📈 【监控 3】时间旅行查询性能监控
SELECT '=== 📈 时间旅行查询性能监控 ===' as monitor_title;

-- 时间旅行查询测试（查看1小时前的数据状态）
SELECT 'FOR SYSTEM_TIME查询测试' as metric,
       COUNT(*) as historical_records,
       AVG(efficiency) as avg_efficiency_1h_ago
FROM fluss_catalog.fluss.dwd_device_timeseries_detail 
FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP - INTERVAL '1' HOUR;

-- 数据时间范围检查
SELECT '时序数据范围' as metric,
       COUNT(*) as total_records,
       MIN(record_time) as earliest_record,
       MAX(record_time) as latest_record,
       EXTRACT(HOUR FROM (MAX(record_time) - MIN(record_time))) as time_span_hours
FROM fluss_catalog.fluss.dwd_device_timeseries_detail;

-- 📋 【监控 4】最终结果验证
SELECT '=== 📋 最终结果验证 ===' as monitor_title;

-- 查看PostgreSQL中的故障分析结果
SELECT '故障分析结果' as result_type, 
       device_id, 
       fault_type, 
       fault_duration_hours,
       performance_impact,
       analysis_time
FROM postgres_fault_analysis_result 
ORDER BY analysis_time DESC 
LIMIT 10;

-- 🎯 【总结】场景3测试完成状态
SELECT '=== 🎯 场景3测试完成总结 ===' as summary_title;

SELECT 
    '时序数据完整性' as metric,
    CONCAT('历史:', hist_count, ' | 时序:', ts_count, ' | 汇总:', summary_count, ' | PostgreSQL:', pg_count) as layer_counts
FROM (
    SELECT 
        (SELECT COUNT(*) FROM fluss_catalog.fluss.ods_device_historical_raw) as hist_count,
        (SELECT COUNT(*) FROM fluss_catalog.fluss.dwd_device_timeseries_detail) as ts_count,
        (SELECT COUNT(*) FROM fluss_catalog.fluss.dws_device_timeseries_summary) as summary_count,
        (SELECT COUNT(*) FROM postgres_fault_analysis_result) as pg_count
);

-- ✅ 【验证】增删改查操作成功验证
SELECT '增删改查验证结果' as final_verification,
       CASE 
           WHEN (SELECT COUNT(*) FROM fluss_catalog.fluss.ods_device_historical_raw WHERE record_id = 'HIST_TEST001') = 1 THEN '✅ 增加成功'
           ELSE '❌ 增加失败'
       END as insert_status,
       CASE 
           WHEN (SELECT efficiency FROM fluss_catalog.fluss.dwd_device_timeseries_detail WHERE device_id = 'TEST001') = 0.99 THEN '✅ 更新成功'
           ELSE '❌ 更新失败'
       END as update_status,
       CASE 
           WHEN (SELECT COUNT(*) FROM fluss_catalog.fluss.ods_device_historical_raw WHERE record_id = 'HIST_TEST003') = 0 THEN '✅ 删除成功'
           ELSE '❌ 删除失败'
       END as delete_status; 