-- ========================================
-- 国网电力监控系统 - Fluss架构
-- 第三步：DWS层数据汇总和聚合
-- ========================================

-- 设置执行环境
SET 'execution.checkpointing.interval' = '60s';
SET 'table.exec.source.idle-timeout' = '30s';

-- 使用Fluss Catalog
USE CATALOG fluss_catalog;

-- ========================================
-- 创建DWS层Fluss表
-- ========================================

-- DWS层设备日监控汇总表
CREATE TABLE IF NOT EXISTS fluss_dws_equipment_daily_summary (
    summary_id BIGINT,
    equipment_id BIGINT,
    equipment_name STRING,
    equipment_type STRING,
    summary_date DATE,
    voltage_avg DECIMAL(8,2),
    voltage_max DECIMAL(8,2),
    voltage_min DECIMAL(8,2),
    voltage_std DECIMAL(8,4),
    current_avg DECIMAL(8,2),
    current_max DECIMAL(8,2),
    current_min DECIMAL(8,2),
    power_active_avg DECIMAL(10,2),
    power_active_max DECIMAL(10,2),
    power_active_min DECIMAL(10,2),
    power_factor_avg DECIMAL(6,4),
    frequency_avg DECIMAL(6,3),
    temperature_avg DECIMAL(6,2),
    temperature_max DECIMAL(6,2),
    monitoring_count INT,
    abnormal_count INT,
    uptime_hours DECIMAL(6,2),
    utilization_rate DECIMAL(6,4),
    created_at TIMESTAMP(3),
    process_time TIMESTAMP(3) METADATA FROM 'timestamp',
    PRIMARY KEY (summary_id) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123',
    'table-name' = 'dws_equipment_daily_summary',
    'bucket' = '4',
    'value.format' = 'json',
    'fluss.table.datalake.enabled' = 'true',
    'fluss.table.log.retention' = '30d',
    'fluss.table.compaction.enabled' = 'true'
);

-- DWS层告警日汇总表
CREATE TABLE IF NOT EXISTS fluss_dws_alarm_daily_summary (
    summary_id BIGINT,
    summary_date DATE,
    equipment_type STRING,
    alarm_type STRING,
    alarm_level INT,
    total_alarms INT,
    resolved_alarms INT,
    unresolved_alarms INT,
    avg_resolution_time_minutes DECIMAL(8,2),
    max_resolution_time_minutes INT,
    critical_alarms INT,
    warning_alarms INT,
    info_alarms INT,
    created_at TIMESTAMP(3),
    process_time TIMESTAMP(3) METADATA FROM 'timestamp',
    PRIMARY KEY (summary_id) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123',
    'table-name' = 'dws_alarm_daily_summary',
    'bucket' = '4',
    'value.format' = 'json',
    'fluss.table.datalake.enabled' = 'true',
    'fluss.table.log.retention' = '30d',
    'fluss.table.compaction.enabled' = 'true'
);

-- DWS层设备小时汇总表
CREATE TABLE IF NOT EXISTS fluss_dws_equipment_hourly_summary (
    summary_id BIGINT,
    equipment_id BIGINT,
    equipment_name STRING,
    equipment_type STRING,
    summary_date DATE,
    summary_hour INT,
    voltage_avg DECIMAL(8,2),
    current_avg DECIMAL(8,2),
    power_active_avg DECIMAL(10,2),
    power_factor_avg DECIMAL(6,4),
    frequency_avg DECIMAL(6,3),
    temperature_avg DECIMAL(6,2),
    monitoring_count INT,
    load_rate DECIMAL(6,4),
    efficiency_score DECIMAL(6,4),
    created_at TIMESTAMP(3),
    process_time TIMESTAMP(3) METADATA FROM 'timestamp',
    PRIMARY KEY (summary_id) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123',
    'table-name' = 'dws_equipment_hourly_summary',
    'bucket' = '8',
    'value.format' = 'json',
    'fluss.table.datalake.enabled' = 'true',
    'fluss.table.log.retention' = '7d',
    'fluss.table.compaction.enabled' = 'true'
);

-- DWS层电力质量指标表
CREATE TABLE IF NOT EXISTS fluss_dws_power_quality_metrics (
    metric_id BIGINT,
    summary_date DATE,
    summary_hour INT,
    voltage_qualification_rate DECIMAL(6,4),
    frequency_qualification_rate DECIMAL(6,4),
    power_factor_avg DECIMAL(6,4),
    voltage_imbalance_rate DECIMAL(6,4),
    current_imbalance_rate DECIMAL(6,4),
    total_equipment_count INT,
    online_equipment_count INT,
    power_quality_score DECIMAL(6,2),
    created_at TIMESTAMP(3),
    process_time TIMESTAMP(3) METADATA FROM 'timestamp',
    PRIMARY KEY (metric_id) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123',
    'table-name' = 'dws_power_quality_metrics',
    'bucket' = '2',
    'value.format' = 'json',
    'fluss.table.datalake.enabled' = 'true',
    'fluss.table.log.retention' = '30d',
    'fluss.table.compaction.enabled' = 'true'
);

-- ========================================
-- DWS层数据处理任务
-- ========================================

-- 设备日监控汇总
INSERT INTO fluss_dws_equipment_daily_summary
SELECT 
    CAST(CONCAT(CAST(equipment_id AS STRING), DATE_FORMAT(monitoring_date, 'yyyyMMdd')) AS BIGINT) as summary_id,
    equipment_id,
    equipment_name,
    equipment_type,
    monitoring_date as summary_date,
    AVG(voltage_avg) as voltage_avg,
    MAX(voltage_avg) as voltage_max,
    MIN(voltage_avg) as voltage_min,
    STDDEV(voltage_avg) as voltage_std,
    AVG(current_avg) as current_avg,
    MAX(current_avg) as current_max,
    MIN(current_avg) as current_min,
    AVG(power_active) as power_active_avg,
    MAX(power_active) as power_active_max,
    MIN(power_active) as power_active_min,
    AVG(power_factor) as power_factor_avg,
    AVG(frequency) as frequency_avg,
    AVG(temperature) as temperature_avg,
    MAX(temperature) as temperature_max,
    COUNT(*) as monitoring_count,
    COUNT(CASE WHEN voltage_imbalance > 0.02 OR current_imbalance > 0.02 THEN 1 END) as abnormal_count,
    COUNT(DISTINCT monitoring_hour) as uptime_hours,
    AVG(power_active) / MAX(CASE WHEN equipment_type = '变压器' THEN 300 
                               WHEN equipment_type = '发电机' THEN 200 
                               ELSE 150 END) as utilization_rate,
    CURRENT_TIMESTAMP as created_at
FROM fluss_dwd_power_monitoring
WHERE monitoring_date >= CURRENT_DATE - INTERVAL '1' DAY
GROUP BY equipment_id, equipment_name, equipment_type, monitoring_date;

-- 告警日汇总
INSERT INTO fluss_dws_alarm_daily_summary
SELECT 
    CAST(CONCAT(DATE_FORMAT(occurred_date, 'yyyyMMdd'), 
                CAST(alarm_level AS STRING), 
                HASH_CODE(CONCAT(equipment_type, alarm_type))) AS BIGINT) as summary_id,
    occurred_date as summary_date,
    equipment_type,
    alarm_type,
    alarm_level,
    COUNT(*) as total_alarms,
    COUNT(CASE WHEN is_resolved = TRUE THEN 1 END) as resolved_alarms,
    COUNT(CASE WHEN is_resolved = FALSE THEN 1 END) as unresolved_alarms,
    AVG(resolution_time_minutes) as avg_resolution_time_minutes,
    MAX(resolution_time_minutes) as max_resolution_time_minutes,
    COUNT(CASE WHEN alarm_level >= 4 THEN 1 END) as critical_alarms,
    COUNT(CASE WHEN alarm_level = 2 OR alarm_level = 3 THEN 1 END) as warning_alarms,
    COUNT(CASE WHEN alarm_level = 1 THEN 1 END) as info_alarms,
    CURRENT_TIMESTAMP as created_at
FROM fluss_dwd_equipment_alarms
WHERE occurred_date >= CURRENT_DATE - INTERVAL '1' DAY
GROUP BY occurred_date, equipment_type, alarm_type, alarm_level;

-- 设备小时汇总
INSERT INTO fluss_dws_equipment_hourly_summary
SELECT 
    CAST(CONCAT(CAST(equipment_id AS STRING), 
                DATE_FORMAT(monitoring_date, 'yyyyMMdd'), 
                LPAD(CAST(monitoring_hour AS STRING), 2, '0')) AS BIGINT) as summary_id,
    equipment_id,
    equipment_name,
    equipment_type,
    monitoring_date as summary_date,
    monitoring_hour as summary_hour,
    AVG(voltage_avg) as voltage_avg,
    AVG(current_avg) as current_avg,
    AVG(power_active) as power_active_avg,
    AVG(power_factor) as power_factor_avg,
    AVG(frequency) as frequency_avg,
    AVG(temperature) as temperature_avg,
    COUNT(*) as monitoring_count,
    AVG(power_active) / MAX(CASE WHEN equipment_type = '变压器' THEN 300 
                               WHEN equipment_type = '发电机' THEN 200 
                               ELSE 150 END) as load_rate,
    -- 效率评分：综合考虑功率因数、电压稳定性、温度等因素
    (AVG(power_factor) * 0.4 + 
     (1 - AVG(voltage_imbalance)) * 0.3 + 
     (1 - GREATEST(0, (AVG(temperature) - 40) / 20)) * 0.3) as efficiency_score,
    CURRENT_TIMESTAMP as created_at
FROM fluss_dwd_power_monitoring
WHERE monitoring_date >= CURRENT_DATE - INTERVAL '1' DAY
GROUP BY equipment_id, equipment_name, equipment_type, monitoring_date, monitoring_hour;

-- 电力质量指标汇总
INSERT INTO fluss_dws_power_quality_metrics
SELECT 
    CAST(CONCAT(DATE_FORMAT(monitoring_date, 'yyyyMMdd'), 
                LPAD(CAST(monitoring_hour AS STRING), 2, '0')) AS BIGINT) as metric_id,
    monitoring_date as summary_date,
    monitoring_hour as summary_hour,
    -- 电压合格率（±5%范围内）
    AVG(CASE WHEN ABS(voltage_avg - 220) / 220 <= 0.05 THEN 1.0 ELSE 0.0 END) as voltage_qualification_rate,
    -- 频率合格率（±0.2Hz范围内）
    AVG(CASE WHEN ABS(frequency - 50) <= 0.2 THEN 1.0 ELSE 0.0 END) as frequency_qualification_rate,
    AVG(power_factor) as power_factor_avg,
    AVG(voltage_imbalance) as voltage_imbalance_rate,
    AVG(current_imbalance) as current_imbalance_rate,
    COUNT(DISTINCT equipment_id) as total_equipment_count,
    COUNT(DISTINCT CASE WHEN monitoring_time >= CURRENT_TIMESTAMP - INTERVAL '10' MINUTE 
                        THEN equipment_id END) as online_equipment_count,
    -- 电力质量综合评分
    (AVG(CASE WHEN ABS(voltage_avg - 220) / 220 <= 0.05 THEN 1.0 ELSE 0.0 END) * 0.3 +
     AVG(CASE WHEN ABS(frequency - 50) <= 0.2 THEN 1.0 ELSE 0.0 END) * 0.3 +
     AVG(power_factor) * 0.2 +
     (1 - AVG(voltage_imbalance)) * 0.1 +
     (1 - AVG(current_imbalance)) * 0.1) * 100 as power_quality_score,
    CURRENT_TIMESTAMP as created_at
FROM fluss_dwd_power_monitoring
WHERE monitoring_date >= CURRENT_DATE - INTERVAL '1' DAY
GROUP BY monitoring_date, monitoring_hour;

-- ========================================
-- 实时窗口聚合
-- ========================================

-- 创建实时监控窗口汇总表
CREATE TABLE IF NOT EXISTS fluss_dws_realtime_monitoring (
    window_id BIGINT,
    equipment_id BIGINT,
    equipment_name STRING,
    equipment_type STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    voltage_avg DECIMAL(8,2),
    current_avg DECIMAL(8,2),
    power_active_avg DECIMAL(10,2),
    power_factor_avg DECIMAL(6,4),
    frequency_avg DECIMAL(6,3),
    temperature_avg DECIMAL(6,2),
    monitoring_count INT,
    abnormal_count INT,
    alert_level STRING,
    created_at TIMESTAMP(3),
    process_time TIMESTAMP(3) METADATA FROM 'timestamp',
    PRIMARY KEY (window_id) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123',
    'table-name' = 'dws_realtime_monitoring',
    'bucket' = '8',
    'value.format' = 'json',
    'fluss.table.datalake.enabled' = 'false',
    'fluss.table.log.retention' = '1d',
    'fluss.table.compaction.enabled' = 'false'
);

-- 实时5分钟窗口汇总
INSERT INTO fluss_dws_realtime_monitoring
SELECT 
    CAST(CONCAT(CAST(equipment_id AS STRING), 
                CAST(UNIX_TIMESTAMP(window_start) AS STRING)) AS BIGINT) as window_id,
    equipment_id,
    equipment_name,
    equipment_type,
    window_start,
    window_end,
    AVG(voltage_avg) as voltage_avg,
    AVG(current_avg) as current_avg,
    AVG(power_active) as power_active_avg,
    AVG(power_factor) as power_factor_avg,
    AVG(frequency) as frequency_avg,
    AVG(temperature) as temperature_avg,
    COUNT(*) as monitoring_count,
    COUNT(CASE WHEN voltage_imbalance > 0.02 OR current_imbalance > 0.02 THEN 1 END) as abnormal_count,
    CASE 
        WHEN COUNT(CASE WHEN voltage_imbalance > 0.05 OR current_imbalance > 0.05 THEN 1 END) > 0 THEN 'CRITICAL'
        WHEN COUNT(CASE WHEN voltage_imbalance > 0.02 OR current_imbalance > 0.02 THEN 1 END) > 0 THEN 'WARNING'
        ELSE 'NORMAL'
    END as alert_level,
    CURRENT_TIMESTAMP as created_at
FROM TABLE(
    TUMBLE(TABLE fluss_dwd_power_monitoring, DESCRIPTOR(monitoring_time), INTERVAL '5' MINUTE)
)
GROUP BY equipment_id, equipment_name, equipment_type, window_start, window_end;

-- ========================================
-- 设备性能排名
-- ========================================

-- 创建设备性能排名表
CREATE TABLE IF NOT EXISTS fluss_dws_equipment_ranking (
    ranking_id BIGINT,
    ranking_date DATE,
    ranking_type STRING,
    equipment_id BIGINT,
    equipment_name STRING,
    equipment_type STRING,
    performance_score DECIMAL(6,2),
    ranking_position INT,
    total_equipment INT,
    created_at TIMESTAMP(3),
    process_time TIMESTAMP(3) METADATA FROM 'timestamp',
    PRIMARY KEY (ranking_id) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123',
    'table-name' = 'dws_equipment_ranking',
    'bucket' = '4',
    'value.format' = 'json',
    'fluss.table.datalake.enabled' = 'true',
    'fluss.table.log.retention' = '30d',
    'fluss.table.compaction.enabled' = 'true'
);

-- 设备效率排名
INSERT INTO fluss_dws_equipment_ranking
SELECT 
    CAST(CONCAT(DATE_FORMAT(summary_date, 'yyyyMMdd'), 
                'EFFICIENCY', 
                CAST(equipment_id AS STRING)) AS BIGINT) as ranking_id,
    summary_date as ranking_date,
    'EFFICIENCY' as ranking_type,
    equipment_id,
    equipment_name,
    equipment_type,
    AVG(efficiency_score) * 100 as performance_score,
    ROW_NUMBER() OVER (PARTITION BY summary_date ORDER BY AVG(efficiency_score) DESC) as ranking_position,
    COUNT(*) OVER (PARTITION BY summary_date) as total_equipment,
    CURRENT_TIMESTAMP as created_at
FROM fluss_dws_equipment_hourly_summary
WHERE summary_date >= CURRENT_DATE - INTERVAL '1' DAY
GROUP BY summary_date, equipment_id, equipment_name, equipment_type;

-- 设备负载率排名
INSERT INTO fluss_dws_equipment_ranking
SELECT 
    CAST(CONCAT(DATE_FORMAT(summary_date, 'yyyyMMdd'), 
                'LOADRATE', 
                CAST(equipment_id AS STRING)) AS BIGINT) as ranking_id,
    summary_date as ranking_date,
    'LOAD_RATE' as ranking_type,
    equipment_id,
    equipment_name,
    equipment_type,
    AVG(utilization_rate) * 100 as performance_score,
    ROW_NUMBER() OVER (PARTITION BY summary_date ORDER BY AVG(utilization_rate) DESC) as ranking_position,
    COUNT(*) OVER (PARTITION BY summary_date) as total_equipment,
    CURRENT_TIMESTAMP as created_at
FROM fluss_dws_equipment_daily_summary
WHERE summary_date >= CURRENT_DATE - INTERVAL '1' DAY
GROUP BY summary_date, equipment_id, equipment_name, equipment_type;

-- ========================================
-- 数据质量检查
-- ========================================

-- DWS层数据质量检查
INSERT INTO fluss_dwd_quality_check
SELECT 
    CAST(CONCAT('DWS', CAST(UNIX_TIMESTAMP() AS STRING)) AS BIGINT) as check_id,
    'dws_equipment_daily_summary' as table_name,
    'completeness_check' as check_type,
    CASE 
        WHEN COUNT(CASE WHEN monitoring_count = 0 THEN 1 END) = 0 
        THEN 'PASS'
        ELSE 'FAIL'
    END as check_result,
    COUNT(CASE WHEN monitoring_count = 0 THEN 1 END) as error_count,
    COUNT(*) as total_count,
    CURRENT_TIMESTAMP as check_time
FROM fluss_dws_equipment_daily_summary
WHERE summary_date >= CURRENT_DATE - INTERVAL '1' DAY; 