-- ========================================
-- 国网电力监控系统 - Fluss架构
-- 第五步：将ADS层数据写入PostgreSQL目标数据库
-- ========================================

-- 设置执行环境
SET 'execution.checkpointing.interval' = '60s';
SET 'table.exec.source.idle-timeout' = '30s';

-- 使用Fluss Catalog
USE CATALOG fluss_catalog;

-- ========================================
-- 创建PostgreSQL目标数据库连接器
-- ========================================

-- PostgreSQL JDBC连接器 - DWD层电力设备明细表
CREATE TABLE IF NOT EXISTS postgres_dwd_power_equipment (
    equipment_id BIGINT,
    equipment_name STRING,
    equipment_type STRING,
    equipment_type_code STRING,
    location STRING,
    province STRING,
    city STRING,
    district STRING,
    voltage_level STRING,
    voltage_level_code INT,
    capacity_mw DECIMAL(10,2),
    capacity_level STRING,
    manufacturer STRING,
    installation_date DATE,
    installation_year INT,
    last_maintenance_date DATE,
    maintenance_interval_days INT,
    status STRING,
    is_critical BOOLEAN,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (equipment_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'sgcc_dw.dwd_power_equipment',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'driver' = 'org.postgresql.Driver'
);

-- PostgreSQL JDBC连接器 - DWD层电力监控明细表
CREATE TABLE IF NOT EXISTS postgres_dwd_power_monitoring (
    monitoring_id BIGINT,
    equipment_id BIGINT,
    equipment_name STRING,
    equipment_type STRING,
    voltage_a DECIMAL(8,2),
    voltage_b DECIMAL(8,2),
    voltage_c DECIMAL(8,2),
    voltage_avg DECIMAL(8,2),
    voltage_imbalance DECIMAL(6,4),
    current_a DECIMAL(8,2),
    current_b DECIMAL(8,2),
    current_c DECIMAL(8,2),
    current_avg DECIMAL(8,2),
    current_imbalance DECIMAL(6,4),
    power_active DECIMAL(10,2),
    power_reactive DECIMAL(10,2),
    power_factor DECIMAL(6,4),
    power_apparent DECIMAL(10,2),
    frequency DECIMAL(6,3),
    frequency_deviation DECIMAL(6,4),
    temperature DECIMAL(6,2),
    humidity DECIMAL(5,2),
    monitoring_date DATE,
    monitoring_hour INT,
    monitoring_minute INT,
    is_peak_hour BOOLEAN,
    is_working_day BOOLEAN,
    monitoring_time TIMESTAMP(3),
    created_at TIMESTAMP(3),
    PRIMARY KEY (monitoring_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'sgcc_dw.dwd_power_monitoring',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'driver' = 'org.postgresql.Driver'
);

-- PostgreSQL JDBC连接器 - DWD层设备告警明细表
CREATE TABLE IF NOT EXISTS postgres_dwd_equipment_alarms (
    alarm_id BIGINT,
    equipment_id BIGINT,
    equipment_name STRING,
    equipment_type STRING,
    alarm_type STRING,
    alarm_level INT,
    alarm_level_name STRING,
    alarm_message STRING,
    alarm_code STRING,
    alarm_category STRING,
    is_resolved BOOLEAN,
    resolution_time_minutes INT,
    occurred_date DATE,
    occurred_hour INT,
    occurred_at TIMESTAMP(3),
    resolved_at TIMESTAMP(3),
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (alarm_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'sgcc_dw.dwd_equipment_alarms',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'driver' = 'org.postgresql.Driver'
);

-- PostgreSQL JDBC连接器 - DWS层设备日监控汇总表
CREATE TABLE IF NOT EXISTS postgres_dws_equipment_daily_summary (
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
    PRIMARY KEY (summary_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'sgcc_dw.dws_equipment_daily_summary',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'driver' = 'org.postgresql.Driver'
);

-- PostgreSQL JDBC连接器 - DWS层告警日汇总表
CREATE TABLE IF NOT EXISTS postgres_dws_alarm_daily_summary (
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
    PRIMARY KEY (summary_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'sgcc_dw.dws_alarm_daily_summary',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'driver' = 'org.postgresql.Driver'
);

-- PostgreSQL JDBC连接器 - ADS层实时监控大屏数据表
CREATE TABLE IF NOT EXISTS postgres_ads_realtime_dashboard (
    dashboard_id BIGINT,
    total_equipment INT,
    online_equipment INT,
    offline_equipment INT,
    maintenance_equipment INT,
    total_capacity_mw DECIMAL(12,2),
    active_capacity_mw DECIMAL(12,2),
    load_rate DECIMAL(6,4),
    total_active_alarms INT,
    critical_alarms INT,
    warning_alarms INT,
    avg_voltage_220kv DECIMAL(8,2),
    avg_voltage_110kv DECIMAL(8,2),
    avg_frequency DECIMAL(6,3),
    system_status STRING,
    last_updated TIMESTAMP(3),
    PRIMARY KEY (dashboard_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'sgcc_dw.ads_realtime_dashboard',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'driver' = 'org.postgresql.Driver'
);

-- PostgreSQL JDBC连接器 - ADS层电力质量分析表
CREATE TABLE IF NOT EXISTS postgres_ads_power_quality_analysis (
    analysis_id BIGINT,
    analysis_date DATE,
    voltage_qualification_rate DECIMAL(6,4),
    frequency_qualification_rate DECIMAL(6,4),
    power_factor_avg DECIMAL(6,4),
    voltage_imbalance_rate DECIMAL(6,4),
    current_imbalance_rate DECIMAL(6,4),
    harmonic_distortion_rate DECIMAL(6,4),
    power_quality_score DECIMAL(6,2),
    quality_level STRING,
    created_at TIMESTAMP(3),
    PRIMARY KEY (analysis_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'sgcc_dw.ads_power_quality_analysis',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'driver' = 'org.postgresql.Driver'
);

-- PostgreSQL JDBC连接器 - ADS层设备健康度评估表
CREATE TABLE IF NOT EXISTS postgres_ads_equipment_health_assessment (
    assessment_id BIGINT,
    equipment_id BIGINT,
    equipment_name STRING,
    equipment_type STRING,
    health_score DECIMAL(6,2),
    health_level STRING,
    reliability_score DECIMAL(6,2),
    maintenance_urgency INT,
    predicted_failure_probability DECIMAL(6,4),
    remaining_life_days INT,
    maintenance_recommendation STRING,
    assessment_date DATE,
    created_at TIMESTAMP(3),
    PRIMARY KEY (assessment_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'sgcc_dw.ads_equipment_health_assessment',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'driver' = 'org.postgresql.Driver'
);

-- ========================================
-- 数据同步任务：Fluss -> PostgreSQL
-- ========================================

-- 同步DWD层电力设备明细数据
INSERT INTO postgres_dwd_power_equipment
SELECT 
    equipment_id,
    equipment_name,
    equipment_type,
    equipment_type_code,
    location,
    province,
    city,
    district,
    voltage_level,
    voltage_level_code,
    capacity_mw,
    capacity_level,
    manufacturer,
    installation_date,
    installation_year,
    last_maintenance_date,
    maintenance_interval_days,
    status,
    is_critical,
    created_at,
    updated_at
FROM fluss_dwd_power_equipment;

-- 同步DWD层电力监控明细数据
INSERT INTO postgres_dwd_power_monitoring
SELECT 
    monitoring_id,
    equipment_id,
    equipment_name,
    equipment_type,
    voltage_a,
    voltage_b,
    voltage_c,
    voltage_avg,
    voltage_imbalance,
    current_a,
    current_b,
    current_c,
    current_avg,
    current_imbalance,
    power_active,
    power_reactive,
    power_factor,
    power_apparent,
    frequency,
    frequency_deviation,
    temperature,
    humidity,
    monitoring_date,
    monitoring_hour,
    monitoring_minute,
    is_peak_hour,
    is_working_day,
    monitoring_time,
    created_at
FROM fluss_dwd_power_monitoring
WHERE monitoring_date >= CURRENT_DATE - INTERVAL '1' DAY;

-- 同步DWD层设备告警明细数据
INSERT INTO postgres_dwd_equipment_alarms
SELECT 
    alarm_id,
    equipment_id,
    equipment_name,
    equipment_type,
    alarm_type,
    alarm_level,
    alarm_level_name,
    alarm_message,
    alarm_code,
    alarm_category,
    is_resolved,
    resolution_time_minutes,
    occurred_date,
    occurred_hour,
    occurred_at,
    resolved_at,
    created_at,
    updated_at
FROM fluss_dwd_equipment_alarms
WHERE occurred_date >= CURRENT_DATE - INTERVAL '7' DAY;

-- 同步DWS层设备日监控汇总数据
INSERT INTO postgres_dws_equipment_daily_summary
SELECT 
    summary_id,
    equipment_id,
    equipment_name,
    equipment_type,
    summary_date,
    voltage_avg,
    voltage_max,
    voltage_min,
    voltage_std,
    current_avg,
    current_max,
    current_min,
    power_active_avg,
    power_active_max,
    power_active_min,
    power_factor_avg,
    frequency_avg,
    temperature_avg,
    temperature_max,
    monitoring_count,
    abnormal_count,
    uptime_hours,
    utilization_rate,
    created_at
FROM fluss_dws_equipment_daily_summary
WHERE summary_date >= CURRENT_DATE - INTERVAL '7' DAY;

-- 同步DWS层告警日汇总数据
INSERT INTO postgres_dws_alarm_daily_summary
SELECT 
    summary_id,
    summary_date,
    equipment_type,
    alarm_type,
    alarm_level,
    total_alarms,
    resolved_alarms,
    unresolved_alarms,
    avg_resolution_time_minutes,
    max_resolution_time_minutes,
    critical_alarms,
    warning_alarms,
    info_alarms,
    created_at
FROM fluss_dws_alarm_daily_summary
WHERE summary_date >= CURRENT_DATE - INTERVAL '7' DAY;

-- 同步ADS层实时监控大屏数据
INSERT INTO postgres_ads_realtime_dashboard
SELECT 
    dashboard_id,
    total_equipment,
    online_equipment,
    offline_equipment,
    maintenance_equipment,
    total_capacity_mw,
    active_capacity_mw,
    load_rate,
    total_active_alarms,
    critical_alarms,
    warning_alarms,
    avg_voltage_220kv,
    avg_voltage_110kv,
    avg_frequency,
    system_status,
    last_updated
FROM fluss_ads_realtime_dashboard;

-- 同步ADS层电力质量分析数据
INSERT INTO postgres_ads_power_quality_analysis
SELECT 
    analysis_id,
    analysis_date,
    voltage_qualification_rate,
    frequency_qualification_rate,
    power_factor_avg,
    voltage_imbalance_rate,
    current_imbalance_rate,
    harmonic_distortion_rate,
    power_quality_score,
    quality_level,
    created_at
FROM fluss_ads_power_quality_analysis
WHERE analysis_date >= CURRENT_DATE - INTERVAL '30' DAY;

-- 同步ADS层设备健康度评估数据
INSERT INTO postgres_ads_equipment_health_assessment
SELECT 
    assessment_id,
    equipment_id,
    equipment_name,
    equipment_type,
    health_score,
    health_level,
    reliability_score,
    maintenance_urgency,
    predicted_failure_probability,
    remaining_life_days,
    maintenance_recommendation,
    assessment_date,
    created_at
FROM fluss_ads_equipment_health_assessment
WHERE assessment_date >= CURRENT_DATE - INTERVAL '30' DAY;

-- ========================================
-- 创建数据同步状态监控表
-- ========================================

-- PostgreSQL JDBC连接器 - 数据同步状态表
CREATE TABLE IF NOT EXISTS postgres_data_sync_status (
    sync_id BIGINT,
    table_name STRING,
    sync_type STRING,
    total_records BIGINT,
    success_records BIGINT,
    error_records BIGINT,
    sync_start_time TIMESTAMP(3),
    sync_end_time TIMESTAMP(3),
    sync_status STRING,
    error_message STRING,
    PRIMARY KEY (sync_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'sgcc_dw.data_sync_status',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'driver' = 'org.postgresql.Driver'
);

-- 插入同步状态记录
INSERT INTO postgres_data_sync_status
VALUES (
    CAST(UNIX_TIMESTAMP() AS BIGINT),
    'fluss_to_postgres_sync',
    'FULL_SYNC',
    0,
    0,
    0,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    'COMPLETED',
    NULL
);

-- ========================================
-- 性能优化和监控查询
-- ========================================

-- 创建用于监控的视图连接器
CREATE TABLE IF NOT EXISTS postgres_monitoring_summary (
    summary_time TIMESTAMP(3),
    table_name STRING,
    record_count BIGINT,
    last_update_time TIMESTAMP(3),
    data_freshness_minutes INT,
    sync_status STRING,
    PRIMARY KEY (summary_time, table_name) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'sgcc_dw.monitoring_summary',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'driver' = 'org.postgresql.Driver'
);

-- 插入监控汇总信息
INSERT INTO postgres_monitoring_summary
SELECT 
    CURRENT_TIMESTAMP as summary_time,
    'dwd_power_equipment' as table_name,
    COUNT(*) as record_count,
    MAX(updated_at) as last_update_time,
    CAST(TIMESTAMPDIFF(MINUTE, MAX(updated_at), CURRENT_TIMESTAMP) AS INT) as data_freshness_minutes,
    CASE 
        WHEN TIMESTAMPDIFF(MINUTE, MAX(updated_at), CURRENT_TIMESTAMP) <= 10 THEN 'FRESH'
        WHEN TIMESTAMPDIFF(MINUTE, MAX(updated_at), CURRENT_TIMESTAMP) <= 60 THEN 'NORMAL'
        ELSE 'STALE'
    END as sync_status
FROM fluss_dwd_power_equipment;

INSERT INTO postgres_monitoring_summary
SELECT 
    CURRENT_TIMESTAMP as summary_time,
    'ads_realtime_dashboard' as table_name,
    COUNT(*) as record_count,
    MAX(last_updated) as last_update_time,
    CAST(TIMESTAMPDIFF(MINUTE, MAX(last_updated), CURRENT_TIMESTAMP) AS INT) as data_freshness_minutes,
    CASE 
        WHEN TIMESTAMPDIFF(MINUTE, MAX(last_updated), CURRENT_TIMESTAMP) <= 5 THEN 'FRESH'
        WHEN TIMESTAMPDIFF(MINUTE, MAX(last_updated), CURRENT_TIMESTAMP) <= 30 THEN 'NORMAL'
        ELSE 'STALE'
    END as sync_status
FROM fluss_ads_realtime_dashboard; 