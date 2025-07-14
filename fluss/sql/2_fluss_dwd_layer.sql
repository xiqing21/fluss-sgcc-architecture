-- ========================================
-- 国网电力监控系统 - Fluss架构
-- 第二步：DWD层数据清洗和增强
-- ========================================

-- 设置执行环境
SET 'execution.checkpointing.interval' = '60s';
SET 'table.exec.source.idle-timeout' = '30s';

-- 使用Fluss Catalog
USE CATALOG fluss_catalog;

-- ========================================
-- 创建DWD层Fluss表
-- ========================================

-- DWD层电力设备明细表
CREATE TABLE IF NOT EXISTS fluss_dwd_power_equipment (
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
    process_time TIMESTAMP(3) METADATA FROM 'timestamp',
    PRIMARY KEY (equipment_id) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123',
    'table-name' = 'dwd_power_equipment',
    'bucket' = '4',
    'value.format' = 'json',
    'fluss.table.datalake.enabled' = 'true',
    'fluss.table.log.retention' = '7d',
    'fluss.table.compaction.enabled' = 'true'
);

-- DWD层电力监控明细表
CREATE TABLE IF NOT EXISTS fluss_dwd_power_monitoring (
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
    process_time TIMESTAMP(3) METADATA FROM 'timestamp',
    PRIMARY KEY (monitoring_id) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123',
    'table-name' = 'dwd_power_monitoring',
    'bucket' = '8',
    'value.format' = 'json',
    'fluss.table.datalake.enabled' = 'true',
    'fluss.table.log.retention' = '3d',
    'fluss.table.compaction.enabled' = 'false'
);

-- DWD层设备告警明细表
CREATE TABLE IF NOT EXISTS fluss_dwd_equipment_alarms (
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
    process_time TIMESTAMP(3) METADATA FROM 'timestamp',
    PRIMARY KEY (alarm_id) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123',
    'table-name' = 'dwd_equipment_alarms',
    'bucket' = '4',
    'value.format' = 'json',
    'fluss.table.datalake.enabled' = 'true',
    'fluss.table.log.retention' = '30d',
    'fluss.table.compaction.enabled' = 'true'
);

-- ========================================
-- DWD层数据处理任务
-- ========================================

-- 处理电力设备数据（数据清洗和增强）
INSERT INTO fluss_dwd_power_equipment
SELECT 
    equipment_id,
    equipment_name,
    equipment_type,
    -- 设备类型编码
    CASE 
        WHEN equipment_type = '变压器' THEN 'TRF'
        WHEN equipment_type = '发电机' THEN 'GEN'
        WHEN equipment_type = '输电线路' THEN 'LINE'
        WHEN equipment_type = '配电设备' THEN 'DIST'
        ELSE 'OTHER'
    END as equipment_type_code,
    location,
    -- 解析地址信息
    CASE 
        WHEN location LIKE '%北京%' THEN '北京'
        WHEN location LIKE '%上海%' THEN '上海'
        WHEN location LIKE '%广州%' THEN '广东'
        WHEN location LIKE '%深圳%' THEN '广东'
        WHEN location LIKE '%杭州%' THEN '浙江'
        ELSE '其他'
    END as province,
    CASE 
        WHEN location LIKE '%朝阳%' THEN '朝阳区'
        WHEN location LIKE '%东城%' THEN '东城区'
        WHEN location LIKE '%海淀%' THEN '海淀区'
        WHEN location LIKE '%丰台%' THEN '丰台区'
        ELSE SUBSTRING(location, LOCATE('区', location) - 2, 3)
    END as city,
    CASE 
        WHEN location LIKE '%区%' THEN SUBSTRING(location, LOCATE('区', location) - 2, 3)
        ELSE NULL
    END as district,
    voltage_level,
    -- 电压等级编码
    CASE 
        WHEN voltage_level = '500kV' THEN 500
        WHEN voltage_level = '220kV' THEN 220
        WHEN voltage_level = '110kV' THEN 110
        WHEN voltage_level = '35kV' THEN 35
        WHEN voltage_level = '10kV' THEN 10
        ELSE 0
    END as voltage_level_code,
    capacity_mw,
    -- 容量级别
    CASE 
        WHEN capacity_mw >= 300 THEN '大型'
        WHEN capacity_mw >= 100 THEN '中型'
        WHEN capacity_mw >= 10 THEN '小型'
        ELSE '微型'
    END as capacity_level,
    manufacturer,
    installation_date,
    YEAR(installation_date) as installation_year,
    last_maintenance_date,
    DATEDIFF(CURRENT_DATE, last_maintenance_date) as maintenance_interval_days,
    status,
    -- 判断是否为关键设备
    CASE 
        WHEN capacity_mw >= 200 OR voltage_level IN ('500kV', '220kV') THEN TRUE
        ELSE FALSE
    END as is_critical,
    created_at,
    updated_at
FROM fluss_ods_power_equipment;

-- 处理实时监控数据（计算派生字段）
INSERT INTO fluss_dwd_power_monitoring
SELECT 
    m.monitoring_id,
    m.equipment_id,
    e.equipment_name,
    e.equipment_type,
    m.voltage_a,
    m.voltage_b,
    m.voltage_c,
    -- 计算平均电压
    (m.voltage_a + m.voltage_b + m.voltage_c) / 3 as voltage_avg,
    -- 计算电压不平衡度
    GREATEST(
        ABS(m.voltage_a - (m.voltage_a + m.voltage_b + m.voltage_c) / 3),
        ABS(m.voltage_b - (m.voltage_a + m.voltage_b + m.voltage_c) / 3),
        ABS(m.voltage_c - (m.voltage_a + m.voltage_b + m.voltage_c) / 3)
    ) / ((m.voltage_a + m.voltage_b + m.voltage_c) / 3) as voltage_imbalance,
    m.current_a,
    m.current_b,
    m.current_c,
    -- 计算平均电流
    (m.current_a + m.current_b + m.current_c) / 3 as current_avg,
    -- 计算电流不平衡度
    GREATEST(
        ABS(m.current_a - (m.current_a + m.current_b + m.current_c) / 3),
        ABS(m.current_b - (m.current_a + m.current_b + m.current_c) / 3),
        ABS(m.current_c - (m.current_a + m.current_b + m.current_c) / 3)
    ) / ((m.current_a + m.current_b + m.current_c) / 3) as current_imbalance,
    m.power_active,
    m.power_reactive,
    -- 计算功率因数
    CASE 
        WHEN SQRT(m.power_active * m.power_active + m.power_reactive * m.power_reactive) > 0 
        THEN m.power_active / SQRT(m.power_active * m.power_active + m.power_reactive * m.power_reactive)
        ELSE 0
    END as power_factor,
    -- 计算视在功率
    SQRT(m.power_active * m.power_active + m.power_reactive * m.power_reactive) as power_apparent,
    m.frequency,
    -- 计算频率偏差（以50Hz为基准）
    m.frequency - 50.0 as frequency_deviation,
    m.temperature,
    m.humidity,
    -- 时间维度字段
    DATE(m.monitoring_time) as monitoring_date,
    EXTRACT(HOUR FROM m.monitoring_time) as monitoring_hour,
    EXTRACT(MINUTE FROM m.monitoring_time) as monitoring_minute,
    -- 判断是否为用电高峰期（8-11点，18-21点）
    CASE 
        WHEN EXTRACT(HOUR FROM m.monitoring_time) BETWEEN 8 AND 11 
             OR EXTRACT(HOUR FROM m.monitoring_time) BETWEEN 18 AND 21 
        THEN TRUE
        ELSE FALSE
    END as is_peak_hour,
    -- 判断是否为工作日（周一到周五）
    CASE 
        WHEN EXTRACT(DAY_OF_WEEK FROM m.monitoring_time) BETWEEN 2 AND 6 
        THEN TRUE
        ELSE FALSE
    END as is_working_day,
    m.monitoring_time,
    m.created_at
FROM fluss_ods_power_monitoring m
LEFT JOIN fluss_ods_power_equipment e ON m.equipment_id = e.equipment_id;

-- 处理设备告警数据（增强告警信息）
INSERT INTO fluss_dwd_equipment_alarms
SELECT 
    a.alarm_id,
    a.equipment_id,
    e.equipment_name,
    e.equipment_type,
    a.alarm_type,
    a.alarm_level,
    -- 告警级别名称
    CASE 
        WHEN a.alarm_level = 1 THEN 'INFO'
        WHEN a.alarm_level = 2 THEN 'WARNING'
        WHEN a.alarm_level = 3 THEN 'MINOR'
        WHEN a.alarm_level = 4 THEN 'MAJOR'
        WHEN a.alarm_level = 5 THEN 'CRITICAL'
        ELSE 'UNKNOWN'
    END as alarm_level_name,
    a.alarm_message,
    a.alarm_code,
    -- 告警分类
    CASE 
        WHEN a.alarm_code LIKE '%TEMP%' THEN '温度告警'
        WHEN a.alarm_code LIKE '%CURRENT%' THEN '电流告警'
        WHEN a.alarm_code LIKE '%VOLTAGE%' THEN '电压告警'
        WHEN a.alarm_code LIKE '%OVERLOAD%' THEN '过载告警'
        WHEN a.alarm_code LIKE '%HUMIDITY%' THEN '湿度告警'
        WHEN a.alarm_code LIKE '%MAINTENANCE%' THEN '维护信息'
        ELSE '其他告警'
    END as alarm_category,
    a.is_resolved,
    -- 计算解决时间（分钟）
    CASE 
        WHEN a.is_resolved = TRUE AND a.resolved_at IS NOT NULL 
        THEN CAST((UNIX_TIMESTAMP(a.resolved_at) - UNIX_TIMESTAMP(a.occurred_at)) / 60 AS INT)
        ELSE NULL
    END as resolution_time_minutes,
    DATE(a.occurred_at) as occurred_date,
    EXTRACT(HOUR FROM a.occurred_at) as occurred_hour,
    a.occurred_at,
    a.resolved_at,
    a.created_at,
    a.updated_at
FROM fluss_ods_equipment_alarms a
LEFT JOIN fluss_ods_power_equipment e ON a.equipment_id = e.equipment_id;

-- ========================================
-- 数据质量检查
-- ========================================

-- 创建数据质量检查结果表
CREATE TABLE IF NOT EXISTS fluss_dwd_quality_check (
    check_id BIGINT,
    table_name STRING,
    check_type STRING,
    check_result STRING,
    error_count BIGINT,
    total_count BIGINT,
    check_time TIMESTAMP(3),
    PRIMARY KEY (check_id) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123',
    'table-name' = 'dwd_quality_check',
    'bucket' = '2',
    'value.format' = 'json',
    'fluss.table.log.retention' = '7d'
);

-- DWD层数据质量检查
INSERT INTO fluss_dwd_quality_check
SELECT 
    CAST(UNIX_TIMESTAMP() AS BIGINT) as check_id,
    'dwd_power_monitoring' as table_name,
    'voltage_range_check' as check_type,
    CASE 
        WHEN COUNT(CASE WHEN voltage_avg < 0 OR voltage_avg > 1000 THEN 1 END) = 0 
        THEN 'PASS'
        ELSE 'FAIL'
    END as check_result,
    COUNT(CASE WHEN voltage_avg < 0 OR voltage_avg > 1000 THEN 1 END) as error_count,
    COUNT(*) as total_count,
    CURRENT_TIMESTAMP as check_time
FROM fluss_dwd_power_monitoring
WHERE monitoring_time >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR;

-- 异常数据告警
CREATE TABLE IF NOT EXISTS fluss_dwd_anomaly_alerts (
    alert_id BIGINT,
    equipment_id BIGINT,
    equipment_name STRING,
    anomaly_type STRING,
    anomaly_description STRING,
    severity_level STRING,
    alert_time TIMESTAMP(3),
    PRIMARY KEY (alert_id) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123',
    'table-name' = 'dwd_anomaly_alerts',
    'bucket' = '4',
    'value.format' = 'json',
    'fluss.table.log.retention' = '30d'
);

-- ========================================
-- 启动DWD数据流作业
-- ========================================

-- 启动电力设备DWD数据流
INSERT INTO fluss_dwd_power_equipment
SELECT 
    equipment_id,
    equipment_name,
    equipment_type,
    CASE 
        WHEN equipment_type = '变压器' THEN 'T'
        WHEN equipment_type = '输电线路' THEN 'L'
        WHEN equipment_type = '发电机' THEN 'G'
        WHEN equipment_type = '配电设备' THEN 'D'
        ELSE 'O'
    END as equipment_type_code,
    location,
    REGEXP_EXTRACT(location, '([^区]+)区') as province,
    REGEXP_EXTRACT(location, '北京([^区]+)区') as city,
    REGEXP_EXTRACT(location, '([^区]+)区') as district,
    voltage_level,
    CASE 
        WHEN voltage_level = '110kV' THEN 110
        WHEN voltage_level = '220kV' THEN 220
        WHEN voltage_level = '500kV' THEN 500
        ELSE 0
    END as voltage_level_code,
    capacity_mw,
    CASE 
        WHEN capacity_mw < 10 THEN 'Small'
        WHEN capacity_mw < 100 THEN 'Medium'
        ELSE 'Large'
    END as capacity_level,
    manufacturer,
    installation_date,
    YEAR(installation_date) as installation_year,
    DATEDIFF(CURRENT_DATE, installation_date) as age_days,
    last_maintenance_date,
    DATEDIFF(CURRENT_DATE, last_maintenance_date) as days_since_maintenance,
    status,
    CASE 
        WHEN status = 'ACTIVE' THEN 1
        ELSE 0
    END as is_active,
    created_at,
    updated_at
FROM fluss_ods_power_equipment;

-- 启动实时监控DWD数据流
INSERT INTO fluss_dwd_power_monitoring
SELECT 
    monitoring_id,
    equipment_id,
    voltage_a,
    voltage_b,
    voltage_c,
    (voltage_a + voltage_b + voltage_c) / 3 as voltage_avg,
    GREATEST(voltage_a, voltage_b, voltage_c) as voltage_max,
    LEAST(voltage_a, voltage_b, voltage_c) as voltage_min,
    (GREATEST(voltage_a, voltage_b, voltage_c) - LEAST(voltage_a, voltage_b, voltage_c)) / 
    ((voltage_a + voltage_b + voltage_c) / 3) as voltage_imbalance,
    current_a,
    current_b,
    current_c,
    (current_a + current_b + current_c) / 3 as current_avg,
    GREATEST(current_a, current_b, current_c) as current_max,
    LEAST(current_a, current_b, current_c) as current_min,
    power_active,
    power_reactive,
    SQRT(power_active * power_active + power_reactive * power_reactive) as power_apparent,
    CASE 
        WHEN power_reactive = 0 THEN 1.0
        ELSE power_active / SQRT(power_active * power_active + power_reactive * power_reactive)
    END as power_factor,
    frequency,
    temperature,
    humidity,
    monitoring_time,
    CASE 
        WHEN voltage_avg BETWEEN 200 AND 240 THEN 'NORMAL'
        WHEN voltage_avg < 200 THEN 'LOW'
        ELSE 'HIGH'
    END as voltage_status,
    CASE 
        WHEN frequency BETWEEN 49.5 AND 50.5 THEN 'NORMAL'
        ELSE 'ABNORMAL'
    END as frequency_status,
    CASE 
        WHEN temperature > 80 THEN 'HIGH'
        WHEN temperature > 60 THEN 'MEDIUM'
        ELSE 'NORMAL'
    END as temperature_status,
    CASE 
        WHEN power_factor > 0.9 THEN 'GOOD'
        WHEN power_factor > 0.8 THEN 'FAIR'
        ELSE 'POOR'
    END as power_quality_level,
    CURRENT_TIMESTAMP as created_at
FROM fluss_ods_power_monitoring;

-- 启动设备告警DWD数据流
INSERT INTO fluss_dwd_equipment_alarms
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
    CASE 
        WHEN is_resolved = true THEN DATEDIFF(HOUR, occurred_at, resolved_at)
        ELSE DATEDIFF(HOUR, occurred_at, CURRENT_TIMESTAMP)
    END as resolution_time_hours,
    CASE 
        WHEN alarm_level = 1 THEN 'INFO'
        WHEN alarm_level = 2 THEN 'WARNING'
        WHEN alarm_level = 3 THEN 'ERROR'
        ELSE 'CRITICAL'
    END as alarm_severity,
    CASE 
        WHEN alarm_type LIKE '%电压%' THEN 'VOLTAGE'
        WHEN alarm_type LIKE '%电流%' THEN 'CURRENT'
        WHEN alarm_type LIKE '%温度%' THEN 'TEMPERATURE'
        WHEN alarm_type LIKE '%频率%' THEN 'FREQUENCY'
        ELSE 'OTHER'
    END as alarm_category,
    created_at,
    updated_at,
    CURRENT_TIMESTAMP as processed_at
FROM fluss_ods_equipment_alarms; 