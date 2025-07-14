-- ========================================
-- 国网电力监控系统 - Fluss架构
-- 第四步：ADS层应用数据服务
-- ========================================

-- 设置执行环境
SET 'execution.checkpointing.interval' = '60s';
SET 'table.exec.source.idle-timeout' = '30s';

-- 使用Fluss Catalog
USE CATALOG fluss_catalog;

-- ========================================
-- 创建ADS层Fluss表
-- ========================================

-- ADS层实时监控大屏数据表
CREATE TABLE IF NOT EXISTS fluss_ads_realtime_dashboard (
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
    process_time TIMESTAMP(3) METADATA FROM 'timestamp',
    PRIMARY KEY (dashboard_id) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123',
    'table-name' = 'ads_realtime_dashboard',
    'bucket' = '1',
    'value.format' = 'json',
    'fluss.table.datalake.enabled' = 'false',
    'fluss.table.log.retention' = '1d',
    'fluss.table.compaction.enabled' = 'true'
);

-- ADS层电力质量分析表
CREATE TABLE IF NOT EXISTS fluss_ads_power_quality_analysis (
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
    process_time TIMESTAMP(3) METADATA FROM 'timestamp',
    PRIMARY KEY (analysis_id) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123',
    'table-name' = 'ads_power_quality_analysis',
    'bucket' = '2',
    'value.format' = 'json',
    'fluss.table.datalake.enabled' = 'true',
    'fluss.table.log.retention' = '30d',
    'fluss.table.compaction.enabled' = 'true'
);

-- ADS层设备健康度评估表
CREATE TABLE IF NOT EXISTS fluss_ads_equipment_health_assessment (
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
    process_time TIMESTAMP(3) METADATA FROM 'timestamp',
    PRIMARY KEY (assessment_id) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123',
    'table-name' = 'ads_equipment_health_assessment',
    'bucket' = '4',
    'value.format' = 'json',
    'fluss.table.datalake.enabled' = 'true',
    'fluss.table.log.retention' = '30d',
    'fluss.table.compaction.enabled' = 'true'
);

-- ADS层区域电网概况表
CREATE TABLE IF NOT EXISTS fluss_ads_regional_grid_overview (
    overview_id BIGINT,
    region_name STRING,
    province STRING,
    city STRING,
    total_equipment INT,
    equipment_types MAP<STRING, INT>,
    total_capacity_mw DECIMAL(12,2),
    current_load_mw DECIMAL(12,2),
    load_rate DECIMAL(6,4),
    power_quality_score DECIMAL(6,2),
    active_alarms INT,
    critical_alarms INT,
    region_status STRING,
    last_updated TIMESTAMP(3),
    process_time TIMESTAMP(3) METADATA FROM 'timestamp',
    PRIMARY KEY (overview_id) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123',
    'table-name' = 'ads_regional_grid_overview',
    'bucket' = '4',
    'value.format' = 'json',
    'fluss.table.datalake.enabled' = 'true',
    'fluss.table.log.retention' = '7d',
    'fluss.table.compaction.enabled' = 'true'
);

-- ADS层告警统计分析表
CREATE TABLE IF NOT EXISTS fluss_ads_alarm_statistics (
    stat_id BIGINT,
    stat_date DATE,
    stat_hour INT,
    equipment_type STRING,
    alarm_category STRING,
    alarm_count INT,
    avg_resolution_time_minutes DECIMAL(8,2),
    resolution_rate DECIMAL(6,4),
    recurring_alarm_count INT,
    top_alarm_equipment STRING,
    trend_indicator STRING,
    created_at TIMESTAMP(3),
    process_time TIMESTAMP(3) METADATA FROM 'timestamp',
    PRIMARY KEY (stat_id) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123',
    'table-name' = 'ads_alarm_statistics',
    'bucket' = '4',
    'value.format' = 'json',
    'fluss.table.datalake.enabled' = 'true',
    'fluss.table.log.retention' = '30d',
    'fluss.table.compaction.enabled' = 'true'
);

-- ADS层设备性能指标表
CREATE TABLE IF NOT EXISTS fluss_ads_equipment_performance_metrics (
    metric_id BIGINT,
    equipment_id BIGINT,
    equipment_name STRING,
    equipment_type STRING,
    metric_date DATE,
    efficiency_score DECIMAL(6,2),
    utilization_rate DECIMAL(6,4),
    availability_rate DECIMAL(6,4),
    performance_trend STRING,
    energy_consumption_kwh DECIMAL(10,2),
    carbon_emission_kg DECIMAL(10,2),
    cost_efficiency_score DECIMAL(6,2),
    created_at TIMESTAMP(3),
    process_time TIMESTAMP(3) METADATA FROM 'timestamp',
    PRIMARY KEY (metric_id) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123',
    'table-name' = 'ads_equipment_performance_metrics',
    'bucket' = '4',
    'value.format' = 'json',
    'fluss.table.datalake.enabled' = 'true',
    'fluss.table.log.retention' = '30d',
    'fluss.table.compaction.enabled' = 'true'
);

-- ========================================
-- ADS层数据处理任务
-- ========================================

-- 实时监控大屏数据更新
INSERT INTO fluss_ads_realtime_dashboard
SELECT 
    1 as dashboard_id,
    COUNT(DISTINCT e.equipment_id) as total_equipment,
    COUNT(DISTINCT CASE WHEN rm.monitoring_time >= CURRENT_TIMESTAMP - INTERVAL '10' MINUTE 
                       THEN e.equipment_id END) as online_equipment,
    COUNT(DISTINCT CASE WHEN rm.monitoring_time < CURRENT_TIMESTAMP - INTERVAL '10' MINUTE 
                       THEN e.equipment_id END) as offline_equipment,
    COUNT(DISTINCT CASE WHEN e.status = 'MAINTENANCE' THEN e.equipment_id END) as maintenance_equipment,
    SUM(e.capacity_mw) as total_capacity_mw,
    SUM(CASE WHEN rm.monitoring_time >= CURRENT_TIMESTAMP - INTERVAL '10' MINUTE 
             THEN e.capacity_mw * COALESCE(rm.power_active, 0) / 
                  CASE WHEN e.equipment_type = '变压器' THEN 300 
                       WHEN e.equipment_type = '发电机' THEN 200 
                       ELSE 150 END
             ELSE 0 END) as active_capacity_mw,
    AVG(CASE WHEN rm.monitoring_time >= CURRENT_TIMESTAMP - INTERVAL '10' MINUTE 
             THEN rm.power_active / 
                  CASE WHEN e.equipment_type = '变压器' THEN 300 
                       WHEN e.equipment_type = '发电机' THEN 200 
                       ELSE 150 END
             ELSE 0 END) as load_rate,
    COUNT(DISTINCT CASE WHEN a.is_resolved = FALSE AND a.occurred_at >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR 
                       THEN a.alarm_id END) as total_active_alarms,
    COUNT(DISTINCT CASE WHEN a.is_resolved = FALSE AND a.alarm_level >= 4 
                       THEN a.alarm_id END) as critical_alarms,
    COUNT(DISTINCT CASE WHEN a.is_resolved = FALSE AND a.alarm_level BETWEEN 2 AND 3 
                       THEN a.alarm_id END) as warning_alarms,
    AVG(CASE WHEN e.voltage_level = '220kV' AND rm.monitoring_time >= CURRENT_TIMESTAMP - INTERVAL '10' MINUTE 
             THEN rm.voltage_avg END) as avg_voltage_220kv,
    AVG(CASE WHEN e.voltage_level = '110kV' AND rm.monitoring_time >= CURRENT_TIMESTAMP - INTERVAL '10' MINUTE 
             THEN rm.voltage_avg END) as avg_voltage_110kv,
    AVG(CASE WHEN rm.monitoring_time >= CURRENT_TIMESTAMP - INTERVAL '10' MINUTE 
             THEN rm.frequency END) as avg_frequency,
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN a.is_resolved = FALSE AND a.alarm_level >= 4 
                               THEN a.alarm_id END) > 0 THEN '故障'
        WHEN COUNT(DISTINCT CASE WHEN a.is_resolved = FALSE AND a.alarm_level BETWEEN 2 AND 3 
                               THEN a.alarm_id END) > 0 THEN '告警'
        ELSE '正常'
    END as system_status,
    CURRENT_TIMESTAMP as last_updated
FROM fluss_dwd_power_equipment e
LEFT JOIN fluss_dws_realtime_monitoring rm ON e.equipment_id = rm.equipment_id
LEFT JOIN fluss_dwd_equipment_alarms a ON e.equipment_id = a.equipment_id;

-- 电力质量分析
INSERT INTO fluss_ads_power_quality_analysis
SELECT 
    CAST(DATE_FORMAT(CURRENT_DATE, 'yyyyMMdd') AS BIGINT) as analysis_id,
    CURRENT_DATE as analysis_date,
    AVG(voltage_qualification_rate) as voltage_qualification_rate,
    AVG(frequency_qualification_rate) as frequency_qualification_rate,
    AVG(power_factor_avg) as power_factor_avg,
    AVG(voltage_imbalance_rate) as voltage_imbalance_rate,
    AVG(current_imbalance_rate) as current_imbalance_rate,
    -- 模拟谐波畸变率
    AVG(voltage_imbalance_rate + current_imbalance_rate) * 0.5 as harmonic_distortion_rate,
    AVG(power_quality_score) as power_quality_score,
    CASE 
        WHEN AVG(power_quality_score) >= 90 THEN '优秀'
        WHEN AVG(power_quality_score) >= 80 THEN '良好'
        WHEN AVG(power_quality_score) >= 70 THEN '一般'
        WHEN AVG(power_quality_score) >= 60 THEN '较差'
        ELSE '差'
    END as quality_level,
    CURRENT_TIMESTAMP as created_at
FROM fluss_dws_power_quality_metrics
WHERE summary_date >= CURRENT_DATE - INTERVAL '1' DAY;

-- 设备健康度评估
INSERT INTO fluss_ads_equipment_health_assessment
SELECT 
    CAST(CONCAT(CAST(e.equipment_id AS STRING), DATE_FORMAT(CURRENT_DATE, 'yyyyMMdd')) AS BIGINT) as assessment_id,
    e.equipment_id,
    e.equipment_name,
    e.equipment_type,
    -- 健康度评分计算
    (COALESCE(AVG(h.efficiency_score), 0.8) * 0.4 +
     COALESCE(1 - AVG(a.alarm_count) / 10.0, 0.9) * 0.3 +
     COALESCE(AVG(d.utilization_rate), 0.7) * 0.2 +
     COALESCE(1 - e.maintenance_interval_days / 365.0, 0.8) * 0.1) * 100 as health_score,
    CASE 
        WHEN (COALESCE(AVG(h.efficiency_score), 0.8) * 0.4 +
              COALESCE(1 - AVG(a.alarm_count) / 10.0, 0.9) * 0.3 +
              COALESCE(AVG(d.utilization_rate), 0.7) * 0.2 +
              COALESCE(1 - e.maintenance_interval_days / 365.0, 0.8) * 0.1) >= 0.9 THEN '优秀'
        WHEN (COALESCE(AVG(h.efficiency_score), 0.8) * 0.4 +
              COALESCE(1 - AVG(a.alarm_count) / 10.0, 0.9) * 0.3 +
              COALESCE(AVG(d.utilization_rate), 0.7) * 0.2 +
              COALESCE(1 - e.maintenance_interval_days / 365.0, 0.8) * 0.1) >= 0.8 THEN '良好'
        WHEN (COALESCE(AVG(h.efficiency_score), 0.8) * 0.4 +
              COALESCE(1 - AVG(a.alarm_count) / 10.0, 0.9) * 0.3 +
              COALESCE(AVG(d.utilization_rate), 0.7) * 0.2 +
              COALESCE(1 - e.maintenance_interval_days / 365.0, 0.8) * 0.1) >= 0.7 THEN '一般'
        ELSE '差'
    END as health_level,
    -- 可靠性评分
    COALESCE(1 - AVG(a.alarm_count) / 10.0, 0.9) * 100 as reliability_score,
    -- 维护紧急度
    CASE 
        WHEN e.maintenance_interval_days > 300 THEN 5
        WHEN e.maintenance_interval_days > 200 THEN 4
        WHEN e.maintenance_interval_days > 150 THEN 3
        WHEN e.maintenance_interval_days > 100 THEN 2
        ELSE 1
    END as maintenance_urgency,
    -- 预测故障概率
    CASE 
        WHEN AVG(a.alarm_count) > 5 OR e.maintenance_interval_days > 250 THEN 0.3
        WHEN AVG(a.alarm_count) > 3 OR e.maintenance_interval_days > 200 THEN 0.2
        WHEN AVG(a.alarm_count) > 1 OR e.maintenance_interval_days > 150 THEN 0.1
        ELSE 0.05
    END as predicted_failure_probability,
    -- 预计剩余寿命
    CASE 
        WHEN AVG(a.alarm_count) > 5 THEN 30
        WHEN AVG(a.alarm_count) > 3 THEN 90
        WHEN AVG(a.alarm_count) > 1 THEN 180
        ELSE 365
    END as remaining_life_days,
    -- 维护建议
    CASE 
        WHEN e.maintenance_interval_days > 300 THEN '建议立即安排维护检修'
        WHEN e.maintenance_interval_days > 200 THEN '建议在一周内安排维护'
        WHEN e.maintenance_interval_days > 150 THEN '建议在一个月内安排维护'
        WHEN AVG(a.alarm_count) > 3 THEN '关注设备告警情况，提前预防'
        ELSE '设备运行正常，按计划维护'
    END as maintenance_recommendation,
    CURRENT_DATE as assessment_date,
    CURRENT_TIMESTAMP as created_at
FROM fluss_dwd_power_equipment e
LEFT JOIN fluss_dws_equipment_hourly_summary h ON e.equipment_id = h.equipment_id 
                                                AND h.summary_date >= CURRENT_DATE - INTERVAL '7' DAY
LEFT JOIN fluss_dws_alarm_daily_summary a ON e.equipment_type = a.equipment_type 
                                           AND a.summary_date >= CURRENT_DATE - INTERVAL '7' DAY
LEFT JOIN fluss_dws_equipment_daily_summary d ON e.equipment_id = d.equipment_id 
                                               AND d.summary_date >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY e.equipment_id, e.equipment_name, e.equipment_type, e.maintenance_interval_days;

-- 区域电网概况
INSERT INTO fluss_ads_regional_grid_overview
SELECT 
    CAST(CONCAT(HASH_CODE(province), HASH_CODE(city)) AS BIGINT) as overview_id,
    CONCAT(province, city) as region_name,
    province,
    city,
    COUNT(DISTINCT e.equipment_id) as total_equipment,
    MAP(
        'transformer', COUNT(CASE WHEN e.equipment_type = '变压器' THEN 1 END),
        'generator', COUNT(CASE WHEN e.equipment_type = '发电机' THEN 1 END),
        'transmission', COUNT(CASE WHEN e.equipment_type = '输电线路' THEN 1 END),
        'distribution', COUNT(CASE WHEN e.equipment_type = '配电设备' THEN 1 END)
    ) as equipment_types,
    SUM(e.capacity_mw) as total_capacity_mw,
    SUM(COALESCE(rm.power_active_avg, 0)) as current_load_mw,
    AVG(COALESCE(d.utilization_rate, 0)) as load_rate,
    AVG(COALESCE(q.power_quality_score, 80)) as power_quality_score,
    COUNT(CASE WHEN a.is_resolved = FALSE AND a.occurred_at >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR 
               THEN a.alarm_id END) as active_alarms,
    COUNT(CASE WHEN a.is_resolved = FALSE AND a.alarm_level >= 4 
               THEN a.alarm_id END) as critical_alarms,
    CASE 
        WHEN COUNT(CASE WHEN a.is_resolved = FALSE AND a.alarm_level >= 4 
                       THEN a.alarm_id END) > 0 THEN '故障'
        WHEN COUNT(CASE WHEN a.is_resolved = FALSE AND a.alarm_level BETWEEN 2 AND 3 
                       THEN a.alarm_id END) > 0 THEN '告警'
        ELSE '正常'
    END as region_status,
    CURRENT_TIMESTAMP as last_updated
FROM fluss_dwd_power_equipment e
LEFT JOIN fluss_dws_realtime_monitoring rm ON e.equipment_id = rm.equipment_id
LEFT JOIN fluss_dws_equipment_daily_summary d ON e.equipment_id = d.equipment_id 
                                              AND d.summary_date = CURRENT_DATE
LEFT JOIN fluss_dws_power_quality_metrics q ON q.summary_date = CURRENT_DATE
LEFT JOIN fluss_dwd_equipment_alarms a ON e.equipment_id = a.equipment_id
GROUP BY province, city;

-- 告警统计分析
INSERT INTO fluss_ads_alarm_statistics
SELECT 
    CAST(CONCAT(DATE_FORMAT(CURRENT_DATE, 'yyyyMMdd'), 
                CAST(EXTRACT(HOUR FROM CURRENT_TIMESTAMP) AS STRING),
                HASH_CODE(CONCAT(equipment_type, alarm_category))) AS BIGINT) as stat_id,
    CURRENT_DATE as stat_date,
    EXTRACT(HOUR FROM CURRENT_TIMESTAMP) as stat_hour,
    equipment_type,
    alarm_category,
    COUNT(*) as alarm_count,
    AVG(resolution_time_minutes) as avg_resolution_time_minutes,
    COUNT(CASE WHEN is_resolved = TRUE THEN 1 END) * 1.0 / COUNT(*) as resolution_rate,
    COUNT(CASE WHEN equipment_id IN (
        SELECT equipment_id 
        FROM fluss_dwd_equipment_alarms 
        WHERE alarm_category = a.alarm_category 
        GROUP BY equipment_id 
        HAVING COUNT(*) > 1
    ) THEN 1 END) as recurring_alarm_count,
    -- 获取告警最多的设备
    (SELECT equipment_name 
     FROM fluss_dwd_equipment_alarms 
     WHERE alarm_category = a.alarm_category 
     GROUP BY equipment_id, equipment_name 
     ORDER BY COUNT(*) DESC 
     LIMIT 1) as top_alarm_equipment,
    CASE 
        WHEN COUNT(*) > LAG(COUNT(*), 1, 0) OVER (PARTITION BY equipment_type, alarm_category ORDER BY occurred_date) 
        THEN '上升'
        WHEN COUNT(*) < LAG(COUNT(*), 1, 0) OVER (PARTITION BY equipment_type, alarm_category ORDER BY occurred_date) 
        THEN '下降'
        ELSE '平稳'
    END as trend_indicator,
    CURRENT_TIMESTAMP as created_at
FROM fluss_dwd_equipment_alarms a
WHERE occurred_date >= CURRENT_DATE - INTERVAL '1' DAY
GROUP BY equipment_type, alarm_category;

-- 设备性能指标
INSERT INTO fluss_ads_equipment_performance_metrics
SELECT 
    CAST(CONCAT(CAST(e.equipment_id AS STRING), DATE_FORMAT(CURRENT_DATE, 'yyyyMMdd')) AS BIGINT) as metric_id,
    e.equipment_id,
    e.equipment_name,
    e.equipment_type,
    CURRENT_DATE as metric_date,
    AVG(h.efficiency_score) * 100 as efficiency_score,
    AVG(d.utilization_rate) as utilization_rate,
    -- 可用性率计算
    AVG(d.uptime_hours) / 24.0 as availability_rate,
    -- 性能趋势
    CASE 
        WHEN AVG(h.efficiency_score) > LAG(AVG(h.efficiency_score), 1, 0) OVER (PARTITION BY e.equipment_id ORDER BY d.summary_date) 
        THEN '改善'
        WHEN AVG(h.efficiency_score) < LAG(AVG(h.efficiency_score), 1, 0) OVER (PARTITION BY e.equipment_id ORDER BY d.summary_date) 
        THEN '恶化'
        ELSE '稳定'
    END as performance_trend,
    -- 模拟能耗计算
    AVG(d.power_active_avg) * 24 as energy_consumption_kwh,
    -- 模拟碳排放计算 (0.5312 kg CO2/kWh)
    AVG(d.power_active_avg) * 24 * 0.5312 as carbon_emission_kg,
    -- 成本效率评分
    (AVG(h.efficiency_score) * 0.6 + AVG(d.utilization_rate) * 0.4) * 100 as cost_efficiency_score,
    CURRENT_TIMESTAMP as created_at
FROM fluss_dwd_power_equipment e
LEFT JOIN fluss_dws_equipment_hourly_summary h ON e.equipment_id = h.equipment_id 
                                                AND h.summary_date >= CURRENT_DATE - INTERVAL '7' DAY
LEFT JOIN fluss_dws_equipment_daily_summary d ON e.equipment_id = d.equipment_id 
                                               AND d.summary_date >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY e.equipment_id, e.equipment_name, e.equipment_type;

-- ========================================
-- 实时预警和推荐
-- ========================================

-- 创建实时预警表
CREATE TABLE IF NOT EXISTS fluss_ads_realtime_alerts (
    alert_id BIGINT,
    alert_type STRING,
    alert_level STRING,
    alert_title STRING,
    alert_content STRING,
    equipment_id BIGINT,
    equipment_name STRING,
    action_required STRING,
    alert_time TIMESTAMP(3),
    is_acknowledged BOOLEAN,
    process_time TIMESTAMP(3) METADATA FROM 'timestamp',
    PRIMARY KEY (alert_id) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123',
    'table-name' = 'ads_realtime_alerts',
    'bucket' = '4',
    'value.format' = 'json',
    'fluss.table.datalake.enabled' = 'false',
    'fluss.table.log.retention' = '7d',
    'fluss.table.compaction.enabled' = 'false'
);

-- 实时预警生成
INSERT INTO fluss_ads_realtime_alerts
SELECT 
    CAST(UNIX_TIMESTAMP() * 1000 + equipment_id AS BIGINT) as alert_id,
    'PERFORMANCE' as alert_type,
    CASE 
        WHEN efficiency_score < 60 THEN 'CRITICAL'
        WHEN efficiency_score < 80 THEN 'WARNING'
        ELSE 'INFO'
    END as alert_level,
    CONCAT(equipment_name, ' 设备性能异常') as alert_title,
    CONCAT('设备效率评分: ', CAST(efficiency_score AS STRING), 
           '，利用率: ', CAST(utilization_rate * 100 AS STRING), '%') as alert_content,
    equipment_id,
    equipment_name,
    CASE 
        WHEN efficiency_score < 60 THEN '立即检修'
        WHEN efficiency_score < 80 THEN '安排维护'
        ELSE '持续监控'
    END as action_required,
    CURRENT_TIMESTAMP as alert_time,
    FALSE as is_acknowledged
FROM fluss_ads_equipment_performance_metrics
WHERE metric_date = CURRENT_DATE
  AND (efficiency_score < 80 OR utilization_rate < 0.5); 