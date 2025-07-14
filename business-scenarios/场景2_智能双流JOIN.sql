-- ===============================================
-- 🔴 场景2：智能双流JOIN + 数仓分层 + PostgreSQL回流
-- 🔥 Fluss vs Kafka 架构升级对比：
-- 1. ✅ 实时双流JOIN：Fluss原生支持，Kafka需要Kafka Streams复杂配置
-- 2. ✅ 状态存储：Fluss内置，Kafka需要RocksDB外部状态存储
-- 3. ✅ 事务一致性：双流JOIN保证ACID，Kafka只有eventually consistent
-- 4. ✅ 查询性能：Fluss支持实时OLAP，Kafka需要导入OLAP引擎
-- 数据流：PostgreSQL CDC双流 → Fluss实时JOIN → 数仓分层 → PostgreSQL
-- ===============================================

SET 'sql-client.execution.result-mode' = 'tableau';

-- ===============================================
-- 1. PostgreSQL CDC双数据源（智能双流JOIN场景）
-- ===============================================

-- 🚀 设备告警CDC流：实时捕获告警事件
CREATE TABLE device_alarm_stream (
    alarm_id STRING,
    device_id STRING,
    alarm_type STRING,
    alarm_level STRING,
    alarm_message STRING,
    alarm_time TIMESTAMP(3),
    reporter_system STRING,
    WATERMARK FOR alarm_time AS alarm_time - INTERVAL '1' SECOND
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres-sgcc-source',
    'port' = '5432',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'database-name' = 'sgcc_source_db',
    'schema-name' = 'public',
    'table-name' = 'device_alarms',
    'slot.name' = 'device_alarms_slot',
    'decoding.plugin.name' = 'pgoutput'
);

-- 🚀 设备状态CDC流：实时捕获状态变化
CREATE TABLE device_status_stream (
    device_id STRING,
    device_name STRING,
    voltage DOUBLE,
    current_val DOUBLE,  -- 避免保留字冲突
    temperature DOUBLE,
    efficiency DOUBLE,
    status STRING,
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres-sgcc-source',
    'port' = '5432',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'database-name' = 'sgcc_source_db',
    'schema-name' = 'public',
    'table-name' = 'device_status',
    'slot.name' = 'device_status_slot',
    'decoding.plugin.name' = 'pgoutput'
);

-- 📝 备用：DataGen双数据源（暂时注释）
/*
-- 设备告警数据流
CREATE TEMPORARY TABLE device_alarm_stream_backup (
    alarm_id STRING,
    device_id STRING,
    alarm_type STRING,
    alarm_level STRING,
    alarm_message STRING,
    alarm_time TIMESTAMP(3),
    reporter_system STRING,
    WATERMARK FOR alarm_time AS alarm_time - INTERVAL '1' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '50',  -- 降低测试数据量
    'fields.alarm_id.kind' = 'sequence',
    'fields.alarm_id.start' = '1',
    'fields.alarm_id.end' = '10000',
    'fields.device_id.kind' = 'sequence',
    'fields.device_id.start' = '100000',
    'fields.device_id.end' = '100020',
    'fields.alarm_type.length' = '10',
    'fields.alarm_level.length' = '8',
    'fields.alarm_message.length' = '50',
    'fields.reporter_system.length' = '10'
);

-- 设备状态数据流
CREATE TEMPORARY TABLE device_status_stream_backup (
    device_id STRING,
    device_name STRING,
    voltage DOUBLE,
    current_val DOUBLE,
    temperature DOUBLE,
    efficiency DOUBLE,
    status STRING,
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '100',  -- 降低测试数据量
    'fields.device_id.kind' = 'sequence',
    'fields.device_id.start' = '100000',
    'fields.device_id.end' = '100020',
    'fields.device_name.length' = '20',
    'fields.voltage.min' = '210.0',
    'fields.voltage.max' = '250.0',
    'fields.current_val.min' = '50.0',
    'fields.current_val.max' = '200.0',
    'fields.temperature.min' = '20.0',
    'fields.temperature.max' = '80.0',
    'fields.efficiency.min' = '0.80',
    'fields.efficiency.max' = '0.98',
    'fields.status.length' = '8'
);
*/

-- ===============================================
-- 2. 创建Fluss Catalog和数仓分层表
-- ===============================================

CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

USE CATALOG fluss_catalog;
CREATE DATABASE IF NOT EXISTS fluss;
USE fluss;

-- ODS层：告警原始数据
CREATE TABLE ods_alarm_raw (
    alarm_id STRING PRIMARY KEY NOT ENFORCED,
    device_id STRING,
    alarm_type STRING,
    alarm_level STRING,
    alarm_message STRING,
    alarm_time TIMESTAMP(3),
    reporter_system STRING
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- ODS层：设备状态原始数据（Fluss vs Kafka：原生支持UPDATE/DELETE）
CREATE TABLE ods_device_status_raw (
    device_id STRING PRIMARY KEY NOT ENFORCED,
    device_name STRING,
    voltage DOUBLE,
    current_val DOUBLE,  -- 避免保留字冲突
    temperature DOUBLE,
    efficiency DOUBLE,
    status STRING,
    event_time TIMESTAMP(3)
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- DWD层：告警设备关联明细
CREATE TABLE dwd_alarm_device_detail (
    join_id STRING PRIMARY KEY NOT ENFORCED,
    alarm_id STRING,
    device_id STRING,
    device_name STRING,
    alarm_type STRING,
    alarm_level STRING,
    alarm_message STRING,
    device_voltage DOUBLE,
    device_current DOUBLE,
    device_temperature DOUBLE,
    device_efficiency DOUBLE,
    device_status STRING,
    location STRING,
    device_type STRING,
    risk_score DOUBLE,
    severity_level STRING,
    join_time TIMESTAMP(3)
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- DWS层：告警汇总分析
CREATE TABLE dws_alarm_summary (
    summary_id STRING PRIMARY KEY NOT ENFORCED,
    time_window STRING,
    location STRING,
    device_type STRING,
    total_alarms BIGINT,
    high_severity_alarms BIGINT,
    avg_device_efficiency DOUBLE,
    avg_temperature DOUBLE,
    affected_devices BIGINT,
    summary_time TIMESTAMP(3)
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- ADS层：智能告警报表
CREATE TABLE ads_alarm_intelligence_report (
    report_id STRING PRIMARY KEY NOT ENFORCED,
    report_type STRING,
    time_period STRING,
    location STRING,
    total_incidents BIGINT,
    critical_devices BIGINT,
    efficiency_impact DOUBLE,
    temperature_anomaly BIGINT,
    risk_assessment STRING,
    suggested_actions STRING,
    priority_level STRING,
    report_time TIMESTAMP(3)
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- ===============================================
-- 3. 回到Default Catalog创建PostgreSQL Sink
-- ===============================================

USE CATALOG default_catalog;

-- PostgreSQL告警智能分析结果表
CREATE TABLE postgres_alarm_intelligence_result (
    report_id STRING,
    report_type STRING,
    time_period STRING,
    location STRING,
    total_incidents BIGINT,
    critical_devices BIGINT,
    efficiency_impact DOUBLE,
    temperature_anomaly BIGINT,
    risk_assessment STRING,
    suggested_actions STRING,
    priority_level STRING,
    report_time TIMESTAMP(3),
    PRIMARY KEY (report_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'alarm_intelligence_result',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024'
);

-- ===============================================
-- 4. ODS层：数据采集
-- ===============================================

-- 🚀 ODS层：采集告警数据
INSERT INTO fluss_catalog.fluss.ods_alarm_raw
SELECT 
    alarm_id,
    device_id,
    alarm_type,
    alarm_level,
    alarm_message,
    alarm_time,
    reporter_system
FROM device_alarm_stream;

-- 🚀 ODS层：CDC设备状态数据同步
-- 💡 Fluss优势：原生支持CDC，比Kafka Connect更高效
INSERT INTO fluss_catalog.fluss.ods_device_status_raw
SELECT 
    device_id,
    device_name,
    voltage,
    current_val,
    temperature,
    efficiency,
    status,
    event_time
FROM device_status_stream;

-- ===============================================
-- 5. DWD层：智能双流JOIN
-- ===============================================

-- 🚀 DWD层：告警与设备状态实时JOIN
INSERT INTO fluss_catalog.fluss.dwd_alarm_device_detail
SELECT 
    CONCAT(a.alarm_id, '_', d.device_id) as join_id,
    a.alarm_id,
    a.device_id,
    d.device_name,
    a.alarm_type,
    a.alarm_level,
    a.alarm_message,
    d.voltage as device_voltage,
    d.current_val as device_current,
    d.temperature as device_temperature,
    d.efficiency as device_efficiency,
    d.status as device_status,
    -- 智能地理位置推断
    CASE 
        WHEN CAST(a.device_id AS INT) % 5 = 0 THEN '北京'
        WHEN CAST(a.device_id AS INT) % 5 = 1 THEN '上海'
        WHEN CAST(a.device_id AS INT) % 5 = 2 THEN '广州'
        WHEN CAST(a.device_id AS INT) % 5 = 3 THEN '深圳'
        ELSE '成都'
    END as location,
    -- 智能设备类型推断
    CASE 
        WHEN CAST(a.device_id AS INT) % 3 = 0 THEN '变压器'
        WHEN CAST(a.device_id AS INT) % 3 = 1 THEN '发电机'
        ELSE '配电设备'
    END as device_type,
    -- 智能风险评分算法
    CASE 
        WHEN a.alarm_level = 'HIGH' AND d.temperature > 70 AND d.efficiency < 0.85 THEN 95.0
        WHEN a.alarm_level = 'HIGH' AND d.temperature > 60 THEN 85.0
        WHEN a.alarm_level = 'MEDIUM' AND d.efficiency < 0.90 THEN 75.0
        WHEN a.alarm_level = 'LOW' THEN 45.0
        ELSE 30.0
    END as risk_score,
    -- 智能严重程度评估
    CASE 
        WHEN a.alarm_level = 'HIGH' AND d.temperature > 70 THEN 'CRITICAL'
        WHEN a.alarm_level = 'HIGH' THEN 'HIGH'
        WHEN a.alarm_level = 'MEDIUM' AND d.efficiency < 0.85 THEN 'HIGH'
        WHEN a.alarm_level = 'MEDIUM' THEN 'MEDIUM'
        ELSE 'LOW'
    END as severity_level,
    CURRENT_TIMESTAMP as join_time
FROM fluss_catalog.fluss.ods_alarm_raw a
JOIN fluss_catalog.fluss.ods_device_status_raw d
ON a.device_id = d.device_id;

-- ===============================================
-- 6. DWS层：告警汇总分析
-- ===============================================

-- 🚀 DWS层：按时间窗口和地区汇总告警
INSERT INTO fluss_catalog.fluss.dws_alarm_summary
SELECT 
    CONCAT(location, '_', device_type, '_', CAST(DATE_FORMAT(join_time, 'yyyyMMddHH') AS STRING)) as summary_id,
    DATE_FORMAT(join_time, 'yyyyMMddHH') as time_window,
    location,
    device_type,
    COUNT(*) as total_alarms,
    SUM(CASE WHEN severity_level IN ('CRITICAL', 'HIGH') THEN 1 ELSE 0 END) as high_severity_alarms,
    AVG(device_efficiency) as avg_device_efficiency,
    AVG(device_temperature) as avg_temperature,
    COUNT(DISTINCT device_id) as affected_devices,
    CURRENT_TIMESTAMP as summary_time
FROM fluss_catalog.fluss.dwd_alarm_device_detail
GROUP BY location, device_type, DATE_FORMAT(join_time, 'yyyyMMddHH');

-- ===============================================
-- 7. ADS层：智能告警报表生成
-- ===============================================

-- 🚀 ADS层：生成智能告警分析报表
INSERT INTO fluss_catalog.fluss.ads_alarm_intelligence_report
SELECT 
    CONCAT('ALARM_RPT_', location, '_', CAST(time_window AS STRING)) as report_id,
    '智能告警分析报表' as report_type,
    time_window as time_period,
    location,
    SUM(total_alarms) as total_incidents,
    SUM(affected_devices) as critical_devices,
    AVG(avg_device_efficiency) * 100 as efficiency_impact,
    SUM(CASE WHEN avg_temperature > 65 THEN 1 ELSE 0 END) as temperature_anomaly,
    -- 智能风险评估
    CASE 
        WHEN SUM(high_severity_alarms) > 50 AND AVG(avg_device_efficiency) < 0.85 THEN 'CRITICAL_RISK'
        WHEN SUM(high_severity_alarms) > 20 THEN 'HIGH_RISK'
        WHEN SUM(total_alarms) > 50 THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END as risk_assessment,
    -- 智能建议措施
    CASE 
        WHEN SUM(high_severity_alarms) > 50 THEN '立即启动应急预案，派遣技术团队现场处理'
        WHEN SUM(high_severity_alarms) > 20 THEN '加强监控，准备维护资源'
        WHEN SUM(total_alarms) > 50 THEN '计划例行检查，优化运维策略'
        ELSE '保持正常监控频率'
    END as suggested_actions,
    -- 智能优先级评估
    CASE 
        WHEN SUM(high_severity_alarms) > 50 THEN 'P0_URGENT'
        WHEN SUM(high_severity_alarms) > 20 THEN 'P1_HIGH'
        WHEN SUM(total_alarms) > 50 THEN 'P2_MEDIUM'
        ELSE 'P3_LOW'
    END as priority_level,
    CURRENT_TIMESTAMP as report_time
FROM fluss_catalog.fluss.dws_alarm_summary
GROUP BY location, time_window;

-- ===============================================
-- 8. 数据回流PostgreSQL
-- ===============================================

-- 🚀 最终回流：Fluss ADS层 → PostgreSQL
INSERT INTO postgres_alarm_intelligence_result
SELECT 
    report_id,
    report_type,
    time_period,
    location,
    total_incidents,
    critical_devices,
    efficiency_impact,
    temperature_anomaly,
    risk_assessment,
    suggested_actions,
    priority_level,
    report_time
FROM fluss_catalog.fluss.ads_alarm_intelligence_report;

-- ===============================================
-- 9. 数据增删改测试查询
-- ===============================================

/*
-- 查看PostgreSQL中的告警智能分析结果
SELECT * FROM postgres_alarm_intelligence_result ORDER BY report_time DESC LIMIT 10;

-- 查看双流JOIN效果
SELECT 
    location,
    COUNT(*) as join_count,
    AVG(risk_score) as avg_risk,
    COUNT(DISTINCT device_id) as unique_devices
FROM fluss_catalog.fluss.dwd_alarm_device_detail 
GROUP BY location
ORDER BY avg_risk DESC;

-- 测试告警数据更新
UPDATE fluss_catalog.fluss.ods_alarm_raw 
SET alarm_level = 'CRITICAL' 
WHERE alarm_id = '1';

-- 测试设备状态删除
DELETE FROM fluss_catalog.fluss.ods_device_status_raw 
WHERE device_id = '100001';

-- 验证JOIN结果变化
SELECT COUNT(*) as updated_joins 
FROM fluss_catalog.fluss.dwd_alarm_device_detail 
WHERE severity_level = 'CRITICAL';
*/ 