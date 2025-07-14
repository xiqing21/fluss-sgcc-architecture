-- ===============================================
-- 🎯 场景1：高频维度表服务 + 数仓分层 + PostgreSQL回流
-- 🔥 Fluss vs Kafka 架构升级对比：
-- 1. ✅ 统一存储计算：无需Kafka+ClickHouse分离架构
-- 2. ✅ 实时OLAP：支持UPDATE/DELETE，Kafka只能append
-- 3. ✅ 数仓分层：ODS→DWD→DWS→ADS一体化，Kafka需要多套系统
-- 4. ✅ 事务一致性：ACID保证，Kafka只有基础去重
-- 数据流：PostgreSQL CDC → Fluss数仓分层 → PostgreSQL结果
-- ===============================================

SET 'sql-client.execution.result-mode' = 'tableau';

-- ===============================================
-- 1. PostgreSQL CDC数据源（替代DataGen）
-- ===============================================

-- 🚀 PostgreSQL CDC源表：实时捕获数据变化
CREATE TABLE device_raw_stream (
    device_id STRING,
    voltage DOUBLE,
    current_val DOUBLE,  -- 避免保留字冲突
    temperature DOUBLE,
    power_output DOUBLE,
    efficiency DOUBLE,
    status STRING,
    alert_level STRING,
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
    'table-name' = 'device_raw_data',
    'slot.name' = 'device_raw_slot',
    'decoding.plugin.name' = 'pgoutput'
);

-- 📝 备用：DataGen数据源（暂时注释）
/*
CREATE TEMPORARY TABLE device_raw_stream_backup (
    device_id STRING,
    voltage DOUBLE,
    current_val DOUBLE,
    temperature DOUBLE,
    power_output DOUBLE,
    efficiency DOUBLE,
    status STRING,
    alert_level STRING,
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '100',  -- 降低测试数据量
    'fields.device_id.kind' = 'sequence',
    'fields.device_id.start' = '100000',
    'fields.device_id.end' = '100020',  -- 只生成20个设备
    'fields.voltage.min' = '220.0',
    'fields.voltage.max' = '240.0',
    'fields.current_val.min' = '50.0',
    'fields.current_val.max' = '200.0',
    'fields.temperature.min' = '20.0',
    'fields.temperature.max' = '80.0',
    'fields.power_output.min' = '10.0',
    'fields.power_output.max' = '500.0',
    'fields.efficiency.min' = '0.85',
    'fields.efficiency.max' = '0.98',
    'fields.status.length' = '1',
    'fields.alert_level.length' = '1'
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

-- ODS层：原始数据层（Fluss存储优势：支持UPDATE/DELETE）
CREATE TABLE IF NOT EXISTS ods_device_raw (
    device_id STRING PRIMARY KEY NOT ENFORCED,
    voltage DOUBLE,
    current_val DOUBLE,  -- 避免保留字冲突
    temperature DOUBLE,
    power_output DOUBLE,
    efficiency DOUBLE,
    status STRING,
    alert_level STRING,
    event_time TIMESTAMP(3)
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- DWD层：数据明细层
CREATE TABLE IF NOT EXISTS dwd_device_detail (
    device_id STRING PRIMARY KEY NOT ENFORCED,
    device_name STRING,
    device_type STRING,
    location STRING,
    voltage_level STRING,
    capacity_mw DOUBLE,
    real_time_voltage DOUBLE,
    real_time_current DOUBLE,
    real_time_temperature DOUBLE,
    efficiency_rate DOUBLE,
    status_desc STRING,
    alert_level_desc STRING,
    last_update_time TIMESTAMP(3)
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- DWS层：数据汇总层
CREATE TABLE IF NOT EXISTS dws_device_summary (
    summary_id STRING PRIMARY KEY NOT ENFORCED,
    location STRING,
    device_type STRING,
    device_count BIGINT,
    avg_efficiency DOUBLE,
    avg_temperature DOUBLE,
    avg_power_output DOUBLE,
    high_alert_count BIGINT,
    summary_time TIMESTAMP(3)
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- ADS层：应用数据层
CREATE TABLE IF NOT EXISTS ads_device_report (
    report_id STRING PRIMARY KEY NOT ENFORCED,
    report_type STRING,
    location STRING,
    total_devices BIGINT,
    efficiency_score DOUBLE,
    health_status STRING,
    alert_summary STRING,
    performance_grade STRING,
    report_time TIMESTAMP(3)
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- ===============================================
-- 3. 回到Default Catalog创建PostgreSQL Sink
-- ===============================================
-- ✅ 恢复PostgreSQL表创建，JDBC连接器已修复

USE CATALOG default_catalog;

-- 💎 PostgreSQL最终结果表 - 体现Fluss vs Kafka架构优势
-- Kafka架构：需要Kafka → Connector → 多个中间件 → PostgreSQL
-- Fluss架构：直接 Fluss → PostgreSQL，一体化数仓
CREATE TABLE IF NOT EXISTS postgres_device_final_report (
    report_id STRING,
    report_type STRING,
    location STRING,
    total_devices BIGINT,
    efficiency_score DOUBLE,
    health_status STRING,
    alert_summary STRING,
    performance_grade STRING,
    report_time TIMESTAMP(3),
    PRIMARY KEY (report_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'device_final_report',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024'
);

-- ===============================================
-- 4. 数据流作业：ODS层数据采集
-- ===============================================

-- 🚀 ODS层：CDC数据实时同步到Fluss
-- 💡 Fluss优势：比Kafka更高效的CDC同步，支持UPDATE/DELETE
INSERT INTO fluss_catalog.fluss.ods_device_raw
SELECT 
    device_id,
    voltage,
    current_val,
    temperature,
    power_output,
    efficiency,
    status,
    alert_level,
    event_time
FROM device_raw_stream;

-- ===============================================
-- 5. DWD层：数据清洗和明细化
-- 💡 Fluss vs Kafka优势：统一存储计算，无需外部数据处理引擎
-- ===============================================

-- 🚀 DWD层：设备明细数据处理（实时OLAP计算）
INSERT INTO fluss_catalog.fluss.dwd_device_detail
SELECT 
    device_id,
    CONCAT('智能电力设备_', device_id) as device_name,
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
    CASE 
        WHEN voltage > 230 THEN '220kV'
        WHEN voltage > 225 THEN '110kV'
        ELSE '35kV'
    END as voltage_level,
    power_output as capacity_mw,
    voltage as real_time_voltage,
    current_val as real_time_current,
    temperature as real_time_temperature,
    efficiency as efficiency_rate,
    CASE 
        WHEN status = 'A' THEN 'ACTIVE'
        WHEN status = 'M' THEN 'MAINTENANCE'
        WHEN status = 'O' THEN 'OFFLINE'
        ELSE 'UNKNOWN'
    END as status_desc,
    CASE 
        WHEN alert_level = 'H' THEN 'HIGH'
        WHEN alert_level = 'M' THEN 'MEDIUM'
        WHEN alert_level = 'L' THEN 'LOW'
        ELSE 'NORMAL'
    END as alert_level_desc,
    event_time as last_update_time
FROM fluss_catalog.fluss.ods_device_raw;

-- ===============================================
-- 6. DWS层：数据汇总统计
-- ===============================================

-- 🚀 DWS层：按地区和设备类型汇总
INSERT INTO fluss_catalog.fluss.dws_device_summary
SELECT 
    CONCAT(location, '_', device_type, '_', CAST(DATE_FORMAT(last_update_time, 'yyyyMMddHH') AS STRING)) as summary_id,
    location,
    device_type,
    COUNT(*) as device_count,
    AVG(efficiency_rate) as avg_efficiency,
    AVG(real_time_temperature) as avg_temperature,
    AVG(capacity_mw) as avg_power_output,
    SUM(CASE WHEN alert_level_desc = 'HIGH' THEN 1 ELSE 0 END) as high_alert_count,
    CURRENT_TIMESTAMP as summary_time
FROM fluss_catalog.fluss.dwd_device_detail
GROUP BY location, device_type, DATE_FORMAT(last_update_time, 'yyyyMMddHH');

-- ===============================================
-- 7. ADS层：应用层报表生成
-- ===============================================

-- 🚀 ADS层：生成设备健康度报表
INSERT INTO fluss_catalog.fluss.ads_device_report
SELECT 
    CONCAT('RPT_', location, '_', CAST(DATE_FORMAT(summary_time, 'yyyyMMddHH') AS STRING)) as report_id,
    '设备健康度报表' as report_type,
    location,
    SUM(device_count) as total_devices,
    AVG(avg_efficiency) * 100 as efficiency_score,
    CASE 
        WHEN AVG(avg_efficiency) > 0.95 THEN 'EXCELLENT'
        WHEN AVG(avg_efficiency) > 0.90 THEN 'GOOD'
        WHEN AVG(avg_efficiency) > 0.85 THEN 'AVERAGE'
        ELSE 'POOR'
    END as health_status,
    CONCAT('高危告警: ', CAST(SUM(high_alert_count) AS STRING), ' 个') as alert_summary,
    CASE 
        WHEN AVG(avg_efficiency) > 0.95 AND SUM(high_alert_count) < 5 THEN 'A级'
        WHEN AVG(avg_efficiency) > 0.90 AND SUM(high_alert_count) < 10 THEN 'B级'
        WHEN AVG(avg_efficiency) > 0.85 AND SUM(high_alert_count) < 20 THEN 'C级'
        ELSE 'D级'
    END as performance_grade,
    CURRENT_TIMESTAMP as report_time
FROM fluss_catalog.fluss.dws_device_summary
GROUP BY location, DATE_FORMAT(summary_time, 'yyyyMMddHH');

-- ===============================================
-- 8. 数据回流PostgreSQL
-- ===============================================

-- 切换回default catalog执行JDBC INSERT
USE CATALOG default_catalog;

-- 🚀 最终回流：Fluss ADS层 → PostgreSQL
INSERT INTO postgres_device_final_report
SELECT 
    report_id,
    report_type,
    location,
    total_devices,
    efficiency_score,
    health_status,
    alert_summary,
    performance_grade,
    report_time
FROM fluss_catalog.fluss.ads_device_report;

-- ===============================================
-- 9. 数据增删改测试查询
-- ===============================================

/*
-- 查看PostgreSQL中的最终结果
SELECT * FROM postgres_device_final_report ORDER BY report_time DESC LIMIT 10;

-- 查看Fluss各层数据量
SELECT 'ODS' as layer, COUNT(*) as record_count FROM fluss_catalog.fluss.ods_device_raw
UNION ALL
SELECT 'DWD' as layer, COUNT(*) as record_count FROM fluss_catalog.fluss.dwd_device_detail
UNION ALL
SELECT 'DWS' as layer, COUNT(*) as record_count FROM fluss_catalog.fluss.dws_device_summary
UNION ALL
SELECT 'ADS' as layer, COUNT(*) as record_count FROM fluss_catalog.fluss.ads_device_report;

-- 测试更新操作
UPDATE fluss_catalog.fluss.dwd_device_detail 
SET efficiency_rate = 0.99 
WHERE device_id = '100001';

-- 测试删除操作
DELETE FROM fluss_catalog.fluss.ods_device_raw 
WHERE device_id = '100002';
*/ 