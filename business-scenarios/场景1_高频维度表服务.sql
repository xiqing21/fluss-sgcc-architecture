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
        WHEN device_id LIKE '%001' OR device_id LIKE '%004' OR device_id LIKE '%007' THEN '变压器'
        WHEN device_id LIKE '%002' OR device_id LIKE '%005' OR device_id LIKE '%008' THEN '发电机'
        ELSE '配电设备'
    END as device_type,
    CASE 
        WHEN device_id LIKE '%001' OR device_id LIKE '%006' THEN '北京'
        WHEN device_id LIKE '%002' OR device_id LIKE '%007' THEN '上海'
        WHEN device_id LIKE '%003' OR device_id LIKE '%008' THEN '广州'
        WHEN device_id LIKE '%004' OR device_id LIKE '%009' THEN '深圳'
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
-- 🎯 9. 增删改查监控测试 + 验证逻辑
-- ===============================================

-- 📊 【监控 1】查看数据流初始状态
SELECT '=== 🎯 场景1：数据流初始状态监控 ===' as monitor_title;

-- 查看Fluss各层数据量
SELECT 'ODS层数据量' as layer, COUNT(*) as record_count FROM fluss_catalog.fluss.ods_device_raw
UNION ALL
SELECT 'DWD层数据量' as layer, COUNT(*) as record_count FROM fluss_catalog.fluss.dwd_device_detail
UNION ALL
SELECT 'DWS层数据量' as layer, COUNT(*) as record_count FROM fluss_catalog.fluss.dws_device_summary
UNION ALL
SELECT 'ADS层数据量' as layer, COUNT(*) as record_count FROM fluss_catalog.fluss.ads_device_report;

-- 查看PostgreSQL最终结果数据量
SELECT 'PostgreSQL结果表' as layer, COUNT(*) as record_count FROM postgres_device_final_report;

-- 📊 【监控 2】数据质量检查
SELECT '=== 📊 数据质量监控 ===' as monitor_title;

-- 检查ODS层数据分布
SELECT 
    status,
    alert_level,
    COUNT(*) as count,
    AVG(voltage) as avg_voltage,
    AVG(efficiency) as avg_efficiency
FROM fluss_catalog.fluss.ods_device_raw 
GROUP BY status, alert_level
ORDER BY count DESC;

-- 检查DWD层地区分布
SELECT 
    location,
    device_type,
    COUNT(*) as device_count,
    AVG(efficiency_rate) as avg_efficiency,
    AVG(real_time_temperature) as avg_temp
FROM fluss_catalog.fluss.dwd_device_detail 
GROUP BY location, device_type
ORDER BY device_count DESC;

-- 🔥 【测试 1】增加操作 - 从PostgreSQL源头插入测试数据
SELECT '=== 🔥 增加操作测试（CDC源头）===' as test_title;

-- 💡 核心逻辑：向PostgreSQL源数据库插入数据 → device_raw_stream CDC捕获 → 流向Fluss各层 → 最终sink到PostgreSQL
-- 创建JDBC表用于向PostgreSQL源数据库插入数据
CREATE TABLE insert_to_postgres_source (
    device_id STRING,
    voltage DOUBLE,
    current_val DOUBLE,
    temperature DOUBLE,
    power_output DOUBLE,
    efficiency DOUBLE,
    status STRING,
    alert_level STRING,
    event_time TIMESTAMP(3),
    PRIMARY KEY (device_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-source:5432/sgcc_source_db',
    'table-name' = 'device_raw_data',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024'
);

-- 向PostgreSQL源数据库插入测试数据 → device_raw_stream会自动CDC捕获 → 流向Fluss
INSERT INTO insert_to_postgres_source VALUES
('TEST001', 235.5, 150.0, 45.2, 350.8, 0.96, 'A', 'H', CURRENT_TIMESTAMP),
('TEST002', 228.3, 125.5, 38.7, 280.3, 0.94, 'M', 'L', CURRENT_TIMESTAMP),
('TEST003', 240.1, 180.2, 52.1, 420.5, 0.92, 'A', 'M', CURRENT_TIMESTAMP);

-- 等待device_raw_stream CDC捕获并流向Fluss（实时流处理）
SELECT 'PostgreSQL→CDC→Fluss数据流等待' as status, 'Waiting for full data pipeline...' as message;

-- 验证完整数据流：PostgreSQL源 → device_raw_stream(CDC) → Fluss ODS层
SELECT 'Step1: CDC→ODS层验证' as step, COUNT(*) as new_records 
FROM fluss_catalog.fluss.ods_device_raw 
WHERE device_id LIKE 'TEST%';

-- 验证数据流：ODS → DWD层（通过数据流作业自动处理）
SELECT 'Step2: ODS→DWD层验证' as step, COUNT(*) as new_records 
FROM fluss_catalog.fluss.dwd_device_detail 
WHERE device_id LIKE 'TEST%';

-- 验证数据流：DWD → DWS层（聚合处理）
SELECT 'Step3: DWD→DWS层验证' as step, COUNT(*) as new_records 
FROM fluss_catalog.fluss.dws_device_summary 
WHERE summary_id LIKE '%TEST%';

-- 验证数据流：DWS → ADS层（报表生成）
SELECT 'Step4: DWS→ADS层验证' as step, COUNT(*) as new_records 
FROM fluss_catalog.fluss.ads_device_report 
WHERE report_id LIKE '%TEST%';

-- 验证最终数据流：ADS → PostgreSQL Sink（完整链路）
SELECT 'Step5: ADS→PostgreSQL Sink验证' as step, COUNT(*) as new_records 
FROM postgres_device_final_report 
WHERE report_id LIKE '%TEST%';

-- 🔄 【测试 2】更新操作测试（CDC源头）
SELECT '=== 🔄 更新操作测试（CDC源头）===' as test_title;

-- 更新前状态查询（Fluss各层状态）
SELECT 'UPDATE前ODS层状态' as layer, device_id, efficiency, status, alert_level
FROM fluss_catalog.fluss.ods_device_raw 
WHERE device_id = 'TEST001';

SELECT 'UPDATE前DWD层状态' as layer, device_id, efficiency_rate, status_desc, alert_level_desc
FROM fluss_catalog.fluss.dwd_device_detail 
WHERE device_id = 'TEST001';

-- 通过JDBC向PostgreSQL源数据库执行更新 → device_raw_stream CDC自动捕获 → 流向Fluss
UPDATE insert_to_postgres_source 
SET efficiency = 0.99, status = 'A', alert_level = 'L'
WHERE device_id = 'TEST001';

-- 等待完整更新数据流传播
SELECT 'PostgreSQL更新→CDC→Fluss更新流等待' as status, 'Waiting for update propagation...' as message;

-- 验证更新流：PostgreSQL源 → device_raw_stream(CDC) → Fluss ODS层
SELECT 'UPDATE Step1: CDC→ODS更新验证' as step, device_id, efficiency, status, alert_level
FROM fluss_catalog.fluss.ods_device_raw 
WHERE device_id = 'TEST001';

-- 验证更新流：ODS → DWD层（实时更新传播）
SELECT 'UPDATE Step2: ODS→DWD更新验证' as step, device_id, efficiency_rate, status_desc, alert_level_desc
FROM fluss_catalog.fluss.dwd_device_detail 
WHERE device_id = 'TEST001';

-- 验证更新最终传播到PostgreSQL Sink
SELECT 'UPDATE Step3: 最终Sink更新验证' as step, report_id, efficiency_score, health_status, report_time
FROM postgres_device_final_report 
WHERE report_id LIKE '%TEST%' 
ORDER BY report_time DESC 
LIMIT 3;

-- ❌ 【测试 3】删除操作测试（CDC源头）
SELECT '=== ❌ 删除操作测试（CDC源头）===' as test_title;

-- 删除前各层数据统计
SELECT 'DELETE前ODS层统计' as layer, COUNT(*) as total_count 
FROM fluss_catalog.fluss.ods_device_raw;

SELECT 'DELETE前DWD层统计' as layer, COUNT(*) as total_count 
FROM fluss_catalog.fluss.dwd_device_detail;

-- 通过JDBC向PostgreSQL源数据库执行删除 → device_raw_stream CDC自动捕获 → 流向Fluss
DELETE FROM insert_to_postgres_source 
WHERE device_id = 'TEST003';

-- 等待完整删除数据流传播
SELECT 'PostgreSQL删除→CDC→Fluss删除流等待' as status, 'Waiting for delete propagation...' as message;

-- 验证删除流：PostgreSQL源 → device_raw_stream(CDC) → Fluss ODS层
SELECT 'DELETE Step1: CDC→ODS删除验证(应为0)' as step, COUNT(*) as should_be_zero 
FROM fluss_catalog.fluss.ods_device_raw 
WHERE device_id = 'TEST003';

-- 验证删除流：ODS → DWD层（实时删除传播）
SELECT 'DELETE Step2: ODS→DWD删除验证(应为0)' as step, COUNT(*) as should_be_zero 
FROM fluss_catalog.fluss.dwd_device_detail 
WHERE device_id = 'TEST003';

-- 验证删除对后续聚合层的影响
SELECT 'DELETE Step3: DWS层影响验证' as step, COUNT(*) as summary_records
FROM fluss_catalog.fluss.dws_device_summary 
WHERE summary_id LIKE '%TEST003%';

-- 验证最终PostgreSQL Sink的数据一致性
SELECT 'DELETE Step4: 最终Sink一致性验证' as step, COUNT(*) as final_records
FROM postgres_device_final_report 
WHERE report_id LIKE '%TEST%';

-- 📈 【监控 3】性能监控
SELECT '=== 📈 性能监控 ===' as monitor_title;

-- 查询响应时间测试
SELECT 'Fluss查询性能' as metric, COUNT(*) as total_records, 
       AVG(efficiency_rate) as avg_efficiency,
       MIN(last_update_time) as earliest_time,
       MAX(last_update_time) as latest_time
FROM fluss_catalog.fluss.dwd_device_detail;

-- 数据一致性检查
SELECT 'ODS→DWD一致性' as consistency_check,
       ods.record_count as ods_count,
       dwd.record_count as dwd_count,
       CASE WHEN ods.record_count = dwd.record_count THEN '✅ 一致' ELSE '❌ 不一致' END as status
FROM (SELECT COUNT(*) as record_count FROM fluss_catalog.fluss.ods_device_raw WHERE device_id NOT LIKE 'TEST%') ods,
     (SELECT COUNT(*) as record_count FROM fluss_catalog.fluss.dwd_device_detail WHERE device_id NOT LIKE 'TEST%') dwd;

-- 📋 【监控 4】最终结果验证
SELECT '=== 📋 最终结果验证 ===' as monitor_title;

-- 查看PostgreSQL中的最终结果
SELECT '最终报表结果' as result_type, 
       location, 
       total_devices, 
       efficiency_score,
       health_status,
       performance_grade,
       report_time
FROM postgres_device_final_report 
ORDER BY report_time DESC 
LIMIT 10;

-- 🎯 【总结】场景1测试完成状态
SELECT '=== 🎯 场景1测试完成总结 ===' as summary_title;

SELECT 
    '数据流完整性' as metric,
    CONCAT('ODS:', ods_count, ' | DWD:', dwd_count, ' | DWS:', dws_count, ' | ADS:', ads_count, ' | PostgreSQL:', pg_count) as layer_counts
FROM (
    SELECT 
        (SELECT COUNT(*) FROM fluss_catalog.fluss.ods_device_raw) as ods_count,
        (SELECT COUNT(*) FROM fluss_catalog.fluss.dwd_device_detail) as dwd_count,
        (SELECT COUNT(*) FROM fluss_catalog.fluss.dws_device_summary) as dws_count,
        (SELECT COUNT(*) FROM fluss_catalog.fluss.ads_device_report) as ads_count,
        (SELECT COUNT(*) FROM postgres_device_final_report) as pg_count
);

-- ✅ 【验证】增删改查操作成功验证
SELECT '增删改查验证结果' as final_verification,
       CASE 
           WHEN (SELECT COUNT(*) FROM fluss_catalog.fluss.ods_device_raw WHERE device_id = 'TEST001') = 1 THEN '✅ 增加成功'
           ELSE '❌ 增加失败'
       END as insert_status,
       CASE 
           WHEN (SELECT efficiency FROM fluss_catalog.fluss.ods_device_raw WHERE device_id = 'TEST001') = 0.99 THEN '✅ 更新成功'
           ELSE '❌ 更新失败'
       END as update_status,
       CASE 
           WHEN (SELECT COUNT(*) FROM fluss_catalog.fluss.ods_device_raw WHERE device_id = 'TEST003') = 0 THEN '✅ 删除成功'
           ELSE '❌ 删除失败'
       END as delete_status;
