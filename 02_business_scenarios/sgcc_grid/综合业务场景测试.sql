-- ===============================================
-- 🔴 综合业务场景测试 + 数仓分层 + PostgreSQL回流
-- 🔥 Fluss vs Kafka 架构升级对比（综合场景）：
-- 1. ✅ 多数据源统一：Fluss一体化处理，Kafka需要多套Connector和中间件
-- 2. ✅ 复杂关联计算：内置SQL引擎，Kafka需要Kafka Streams + 外部计算引擎  
-- 3. ✅ 实时数仓：ODS→DWD→DWS→ADS一站式，Kafka需要Lambda架构
-- 4. ✅ 运维简化：单一平台管理，Kafka需要管理多个组件(Zookeeper、Broker、Connect等)
-- 数据流：PostgreSQL CDC多源 → Fluss统一数仓 → 智能分析 → PostgreSQL综合报表
-- ===============================================

SET 'sql-client.execution.result-mode' = 'tableau';

-- ===============================================
-- 1. 在Default Catalog创建DataGen双数据源
-- ===============================================

-- 实时电力调度数据流
CREATE TEMPORARY TABLE power_dispatch_stream (
    dispatch_id STRING,
    event_time TIMESTAMP(3),
    grid_region STRING,
    total_demand_mw DOUBLE,
    total_supply_mw DOUBLE,
    frequency_hz DOUBLE,
    voltage_level_kv DOUBLE,
    load_balance_status STRING,
    emergency_level STRING,
    dispatch_command STRING,
    response_time_ms BIGINT,
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND  -- 修复：使用SECOND而不是MILLIS
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '6000',  -- 6000 QPS
    'fields.dispatch_id.kind' = 'sequence',
    'fields.dispatch_id.start' = '1',
    'fields.dispatch_id.end' = '999999',
    'fields.grid_region.length' = '10',
    'fields.total_demand_mw.min' = '8000.0',
    'fields.total_demand_mw.max' = '12000.0',
    'fields.total_supply_mw.min' = '8000.0',
    'fields.total_supply_mw.max' = '12000.0',
    'fields.frequency_hz.min' = '49.8',
    'fields.frequency_hz.max' = '50.2',
    'fields.voltage_level_kv.min' = '500.0',
    'fields.voltage_level_kv.max' = '800.0',
    'fields.load_balance_status.length' = '8',
    'fields.emergency_level.length' = '6',
    'fields.dispatch_command.length' = '20',
    'fields.response_time_ms.min' = '10',
    'fields.response_time_ms.max' = '100'
);

-- 实时设备维度数据流
CREATE TEMPORARY TABLE device_dimension_stream (
    device_id STRING,
    device_name STRING,
    device_type STRING,
    location STRING,
    capacity_mw DOUBLE,
    status STRING,
    real_time_voltage DOUBLE,
    real_time_current DOUBLE,
    real_time_temperature DOUBLE,
    efficiency_rate DOUBLE,
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '4000',  -- 4000 QPS
    'fields.device_id.kind' = 'sequence',
    'fields.device_id.start' = '100000',
    'fields.device_id.end' = '110000',
    'fields.device_name.length' = '20',
    'fields.device_type.length' = '10',
    'fields.location.length' = '15',
    'fields.capacity_mw.min' = '50.0',
    'fields.capacity_mw.max' = '800.0',
    'fields.status.length' = '8',
    'fields.real_time_voltage.min' = '210.0',
    'fields.real_time_voltage.max' = '250.0',
    'fields.real_time_current.min' = '50.0',
    'fields.real_time_current.max' = '200.0',
    'fields.real_time_temperature.min' = '20.0',
    'fields.real_time_temperature.max' = '80.0',
    'fields.efficiency_rate.min' = '0.80',
    'fields.efficiency_rate.max' = '0.98'
);

-- ===============================================
-- 2. 创建Fluss Catalog和智能电网数仓分层表
-- ===============================================

CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

USE CATALOG fluss_catalog;
CREATE DATABASE IF NOT EXISTS fluss;
USE fluss;

-- ODS层：电力调度原始数据
CREATE TABLE ods_power_dispatch_raw (
    dispatch_id STRING PRIMARY KEY NOT ENFORCED,
    event_time TIMESTAMP(3),
    grid_region STRING,
    total_demand_mw DOUBLE,
    total_supply_mw DOUBLE,
    frequency_hz DOUBLE,
    voltage_level_kv DOUBLE,
    load_balance_status STRING,
    emergency_level STRING,
    dispatch_command STRING,
    response_time_ms BIGINT
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- ODS层：设备维度原始数据
CREATE TABLE ods_device_dimension_raw (
    device_id STRING PRIMARY KEY NOT ENFORCED,
    device_name STRING,
    device_type STRING,
    location STRING,
    capacity_mw DOUBLE,
    status STRING,
    real_time_voltage DOUBLE,
    real_time_current DOUBLE,
    real_time_temperature DOUBLE,
    efficiency_rate DOUBLE,
    event_time TIMESTAMP(3)
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- DWD层：智能电网运行明细
CREATE TABLE dwd_smart_grid_detail (
    grid_detail_id STRING PRIMARY KEY NOT ENFORCED,
    dispatch_id STRING,
    device_id STRING,
    grid_region STRING,
    device_location STRING,
    device_type STRING,
    dispatch_time TIMESTAMP(3),
    supply_demand_balance DOUBLE,
    frequency_stability STRING,
    voltage_quality STRING,
    device_health_score DOUBLE,
    grid_efficiency DOUBLE,
    risk_level STRING,
    operational_status STRING
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- DWS层：电网运行汇总
CREATE TABLE dws_grid_operation_summary (
    summary_id STRING PRIMARY KEY NOT ENFORCED,
    grid_region STRING,
    time_window STRING,
    total_dispatches BIGINT,
    total_devices BIGINT,
    avg_supply_demand_balance DOUBLE,
    frequency_stability_rate DOUBLE,
    voltage_quality_rate DOUBLE,
    avg_device_health DOUBLE,
    grid_efficiency_score DOUBLE,
    high_risk_incidents BIGINT,
    emergency_responses BIGINT
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- ADS层：智能电网综合报表
CREATE TABLE ads_smart_grid_comprehensive_report (
    report_id STRING PRIMARY KEY NOT ENFORCED,
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
    report_time TIMESTAMP(3)
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- ===============================================
-- 3. 回到Default Catalog创建PostgreSQL Sink
-- ===============================================

USE CATALOG default_catalog;

-- PostgreSQL智能电网综合分析结果表
CREATE TABLE postgres_smart_grid_comprehensive_result (
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
    'url' = 'jdbc:postgresql://postgres-sink-sgcc:5432/sgcc_target',
    'table-name' = 'smart_grid_comprehensive_result',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass'
);

-- ===============================================
-- 4. ODS层：双流数据采集
-- ===============================================

-- 🚀 ODS层：采集电力调度数据
INSERT INTO fluss_catalog.fluss.ods_power_dispatch_raw
SELECT 
    dispatch_id,
    event_time,
    grid_region,
    total_demand_mw,
    total_supply_mw,
    frequency_hz,
    voltage_level_kv,
    load_balance_status,
    emergency_level,
    dispatch_command,
    response_time_ms
FROM power_dispatch_stream;

-- 🚀 ODS层：采集设备维度数据
INSERT INTO fluss_catalog.fluss.ods_device_dimension_raw
SELECT 
    device_id,
    device_name,
    device_type,
    location,
    capacity_mw,
    status,
    real_time_voltage,
    real_time_current,
    real_time_temperature,
    efficiency_rate,
    event_time
FROM device_dimension_stream;

-- ===============================================
-- 5. DWD层：智能电网数据关联处理
-- ===============================================

-- 🚀 DWD层：电网调度与设备关联分析
INSERT INTO fluss_catalog.fluss.dwd_smart_grid_detail
SELECT 
    CONCAT(d.dispatch_id, '_', dev.device_id) as grid_detail_id,
    d.dispatch_id,
    dev.device_id,
    d.grid_region,
    dev.location as device_location,
    dev.device_type,
    d.event_time as dispatch_time,
    (d.total_supply_mw - d.total_demand_mw) as supply_demand_balance,
    -- 频率稳定性评估
    CASE 
        WHEN d.frequency_hz BETWEEN 49.9 AND 50.1 THEN 'STABLE'
        WHEN d.frequency_hz BETWEEN 49.8 AND 50.2 THEN 'ACCEPTABLE'
        ELSE 'UNSTABLE'
    END as frequency_stability,
    -- 电压质量评估
    CASE 
        WHEN d.voltage_level_kv > 750 AND dev.real_time_voltage > 240 THEN 'EXCELLENT'
        WHEN d.voltage_level_kv > 600 AND dev.real_time_voltage > 230 THEN 'GOOD'
        WHEN dev.real_time_voltage > 220 THEN 'ACCEPTABLE'
        ELSE 'POOR'
    END as voltage_quality,
    -- 设备健康评分
    (dev.efficiency_rate * 0.4 + (250 - dev.real_time_temperature) * 0.003 + 
     CASE WHEN dev.status = 'ACTIVE' THEN 0.3 ELSE 0.1 END) * 100 as device_health_score,
    -- 电网效率计算
    (d.total_supply_mw / d.total_demand_mw) * dev.efficiency_rate * 100 as grid_efficiency,
    -- 风险等级评估
    CASE 
        WHEN d.emergency_level = 'HIGH' OR dev.real_time_temperature > 75 THEN 'HIGH_RISK'
        WHEN d.emergency_level = 'MEDIUM' OR dev.efficiency_rate < 0.85 THEN 'MEDIUM_RISK'
        WHEN d.load_balance_status <> 'BALANCED' THEN 'LOW_RISK'
        ELSE 'NORMAL'
    END as risk_level,
    -- 运行状态评估
    CASE 
        WHEN d.frequency_hz BETWEEN 49.9 AND 50.1 AND dev.efficiency_rate > 0.95 THEN 'OPTIMAL'
        WHEN d.frequency_hz BETWEEN 49.8 AND 50.2 AND dev.efficiency_rate > 0.90 THEN 'GOOD'
        WHEN dev.efficiency_rate > 0.85 THEN 'ACCEPTABLE'
        ELSE 'NEEDS_ATTENTION'
    END as operational_status
FROM fluss_catalog.fluss.ods_power_dispatch_raw d
CROSS JOIN fluss_catalog.fluss.ods_device_dimension_raw dev
WHERE d.grid_region = SUBSTRING(dev.location, 1, 2);  -- 简单的地理关联

-- ===============================================
-- 6. DWS层：电网运行汇总统计
-- ===============================================

-- 🚀 DWS层：按地区和时间窗口汇总电网运行状况
INSERT INTO fluss_catalog.fluss.dws_grid_operation_summary
SELECT 
    CONCAT(grid_region, '_', CAST(DATE_FORMAT(dispatch_time, 'yyyyMMddHH') AS STRING)) as summary_id,
    grid_region,
    DATE_FORMAT(dispatch_time, 'yyyyMMddHH') as time_window,
    COUNT(DISTINCT dispatch_id) as total_dispatches,
    COUNT(DISTINCT device_id) as total_devices,
    AVG(supply_demand_balance) as avg_supply_demand_balance,
    (SUM(CASE WHEN frequency_stability = 'STABLE' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as frequency_stability_rate,
    (SUM(CASE WHEN voltage_quality IN ('EXCELLENT', 'GOOD') THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as voltage_quality_rate,
    AVG(device_health_score) as avg_device_health,
    AVG(grid_efficiency) as grid_efficiency_score,
    SUM(CASE WHEN risk_level = 'HIGH_RISK' THEN 1 ELSE 0 END) as high_risk_incidents,
    SUM(CASE WHEN operational_status = 'NEEDS_ATTENTION' THEN 1 ELSE 0 END) as emergency_responses
FROM fluss_catalog.fluss.dwd_smart_grid_detail
GROUP BY grid_region, DATE_FORMAT(dispatch_time, 'yyyyMMddHH');

-- ===============================================
-- 7. ADS层：智能电网综合报表生成
-- ===============================================

-- 🚀 ADS层：生成智能电网综合分析报表
INSERT INTO fluss_catalog.fluss.ads_smart_grid_comprehensive_report
SELECT 
    CONCAT('GRID_COMPREHENSIVE_', grid_region, '_', CAST(time_window AS STRING)) as report_id,
    '智能电网综合运行报表' as report_type,
    time_window as analysis_period,
    grid_region,
    -- 电网稳定性指数
    (frequency_stability_rate * 0.4 + voltage_quality_rate * 0.3 + 
     CASE WHEN ABS(avg_supply_demand_balance) < 100 THEN 30.0 ELSE 10.0 END) as grid_stability_index,
    -- 运行效率
    LEAST(grid_efficiency_score, 100.0) as operational_efficiency,
    -- 能源优化评分
    (avg_device_health * 0.6 + (100 - high_risk_incidents * 5) * 0.4) as energy_optimization_score,
    -- 可靠性评级
    CASE 
        WHEN frequency_stability_rate > 95 AND voltage_quality_rate > 90 THEN 'A+'
        WHEN frequency_stability_rate > 90 AND voltage_quality_rate > 85 THEN 'A'
        WHEN frequency_stability_rate > 85 AND voltage_quality_rate > 80 THEN 'B+'
        WHEN frequency_stability_rate > 80 AND voltage_quality_rate > 75 THEN 'B'
        ELSE 'C'
    END as reliability_rating,
    -- 风险评估
    CASE 
        WHEN high_risk_incidents > 10 THEN 'CRITICAL_多起高风险事件，需立即干预'
        WHEN high_risk_incidents > 5 THEN 'HIGH_存在安全隐患，加强监控'
        WHEN emergency_responses > 5 THEN 'MEDIUM_运行异常较多，优化调度'
        ELSE 'LOW_运行正常，保持现状'
    END as risk_assessment,
    -- 性能趋势
    CASE 
        WHEN grid_efficiency_score > 105 THEN 'IMPROVING_效率持续提升'
        WHEN grid_efficiency_score > 100 THEN 'STABLE_运行稳定'
        WHEN grid_efficiency_score > 95 THEN 'DECLINING_效率下降'
        ELSE 'CRITICAL_性能严重下降'
    END as performance_trends,
    -- 优化建议
    CASE 
        WHEN high_risk_incidents > 10 THEN '建议立即启动应急响应，重新评估电网架构'
        WHEN voltage_quality_rate < 80 THEN '建议优化电压调节系统，提升供电质量'
        WHEN frequency_stability_rate < 85 THEN '建议调整发电机组配比，改善频率稳定性'
        WHEN avg_device_health < 80 THEN '建议加强设备维护，更换老化设备'
        ELSE '建议继续保持当前运行策略，定期优化调度算法'
    END as optimization_recommendations,
    -- 成本效益分析
    (total_dispatches * 1000 + total_devices * 500 - high_risk_incidents * 10000 - emergency_responses * 5000) as cost_benefit_analysis,
    CURRENT_TIMESTAMP as report_time
FROM fluss_catalog.fluss.dws_grid_operation_summary;

-- ===============================================
-- 8. 数据回流PostgreSQL
-- ===============================================

-- 🚀 最终回流：Fluss ADS层 → PostgreSQL
INSERT INTO postgres_smart_grid_comprehensive_result
SELECT 
    report_id,
    report_type,
    analysis_period,
    grid_region,
    grid_stability_index,
    operational_efficiency,
    energy_optimization_score,
    reliability_rating,
    risk_assessment,
    performance_trends,
    optimization_recommendations,
    cost_benefit_analysis,
    report_time
FROM fluss_catalog.fluss.ads_smart_grid_comprehensive_report;

-- ===============================================
-- 9. 综合数据增删改测试查询
-- ===============================================

/*
-- 查看PostgreSQL中的智能电网综合分析结果
SELECT * FROM postgres_smart_grid_comprehensive_result ORDER BY report_time DESC LIMIT 10;

-- 🎯 实时电网监控视图
SELECT 
    d.dispatch_id,
    d.grid_region,
    d.total_demand_mw,
    d.total_supply_mw,
    (d.total_supply_mw - d.total_demand_mw) as supply_demand_balance,
    d.frequency_hz,
    d.voltage_level_kv,
    d.load_balance_status,
    d.emergency_level,
    d.response_time_ms,
    CASE 
        WHEN d.frequency_hz < 49.9 THEN 'FREQUENCY_LOW'
        WHEN d.frequency_hz > 50.1 THEN 'FREQUENCY_HIGH'
        WHEN (d.total_supply_mw - d.total_demand_mw) < 100 THEN 'SUPPLY_SHORTAGE'
        WHEN d.voltage_level_kv < 550 THEN 'VOLTAGE_LOW'
        ELSE 'NORMAL'
    END as grid_status,
    CASE 
        WHEN d.emergency_level = 'HIGH' THEN 100
        WHEN d.emergency_level = 'MEDIUM' THEN 70
        WHEN d.emergency_level = 'LOW' THEN 40
        ELSE 20
    END as priority_score
FROM fluss_catalog.fluss.ods_power_dispatch_raw d
WHERE d.event_time >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR
ORDER BY d.event_time DESC
LIMIT 100;

-- 🎯 设备效率分析
SELECT 
    device_id,
    device_name,
    device_type,
    location,
    capacity_mw,
    efficiency_rate,
    real_time_voltage,
    real_time_current,
    real_time_temperature,
    CASE 
        WHEN efficiency_rate > 0.95 THEN 'EXCELLENT'
        WHEN efficiency_rate > 0.90 THEN 'GOOD'
        WHEN efficiency_rate > 0.85 THEN 'AVERAGE'
        ELSE 'POOR'
    END as performance_grade,
    CASE 
        WHEN real_time_temperature > 70 THEN '立即检修'
        WHEN efficiency_rate < 0.85 THEN '计划维护'
        ELSE '正常运行'
    END as maintenance_suggestion
FROM fluss_catalog.fluss.ods_device_dimension_raw
WHERE efficiency_rate < 0.95
ORDER BY efficiency_rate ASC, real_time_temperature DESC
LIMIT 50;

-- ===============================================
-- 🎯 增删改查监控测试 + 验证逻辑 (CDC源头)
-- ===============================================

-- 📊 【监控 1】综合业务初始状态
SELECT '=== 🎯 综合业务场景：智能电网数据监控 ===' as monitor_title;

-- 查看各层数据量
SELECT '电力调度数据' as layer, COUNT(*) as record_count FROM fluss_catalog.fluss.ods_power_dispatch_raw
UNION ALL
SELECT '设备维度数据' as layer, COUNT(*) as record_count FROM fluss_catalog.fluss.ods_device_dimension_raw
UNION ALL
SELECT '智能电网明细' as layer, COUNT(*) as record_count FROM fluss_catalog.fluss.dwd_smart_grid_detail
UNION ALL
SELECT '电网运行汇总' as layer, COUNT(*) as record_count FROM fluss_catalog.fluss.dws_grid_operation_summary
UNION ALL
SELECT '综合分析报表' as layer, COUNT(*) as record_count FROM fluss_catalog.fluss.ads_smart_grid_comprehensive_report;

-- 🔥 【测试 1】增加操作 - 从PostgreSQL源头插入测试数据
SELECT '=== 🔥 增加操作测试（CDC源头）===' as test_title;

-- 创建PostgreSQL电力调度数据插入连接
CREATE TABLE postgres_dispatch_insert (
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
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-source:5432/sgcc_source_db',
    'table-name' = 'power_dispatch_data',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024'
);

-- 创建PostgreSQL设备维度数据插入连接
CREATE TABLE postgres_device_dimension_insert (
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
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-source:5432/sgcc_source_db',
    'table-name' = 'device_dimension_data',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024'
);

-- 向PostgreSQL源插入电力调度测试数据
INSERT INTO postgres_dispatch_insert VALUES
('DISPATCH_TEST001', '华北电网', CURRENT_TIMESTAMP, 25000.0, 30000.0, 'NORMAL', 'BALANCED', 50.0, 220.5),
('DISPATCH_TEST002', '华东电网', CURRENT_TIMESTAMP, 35000.0, 32000.0, 'HIGH', 'STRESSED', 49.8, 218.2),
('DISPATCH_TEST003', '华南电网', CURRENT_TIMESTAMP, 28000.0, 35000.0, 'LOW', 'SURPLUS', 50.2, 221.8);

-- 向PostgreSQL源插入设备维度测试数据
INSERT INTO postgres_device_dimension_insert VALUES
('GRID_TEST001', '华北变压器_TEST001', '变压器', '北京', 500.0, CURRENT_TIMESTAMP, '华为', 'HW-T500', 0.96, 'NORMAL', 235.5, 150.0, 45.2),
('GRID_TEST002', '华东发电机_TEST002', '发电机', '上海', 800.0, CURRENT_TIMESTAMP, '西门子', 'SIE-G800', 0.94, 'WARNING', 228.3, 125.5, 52.7),
('GRID_TEST003', '华南配电_TEST003', '配电设备', '深圳', 300.0, CURRENT_TIMESTAMP, 'ABB', 'ABB-D300', 0.92, 'MAINTENANCE', 220.1, 180.2, 38.9);

-- 等待CDC同步
SELECT 'CDC综合同步等待' as status, 'Waiting for comprehensive CDC sync...' as message;

-- 验证电力调度数据CDC同步
SELECT 'CDC调度数据同步验证' as verification, COUNT(*) as new_records 
FROM fluss_catalog.fluss.ods_power_dispatch_raw 
WHERE dispatch_id LIKE 'DISPATCH_TEST%';

-- 验证设备维度数据CDC同步
SELECT 'CDC设备数据同步验证' as verification, COUNT(*) as new_records 
FROM fluss_catalog.fluss.ods_device_dimension_raw 
WHERE device_id LIKE 'GRID_TEST%';

-- 🔄 【测试 2】更新操作测试（CDC源头）
SELECT '=== 🔄 更新操作测试（CDC源头）===' as test_title;

-- 更新前状态查询
SELECT 'UPDATE前调度状态' as status, dispatch_id, emergency_level, load_balance_status
FROM postgres_dispatch_insert 
WHERE dispatch_id = 'DISPATCH_TEST002';

-- 在PostgreSQL源执行电力调度数据更新
UPDATE postgres_dispatch_insert 
SET emergency_level = 'CRITICAL', load_balance_status = 'IMBALANCED', load_demand_mw = 38000.0
WHERE dispatch_id = 'DISPATCH_TEST002';

-- 更新设备维度数据
UPDATE postgres_device_dimension_insert 
SET maintenance_status = 'CRITICAL', efficiency_rate = 0.88, real_time_temperature = 85.5
WHERE device_id = 'GRID_TEST002';

-- 等待CDC同步更新
SELECT 'CDC更新同步等待' as status, 'Waiting for CDC update sync...' as message;

-- 验证调度数据更新同步
SELECT 'UPDATE后调度验证' as status, dispatch_id, emergency_level, load_balance_status, load_demand_mw
FROM fluss_catalog.fluss.ods_power_dispatch_raw 
WHERE dispatch_id = 'DISPATCH_TEST002';

-- 验证设备数据更新同步
SELECT 'UPDATE后设备验证' as status, device_id, maintenance_status, efficiency_rate, real_time_temperature
FROM fluss_catalog.fluss.ods_device_dimension_raw 
WHERE device_id = 'GRID_TEST002';

-- ❌ 【测试 3】删除操作测试（CDC源头）
SELECT '=== ❌ 删除操作测试（CDC源头）===' as test_title;

-- 删除前统计
SELECT 'DELETE前调度数据统计' as phase, COUNT(*) as total_count 
FROM postgres_dispatch_insert;

-- 在PostgreSQL源执行删除
DELETE FROM postgres_dispatch_insert 
WHERE dispatch_id = 'DISPATCH_TEST003';

-- 删除设备维度数据
DELETE FROM postgres_device_dimension_insert 
WHERE device_id = 'GRID_TEST003';

-- 等待CDC同步删除
SELECT 'CDC删除同步等待' as status, 'Waiting for CDC delete sync...' as message;

-- 验证删除同步
SELECT 'DELETE调度数据验证(应为0)' as verification, COUNT(*) as should_be_zero 
FROM fluss_catalog.fluss.ods_power_dispatch_raw 
WHERE dispatch_id = 'DISPATCH_TEST003';

SELECT 'DELETE设备数据验证(应为0)' as verification, COUNT(*) as should_be_zero 
FROM fluss_catalog.fluss.ods_device_dimension_raw 
WHERE device_id = 'GRID_TEST003';

-- 📈 【监控 3】智能电网综合性能监控
SELECT '=== 📈 智能电网综合性能监控 ===' as monitor_title;

-- 验证综合分析结果变化
SELECT 
    grid_region,
    COUNT(*) as analysis_count,
    AVG(grid_stability_index) as avg_stability,
    COUNT(CASE WHEN risk_assessment LIKE 'CRITICAL%' THEN 1 END) as critical_risks,
    AVG(CASE WHEN emergency_response_time IS NOT NULL THEN emergency_response_time ELSE 0 END) as avg_response_time
FROM fluss_catalog.fluss.ads_smart_grid_comprehensive_report 
GROUP BY grid_region
ORDER BY critical_risks DESC, avg_stability DESC;

-- 电网负载平衡分析
SELECT 
    '电网负载分析' as metric,
    COUNT(*) as total_dispatches,
    COUNT(CASE WHEN emergency_level = 'CRITICAL' THEN 1 END) as critical_count,
    COUNT(CASE WHEN load_balance_status = 'IMBALANCED' THEN 1 END) as imbalanced_count,
    AVG(load_demand_mw) as avg_demand,
    AVG(supply_capacity_mw) as avg_supply
FROM fluss_catalog.fluss.ods_power_dispatch_raw;

-- 设备健康度综合分析
SELECT 
    device_type,
    location,
    COUNT(*) as device_count,
    AVG(efficiency_rate) as avg_efficiency,
    COUNT(CASE WHEN maintenance_status = 'CRITICAL' THEN 1 END) as critical_devices,
    AVG(real_time_temperature) as avg_temperature
FROM fluss_catalog.fluss.ods_device_dimension_raw
GROUP BY device_type, location
ORDER BY critical_devices DESC, avg_efficiency ASC;

-- 📋 【监控 4】最终结果验证
SELECT '=== 📋 最终结果验证 ===' as monitor_title;

-- 查看PostgreSQL中的智能电网综合报表结果
SELECT '智能电网综合报表' as result_type, 
       grid_region, 
       total_devices, 
       avg_efficiency,
       grid_stability_index,
       risk_assessment,
       emergency_response_time,
       report_time
FROM postgres_smart_grid_comprehensive_result 
ORDER BY report_time DESC 
LIMIT 10;

-- 🎯 【总结】综合业务场景测试完成状态
SELECT '=== 🎯 综合业务场景测试完成总结 ===' as summary_title;

SELECT 
    '智能电网数据完整性' as metric,
    CONCAT('调度:', dispatch_count, ' | 设备:', device_count, ' | 明细:', detail_count, ' | 汇总:', summary_count, ' | 报表:', report_count) as layer_counts
FROM (
    SELECT 
        (SELECT COUNT(*) FROM fluss_catalog.fluss.ods_power_dispatch_raw) as dispatch_count,
        (SELECT COUNT(*) FROM fluss_catalog.fluss.ods_device_dimension_raw) as device_count,
        (SELECT COUNT(*) FROM fluss_catalog.fluss.dwd_smart_grid_detail) as detail_count,
        (SELECT COUNT(*) FROM fluss_catalog.fluss.dws_grid_operation_summary) as summary_count,
        (SELECT COUNT(*) FROM fluss_catalog.fluss.ads_smart_grid_comprehensive_report) as report_count
);

-- ✅ 【验证】增删改查操作成功验证
SELECT '增删改查验证结果' as final_verification,
       CASE 
           WHEN (SELECT COUNT(*) FROM fluss_catalog.fluss.ods_power_dispatch_raw WHERE dispatch_id = 'DISPATCH_TEST001') = 1 THEN '✅ 增加成功'
           ELSE '❌ 增加失败'
       END as insert_status,
       CASE 
           WHEN (SELECT emergency_level FROM fluss_catalog.fluss.ods_power_dispatch_raw WHERE dispatch_id = 'DISPATCH_TEST002') = 'CRITICAL' THEN '✅ 更新成功'
           ELSE '❌ 更新失败'
       END as update_status,
       CASE 
           WHEN (SELECT COUNT(*) FROM fluss_catalog.fluss.ods_power_dispatch_raw WHERE dispatch_id = 'DISPATCH_TEST003') = 0 THEN '✅ 删除成功'
           ELSE '❌ 删除失败'
       END as delete_status; 