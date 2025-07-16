-- ===============================================
-- 🚀 融合版业务场景测试：适配现有大屏系统
-- 💎 基于用户两个业务脚本，融合到现有大屏数据结构
-- 🔥 数据流：DataGen高频数据 → Fluss统一数仓 → PostgreSQL大屏
-- ===============================================

SET 'sql-client.execution.result-mode' = 'tableau';

-- ===============================================
-- 1. 在Default Catalog创建高频数据流（融合版）
-- ===============================================

-- 🚀 电网调度数据流（适配大屏）
CREATE TEMPORARY TABLE power_grid_dispatch_stream (
    dispatch_id STRING,
    event_time TIMESTAMP(3),
    grid_region STRING,
    -- 基础电力指标
    total_demand_mw DOUBLE,
    total_supply_mw DOUBLE,
    frequency_hz DOUBLE,
    voltage_kv DOUBLE,
    -- 扩展指标（来自架构优势版）
    instant_load_mw DOUBLE,
    instant_generation_mw DOUBLE,
    grid_stability_index DOUBLE,
    load_forecast_next_min DOUBLE,
    emergency_level STRING,
    auto_dispatch_action STRING,
    response_time_ms BIGINT,
    load_balance_status STRING,
    dispatch_command STRING,
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '5000',  -- 5000 QPS
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
    'fields.voltage_kv.min' = '500.0',
    'fields.voltage_kv.max' = '800.0',
    'fields.instant_load_mw.min' = '15000.0',
    'fields.instant_load_mw.max' = '25000.0',
    'fields.instant_generation_mw.min' = '15000.0',
    'fields.instant_generation_mw.max' = '25000.0',
    'fields.grid_stability_index.min' = '0.8',
    'fields.grid_stability_index.max' = '1.0',
    'fields.load_forecast_next_min.min' = '15000.0',
    'fields.load_forecast_next_min.max' = '25000.0',
    'fields.emergency_level.length' = '8',
    'fields.auto_dispatch_action.length' = '20',
    'fields.response_time_ms.min' = '10',
    'fields.response_time_ms.max' = '100',
    'fields.load_balance_status.length' = '8',
    'fields.dispatch_command.length' = '20'
);

-- 🚀 智能设备状态流（融合版）
CREATE TEMPORARY TABLE smart_device_status_stream (
    device_id STRING,
    device_name STRING,
    device_type STRING,
    location STRING,
    capacity_mw DOUBLE,
    status STRING,
    event_time TIMESTAMP(3),
    -- 实时运行参数
    real_time_voltage DOUBLE,
    real_time_current DOUBLE,
    real_time_temperature DOUBLE,
    efficiency_rate DOUBLE,
    -- 扩展健康指标（来自架构优势版）
    real_power_mw DOUBLE,
    temperature_celsius DOUBLE,
    vibration_level DOUBLE,
    efficiency_percent DOUBLE,
    health_score DOUBLE,
    fault_probability DOUBLE,
    maintenance_urgency STRING,
    estimated_lifetime_hours BIGINT,
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '3000',  -- 3000 QPS
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
    'fields.efficiency_rate.max' = '0.98',
    'fields.real_power_mw.min' = '50.0',
    'fields.real_power_mw.max' = '500.0',
    'fields.temperature_celsius.min' = '25.0',
    'fields.temperature_celsius.max' = '85.0',
    'fields.vibration_level.min' = '0.1',
    'fields.vibration_level.max' = '2.0',
    'fields.efficiency_percent.min' = '85.0',
    'fields.efficiency_percent.max' = '98.5',
    'fields.health_score.min' = '70.0',
    'fields.health_score.max' = '100.0',
    'fields.fault_probability.min' = '0.01',
    'fields.fault_probability.max' = '0.15',
    'fields.maintenance_urgency.length' = '10',
    'fields.estimated_lifetime_hours.min' = '1000',
    'fields.estimated_lifetime_hours.max' = '50000'
);

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

-- ODS层：电网调度原始数据
CREATE TABLE ods_power_dispatch_raw (
    dispatch_id STRING PRIMARY KEY NOT ENFORCED,
    event_time TIMESTAMP(3),
    grid_region STRING,
    total_demand_mw DOUBLE,
    total_supply_mw DOUBLE,
    frequency_hz DOUBLE,
    voltage_kv DOUBLE,
    instant_load_mw DOUBLE,
    instant_generation_mw DOUBLE,
    grid_stability_index DOUBLE,
    load_forecast_next_min DOUBLE,
    emergency_level STRING,
    auto_dispatch_action STRING,
    response_time_ms BIGINT,
    load_balance_status STRING,
    dispatch_command STRING
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123',
    'bucket' = '8'
);

-- ODS层：设备状态原始数据
CREATE TABLE ods_device_status_raw (
    device_id STRING PRIMARY KEY NOT ENFORCED,
    device_name STRING,
    device_type STRING,
    location STRING,
    capacity_mw DOUBLE,
    status STRING,
    event_time TIMESTAMP(3),
    real_time_voltage DOUBLE,
    real_time_current DOUBLE,
    real_time_temperature DOUBLE,
    efficiency_rate DOUBLE,
    real_power_mw DOUBLE,
    temperature_celsius DOUBLE,
    vibration_level DOUBLE,
    efficiency_percent DOUBLE,
    health_score DOUBLE,
    fault_probability DOUBLE,
    maintenance_urgency STRING,
    estimated_lifetime_hours BIGINT
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123',
    'bucket' = '8'
);

-- DWD层：智能电网运行明细（融合版）
CREATE TABLE dwd_smart_grid_detail (
    grid_detail_id STRING PRIMARY KEY NOT ENFORCED,
    dispatch_id STRING,
    device_id STRING,
    grid_region STRING,
    device_location STRING,
    device_type STRING,
    dispatch_time TIMESTAMP(3),
    -- 电网运行指标
    supply_demand_balance DOUBLE,
    frequency_stability STRING,
    voltage_quality STRING,
    device_health_score DOUBLE,
    grid_efficiency DOUBLE,
    risk_level STRING,
    operational_status STRING,
    -- 扩展指标
    load_forecast_accuracy DOUBLE,
    emergency_response_time DOUBLE,
    maintenance_priority STRING,
    energy_efficiency_index DOUBLE
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123',
    'bucket' = '12'
);

-- DWS层：电网运行汇总（适配大屏）
CREATE TABLE dws_grid_operation_summary (
    summary_id STRING PRIMARY KEY NOT ENFORCED,
    grid_region STRING,
    time_window STRING,
    -- 基础统计
    total_dispatches BIGINT,
    total_devices BIGINT,
    avg_supply_demand_balance DOUBLE,
    frequency_stability_rate DOUBLE,
    voltage_quality_rate DOUBLE,
    avg_device_health DOUBLE,
    grid_efficiency_score DOUBLE,
    high_risk_incidents BIGINT,
    emergency_responses BIGINT,
    -- 扩展指标（适配大屏）
    current_load_mw DOUBLE,
    current_generation_mw DOUBLE,
    grid_frequency_hz DOUBLE,
    voltage_stability_index DOUBLE,
    overall_efficiency_pct DOUBLE,
    device_health_average DOUBLE,
    emergency_response_time_ms DOUBLE,
    carbon_emission_rate DOUBLE,
    realtime_trading_profit DOUBLE,
    cost_per_mwh DOUBLE,
    energy_waste_percentage DOUBLE,
    next_hour_load_prediction DOUBLE,
    equipment_failure_risk DOUBLE,
    maintenance_recommendation STRING,
    optimization_potential DOUBLE
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123',
    'bucket' = '6'
);

-- ===============================================
-- 3. 适配现有大屏的PostgreSQL Sink表
-- ===============================================

USE CATALOG default_catalog;

-- 大屏实时数据表（适配现有结构）
CREATE TABLE postgres_dashboard_realtime (
    metric_id STRING,
    metric_category STRING,
    grid_region STRING,
    update_time TIMESTAMP(3),
    current_load_mw DOUBLE,
    current_generation_mw DOUBLE,
    load_forecast_accuracy DOUBLE,
    grid_frequency_hz DOUBLE,
    voltage_stability_index DOUBLE,
    overall_efficiency_pct DOUBLE,
    device_health_average DOUBLE,
    emergency_response_time_ms DOUBLE,
    carbon_emission_rate DOUBLE,
    realtime_trading_profit DOUBLE,
    cost_per_mwh DOUBLE,
    energy_waste_percentage DOUBLE,
    next_hour_load_prediction DOUBLE,
    equipment_failure_risk DOUBLE,
    maintenance_recommendation STRING,
    optimization_potential DOUBLE,
    PRIMARY KEY (metric_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'dashboard_realtime_metrics',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024'
);

-- 设备状态表（适配现有结构）
CREATE TABLE postgres_device_status (
    device_id STRING,
    device_name STRING,
    device_type STRING,
    location STRING,
    status STRING,
    capacity_mw DOUBLE,
    efficiency_rate DOUBLE,
    health_score DOUBLE,
    real_time_voltage DOUBLE,
    real_time_current DOUBLE,
    real_time_temperature DOUBLE,
    fault_probability DOUBLE,
    maintenance_urgency STRING,
    last_update TIMESTAMP(3),
    PRIMARY KEY (device_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'device_status_realtime',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024'
);

-- 电网运行汇总表（适配现有结构）
CREATE TABLE postgres_grid_summary (
    summary_id STRING,
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
    emergency_responses BIGINT,
    report_time TIMESTAMP(3),
    PRIMARY KEY (summary_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'grid_operation_summary',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024'
);

-- ===============================================
-- 4. 数据流处理流水线（分阶段启动）
-- ===============================================

-- 🚀 阶段1：ODS层数据采集
INSERT INTO fluss_catalog.fluss.ods_power_dispatch_raw
SELECT 
    dispatch_id,
    event_time,
    grid_region,
    total_demand_mw,
    total_supply_mw,
    frequency_hz,
    voltage_kv,
    instant_load_mw,
    instant_generation_mw,
    grid_stability_index,
    load_forecast_next_min,
    emergency_level,
    auto_dispatch_action,
    response_time_ms,
    load_balance_status,
    dispatch_command
FROM power_grid_dispatch_stream;

INSERT INTO fluss_catalog.fluss.ods_device_status_raw
SELECT 
    device_id,
    device_name,
    device_type,
    location,
    capacity_mw,
    status,
    event_time,
    real_time_voltage,
    real_time_current,
    real_time_temperature,
    efficiency_rate,
    real_power_mw,
    temperature_celsius,
    vibration_level,
    efficiency_percent,
    health_score,
    fault_probability,
    maintenance_urgency,
    estimated_lifetime_hours
FROM smart_device_status_stream;

-- 🚀 阶段2：DWD层数据关联处理
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
        WHEN d.voltage_kv > 750 AND dev.real_time_voltage > 240 THEN 'EXCELLENT'
        WHEN d.voltage_kv > 600 AND dev.real_time_voltage > 230 THEN 'GOOD'
        WHEN dev.real_time_voltage > 220 THEN 'ACCEPTABLE'
        ELSE 'POOR'
    END as voltage_quality,
    -- 设备健康评分
    COALESCE(dev.health_score, 
             (dev.efficiency_rate * 0.4 + (250 - dev.real_time_temperature) * 0.003 + 
              CASE WHEN dev.status = 'ACTIVE' THEN 0.3 ELSE 0.1 END) * 100) as device_health_score,
    -- 电网效率计算
    (d.total_supply_mw / NULLIF(d.total_demand_mw, 0)) * dev.efficiency_rate * 100 as grid_efficiency,
    -- 风险等级评估
    CASE 
        WHEN d.emergency_level = 'CRITICAL' OR dev.real_time_temperature > 75 THEN 'HIGH_RISK'
        WHEN d.emergency_level = 'HIGH' OR dev.efficiency_rate < 0.85 THEN 'MEDIUM_RISK'
        WHEN d.load_balance_status <> 'BALANCED' THEN 'LOW_RISK'
        ELSE 'NORMAL'
    END as risk_level,
    -- 运行状态评估
    CASE 
        WHEN d.frequency_hz BETWEEN 49.9 AND 50.1 AND dev.efficiency_rate > 0.95 THEN 'OPTIMAL'
        WHEN d.frequency_hz BETWEEN 49.8 AND 50.2 AND dev.efficiency_rate > 0.90 THEN 'GOOD'
        WHEN dev.efficiency_rate > 0.85 THEN 'ACCEPTABLE'
        ELSE 'NEEDS_ATTENTION'
    END as operational_status,
    -- 扩展指标
    ABS(d.load_forecast_next_min - d.instant_load_mw) / NULLIF(d.instant_load_mw, 0) * 100 as load_forecast_accuracy,
    d.response_time_ms as emergency_response_time,
    CASE 
        WHEN dev.health_score < 70 THEN 'URGENT'
        WHEN dev.real_time_temperature > 70 THEN 'HIGH'
        ELSE 'NORMAL'
    END as maintenance_priority,
    d.grid_stability_index * dev.efficiency_rate * 100 as energy_efficiency_index
FROM fluss_catalog.fluss.ods_power_dispatch_raw d
JOIN fluss_catalog.fluss.ods_device_status_raw dev
    ON SUBSTRING(d.grid_region, 1, 2) = SUBSTRING(dev.location, 1, 2)
WHERE d.event_time >= dev.event_time - INTERVAL '5' SECOND
  AND d.event_time <= dev.event_time + INTERVAL '5' SECOND;

-- 🚀 阶段3：DWS层汇总统计（适配大屏）
INSERT INTO fluss_catalog.fluss.dws_grid_operation_summary
SELECT 
    CONCAT(grid_region, '_', DATE_FORMAT(window_start, 'yyyyMMddHHmm')) as summary_id,
    grid_region,
    DATE_FORMAT(window_start, 'yyyyMMddHHmm') as time_window,
    -- 基础统计
    COUNT(DISTINCT dispatch_id) as total_dispatches,
    COUNT(DISTINCT device_id) as total_devices,
    AVG(supply_demand_balance) as avg_supply_demand_balance,
    (SUM(CASE WHEN frequency_stability = 'STABLE' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as frequency_stability_rate,
    (SUM(CASE WHEN voltage_quality IN ('EXCELLENT', 'GOOD') THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as voltage_quality_rate,
    AVG(device_health_score) as avg_device_health,
    AVG(grid_efficiency) as grid_efficiency_score,
    SUM(CASE WHEN risk_level = 'HIGH_RISK' THEN 1 ELSE 0 END) as high_risk_incidents,
    SUM(CASE WHEN operational_status = 'NEEDS_ATTENTION' THEN 1 ELSE 0 END) as emergency_responses,
    -- 扩展指标（适配大屏）
    AVG(supply_demand_balance) as current_load_mw,
    AVG(supply_demand_balance) + AVG(supply_demand_balance) * 1.2 as current_generation_mw,
    AVG(load_forecast_accuracy) as load_forecast_accuracy,
    50.0 as grid_frequency_hz,
    AVG(device_health_score) / 100.0 as voltage_stability_index,
    AVG(grid_efficiency) as overall_efficiency_pct,
    AVG(device_health_score) as device_health_average,
    AVG(emergency_response_time) as emergency_response_time_ms,
    AVG(supply_demand_balance) * 0.4 as carbon_emission_rate,
    AVG(supply_demand_balance) * 500 as realtime_trading_profit,
    AVG(supply_demand_balance) / 100 as cost_per_mwh,
    GREATEST(0, AVG(supply_demand_balance) / 1000) as energy_waste_percentage,
    AVG(supply_demand_balance) * 1.1 as next_hour_load_prediction,
    AVG(CASE WHEN risk_level = 'HIGH_RISK' THEN 80 ELSE 20 END) as equipment_failure_risk,
    CASE 
        WHEN AVG(device_health_score) < 80 THEN '建议立即检修设备'
        WHEN SUM(CASE WHEN risk_level = 'HIGH_RISK' THEN 1 ELSE 0 END) > 5 THEN '注意高风险设备'
        ELSE '系统运行正常'
    END as maintenance_recommendation,
    (100 - AVG(emergency_response_time) / 10) + AVG(device_health_score) as optimization_potential
FROM (
    SELECT *,
           TUMBLE_START(dispatch_time, INTERVAL '1' MINUTE) as window_start
    FROM fluss_catalog.fluss.dwd_smart_grid_detail
) 
GROUP BY grid_region, window_start;

-- 🚀 阶段4：数据回流PostgreSQL（适配大屏）
INSERT INTO postgres_dashboard_realtime
SELECT 
    CONCAT('DASHBOARD_', grid_region, '_', CAST(UNIX_TIMESTAMP() AS STRING)) as metric_id,
    'REALTIME' as metric_category,
    grid_region,
    CURRENT_TIMESTAMP as update_time,
    current_load_mw,
    current_generation_mw,
    load_forecast_accuracy,
    grid_frequency_hz,
    voltage_stability_index,
    overall_efficiency_pct,
    device_health_average,
    emergency_response_time_ms,
    carbon_emission_rate,
    realtime_trading_profit,
    cost_per_mwh,
    energy_waste_percentage,
    next_hour_load_prediction,
    equipment_failure_risk,
    maintenance_recommendation,
    optimization_potential
FROM fluss_catalog.fluss.dws_grid_operation_summary
WHERE time_window = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyyMMddHHmm');

INSERT INTO postgres_device_status
SELECT 
    device_id,
    device_name,
    device_type,
    location,
    status,
    capacity_mw,
    efficiency_rate,
    health_score,
    real_time_voltage,
    real_time_current,
    real_time_temperature,
    fault_probability,
    maintenance_urgency,
    event_time as last_update
FROM fluss_catalog.fluss.ods_device_status_raw;

INSERT INTO postgres_grid_summary
SELECT 
    summary_id,
    grid_region,
    time_window,
    total_dispatches,
    total_devices,
    avg_supply_demand_balance,
    frequency_stability_rate,
    voltage_quality_rate,
    avg_device_health,
    grid_efficiency_score,
    high_risk_incidents,
    emergency_responses,
    CURRENT_TIMESTAMP as report_time
FROM fluss_catalog.fluss.dws_grid_operation_summary; 