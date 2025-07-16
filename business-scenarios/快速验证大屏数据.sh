#!/bin/bash

# ===============================================
# 🚀 快速验证大屏数据脚本
# 💎 诊断大屏无数据问题并提供解决方案
# 🔥 基于用户业务脚本适配现有系统
# ===============================================

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}===============================================${NC}"
echo -e "${BLUE}🚀 快速验证大屏数据 - 诊断无数据问题${NC}"
echo -e "${BLUE}===============================================${NC}"

# 函数：检查数据库连接
check_database() {
    echo -e "${YELLOW}📊 检查PostgreSQL数据库连接${NC}"
    
    # 检查sgcc_dw_db数据库
    if docker exec -it postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "SELECT 1;" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ sgcc_dw_db数据库连接正常${NC}"
    else
        echo -e "${RED}❌ sgcc_dw_db数据库连接失败${NC}"
        echo -e "${YELLOW}💡 正在创建数据库...${NC}"
        docker exec -it postgres-sgcc-sink psql -U sgcc_user -d postgres -c "CREATE DATABASE sgcc_dw_db;"
    fi
    
    # 检查表结构
    echo -e "${YELLOW}📋 检查大屏所需表结构${NC}"
    docker exec -it postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
    AND table_name IN ('dashboard_realtime_metrics', 'device_status_realtime', 'grid_operation_summary', 'fluss_sink_table');"
}

# 函数：创建大屏所需表
create_dashboard_tables() {
    echo -e "${YELLOW}🔧 创建大屏所需表结构${NC}"
    
    docker exec -i postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db << 'EOF'
-- 创建大屏实时数据表
CREATE TABLE IF NOT EXISTS dashboard_realtime_metrics (
    metric_id VARCHAR PRIMARY KEY,
    metric_category VARCHAR,
    grid_region VARCHAR,
    update_time TIMESTAMP,
    current_load_mw DOUBLE PRECISION,
    current_generation_mw DOUBLE PRECISION,
    load_forecast_accuracy DOUBLE PRECISION,
    grid_frequency_hz DOUBLE PRECISION,
    voltage_stability_index DOUBLE PRECISION,
    overall_efficiency_pct DOUBLE PRECISION,
    device_health_average DOUBLE PRECISION,
    emergency_response_time_ms DOUBLE PRECISION,
    carbon_emission_rate DOUBLE PRECISION,
    realtime_trading_profit DOUBLE PRECISION,
    cost_per_mwh DOUBLE PRECISION,
    energy_waste_percentage DOUBLE PRECISION,
    next_hour_load_prediction DOUBLE PRECISION,
    equipment_failure_risk DOUBLE PRECISION,
    maintenance_recommendation TEXT,
    optimization_potential DOUBLE PRECISION
);

-- 创建设备状态表
CREATE TABLE IF NOT EXISTS device_status_realtime (
    device_id VARCHAR PRIMARY KEY,
    device_name VARCHAR,
    device_type VARCHAR,
    location VARCHAR,
    status VARCHAR,
    capacity_mw DOUBLE PRECISION,
    efficiency_rate DOUBLE PRECISION,
    health_score DOUBLE PRECISION,
    real_time_voltage DOUBLE PRECISION,
    real_time_current DOUBLE PRECISION,
    real_time_temperature DOUBLE PRECISION,
    fault_probability DOUBLE PRECISION,
    maintenance_urgency VARCHAR,
    last_update TIMESTAMP
);

-- 创建电网运行汇总表
CREATE TABLE IF NOT EXISTS grid_operation_summary (
    summary_id VARCHAR PRIMARY KEY,
    grid_region VARCHAR,
    time_window VARCHAR,
    total_dispatches BIGINT,
    total_devices BIGINT,
    avg_supply_demand_balance DOUBLE PRECISION,
    frequency_stability_rate DOUBLE PRECISION,
    voltage_quality_rate DOUBLE PRECISION,
    avg_device_health DOUBLE PRECISION,
    grid_efficiency_score DOUBLE PRECISION,
    high_risk_incidents BIGINT,
    emergency_responses BIGINT,
    report_time TIMESTAMP
);

-- 创建兼容原有系统的fluss_sink_table
CREATE TABLE IF NOT EXISTS fluss_sink_table (
    id SERIAL PRIMARY KEY,
    station_id VARCHAR,
    device_id VARCHAR,
    device_name VARCHAR,
    device_type VARCHAR,
    location VARCHAR,
    status VARCHAR,
    capacity_mw DOUBLE PRECISION,
    current_load_mw DOUBLE PRECISION,
    voltage_kv DOUBLE PRECISION,
    current_ampere DOUBLE PRECISION,
    temperature_celsius DOUBLE PRECISION,
    efficiency_rate DOUBLE PRECISION,
    operating_hours BIGINT,
    last_maintenance_date DATE,
    next_maintenance_date DATE,
    health_score DOUBLE PRECISION,
    alert_level VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
EOF

    echo -e "${GREEN}✅ 大屏所需表结构创建完成${NC}"
}

# 函数：插入测试数据
insert_test_data() {
    echo -e "${YELLOW}📈 插入测试数据${NC}"
    
    docker exec -i postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db << 'EOF'
-- 插入大屏实时数据
INSERT INTO dashboard_realtime_metrics (
    metric_id, metric_category, grid_region, update_time,
    current_load_mw, current_generation_mw, load_forecast_accuracy,
    grid_frequency_hz, voltage_stability_index, overall_efficiency_pct,
    device_health_average, emergency_response_time_ms, carbon_emission_rate,
    realtime_trading_profit, cost_per_mwh, energy_waste_percentage,
    next_hour_load_prediction, equipment_failure_risk,
    maintenance_recommendation, optimization_potential
) VALUES 
('DASHBOARD_华北电网_001', 'REALTIME', '华北电网', NOW(),
 18500.0, 19200.0, 95.5, 50.0, 0.95, 92.3,
 88.7, 25.5, 350.2, 125000.0, 0.32, 2.1,
 19100.0, 15.2, '系统运行正常', 85.6),
('DASHBOARD_华东电网_002', 'REALTIME', '华东电网', NOW(),
 22000.0, 22800.0, 97.2, 49.9, 0.97, 94.1,
 91.3, 18.3, 420.8, 145000.0, 0.28, 1.8,
 22500.0, 12.7, '设备状态良好', 88.9),
('DASHBOARD_华南电网_003', 'REALTIME', '华南电网', NOW(),
 16200.0, 16900.0, 93.8, 50.1, 0.93, 89.5,
 85.2, 32.1, 288.7, 98000.0, 0.35, 2.5,
 16800.0, 18.9, '建议关注设备温度', 82.4)
ON CONFLICT (metric_id) DO UPDATE SET
    update_time = EXCLUDED.update_time,
    current_load_mw = EXCLUDED.current_load_mw,
    current_generation_mw = EXCLUDED.current_generation_mw,
    load_forecast_accuracy = EXCLUDED.load_forecast_accuracy,
    grid_frequency_hz = EXCLUDED.grid_frequency_hz,
    voltage_stability_index = EXCLUDED.voltage_stability_index,
    overall_efficiency_pct = EXCLUDED.overall_efficiency_pct,
    device_health_average = EXCLUDED.device_health_average,
    emergency_response_time_ms = EXCLUDED.emergency_response_time_ms,
    carbon_emission_rate = EXCLUDED.carbon_emission_rate,
    realtime_trading_profit = EXCLUDED.realtime_trading_profit,
    cost_per_mwh = EXCLUDED.cost_per_mwh,
    energy_waste_percentage = EXCLUDED.energy_waste_percentage,
    next_hour_load_prediction = EXCLUDED.next_hour_load_prediction,
    equipment_failure_risk = EXCLUDED.equipment_failure_risk,
    maintenance_recommendation = EXCLUDED.maintenance_recommendation,
    optimization_potential = EXCLUDED.optimization_potential;

-- 插入设备状态数据
INSERT INTO device_status_realtime (
    device_id, device_name, device_type, location, status,
    capacity_mw, efficiency_rate, health_score,
    real_time_voltage, real_time_current, real_time_temperature,
    fault_probability, maintenance_urgency, last_update
) VALUES 
('DEV_001', '华北主变压器1', '变压器', '北京', 'ACTIVE',
 500.0, 0.96, 92.5, 220.5, 125.3, 45.2, 0.05, 'NORMAL', NOW()),
('DEV_002', '华东发电机组2', '发电机', '上海', 'ACTIVE',
 800.0, 0.94, 88.7, 235.8, 180.2, 52.1, 0.08, 'NORMAL', NOW()),
('DEV_003', '华南配电设备3', '配电设备', '深圳', 'MAINTENANCE',
 300.0, 0.91, 85.3, 218.9, 95.7, 48.8, 0.12, 'HIGH', NOW()),
('DEV_004', '华北风机4', '风机', '内蒙古', 'ACTIVE',
 200.0, 0.93, 90.1, 225.4, 88.5, 38.9, 0.06, 'NORMAL', NOW()),
('DEV_005', '华东太阳能5', '太阳能', '江苏', 'ACTIVE',
 150.0, 0.89, 86.9, 231.2, 62.3, 35.4, 0.07, 'NORMAL', NOW())
ON CONFLICT (device_id) DO UPDATE SET
    device_name = EXCLUDED.device_name,
    device_type = EXCLUDED.device_type,
    location = EXCLUDED.location,
    status = EXCLUDED.status,
    capacity_mw = EXCLUDED.capacity_mw,
    efficiency_rate = EXCLUDED.efficiency_rate,
    health_score = EXCLUDED.health_score,
    real_time_voltage = EXCLUDED.real_time_voltage,
    real_time_current = EXCLUDED.real_time_current,
    real_time_temperature = EXCLUDED.real_time_temperature,
    fault_probability = EXCLUDED.fault_probability,
    maintenance_urgency = EXCLUDED.maintenance_urgency,
    last_update = EXCLUDED.last_update;

-- 插入电网运行汇总数据
INSERT INTO grid_operation_summary (
    summary_id, grid_region, time_window,
    total_dispatches, total_devices, avg_supply_demand_balance,
    frequency_stability_rate, voltage_quality_rate, avg_device_health,
    grid_efficiency_score, high_risk_incidents, emergency_responses,
    report_time
) VALUES 
('SUMMARY_华北_202412201530', '华北电网', '202412201530',
 1250, 45, 850.5, 96.5, 92.3, 89.2, 94.1, 2, 1, NOW()),
('SUMMARY_华东_202412201530', '华东电网', '202412201530',
 1380, 52, 920.3, 97.8, 94.1, 91.5, 95.7, 1, 0, NOW()),
('SUMMARY_华南_202412201530', '华南电网', '202412201530',
 1120, 38, 720.8, 94.2, 89.7, 86.8, 91.3, 3, 2, NOW())
ON CONFLICT (summary_id) DO UPDATE SET
    total_dispatches = EXCLUDED.total_dispatches,
    total_devices = EXCLUDED.total_devices,
    avg_supply_demand_balance = EXCLUDED.avg_supply_demand_balance,
    frequency_stability_rate = EXCLUDED.frequency_stability_rate,
    voltage_quality_rate = EXCLUDED.voltage_quality_rate,
    avg_device_health = EXCLUDED.avg_device_health,
    grid_efficiency_score = EXCLUDED.grid_efficiency_score,
    high_risk_incidents = EXCLUDED.high_risk_incidents,
    emergency_responses = EXCLUDED.emergency_responses,
    report_time = EXCLUDED.report_time;

-- 插入兼容原有系统的数据
INSERT INTO fluss_sink_table (
    station_id, device_id, device_name, device_type, location, status,
    capacity_mw, current_load_mw, voltage_kv, current_ampere,
    temperature_celsius, efficiency_rate, operating_hours,
    last_maintenance_date, next_maintenance_date, health_score, alert_level
) VALUES 
('ST_001', 'DEV_001', '华北主变压器1', '变压器', '北京', 'ONLINE',
 500.0, 420.5, 220.5, 125.3, 45.2, 0.96, 8760,
 '2024-11-15', '2024-12-15', 92.5, 'NORMAL'),
('ST_002', 'DEV_002', '华东发电机组2', '发电机', '上海', 'ONLINE',
 800.0, 680.3, 235.8, 180.2, 52.1, 0.94, 7320,
 '2024-10-20', '2024-12-20', 88.7, 'NORMAL'),
('ST_003', 'DEV_003', '华南配电设备3', '配电设备', '深圳', 'MAINTENANCE',
 300.0, 180.9, 218.9, 95.7, 48.8, 0.91, 6540,
 '2024-09-10', '2024-12-10', 85.3, 'WARNING'),
('ST_004', 'DEV_004', '华北风机4', '风机', '内蒙古', 'ONLINE',
 200.0, 165.4, 225.4, 88.5, 38.9, 0.93, 9120,
 '2024-11-01', '2025-01-01', 90.1, 'NORMAL'),
('ST_005', 'DEV_005', '华东太阳能5', '太阳能', '江苏', 'ONLINE',
 150.0, 125.8, 231.2, 62.3, 35.4, 0.89, 4380,
 '2024-10-15', '2025-01-15', 86.9, 'NORMAL')
ON CONFLICT (id) DO NOTHING;
EOF

    echo -e "${GREEN}✅ 测试数据插入完成${NC}"
}

# 函数：验证数据
verify_data() {
    echo -e "${YELLOW}🔍 验证数据插入结果${NC}"
    
    echo -e "${BLUE}📊 dashboard_realtime_metrics表数据：${NC}"
    docker exec -it postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
    SELECT grid_region, current_load_mw, current_generation_mw, 
           overall_efficiency_pct, device_health_average, update_time 
    FROM dashboard_realtime_metrics ORDER BY update_time DESC LIMIT 5;"
    
    echo -e "${BLUE}📊 device_status_realtime表数据：${NC}"
    docker exec -it postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
    SELECT device_id, device_name, status, health_score, 
           real_time_temperature, last_update 
    FROM device_status_realtime ORDER BY last_update DESC LIMIT 5;"
    
    echo -e "${BLUE}📊 grid_operation_summary表数据：${NC}"
    docker exec -it postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
    SELECT grid_region, total_dispatches, total_devices, 
           avg_device_health, grid_efficiency_score, report_time 
    FROM grid_operation_summary ORDER BY report_time DESC LIMIT 5;"
    
    echo -e "${BLUE}📊 fluss_sink_table表数据：${NC}"
    docker exec -it postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
    SELECT device_id, device_name, status, health_score, 
           current_load_mw, updated_at 
    FROM fluss_sink_table ORDER BY updated_at DESC LIMIT 5;"
}

# 函数：启动简化的数据生成器
start_data_generator() {
    echo -e "${YELLOW}🔄 启动简化数据生成器${NC}"
    
    # 创建简化的Flink作业
    docker exec -i sql-client-sgcc /opt/flink/bin/sql-client.sh << 'EOF'
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';

-- 创建简化的数据源
CREATE TEMPORARY TABLE simple_grid_data (
    id STRING,
    grid_region STRING,
    current_load_mw DOUBLE,
    current_generation_mw DOUBLE,
    grid_frequency_hz DOUBLE,
    device_health_avg DOUBLE,
    efficiency_pct DOUBLE,
    update_time TIMESTAMP(3),
    WATERMARK FOR update_time AS update_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1',
    'fields.id.kind' = 'sequence',
    'fields.id.start' = '1',
    'fields.id.end' = '1000',
    'fields.grid_region.length' = '8',
    'fields.current_load_mw.min' = '15000.0',
    'fields.current_load_mw.max' = '25000.0',
    'fields.current_generation_mw.min' = '15500.0',
    'fields.current_generation_mw.max' = '25500.0',
    'fields.grid_frequency_hz.min' = '49.8',
    'fields.grid_frequency_hz.max' = '50.2',
    'fields.device_health_avg.min' = '80.0',
    'fields.device_health_avg.max' = '98.0',
    'fields.efficiency_pct.min' = '85.0',
    'fields.efficiency_pct.max' = '96.0'
);

-- 创建PostgreSQL Sink
CREATE TABLE postgres_dashboard_sink (
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

-- 启动数据流
INSERT INTO postgres_dashboard_sink
SELECT 
    CONCAT('DASHBOARD_', grid_region, '_', id) as metric_id,
    'REALTIME' as metric_category,
    grid_region,
    update_time,
    current_load_mw,
    current_generation_mw,
    95.5 as load_forecast_accuracy,
    grid_frequency_hz,
    device_health_avg / 100.0 as voltage_stability_index,
    efficiency_pct as overall_efficiency_pct,
    device_health_avg as device_health_average,
    25.0 as emergency_response_time_ms,
    current_load_mw * 0.4 as carbon_emission_rate,
    (current_generation_mw - current_load_mw) * 500 as realtime_trading_profit,
    current_load_mw / 100 as cost_per_mwh,
    CASE WHEN current_generation_mw > current_load_mw THEN 2.0 ELSE 0.5 END as energy_waste_percentage,
    current_load_mw * 1.05 as next_hour_load_prediction,
    CASE WHEN device_health_avg < 85 THEN 25.0 ELSE 10.0 END as equipment_failure_risk,
    CASE WHEN device_health_avg < 85 THEN '建议检修设备' ELSE '系统运行正常' END as maintenance_recommendation,
    device_health_avg + efficiency_pct - 80 as optimization_potential
FROM simple_grid_data;
EOF

    echo -e "${GREEN}✅ 简化数据生成器启动完成${NC}"
}

# 函数：检查Flink作业状态
check_flink_jobs() {
    echo -e "${YELLOW}📊 检查Flink作业状态${NC}"
    local jobs=$(curl -s http://localhost:8091/jobs | jq -r '.jobs | length')
    echo -e "${GREEN}当前运行的Flink作业数量：${jobs}${NC}"
    
    if [ "$jobs" -gt 0 ]; then
        curl -s http://localhost:8091/jobs | jq -r '.jobs[] | "Job ID: \(.id), Status: \(.status)"'
    else
        echo -e "${RED}❌ 没有运行的Flink作业${NC}"
    fi
    echo
}

# 主流程
main() {
    echo -e "${YELLOW}🚀 开始快速验证大屏数据${NC}"
    
    # 1. 检查数据库连接
    check_database
    
    # 2. 创建大屏所需表
    create_dashboard_tables
    
    # 3. 插入测试数据
    insert_test_data
    
    # 4. 验证数据
    verify_data
    
    # 5. 检查Flink作业状态
    check_flink_jobs
    
    # 6. 启动简化数据生成器
    start_data_generator
    
    # 7. 再次检查Flink作业状态
    sleep 5
    check_flink_jobs
    
    echo -e "${BLUE}===============================================${NC}"
    echo -e "${GREEN}🎉 快速验证完成！${NC}"
    echo -e "${BLUE}===============================================${NC}"
    
    echo -e "${GREEN}✅ 大屏数据准备完成${NC}"
    echo -e "${YELLOW}📋 接下来可以：${NC}"
    echo -e "${YELLOW}  1. 访问大屏：http://localhost:3000${NC}"
    echo -e "${YELLOW}  2. 用户名：admin，密码：admin${NC}"
    echo -e "${YELLOW}  3. 检查各个图表是否显示数据${NC}"
    echo -e "${YELLOW}  4. 如果需要更多数据，运行分阶段启动脚本${NC}"
    
    echo -e "${BLUE}💡 数据表状态：${NC}"
    echo -e "${BLUE}  - dashboard_realtime_metrics: 大屏主要数据源${NC}"
    echo -e "${BLUE}  - device_status_realtime: 设备状态数据${NC}"
    echo -e "${BLUE}  - grid_operation_summary: 电网运行汇总${NC}"
    echo -e "${BLUE}  - fluss_sink_table: 兼容原有系统${NC}"
}

# 运行主流程
main "$@" 