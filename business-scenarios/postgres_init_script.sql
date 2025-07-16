-- ===============================================
-- 🎯 Fluss SGCC 5大业务场景 PostgreSQL 初始化脚本
-- 涵盖CDC源表、Sink目标表和测试数据
-- ===============================================

-- ===============================================
-- 🗃️ 1. 数据库和用户创建
-- ===============================================

-- 创建源数据库
CREATE DATABASE sgcc_source_db;
-- 创建数据仓库数据库
CREATE DATABASE sgcc_dw_db;
-- 创建目标分析数据库
CREATE DATABASE sgcc_target;

-- 创建用户
CREATE USER sgcc_user WITH PASSWORD 'sgcc_pass_2024';

-- 授权
GRANT ALL PRIVILEGES ON DATABASE sgcc_source_db TO sgcc_user;
GRANT ALL PRIVILEGES ON DATABASE sgcc_dw_db TO sgcc_user;
GRANT ALL PRIVILEGES ON DATABASE sgcc_target TO sgcc_user;

-- ===============================================
-- 🔌 2. 源数据库表结构（CDC源表）
-- ===============================================

\c sgcc_source_db;

-- 授权schema权限
GRANT ALL PRIVILEGES ON SCHEMA public TO sgcc_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO sgcc_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO sgcc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO sgcc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO sgcc_user;

-- 🎯 场景1：高频维度表服务 - 设备原始数据表
CREATE TABLE device_raw_data (
    device_id VARCHAR(50) PRIMARY KEY,
    voltage DOUBLE PRECISION,
    current_val DOUBLE PRECISION,  -- 避免保留字冲突
    temperature DOUBLE PRECISION,
    power_output DOUBLE PRECISION,
    efficiency DOUBLE PRECISION,
    status VARCHAR(10),
    alert_level VARCHAR(10),
    event_time TIMESTAMP
);

-- 🔴 场景2：智能双流JOIN - 设备告警表
CREATE TABLE device_alarms (
    alarm_id VARCHAR(50) PRIMARY KEY,
    device_id VARCHAR(50),
    alarm_type VARCHAR(50),
    alarm_level VARCHAR(20),
    alarm_message TEXT,
    alarm_time TIMESTAMP,
    reporter_system VARCHAR(50)
);

-- 🔴 场景2：智能双流JOIN - 设备状态表
CREATE TABLE device_status (
    device_id VARCHAR(50) PRIMARY KEY,
    device_name VARCHAR(100),
    voltage DOUBLE PRECISION,
    current_val DOUBLE PRECISION,
    temperature DOUBLE PRECISION,
    efficiency DOUBLE PRECISION,
    status VARCHAR(20),
    event_time TIMESTAMP
);

-- 🔴 场景3：时间旅行查询 - 设备历史数据表
CREATE TABLE device_historical_data (
    record_id VARCHAR(50) PRIMARY KEY,
    device_id VARCHAR(50),
    record_time TIMESTAMP,
    voltage DOUBLE PRECISION,
    current_val DOUBLE PRECISION,
    temperature DOUBLE PRECISION,
    power_output DOUBLE PRECISION,
    efficiency DOUBLE PRECISION,
    load_percentage DOUBLE PRECISION,
    operational_mode VARCHAR(20),
    error_codes VARCHAR(50)
);

-- 🔴 场景4：柱状流优化 - 大规模监控数据表
CREATE TABLE large_scale_monitoring_data (
    monitoring_id VARCHAR(50) PRIMARY KEY,
    device_id VARCHAR(50),
    record_time TIMESTAMP,
    voltage_a DOUBLE PRECISION,
    voltage_b DOUBLE PRECISION,
    voltage_c DOUBLE PRECISION,
    current_a DOUBLE PRECISION,
    current_b DOUBLE PRECISION,
    current_c DOUBLE PRECISION,
    power_active DOUBLE PRECISION,
    power_reactive DOUBLE PRECISION,
    temperature_core DOUBLE PRECISION,
    temperature_ambient DOUBLE PRECISION,
    load_percentage DOUBLE PRECISION,
    efficiency DOUBLE PRECISION,
    operating_hours BIGINT,
    energy_produced_kwh DOUBLE PRECISION,
    energy_consumed_kwh DOUBLE PRECISION,
    cost_per_kwh DOUBLE PRECISION,
    revenue_generated DOUBLE PRECISION,
    risk_score DOUBLE PRECISION
);

-- 🔴 综合场景：电力调度数据表
CREATE TABLE power_dispatch_data (
    dispatch_id VARCHAR(50) PRIMARY KEY,
    event_time TIMESTAMP,
    grid_region VARCHAR(50),
    total_demand_mw DOUBLE PRECISION,
    total_supply_mw DOUBLE PRECISION,
    frequency_hz DOUBLE PRECISION,
    voltage_level_kv DOUBLE PRECISION,
    load_balance_status VARCHAR(20),
    emergency_level VARCHAR(20),
    dispatch_command VARCHAR(100),
    response_time_ms BIGINT
);

-- 🔴 综合场景：设备维度数据表
CREATE TABLE device_dimension_data (
    device_id VARCHAR(50) PRIMARY KEY,
    device_name VARCHAR(100),
    device_type VARCHAR(50),
    location VARCHAR(50),
    capacity_mw DOUBLE PRECISION,
    installation_date TIMESTAMP,
    manufacturer VARCHAR(100),
    model VARCHAR(100),
    efficiency_rate DOUBLE PRECISION,
    maintenance_status VARCHAR(50),
    real_time_voltage DOUBLE PRECISION,
    real_time_current DOUBLE PRECISION,
    real_time_temperature DOUBLE PRECISION
);

-- ===============================================
-- 🎯 3. 目标数据库表结构（Sink表）
-- ===============================================

\c sgcc_dw_db;

-- 授权schema权限
GRANT ALL PRIVILEGES ON SCHEMA public TO sgcc_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO sgcc_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO sgcc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO sgcc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO sgcc_user;

-- 🎯 场景1：设备最终报表
CREATE TABLE device_final_report (
    report_id VARCHAR(100) PRIMARY KEY,
    report_type VARCHAR(50),
    location VARCHAR(50),
    total_devices BIGINT,
    efficiency_score DOUBLE PRECISION,
    health_status VARCHAR(20),
    alert_summary TEXT,
    performance_grade VARCHAR(10),
    report_time TIMESTAMP
);

-- 🔴 场景2：告警智能分析结果
CREATE TABLE alarm_intelligence_result (
    report_id VARCHAR(100) PRIMARY KEY,
    report_type VARCHAR(50),
    time_period VARCHAR(50),
    location VARCHAR(50),
    total_incidents BIGINT,
    critical_devices BIGINT,
    efficiency_impact DOUBLE PRECISION,
    temperature_anomaly BIGINT,
    risk_assessment VARCHAR(50),
    suggested_actions TEXT,
    priority_level VARCHAR(20),
    report_time TIMESTAMP
);

-- 🔴 场景3：故障分析结果
CREATE TABLE fault_analysis_result (
    analysis_id VARCHAR(100) PRIMARY KEY,
    device_id VARCHAR(50),
    analysis_type VARCHAR(50),
    fault_time_period VARCHAR(50),
    pre_fault_performance DOUBLE PRECISION,
    fault_indicators TEXT,
    root_cause_analysis TEXT,
    performance_degradation DOUBLE PRECISION,
    recovery_suggestions TEXT,
    maintenance_priority VARCHAR(20),
    analysis_time TIMESTAMP
);

-- ===============================================
-- 🎯 4. 目标分析数据库表结构
-- ===============================================

\c sgcc_target;

-- 授权schema权限
GRANT ALL PRIVILEGES ON SCHEMA public TO sgcc_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO sgcc_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO sgcc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO sgcc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO sgcc_user;

-- 🔴 场景4：柱状流性能优化结果
CREATE TABLE columnar_performance_result (
    report_id VARCHAR(100) PRIMARY KEY,
    report_type VARCHAR(50),
    analysis_period VARCHAR(50),
    location VARCHAR(50),
    io_optimization_ratio DOUBLE PRECISION,
    network_saving_ratio DOUBLE PRECISION,
    query_performance_boost DOUBLE PRECISION,
    storage_efficiency DOUBLE PRECISION,
    recommended_optimizations TEXT,
    cost_savings_estimate DOUBLE PRECISION,
    report_time TIMESTAMP
);

-- 🔴 综合场景：智能电网综合分析结果
CREATE TABLE smart_grid_comprehensive_result (
    report_id VARCHAR(100) PRIMARY KEY,
    report_type VARCHAR(50),
    analysis_period VARCHAR(50),
    grid_region VARCHAR(50),
    grid_stability_index DOUBLE PRECISION,
    operational_efficiency DOUBLE PRECISION,
    energy_optimization_score DOUBLE PRECISION,
    reliability_rating VARCHAR(20),
    risk_assessment VARCHAR(50),
    performance_trends TEXT,
    optimization_recommendations TEXT,
    cost_benefit_analysis DOUBLE PRECISION,
    report_time TIMESTAMP
);

-- ===============================================
-- 🔌 5. 初始化测试数据
-- ===============================================

\c sgcc_source_db;

-- 🎯 场景1：设备原始数据初始化
INSERT INTO device_raw_data VALUES
('100001', 235.5, 150.0, 45.2, 350.8, 0.96, 'A', 'L', NOW()),
('100002', 228.3, 125.5, 38.7, 280.3, 0.94, 'A', 'L', NOW()),
('100003', 240.1, 180.2, 52.1, 420.5, 0.92, 'M', 'M', NOW()),
('100004', 221.7, 95.8, 41.3, 210.9, 0.89, 'A', 'H', NOW()),
('100005', 238.9, 165.4, 48.8, 395.7, 0.95, 'A', 'L', NOW()),
('100006', 233.2, 140.1, 43.5, 325.6, 0.93, 'M', 'M', NOW()),
('100007', 226.8, 110.9, 36.2, 250.4, 0.91, 'A', 'L', NOW()),
('100008', 242.3, 175.7, 55.9, 445.2, 0.97, 'A', 'L', NOW()),
('100009', 219.4, 88.3, 39.1, 195.7, 0.87, 'O', 'H', NOW()),
('100010', 237.6, 158.2, 46.7, 378.1, 0.94, 'A', 'M', NOW()),
('100011', 232.1, 135.9, 42.8, 315.3, 0.92, 'M', 'M', NOW()),
('100012', 229.7, 118.6, 40.5, 265.8, 0.90, 'A', 'L', NOW()),
('100013', 241.8, 182.4, 54.3, 435.9, 0.96, 'A', 'L', NOW()),
('100014', 224.5, 102.7, 37.9, 225.1, 0.88, 'A', 'M', NOW()),
('100015', 236.3, 162.8, 49.1, 388.4, 0.95, 'A', 'L', NOW()),
('100016', 230.9, 128.3, 44.2, 295.7, 0.91, 'M', 'M', NOW()),
('100017', 239.4, 171.5, 51.6, 410.2, 0.93, 'A', 'L', NOW()),
('100018', 222.1, 92.4, 35.8, 205.3, 0.86, 'O', 'H', NOW()),
('100019', 234.7, 145.2, 47.3, 340.6, 0.94, 'A', 'M', NOW()),
('100020', 227.5, 115.7, 41.9, 255.9, 0.89, 'A', 'L', NOW());

-- 🔴 场景2：设备告警初始化
INSERT INTO device_alarms VALUES
('ALARM001', '100001', 'TEMPERATURE', 'HIGH', '设备温度过高告警', NOW() - INTERVAL '1 hour', 'MONITORING_SYS'),
('ALARM002', '100003', 'EFFICIENCY', 'MEDIUM', '效率下降告警', NOW() - INTERVAL '2 hours', 'MONITORING_SYS'),
('ALARM003', '100004', 'VOLTAGE', 'HIGH', '电压异常告警', NOW() - INTERVAL '30 minutes', 'MONITORING_SYS'),
('ALARM004', '100009', 'STATUS', 'CRITICAL', '设备离线告警', NOW() - INTERVAL '15 minutes', 'MONITORING_SYS'),
('ALARM005', '100018', 'POWER', 'HIGH', '功率异常告警', NOW() - INTERVAL '45 minutes', 'MONITORING_SYS'),
('ALARM006', '100002', 'TEMPERATURE', 'MEDIUM', '温度预警', NOW() - INTERVAL '3 hours', 'MONITORING_SYS'),
('ALARM007', '100007', 'EFFICIENCY', 'LOW', '效率轻微下降', NOW() - INTERVAL '1.5 hours', 'MONITORING_SYS'),
('ALARM008', '100012', 'VOLTAGE', 'MEDIUM', '电压波动告警', NOW() - INTERVAL '20 minutes', 'MONITORING_SYS'),
('ALARM009', '100015', 'CURRENT', 'HIGH', '电流过大告警', NOW() - INTERVAL '40 minutes', 'MONITORING_SYS'),
('ALARM010', '100020', 'MAINTENANCE', 'MEDIUM', '维护提醒', NOW() - INTERVAL '2.5 hours', 'MONITORING_SYS');

-- 🔴 场景2：设备状态初始化
INSERT INTO device_status VALUES
('100001', '智能变压器_001', 235.5, 150.0, 45.2, 0.96, 'NORMAL', NOW()),
('100002', '智能发电机_002', 228.3, 125.5, 38.7, 0.94, 'NORMAL', NOW()),
('100003', '配电设备_003', 240.1, 180.2, 52.1, 0.92, 'WARNING', NOW()),
('100004', '智能变压器_004', 221.7, 95.8, 41.3, 0.89, 'NORMAL', NOW()),
('100005', '智能发电机_005', 238.9, 165.4, 48.8, 0.95, 'NORMAL', NOW()),
('100006', '配电设备_006', 233.2, 140.1, 43.5, 0.93, 'WARNING', NOW()),
('100007', '智能变压器_007', 226.8, 110.9, 36.2, 0.91, 'NORMAL', NOW()),
('100008', '智能发电机_008', 242.3, 175.7, 55.9, 0.97, 'NORMAL', NOW()),
('100009', '配电设备_009', 219.4, 88.3, 39.1, 0.87, 'OFFLINE', NOW()),
('100010', '智能变压器_010', 237.6, 158.2, 46.7, 0.94, 'NORMAL', NOW()),
('100011', '智能发电机_011', 232.1, 135.9, 42.8, 0.92, 'WARNING', NOW()),
('100012', '配电设备_012', 229.7, 118.6, 40.5, 0.90, 'NORMAL', NOW()),
('100013', '智能变压器_013', 241.8, 182.4, 54.3, 0.96, 'NORMAL', NOW()),
('100014', '智能发电机_014', 224.5, 102.7, 37.9, 0.88, 'NORMAL', NOW()),
('100015', '配电设备_015', 236.3, 162.8, 49.1, 0.95, 'NORMAL', NOW()),
('100016', '智能变压器_016', 230.9, 128.3, 44.2, 0.91, 'WARNING', NOW()),
('100017', '智能发电机_017', 239.4, 171.5, 51.6, 0.93, 'NORMAL', NOW()),
('100018', '配电设备_018', 222.1, 92.4, 35.8, 0.86, 'OFFLINE', NOW()),
('100019', '智能变压器_019', 234.7, 145.2, 47.3, 0.94, 'NORMAL', NOW()),
('100020', '智能发电机_020', 227.5, 115.7, 41.9, 0.89, 'NORMAL', NOW());

-- 🔴 场景3：设备历史数据初始化（多时间点数据）
INSERT INTO device_historical_data VALUES
-- 2小时前数据
('HIST001_2H', '100001', NOW() - INTERVAL '2 hours', 233.2, 148.5, 43.1, 345.2, 0.95, 85.2, 'AUTO', ''),
('HIST002_2H', '100002', NOW() - INTERVAL '2 hours', 226.8, 123.1, 37.5, 275.8, 0.93, 78.9, 'AUTO', ''),
('HIST003_2H', '100003', NOW() - INTERVAL '2 hours', 238.7, 178.9, 50.8, 415.3, 0.91, 92.1, 'MANUAL', 'TEMP_HIGH'),
-- 1小时前数据
('HIST001_1H', '100001', NOW() - INTERVAL '1 hour', 234.8, 149.2, 44.7, 348.6, 0.955, 87.3, 'AUTO', ''),
('HIST002_1H', '100002', NOW() - INTERVAL '1 hour', 227.5, 124.8, 38.1, 278.4, 0.935, 79.8, 'AUTO', ''),
('HIST003_1H', '100003', NOW() - INTERVAL '1 hour', 239.3, 179.5, 51.5, 418.7, 0.915, 93.2, 'MANUAL', 'TEMP_HIGH'),
-- 30分钟前数据
('HIST001_30M', '100001', NOW() - INTERVAL '30 minutes', 235.1, 149.8, 44.9, 350.1, 0.958, 88.1, 'AUTO', ''),
('HIST002_30M', '100002', NOW() - INTERVAL '30 minutes', 228.0, 125.2, 38.4, 279.8, 0.938, 80.2, 'AUTO', ''),
('HIST003_30M', '100003', NOW() - INTERVAL '30 minutes', 239.8, 180.0, 51.9, 420.1, 0.918, 93.8, 'MANUAL', 'TEMP_HIGH'),
-- 当前数据
('HIST001_NOW', '100001', NOW(), 235.5, 150.0, 45.2, 350.8, 0.96, 88.5, 'AUTO', ''),
('HIST002_NOW', '100002', NOW(), 228.3, 125.5, 38.7, 280.3, 0.94, 80.5, 'AUTO', ''),
('HIST003_NOW', '100003', NOW(), 240.1, 180.2, 52.1, 420.5, 0.92, 94.1, 'MANUAL', 'TEMP_HIGH');

-- 🔴 场景4：大规模监控数据初始化
INSERT INTO large_scale_monitoring_data VALUES
('LARGE001', '100001', NOW(), 235.5, 234.8, 236.2, 150.0, 148.7, 151.3, 350.8, 45.2, 65.3, 28.5, 88.5, 0.96, 15420, 1250.8, 1200.3, 0.12, 2950.5, 15.3),
('LARGE002', '100002', NOW(), 228.3, 227.9, 229.1, 125.5, 124.2, 126.8, 280.3, 38.7, 58.9, 25.2, 80.5, 0.94, 13890, 980.3, 940.7, 0.11, 2370.9, 22.1),
('LARGE003', '100003', NOW(), 240.1, 239.5, 240.8, 180.2, 179.1, 181.3, 420.5, 52.1, 72.4, 31.8, 94.1, 0.92, 18760, 1580.2, 1520.8, 0.13, 3445.1, 45.7),
('LARGE004', '100004', NOW(), 221.7, 220.9, 222.5, 95.8, 94.5, 97.1, 210.9, 41.3, 61.2, 26.7, 75.8, 0.89, 12340, 785.4, 755.2, 0.10, 1890.3, 38.2),
('LARGE005', '100005', NOW(), 238.9, 238.1, 239.7, 165.4, 164.2, 166.6, 395.7, 48.8, 68.7, 29.3, 91.2, 0.95, 16850, 1420.6, 1380.4, 0.12, 3250.8, 18.9);

-- 🔴 综合场景：电力调度数据初始化
INSERT INTO power_dispatch_data VALUES
('DISPATCH001', NOW(), '华北电网', 10500.5, 10800.2, 50.02, 500.8, 'BALANCED', 'NORMAL', 'MAINTAIN_LOAD', 25),
('DISPATCH002', NOW() - INTERVAL '5 minutes', '华东电网', 11200.8, 11150.3, 49.98, 525.2, 'UNDERSUPPLY', 'MEDIUM', 'INCREASE_GENERATION', 18),
('DISPATCH003', NOW() - INTERVAL '10 minutes', '华南电网', 9800.2, 10100.5, 50.05, 480.1, 'OVERSUPPLY', 'LOW', 'REDUCE_GENERATION', 32),
('DISPATCH004', NOW() - INTERVAL '15 minutes', '西北电网', 8500.7, 8600.9, 49.99, 440.8, 'BALANCED', 'NORMAL', 'MAINTAIN_LOAD', 28),
('DISPATCH005', NOW() - INTERVAL '20 minutes', '东北电网', 7200.3, 7150.8, 50.01, 420.5, 'UNDERSUPPLY', 'HIGH', 'EMERGENCY_GENERATION', 15);

-- 🔴 综合场景：设备维度数据初始化
INSERT INTO device_dimension_data VALUES
('100001', '智能变压器_北京001', '变压器', '北京', 500.0, NOW() - INTERVAL '2 years', '西门子', 'SGB-500/220', 0.96, 'NORMAL', 235.5, 150.0, 45.2),
('100002', '智能发电机_上海002', '发电机', '上海', 350.0, NOW() - INTERVAL '1.5 years', '通用电气', 'GE-350MW', 0.94, 'NORMAL', 228.3, 125.5, 38.7),
('100003', '配电设备_广州003', '配电设备', '广州', 280.0, NOW() - INTERVAL '3 years', '施耐德', 'SE-280kV', 0.92, 'WARNING', 240.1, 180.2, 52.1),
('100004', '智能变压器_深圳004', '变压器', '深圳', 450.0, NOW() - INTERVAL '1 year', 'ABB', 'ABB-450/220', 0.89, 'NORMAL', 221.7, 95.8, 41.3),
('100005', '智能发电机_成都005', '发电机', '成都', 400.0, NOW() - INTERVAL '2.5 years', '东方电气', 'DEC-400MW', 0.95, 'NORMAL', 238.9, 165.4, 48.8);

-- ===============================================
-- 🔧 6. 启用逻辑复制（CDC支持）
-- ===============================================

-- 创建复制槽（用于CDC）
SELECT pg_create_logical_replication_slot('device_raw_slot', 'pgoutput');
SELECT pg_create_logical_replication_slot('device_alarms_slot', 'pgoutput');
SELECT pg_create_logical_replication_slot('device_status_slot', 'pgoutput');
SELECT pg_create_logical_replication_slot('device_historical_slot', 'pgoutput');

-- ===============================================
-- 🔍 7. 数据验证查询
-- ===============================================

-- 验证各表数据量
SELECT 'device_raw_data' as table_name, COUNT(*) as record_count FROM device_raw_data
UNION ALL
SELECT 'device_alarms' as table_name, COUNT(*) as record_count FROM device_alarms
UNION ALL
SELECT 'device_status' as table_name, COUNT(*) as record_count FROM device_status
UNION ALL
SELECT 'device_historical_data' as table_name, COUNT(*) as record_count FROM device_historical_data
UNION ALL
SELECT 'large_scale_monitoring_data' as table_name, COUNT(*) as record_count FROM large_scale_monitoring_data
UNION ALL
SELECT 'power_dispatch_data' as table_name, COUNT(*) as record_count FROM power_dispatch_data
UNION ALL
SELECT 'device_dimension_data' as table_name, COUNT(*) as record_count FROM device_dimension_data;

-- ===============================================
-- 🎯 8. 使用说明
-- ===============================================

/*
🎯 初始化完成！使用说明：

1. 📊 源数据库：sgcc_source_db
   - device_raw_data (20条) - 场景1：高频维度表服务
   - device_alarms (10条) - 场景2：智能双流JOIN
   - device_status (20条) - 场景2：智能双流JOIN
   - device_historical_data (12条) - 场景3：时间旅行查询
   - large_scale_monitoring_data (5条) - 场景4：柱状流优化
   - power_dispatch_data (5条) - 综合场景
   - device_dimension_data (5条) - 综合场景

2. 🎯 目标数据库：sgcc_dw_db
   - device_final_report - 场景1结果表
   - alarm_intelligence_result - 场景2结果表
   - fault_analysis_result - 场景3结果表

3. 🎯 目标数据库：sgcc_target
   - columnar_performance_result - 场景4结果表
   - smart_grid_comprehensive_result - 综合场景结果表

4. 🔧 CDC支持：
   - 已创建逻辑复制槽，支持Fluss CDC连接器
   - 已授权用户权限，可直接连接

5. 🧪 测试建议：
   - 手动INSERT/UPDATE/DELETE源表数据观察CDC效果
   - 各表已有足够测试数据，可直接运行Fluss作业
   - 推荐先测试单个场景，再测试综合场景
*/

SELECT '🎉 PostgreSQL 初始化完成！所有表和数据已就绪。' as status; 