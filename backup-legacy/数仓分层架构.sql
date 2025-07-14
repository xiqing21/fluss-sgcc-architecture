-- 🔴 Fluss数仓分层架构实现
-- 基于国网电力监控系统的DWD/DWS/ADS三层架构

-- 设置输出模式
SET 'sql-client.execution.result-mode' = 'tableau';

-- 创建Fluss catalog
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- 切换到Fluss catalog
USE CATALOG fluss_catalog;
USE fluss;

-- ===============================================
-- 1. DWD层（数据明细层）- 已存在CDC源表
-- ===============================================
-- power_equipment_fluss_target 作为DWD源表（来自CDC）

-- ===============================================
-- 2. DWS层（数据汇总层）- 创建设备统计表
-- ===============================================

-- 2.1 按设备类型统计表（DWS层）
CREATE TABLE dws_equipment_type_stats (
    equipment_type STRING,
    total_count BIGINT,
    avg_capacity_mw DOUBLE,
    max_capacity_mw DOUBLE,
    min_capacity_mw DOUBLE,
    total_capacity_mw DOUBLE,
    last_update_time TIMESTAMP(3),
    PRIMARY KEY (equipment_type) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- 2.2 按地区统计表（DWS层）
CREATE TABLE dws_location_stats (
    location STRING,
    total_count BIGINT,
    equipment_types STRING,  -- 设备类型列表
    total_capacity_mw DOUBLE,
    avg_capacity_mw DOUBLE,
    last_update_time TIMESTAMP(3),
    PRIMARY KEY (location) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- 2.3 按电压等级统计表（DWS层）
CREATE TABLE dws_voltage_level_stats (
    voltage_level STRING,
    total_count BIGINT,
    total_capacity_mw DOUBLE,
    avg_capacity_mw DOUBLE,
    equipment_count_by_type STRING,  -- JSON格式的设备类型计数
    last_update_time TIMESTAMP(3),
    PRIMARY KEY (voltage_level) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- ===============================================
-- 3. ADS层（应用数据服务层）- 创建业务主题表
-- ===============================================

-- 3.1 设备健康度监控表（ADS层）
CREATE TABLE ads_equipment_health_monitor (
    monitor_date DATE,
    total_equipment BIGINT,
    active_equipment BIGINT,
    maintenance_equipment BIGINT,
    offline_equipment BIGINT,
    health_score DOUBLE,  -- 健康度评分 0-100
    risk_level STRING,    -- 风险等级：LOW/MEDIUM/HIGH
    PRIMARY KEY (monitor_date) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- 3.2 容量规划分析表（ADS层）
CREATE TABLE ads_capacity_planning (
    region STRING,
    voltage_level STRING,
    current_capacity_mw DOUBLE,
    utilization_rate DOUBLE,
    recommended_capacity_mw DOUBLE,
    gap_analysis STRING,
    planning_priority STRING,
    update_time TIMESTAMP(3),
    PRIMARY KEY (region, voltage_level) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- 3.3 实时运行状态大屏表（ADS层）
CREATE TABLE ads_realtime_dashboard (
    dashboard_id STRING,
    total_equipment BIGINT,
    online_equipment BIGINT,
    total_capacity_mw DOUBLE,
    current_load_mw DOUBLE,
    load_rate DOUBLE,
    alert_count BIGINT,
    system_status STRING,
    last_refresh_time TIMESTAMP(3),
    PRIMARY KEY (dashboard_id) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- 显示所有创建的表
SHOW TABLES; 