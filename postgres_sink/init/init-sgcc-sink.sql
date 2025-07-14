-- ========================================
-- 国网电力监控系统 - 目标数据库初始化脚本
-- Fluss架构下的数据仓库分层结果存储
-- ========================================

-- 创建数据仓库模式
CREATE SCHEMA IF NOT EXISTS sgcc_dw;

-- 设置数据库时区
SET timezone = 'Asia/Shanghai';

-- ========================================
-- DWD层 - 数据明细层 (Data Warehouse Detail)
-- ========================================

-- DWD层 - 电力设备明细表
CREATE TABLE sgcc_dw.dwd_power_equipment (
    equipment_id BIGINT PRIMARY KEY,
    equipment_name VARCHAR(200) NOT NULL,
    equipment_type VARCHAR(50) NOT NULL,
    equipment_type_code VARCHAR(10),  -- 设备类型编码
    location VARCHAR(200),
    province VARCHAR(50),             -- 省份
    city VARCHAR(50),                 -- 城市
    district VARCHAR(50),             -- 区县
    voltage_level VARCHAR(20),
    voltage_level_code INTEGER,       -- 电压等级编码
    capacity_mw DECIMAL(10,2),
    capacity_level VARCHAR(20),       -- 容量级别：小型、中型、大型
    manufacturer VARCHAR(100),
    installation_date DATE,
    installation_year INTEGER,
    last_maintenance_date DATE,
    maintenance_interval_days INTEGER,
    status VARCHAR(20),
    is_critical BOOLEAN DEFAULT FALSE, -- 是否关键设备
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- DWD层 - 电力监控明细表
CREATE TABLE sgcc_dw.dwd_power_monitoring (
    monitoring_id BIGINT PRIMARY KEY,
    equipment_id BIGINT NOT NULL,
    equipment_name VARCHAR(200),
    equipment_type VARCHAR(50),
    voltage_a DECIMAL(8,2),
    voltage_b DECIMAL(8,2),
    voltage_c DECIMAL(8,2),
    voltage_avg DECIMAL(8,2),         -- 平均电压
    voltage_imbalance DECIMAL(6,4),   -- 电压不平衡度
    current_a DECIMAL(8,2),
    current_b DECIMAL(8,2),
    current_c DECIMAL(8,2),
    current_avg DECIMAL(8,2),         -- 平均电流
    current_imbalance DECIMAL(6,4),   -- 电流不平衡度
    power_active DECIMAL(10,2),
    power_reactive DECIMAL(10,2),
    power_factor DECIMAL(6,4),        -- 功率因数
    power_apparent DECIMAL(10,2),     -- 视在功率
    frequency DECIMAL(6,3),
    frequency_deviation DECIMAL(6,4), -- 频率偏差
    temperature DECIMAL(6,2),
    humidity DECIMAL(5,2),
    monitoring_date DATE,
    monitoring_hour INTEGER,
    monitoring_minute INTEGER,
    is_peak_hour BOOLEAN,             -- 是否高峰时段
    is_working_day BOOLEAN,           -- 是否工作日
    monitoring_time TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- DWD层 - 设备告警明细表
CREATE TABLE sgcc_dw.dwd_equipment_alarms (
    alarm_id BIGINT PRIMARY KEY,
    equipment_id BIGINT NOT NULL,
    equipment_name VARCHAR(200),
    equipment_type VARCHAR(50),
    alarm_type VARCHAR(50) NOT NULL,
    alarm_level INTEGER NOT NULL,
    alarm_level_name VARCHAR(20),     -- 告警级别名称
    alarm_message TEXT,
    alarm_code VARCHAR(20),
    alarm_category VARCHAR(50),       -- 告警分类
    is_resolved BOOLEAN DEFAULT FALSE,
    resolution_time_minutes INTEGER,  -- 解决时间（分钟）
    occurred_date DATE,
    occurred_hour INTEGER,
    occurred_at TIMESTAMP,
    resolved_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ========================================
-- DWS层 - 数据汇总层 (Data Warehouse Summary)
-- ========================================

-- DWS层 - 设备日监控汇总表
CREATE TABLE sgcc_dw.dws_equipment_daily_summary (
    summary_id BIGINT PRIMARY KEY,
    equipment_id BIGINT NOT NULL,
    equipment_name VARCHAR(200),
    equipment_type VARCHAR(50),
    summary_date DATE,
    voltage_avg DECIMAL(8,2),
    voltage_max DECIMAL(8,2),
    voltage_min DECIMAL(8,2),
    voltage_std DECIMAL(8,4),         -- 电压标准差
    current_avg DECIMAL(8,2),
    current_max DECIMAL(8,2),
    current_min DECIMAL(8,2),
    power_active_avg DECIMAL(10,2),
    power_active_max DECIMAL(10,2),
    power_active_min DECIMAL(10,2),
    power_factor_avg DECIMAL(6,4),
    frequency_avg DECIMAL(6,3),
    temperature_avg DECIMAL(6,2),
    temperature_max DECIMAL(6,2),
    monitoring_count INTEGER,         -- 监控数据点数
    abnormal_count INTEGER,           -- 异常数据点数
    uptime_hours DECIMAL(6,2),        -- 运行时间（小时）
    utilization_rate DECIMAL(6,4),    -- 利用率
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- DWS层 - 告警日汇总表
CREATE TABLE sgcc_dw.dws_alarm_daily_summary (
    summary_id BIGINT PRIMARY KEY,
    summary_date DATE,
    equipment_type VARCHAR(50),
    alarm_type VARCHAR(50),
    alarm_level INTEGER,
    total_alarms INTEGER,
    resolved_alarms INTEGER,
    unresolved_alarms INTEGER,
    avg_resolution_time_minutes DECIMAL(8,2),
    max_resolution_time_minutes INTEGER,
    critical_alarms INTEGER,
    warning_alarms INTEGER,
    info_alarms INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ========================================
-- ADS层 - 应用数据服务层 (Application Data Service)
-- ========================================

-- ADS层 - 实时监控大屏数据表
CREATE TABLE sgcc_dw.ads_realtime_dashboard (
    dashboard_id BIGINT PRIMARY KEY,
    total_equipment INTEGER,
    online_equipment INTEGER,
    offline_equipment INTEGER,
    maintenance_equipment INTEGER,
    total_capacity_mw DECIMAL(12,2),
    active_capacity_mw DECIMAL(12,2),
    load_rate DECIMAL(6,4),
    total_active_alarms INTEGER,
    critical_alarms INTEGER,
    warning_alarms INTEGER,
    avg_voltage_220kv DECIMAL(8,2),
    avg_voltage_110kv DECIMAL(8,2),
    avg_frequency DECIMAL(6,3),
    system_status VARCHAR(20),        -- 系统状态：正常、告警、故障
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ADS层 - 电力质量分析表
CREATE TABLE sgcc_dw.ads_power_quality_analysis (
    analysis_id BIGINT PRIMARY KEY,
    analysis_date DATE,
    voltage_qualification_rate DECIMAL(6,4),  -- 电压合格率
    frequency_qualification_rate DECIMAL(6,4), -- 频率合格率
    power_factor_avg DECIMAL(6,4),             -- 平均功率因数
    voltage_imbalance_rate DECIMAL(6,4),       -- 电压不平衡率
    current_imbalance_rate DECIMAL(6,4),       -- 电流不平衡率
    harmonic_distortion_rate DECIMAL(6,4),     -- 谐波畸变率
    power_quality_score DECIMAL(6,2),          -- 电力质量评分
    quality_level VARCHAR(20),                 -- 质量等级
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ADS层 - 设备健康度评估表
CREATE TABLE sgcc_dw.ads_equipment_health_assessment (
    assessment_id BIGINT PRIMARY KEY,
    equipment_id BIGINT NOT NULL,
    equipment_name VARCHAR(200),
    equipment_type VARCHAR(50),
    health_score DECIMAL(6,2),        -- 健康度评分(0-100)
    health_level VARCHAR(20),         -- 健康等级：优秀、良好、一般、差
    reliability_score DECIMAL(6,2),   -- 可靠性评分
    maintenance_urgency INTEGER,      -- 维护紧急度(1-5)
    predicted_failure_probability DECIMAL(6,4), -- 预测故障概率
    remaining_life_days INTEGER,      -- 预计剩余寿命(天)
    maintenance_recommendation TEXT,  -- 维护建议
    assessment_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ========================================
-- 创建索引优化查询性能
-- ========================================

-- DWD层索引
CREATE INDEX idx_dwd_power_monitoring_equipment_time ON sgcc_dw.dwd_power_monitoring(equipment_id, monitoring_time);
CREATE INDEX idx_dwd_power_monitoring_date ON sgcc_dw.dwd_power_monitoring(monitoring_date);
CREATE INDEX idx_dwd_equipment_alarms_equipment_time ON sgcc_dw.dwd_equipment_alarms(equipment_id, occurred_at);
CREATE INDEX idx_dwd_equipment_alarms_date ON sgcc_dw.dwd_equipment_alarms(occurred_date);

-- DWS层索引
CREATE INDEX idx_dws_equipment_daily_equipment_date ON sgcc_dw.dws_equipment_daily_summary(equipment_id, summary_date);
CREATE INDEX idx_dws_alarm_daily_date_type ON sgcc_dw.dws_alarm_daily_summary(summary_date, equipment_type);

-- ADS层索引
CREATE INDEX idx_ads_equipment_health_equipment ON sgcc_dw.ads_equipment_health_assessment(equipment_id);
CREATE INDEX idx_ads_power_quality_date ON sgcc_dw.ads_power_quality_analysis(analysis_date);

-- ========================================
-- 创建更新触发器
-- ========================================
CREATE OR REPLACE FUNCTION update_sgcc_dw_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 为相关表创建更新触发器
CREATE TRIGGER update_dwd_power_equipment_updated_at 
    BEFORE UPDATE ON sgcc_dw.dwd_power_equipment
    FOR EACH ROW EXECUTE FUNCTION update_sgcc_dw_updated_at();

CREATE TRIGGER update_dwd_equipment_alarms_updated_at 
    BEFORE UPDATE ON sgcc_dw.dwd_equipment_alarms
    FOR EACH ROW EXECUTE FUNCTION update_sgcc_dw_updated_at();

-- ========================================
-- 创建视图便于查询
-- ========================================

-- 实时设备状态视图
CREATE VIEW sgcc_dw.v_equipment_realtime_status AS
SELECT 
    e.equipment_id,
    e.equipment_name,
    e.equipment_type,
    e.location,
    e.voltage_level,
    e.capacity_mw,
    e.status,
    m.voltage_avg,
    m.current_avg,
    m.power_active,
    m.power_factor,
    m.frequency,
    m.temperature,
    m.monitoring_time,
    CASE 
        WHEN m.monitoring_time > NOW() - INTERVAL '10 minutes' THEN 'ONLINE'
        ELSE 'OFFLINE'
    END as connection_status
FROM sgcc_dw.dwd_power_equipment e
LEFT JOIN sgcc_dw.dwd_power_monitoring m ON e.equipment_id = m.equipment_id
WHERE m.monitoring_time = (
    SELECT MAX(monitoring_time) 
    FROM sgcc_dw.dwd_power_monitoring 
    WHERE equipment_id = e.equipment_id
);

-- 告警统计视图
CREATE VIEW sgcc_dw.v_alarm_statistics AS
SELECT 
    equipment_type,
    alarm_type,
    alarm_level,
    COUNT(*) as total_count,
    COUNT(CASE WHEN is_resolved = TRUE THEN 1 END) as resolved_count,
    COUNT(CASE WHEN is_resolved = FALSE THEN 1 END) as unresolved_count,
    AVG(resolution_time_minutes) as avg_resolution_time
FROM sgcc_dw.dwd_equipment_alarms
WHERE occurred_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY equipment_type, alarm_type, alarm_level; 