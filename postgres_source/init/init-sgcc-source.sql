-- ========================================
-- 国网电力监控系统 - 源数据库初始化脚本
-- ========================================

-- 创建国网业务模式
CREATE SCHEMA IF NOT EXISTS sgcc_power;

-- 设置数据库时区
SET timezone = 'Asia/Shanghai';

-- ========================================
-- 1. 电力设备主数据表
-- ========================================
CREATE TABLE sgcc_power.power_equipment (
    equipment_id BIGINT PRIMARY KEY,
    equipment_name VARCHAR(200) NOT NULL,
    equipment_type VARCHAR(50) NOT NULL,  -- 变压器、发电机、输电线路、配电设备等
    location VARCHAR(200),
    voltage_level VARCHAR(20),  -- 电压等级：110kV、220kV、500kV等
    capacity_mw DECIMAL(10,2),  -- 容量(MW)
    manufacturer VARCHAR(100),
    installation_date DATE,
    last_maintenance_date DATE,
    status VARCHAR(20) DEFAULT 'ACTIVE',  -- ACTIVE、INACTIVE、MAINTENANCE
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ========================================
-- 2. 实时电力监控数据表
-- ========================================
CREATE TABLE sgcc_power.power_monitoring (
    monitoring_id BIGINT PRIMARY KEY,
    equipment_id BIGINT NOT NULL,
    voltage_a DECIMAL(8,2),     -- A相电压(kV)
    voltage_b DECIMAL(8,2),     -- B相电压(kV)
    voltage_c DECIMAL(8,2),     -- C相电压(kV)
    current_a DECIMAL(8,2),     -- A相电流(A)
    current_b DECIMAL(8,2),     -- B相电流(A)
    current_c DECIMAL(8,2),     -- C相电流(A)
    power_active DECIMAL(10,2), -- 有功功率(MW)
    power_reactive DECIMAL(10,2), -- 无功功率(MVar)
    frequency DECIMAL(6,3),     -- 频率(Hz)
    temperature DECIMAL(6,2),   -- 设备温度(°C)
    humidity DECIMAL(5,2),      -- 湿度(%)
    monitoring_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ========================================
-- 3. 设备告警事件表
-- ========================================
CREATE TABLE sgcc_power.equipment_alarms (
    alarm_id BIGINT PRIMARY KEY,
    equipment_id BIGINT NOT NULL,
    alarm_type VARCHAR(50) NOT NULL,  -- FAULT、WARNING、CRITICAL、INFO
    alarm_level INTEGER NOT NULL,     -- 1-5级告警
    alarm_message TEXT,
    alarm_code VARCHAR(20),
    is_resolved BOOLEAN DEFAULT FALSE,
    occurred_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ========================================
-- 4. 客户用电信息表
-- ========================================
CREATE TABLE sgcc_power.customer_usage (
    usage_id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    customer_name VARCHAR(200) NOT NULL,
    customer_type VARCHAR(50),  -- 工业、商业、居民
    meter_reading DECIMAL(10,2), -- 电表读数(kWh)
    billing_period DATE,
    usage_amount DECIMAL(10,2), -- 用电量(kWh)
    peak_demand DECIMAL(8,2),   -- 最大需量(kW)
    tariff_type VARCHAR(20),    -- 电价类型
    billing_amount DECIMAL(10,2), -- 电费金额
    reading_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ========================================
-- 插入测试数据
-- ========================================

-- 插入设备数据
INSERT INTO sgcc_power.power_equipment (equipment_id, equipment_name, equipment_type, location, voltage_level, capacity_mw, manufacturer, installation_date, last_maintenance_date) VALUES
(1001, '中心变电站主变1号', '变压器', '北京朝阳区', '220kV', 300.00, '中国西电', '2020-01-15', '2024-01-15'),
(1002, '中心变电站主变2号', '变压器', '北京朝阳区', '220kV', 300.00, '中国西电', '2020-03-20', '2024-03-20'),
(1003, '东城输电线路1', '输电线路', '北京东城区', '110kV', 150.00, '国网北京', '2019-06-10', '2023-12-10'),
(1004, '丰台发电机组1', '发电机', '北京丰台区', '110kV', 200.00, '哈电集团', '2018-09-15', '2024-02-15'),
(1005, '海淀配电站1', '配电设备', '北京海淀区', '35kV', 50.00, '许继集团', '2021-05-08', '2024-05-08');

-- 插入监控数据
INSERT INTO sgcc_power.power_monitoring (monitoring_id, equipment_id, voltage_a, voltage_b, voltage_c, current_a, current_b, current_c, power_active, power_reactive, frequency, temperature, humidity, monitoring_time) VALUES
(2001, 1001, 220.5, 220.2, 220.8, 850.5, 848.2, 852.1, 315.8, 45.2, 50.01, 45.5, 65.2, CURRENT_TIMESTAMP - INTERVAL '1 hour'),
(2002, 1002, 219.8, 220.1, 220.5, 825.1, 830.5, 828.9, 290.5, 42.8, 50.02, 48.2, 67.5, CURRENT_TIMESTAMP - INTERVAL '30 minutes'),
(2003, 1003, 110.2, 110.5, 110.1, 1205.8, 1208.2, 1203.5, 145.2, 25.8, 49.98, 38.5, 58.9, CURRENT_TIMESTAMP - INTERVAL '15 minutes'),
(2004, 1004, 109.8, 110.2, 110.0, 1580.5, 1575.2, 1582.1, 195.8, 35.5, 50.00, 65.8, 72.1, CURRENT_TIMESTAMP - INTERVAL '10 minutes'),
(2005, 1005, 35.2, 35.1, 35.3, 1205.5, 1208.8, 1202.1, 48.5, 8.2, 50.01, 32.5, 55.8, CURRENT_TIMESTAMP - INTERVAL '5 minutes');

-- 插入告警数据
INSERT INTO sgcc_power.equipment_alarms (alarm_id, equipment_id, alarm_type, alarm_level, alarm_message, alarm_code, is_resolved, occurred_at) VALUES
(3001, 1001, 'WARNING', 2, '变压器温度偏高，当前温度45.5°C', 'TEMP_HIGH', FALSE, CURRENT_TIMESTAMP - INTERVAL '2 hours'),
(3002, 1003, 'CRITICAL', 4, '输电线路电流不平衡，可能存在故障', 'CURRENT_IMBALANCE', FALSE, CURRENT_TIMESTAMP - INTERVAL '1 hour'),
(3003, 1004, 'INFO', 1, '发电机组完成定期维护', 'MAINTENANCE_COMPLETE', TRUE, CURRENT_TIMESTAMP - INTERVAL '30 minutes'),
(3004, 1002, 'FAULT', 5, '变压器过载保护动作', 'OVERLOAD_PROTECTION', FALSE, CURRENT_TIMESTAMP - INTERVAL '15 minutes'),
(3005, 1005, 'WARNING', 3, '配电设备湿度过高，需要检查', 'HUMIDITY_HIGH', FALSE, CURRENT_TIMESTAMP - INTERVAL '10 minutes');

-- 插入客户用电数据
INSERT INTO sgcc_power.customer_usage (usage_id, customer_id, customer_name, customer_type, meter_reading, billing_period, usage_amount, peak_demand, tariff_type, billing_amount, reading_time) VALUES
(4001, 101, '北京钢铁集团', '工业', 125680.50, '2024-01-01', 8560.30, 1250.5, '大工业', 6848.24, CURRENT_TIMESTAMP - INTERVAL '1 day'),
(4002, 102, '朝阳购物中心', '商业', 45280.20, '2024-01-01', 3520.80, 580.2, '一般工商业', 2816.64, CURRENT_TIMESTAMP - INTERVAL '1 day'),
(4003, 103, '东城区居民小区A', '居民', 15680.10, '2024-01-01', 1280.50, 85.2, '居民生活', 768.30, CURRENT_TIMESTAMP - INTERVAL '1 day'),
(4004, 104, '海淀科技园', '工业', 89520.80, '2024-01-01', 5680.20, 920.8, '大工业', 4544.16, CURRENT_TIMESTAMP - INTERVAL '1 day'),
(4005, 105, '丰台医院', '商业', 32580.60, '2024-01-01', 2180.40, 320.5, '一般工商业', 1744.32, CURRENT_TIMESTAMP - INTERVAL '1 day');

-- ========================================
-- 创建更新触发器
-- ========================================
CREATE OR REPLACE FUNCTION update_sgcc_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 为各表创建更新触发器
CREATE TRIGGER update_power_equipment_updated_at 
    BEFORE UPDATE ON sgcc_power.power_equipment
    FOR EACH ROW EXECUTE FUNCTION update_sgcc_updated_at();

CREATE TRIGGER update_equipment_alarms_updated_at 
    BEFORE UPDATE ON sgcc_power.equipment_alarms
    FOR EACH ROW EXECUTE FUNCTION update_sgcc_updated_at();

CREATE TRIGGER update_customer_usage_updated_at 
    BEFORE UPDATE ON sgcc_power.customer_usage
    FOR EACH ROW EXECUTE FUNCTION update_sgcc_updated_at();

-- ========================================
-- 创建索引优化查询性能
-- ========================================
CREATE INDEX idx_power_monitoring_equipment_time ON sgcc_power.power_monitoring(equipment_id, monitoring_time);
CREATE INDEX idx_equipment_alarms_equipment_time ON sgcc_power.equipment_alarms(equipment_id, occurred_at);
CREATE INDEX idx_customer_usage_customer_period ON sgcc_power.customer_usage(customer_id, billing_period);
CREATE INDEX idx_power_equipment_type ON sgcc_power.power_equipment(equipment_type); 