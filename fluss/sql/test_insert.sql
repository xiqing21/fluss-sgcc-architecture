-- 测试数据插入
USE CATALOG fluss_catalog;

-- 插入测试数据到Fluss表
INSERT INTO fluss_ods_power_equipment (
    equipment_id, equipment_name, equipment_type, location, 
    voltage_level, capacity_mw, manufacturer, installation_date, 
    last_maintenance_date, status, created_at, updated_at
) VALUES (
    1001, '变压器001', '变压器', '北京变电站A', 
    '220kV', 50.00, '西门子', '2023-01-15',
    '2024-06-01', '运行', 
    TIMESTAMP '2024-01-15 10:00:00', TIMESTAMP '2024-12-01 15:30:00'
);
