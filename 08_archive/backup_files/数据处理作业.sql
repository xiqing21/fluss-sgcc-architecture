-- 🔴 数据处理作业：DWD层到DWS层的实时汇聚
-- 从CDC源数据实时计算汇总统计

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
-- 作业1：按设备类型统计汇聚（DWD → DWS）
-- ===============================================
-- 实时计算每种设备类型的统计信息
INSERT INTO dws_equipment_type_stats
SELECT 
    equipment_type,
    COUNT(*) as total_count,
    AVG(CAST(capacity_mw AS DOUBLE)) as avg_capacity_mw,
    MAX(CAST(capacity_mw AS DOUBLE)) as max_capacity_mw,
    MIN(CAST(capacity_mw AS DOUBLE)) as min_capacity_mw,
    SUM(CAST(capacity_mw AS DOUBLE)) as total_capacity_mw,
    CURRENT_TIMESTAMP as last_update_time
FROM power_equipment_fluss_target
WHERE capacity_mw IS NOT NULL
GROUP BY equipment_type; 