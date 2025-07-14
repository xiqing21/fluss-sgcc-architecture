-- 🔴 验证DWS层数据汇聚结果

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

-- 查看设备类型统计（DWS层）
SELECT equipment_type, total_count, avg_capacity_mw, total_capacity_mw, last_update_time 
FROM dws_equipment_type_stats 
LIMIT 10; 