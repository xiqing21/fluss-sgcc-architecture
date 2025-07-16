-- 🔴 验证UPDATE/DELETE操作的CDC同步结果

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

-- 查看所有最近的CDC操作（显示操作类型）
-- 注意：这是流式查询，会持续显示新的变更
SELECT equipment_id, equipment_name, equipment_type, voltage_level, capacity_mw
FROM power_equipment_fluss_target 
WHERE equipment_id >= 8000 AND equipment_id <= 9000
ORDER BY equipment_id
LIMIT 10; 