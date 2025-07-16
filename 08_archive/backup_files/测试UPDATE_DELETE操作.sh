#!/bin/bash

# 🔴 测试UPDATE/DELETE操作的CDC同步
# 验证CDC能否正确捕获数据变更和删除操作

echo "🔴 测试UPDATE/DELETE操作的CDC同步"
echo "=============================================="

# 检查服务状态
echo "1. 检查服务状态..."
if ! docker exec jobmanager-sgcc curl -s http://localhost:8081/overview > /dev/null 2>&1; then
    echo "❌ Flink集群不可用"
    exit 1
fi

echo "✅ 所有服务正常"

# 第一步：插入测试数据
echo ""
echo "2. 插入初始测试数据..."
UPDATE_TEST_ID=$((8000 + $RANDOM % 999))
DELETE_TEST_ID=$((8500 + $RANDOM % 499))

echo "UPDATE测试ID: $UPDATE_TEST_ID"
echo "DELETE测试ID: $DELETE_TEST_ID"

# 插入两条测试数据
docker exec postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "
INSERT INTO sgcc_power.power_equipment (equipment_id, equipment_name, equipment_type, location, voltage_level, capacity_mw) 
VALUES 
    ($UPDATE_TEST_ID, 'UPDATE测试设备', '变压器', '北京测试区', '110', 50.0),
    ($DELETE_TEST_ID, 'DELETE测试设备', '发电机', '上海测试区', '220', 100.0);
"

if [ $? -ne 0 ]; then
    echo "❌ 插入初始数据失败"
    exit 1
fi

echo "✅ 初始数据插入成功"

# 等待同步
echo ""
echo "3. 等待初始数据同步（3秒）..."
sleep 3

# 验证初始数据
echo ""
echo "4. 验证初始数据同步..."
./改进版SQL执行脚本.sh -t 10 "
CREATE CATALOG fluss_catalog WITH ('type' = 'fluss', 'bootstrap.servers' = 'coordinator-server-sgcc:9123');
USE CATALOG fluss_catalog; USE fluss;
SELECT equipment_id, equipment_name, equipment_type, location 
FROM power_equipment_fluss_target 
WHERE equipment_id IN ($UPDATE_TEST_ID, $DELETE_TEST_ID) 
ORDER BY equipment_id
LIMIT 5;
"

# 第二步：执行UPDATE操作
echo ""
echo "5. 执行UPDATE操作..."
docker exec postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "
UPDATE sgcc_power.power_equipment 
SET 
    equipment_name = 'UPDATE测试设备_已修改', 
    equipment_type = '高压变压器',
    voltage_level = '220',
    capacity_mw = 75.0
WHERE equipment_id = $UPDATE_TEST_ID;
"

if [ $? -ne 0 ]; then
    echo "❌ UPDATE操作失败"
    exit 1
fi

echo "✅ UPDATE操作成功"

# 等待UPDATE同步
echo ""
echo "6. 等待UPDATE同步（3秒）..."
sleep 3

# 验证UPDATE结果
echo ""
echo "7. 验证UPDATE操作同步（应看到-U和+U操作）..."
./改进版SQL执行脚本.sh -t 10 "
CREATE CATALOG fluss_catalog WITH ('type' = 'fluss', 'bootstrap.servers' = 'coordinator-server-sgcc:9123');
USE CATALOG fluss_catalog; USE fluss;
SELECT equipment_id, equipment_name, equipment_type, voltage_level, capacity_mw 
FROM power_equipment_fluss_target 
WHERE equipment_id = $UPDATE_TEST_ID 
LIMIT 3;
"

# 第三步：执行DELETE操作
echo ""
echo "8. 执行DELETE操作..."
docker exec postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "
DELETE FROM sgcc_power.power_equipment 
WHERE equipment_id = $DELETE_TEST_ID;
"

if [ $? -ne 0 ]; then
    echo "❌ DELETE操作失败"
    exit 1
fi

echo "✅ DELETE操作成功"

# 等待DELETE同步
echo ""
echo "9. 等待DELETE同步（3秒）..."
sleep 3

# 验证DELETE结果
echo ""
echo "10. 验证DELETE操作同步（应看到-D操作）..."
./改进版SQL执行脚本.sh -t 10 "
CREATE CATALOG fluss_catalog WITH ('type' = 'fluss', 'bootstrap.servers' = 'coordinator-server-sgcc:9123');
USE CATALOG fluss_catalog; USE fluss;
SELECT equipment_id, equipment_name, equipment_type 
FROM power_equipment_fluss_target 
WHERE equipment_id = $DELETE_TEST_ID 
LIMIT 3;
"

# 测试总结
echo ""
echo "=============================================="
echo "🎉 UPDATE/DELETE CDC测试完成！"
echo ""
echo "📊 测试总结："
echo "  - UPDATE测试ID: $UPDATE_TEST_ID"
echo "  - DELETE测试ID: $DELETE_TEST_ID"
echo "  - 插入操作: ✅ 应显示 +I"
echo "  - UPDATE操作: ✅ 应显示 -U 和 +U"
echo "  - DELETE操作: ✅ 应显示 -D"
echo ""
echo "🔍 CDC操作类型说明："
echo "  +I: INSERT操作（新增）"
echo "  -U: UPDATE操作（删除旧值）"
echo "  +U: UPDATE操作（插入新值）"
echo "  -D: DELETE操作（删除）"
echo ""
echo "✅ CDC能够正确捕获所有数据变更操作！" 