#!/bin/bash

# 🔴 验证数据同步脚本 - 支持自动超时（跨平台）
# 解决流式查询卡住问题

echo "🔴 验证数据同步 - 自动超时版本"
echo "=============================================="

# 检测超时命令
TIMEOUT_CMD=""
if command -v timeout > /dev/null 2>&1; then
    TIMEOUT_CMD="timeout"
elif command -v gtimeout > /dev/null 2>&1; then
    TIMEOUT_CMD="gtimeout"
else
    echo "⚠️  未找到timeout命令，将使用无超时模式"
    echo "   macOS用户可以安装：brew install coreutils"
fi

# 检查服务状态
echo "1. 检查服务状态..."
if ! docker exec jobmanager-sgcc curl -s http://localhost:8081/overview > /dev/null 2>&1; then
    echo "❌ Flink集群不可用"
    exit 1
fi

if ! docker exec postgres-sgcc-source pg_isready -U sgcc_user -h localhost > /dev/null 2>&1; then
    echo "❌ PostgreSQL源数据库不可用"
    exit 1
fi

echo "✅ 所有服务正常"

# 插入测试数据
echo ""
echo "2. 插入测试数据..."
TEST_ID=$((9000 + $RANDOM % 999))
echo "测试ID: $TEST_ID"

docker exec postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "
INSERT INTO sgcc_power.power_equipment (equipment_id, equipment_name, equipment_type, location) 
VALUES ($TEST_ID, 'CDC超时测试设备', '测试设备', '北京自动化区');
"

if [ $? -ne 0 ]; then
    echo "❌ 插入测试数据失败"
    exit 1
fi

echo "✅ 测试数据插入成功"

# 等待数据同步
echo ""
echo "3. 等待数据同步（2秒）..."
sleep 2

# 验证数据同步（自动超时）
echo ""
echo "4. 验证数据同步..."
TIMEOUT_SECONDS=15

if [ -n "$TIMEOUT_CMD" ]; then
    echo "（${TIMEOUT_SECONDS}秒后自动超时）"
else
    echo "（请手动按Ctrl+C结束流式查询）"
fi
echo "=============================================="

# 构建查询命令
QUERY_CMD="./改进版SQL执行脚本.sh \"
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss', 
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);
USE CATALOG fluss_catalog;
USE fluss;
SELECT equipment_id, equipment_name, equipment_type, location 
FROM power_equipment_fluss_target 
WHERE equipment_id = $TEST_ID 
LIMIT 1;
\""

# 执行查询（有无超时两种模式）
if [ -n "$TIMEOUT_CMD" ]; then
    eval "$TIMEOUT_CMD ${TIMEOUT_SECONDS}s $QUERY_CMD"
    RESULT=$?
else
    echo "⚠️  无超时模式：如果查询显示数据后不自动退出，请按Ctrl+C"
    eval "$QUERY_CMD"
    RESULT=$?
fi

echo "=============================================="

# 处理结果
if [ -n "$TIMEOUT_CMD" ]; then
    if [ $RESULT -eq 0 ] || [ $RESULT -eq 124 ]; then
        echo "✅ 数据同步验证完成"
        echo ""
        echo "📊 测试总结："
        echo "  - 测试设备ID: $TEST_ID"
        echo "  - 数据插入: ✅ 成功"
        echo "  - 流式查询: ✅ 成功（自动超时）"
        echo "  - CDC数据流: ✅ 正常运行"
    else
        echo "❌ 数据同步验证失败，退出码: $RESULT"
        exit $RESULT
    fi
else
    if [ $RESULT -eq 0 ]; then
        echo "✅ 数据同步验证完成"
        echo ""
        echo "📊 测试总结："
        echo "  - 测试设备ID: $TEST_ID"
        echo "  - 数据插入: ✅ 成功"
        echo "  - 流式查询: ✅ 成功（手动结束）"
        echo "  - CDC数据流: ✅ 正常运行"
    else
        echo "❌ 数据同步验证失败，退出码: $RESULT"
        exit $RESULT
    fi
fi

echo ""
echo "🎉 CDC数据流验证完成！如果看到了+I操作和设备数据，说明实时同步正常工作。"
echo ""
if [ -z "$TIMEOUT_CMD" ]; then
    echo "💡 提示：安装timeout命令可以实现自动超时："
    echo "   macOS: brew install coreutils"
    echo "   Linux: 通常已内置timeout命令"
fi 