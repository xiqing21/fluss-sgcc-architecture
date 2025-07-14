#!/bin/bash

# 🔴 标准化SQL执行脚本
# 这个脚本遵循我们发现的最佳实践

# 使用方法：
# ./执行SQL脚本.sh filename.sql

if [ $# -eq 0 ]; then
    echo "🔴 使用方法: $0 <sql_file>"
    echo "例如: $0 simple_cdc_test.sql"
    exit 1
fi

SQL_FILE="$1"
CONTAINER_SQL_FILE="/opt/flink/$(basename $SQL_FILE)"

# 检查SQL文件是否存在
if [ ! -f "$SQL_FILE" ]; then
    echo "❌ 错误：SQL文件 $SQL_FILE 不存在"
    exit 1
fi

# 检查是否包含TABLEAU模式设置
if ! grep -q "sql-client.execution.result-mode.*TABLEAU" "$SQL_FILE"; then
    echo "⚠️  警告：SQL文件开头应该包含："
    echo "SET 'sql-client.execution.result-mode' = 'TABLEAU';"
    echo ""
    read -p "是否继续执行？(y/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "❌ 执行已取消"
        exit 1
    fi
fi

echo "🔴 开始执行SQL文件: $SQL_FILE"
echo "======================================"

# 1. 检查容器状态
echo "1. 检查Flink集群状态..."
if ! docker exec jobmanager-sgcc curl -s http://localhost:8081/overview > /dev/null 2>&1; then
    echo "❌ 错误：Flink集群不可用"
    exit 1
fi
echo "✅ Flink集群正常"

# 2. 复制SQL文件到容器
echo "2. 复制SQL文件到容器..."
if ! docker cp "$SQL_FILE" "jobmanager-sgcc:$CONTAINER_SQL_FILE"; then
    echo "❌ 错误：复制文件失败"
    exit 1
fi
echo "✅ 文件复制成功"

# 3. 执行SQL文件
echo "3. 执行SQL文件..."
echo "======================================"
docker exec jobmanager-sgcc /opt/flink/bin/sql-client.sh -f "$CONTAINER_SQL_FILE"
RESULT=$?

# 4. 清理临时文件
echo "======================================"
echo "4. 清理临时文件..."
docker exec jobmanager-sgcc rm -f "$CONTAINER_SQL_FILE"

# 5. 显示结果
if [ $RESULT -eq 0 ]; then
    echo "✅ SQL执行成功完成"
else
    echo "❌ SQL执行失败，退出码: $RESULT"
    exit $RESULT
fi

echo "🔴 执行完成！" 