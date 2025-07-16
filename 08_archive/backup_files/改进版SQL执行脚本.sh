#!/bin/bash

# 🔴 改进版SQL执行脚本 - 支持自动超时（跨平台）
# 解决流式查询卡住问题

echo "🔴 改进版SQL执行脚本 - 支持自动超时"
echo "=============================================="

# 检测超时命令
TIMEOUT_CMD=""
if command -v timeout > /dev/null 2>&1; then
    TIMEOUT_CMD="timeout"
elif command -v gtimeout > /dev/null 2>&1; then
    TIMEOUT_CMD="gtimeout"
else
    echo "⚠️  未找到timeout命令，流式查询可能需要手动Ctrl+C结束"
    echo "   macOS用户可以安装：brew install coreutils"
fi

# 检查Flink集群状态
echo "1. 检查Flink集群状态..."
if ! docker exec jobmanager-sgcc curl -s http://localhost:8081/overview > /dev/null 2>&1; then
    echo "❌ 错误：Flink集群不可用"
    exit 1
fi
echo "✅ Flink集群正常"

# 获取SQL命令和超时设置
SQL_COMMANDS=""
TIMEOUT_SECONDS=15  # 默认15秒超时

# 解析参数
while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--timeout)
            TIMEOUT_SECONDS="$2"
            shift 2
            ;;
        *)
            SQL_COMMANDS="$1"
            shift
            ;;
    esac
done

# 如果没有从参数获取，尝试从标准输入获取
if [ -z "$SQL_COMMANDS" ] && [ ! -t 0 ]; then
    SQL_COMMANDS=$(cat)
fi

# 检查是否有SQL命令
if [ -z "$SQL_COMMANDS" ]; then
    echo "❌ 错误：请提供SQL命令"
    echo "使用方法："
    echo "  $0 'SHOW TABLES;'"
    echo "  $0 -t 30 'SELECT * FROM streaming_table LIMIT 5;'  # 30秒超时"
    echo "  echo 'SHOW TABLES;' | $0"
    exit 1
fi

# 检测是否为流式查询（包含SELECT但不是SHOW/DESC/EXPLAIN）
IS_STREAMING_QUERY=false
if echo "$SQL_COMMANDS" | grep -iE "SELECT.*FROM" | grep -ivE "(SHOW|DESC|DESCRIBE|EXPLAIN)" > /dev/null; then
    IS_STREAMING_QUERY=true
fi

# 自动添加分号（如果没有）
if ! echo "$SQL_COMMANDS" | grep -q ";"; then
    SQL_COMMANDS="$SQL_COMMANDS;"
fi

# 构建完整的SQL
FULL_SQL="SET 'sql-client.execution.result-mode' = 'tableau';
$SQL_COMMANDS"

echo "2. 执行SQL命令..."
if [ "$IS_STREAMING_QUERY" = true ]; then
    if [ -n "$TIMEOUT_CMD" ]; then
        echo "🔄 检测到流式查询，将在${TIMEOUT_SECONDS}秒后自动超时"
    else
        echo "🔄 检测到流式查询，如需结束请按Ctrl+C"
    fi
fi
echo "=============================================="

# 根据是否为流式查询决定是否使用超时
if [ "$IS_STREAMING_QUERY" = true ] && [ -n "$TIMEOUT_CMD" ]; then
    # 流式查询使用超时
    $TIMEOUT_CMD ${TIMEOUT_SECONDS}s docker exec -i jobmanager-sgcc /opt/flink/bin/sql-client.sh <<EOF
$FULL_SQL
EOF
    RESULT=$?
    
    # 超时是正常情况
    if [ $RESULT -eq 124 ]; then
        echo ""
        echo "=============================================="
        echo "⏰ 查询在${TIMEOUT_SECONDS}秒后自动超时（这是正常的流式查询行为）"
        echo "✅ 流式查询执行成功"
        RESULT=0
    fi
else
    # 非流式查询或无超时命令时正常执行
    docker exec -i jobmanager-sgcc /opt/flink/bin/sql-client.sh <<EOF
$FULL_SQL
EOF
    RESULT=$?
fi

echo "=============================================="
if [ $RESULT -eq 0 ]; then
    echo "✅ SQL执行成功完成"
else
    echo "❌ SQL执行失败，退出码: $RESULT"
    exit $RESULT
fi

echo "🔴 执行完成！" 