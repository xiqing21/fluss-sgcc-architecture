#!/bin/bash

echo "🚀 启动智能SQL执行模式..."
echo "📝 此模式会先执行所有SQL语句，然后保持交互式session"
echo "⚡ 你可以在session中继续执行其他SQL命令"
echo "🔍 使用 'SHOW TABLES;' 查看表，'SELECT * FROM table_name LIMIT 10;' 查看数据"
echo "❌ 输入 'quit;' 退出session"
echo ""

# 重启Flink集群确保clean state
echo "🔄 重启Flink集群..."
docker-compose restart jobmanager taskmanager-1 taskmanager-2
sleep 15

echo "🎯 开始执行SQL文件并保持交互式session..."
docker exec -it sql-client-sgcc /opt/flink/bin/sql-client.sh embedded -i /opt/sql/1_fluss_all_chain.sql

echo "✅ Session已结束" 