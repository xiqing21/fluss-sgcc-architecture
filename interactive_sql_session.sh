#!/bin/bash

# 🔄 交互式SQL会话保持脚本
# 解决每次执行SQL文件自动退出的问题

echo "🚀 启动交互式SQL会话..."
echo "💡 这个会话将保持打开状态，你可以逐步执行SQL语句"
echo "📋 可用的快捷命令："
echo "  1. 执行完整链路: 复制粘贴 1_fluss_all_chain.sql 中的内容"
echo "  2. 查看任务状态: SHOW JOBS;"
echo "  3. 查看表列表: SHOW TABLES;"
echo "  4. 退出会话: exit;"
echo ""
echo "🔧 正在启动SQL客户端..."

# 启动交互式SQL客户端
docker exec -it sql-client-sgcc /opt/flink/bin/sql-client.sh embedded

echo "✅ SQL会话已结束" 