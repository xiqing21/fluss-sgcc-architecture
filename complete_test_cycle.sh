#!/bin/bash

# 🔄 完整测试循环脚本
# 解决任务积累问题，完整测试数据流

echo "🚀 开始完整测试循环..."

# 1. 停止所有任务
echo "🛑 步骤1: 停止所有Flink任务..."
docker-compose restart jobmanager taskmanager-1 taskmanager-2 sql-client
echo "⏳ 等待服务重启..."
sleep 15

# 2. 检查任务状态
echo "📊 步骤2: 检查任务状态..."
docker exec jobmanager-sgcc /opt/flink/bin/flink list

# 3. 执行SQL文件
echo "🔧 步骤3: 执行SQL文件..."
docker exec -it sql-client-sgcc /opt/flink/bin/sql-client.sh embedded -f /opt/sql/1_fluss_all_chain.sql

# 4. 等待数据处理
echo "⏳ 步骤4: 等待数据处理..."
sleep 20

# 5. 验证数据
echo "🔍 步骤5: 验证数据..."
./quick_verify_data.sh

# 6. 提供后续选择
echo ""
echo "🎯 后续操作选择:"
echo "1. 启动交互式session: ./interactive_sql_session.sh"
echo "2. 重新运行测试: ./complete_test_cycle.sh"
echo "3. 只验证数据: ./quick_verify_data.sh"
echo "4. 添加更多测试数据: 手动执行INSERT语句"

echo ""
echo "✅ 完整测试循环结束" 