#!/bin/bash

echo "🔄 Fluss实时更新演示 - 原生UPSERT特性"
echo "展示Fluss相较于Kafka的核心优势：原生支持数据更新"
echo "=================================================="

# 设置颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

echo ""
echo -e "${BLUE}🎯 Fluss实时更新特性展示${NC}"
echo -e "${YELLOW}传统Kafka架构痛点：${NC}"
echo "• 不支持原生UPDATE，需要复杂的changelog处理"
echo "• 状态更新需要外部存储系统支持"
echo "• 数据一致性难以保证"
echo "• 需要额外的compaction和状态管理"
echo ""
echo -e "${GREEN}Fluss解决方案：${NC}"
echo "• 原生支持UPSERT操作"
echo "• 自动处理数据更新和状态管理"
echo "• 保证数据一致性"
echo "• 简化开发和运维"

echo ""
echo -e "${CYAN}=================== 实时更新演示 ===================${NC}"

# 等待用户确认
echo -e "${YELLOW}按Enter键开始演示...${NC}"
read

echo ""
echo -e "${BLUE}1. 查看当前工单状态（更新前）${NC}"
echo -e "${YELLOW}查询待处理工单：${NC}"
docker exec -it sql-client-sgcc /opt/flink/bin/sql-client.sh embedded -e "
SELECT 
    ticket_id,
    title,
    status,
    priority,
    agent_id,
    created_at,
    updated_at
FROM fluss.fluss.ods_tickets 
WHERE status = 'pending'
ORDER BY ticket_id;
"

echo ""
echo -e "${YELLOW}按Enter键继续演示更新操作...${NC}"
read

echo ""
echo -e "${BLUE}2. 执行实时更新操作${NC}"
echo -e "${YELLOW}模拟工单状态更新（体现UPSERT特性）${NC}"

# 更新工单1009：从pending变为resolved
echo -e "${PURPLE}更新工单1009: pending -> resolved${NC}"
docker exec -it postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "
UPDATE tickets SET 
    status = 'resolved', 
    agent_id = 1,
    closed_at = CURRENT_TIMESTAMP 
WHERE ticket_id = 1009;
"

echo ""
echo -e "${PURPLE}更新工单1010: pending -> in_progress${NC}"
docker exec -it postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "
UPDATE tickets SET 
    status = 'in_progress', 
    agent_id = 2 
WHERE ticket_id = 1010;
"

echo ""
echo -e "${PURPLE}更新工单1011: 修改优先级 medium -> high${NC}"
docker exec -it postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "
UPDATE tickets SET 
    priority = 'high', 
    agent_id = 5 
WHERE ticket_id = 1011;
"

echo ""
echo -e "${PURPLE}添加新工单1013${NC}"
docker exec -it postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "
INSERT INTO tickets (ticket_id, customer_id, title, description, priority, status, category) 
VALUES (1013, 113, '新功能请求', '希望增加新的功能', 'low', 'pending', '功能建议');
"

echo ""
echo -e "${PURPLE}更新客服状态：张小明变为忙碌${NC}"
docker exec -it postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "
UPDATE agents SET status = 'busy' WHERE agent_id = 1;
"

echo ""
echo -e "${YELLOW}等待CDC同步到Fluss...${NC}"
sleep 5

echo ""
echo -e "${BLUE}3. 验证Fluss中的实时更新结果${NC}"
echo -e "${YELLOW}查询更新后的工单状态：${NC}"
docker exec -it sql-client-sgcc /opt/flink/bin/sql-client.sh embedded -e "
SELECT 
    ticket_id,
    title,
    status,
    priority,
    agent_id,
    created_at,
    updated_at,
    closed_at
FROM fluss.fluss.ods_tickets 
WHERE ticket_id IN (1009, 1010, 1011, 1013)
ORDER BY ticket_id;
"

echo ""
echo -e "${YELLOW}按Enter键继续...${NC}"
read

echo ""
echo -e "${BLUE}4. 查看客服状态更新结果${NC}"
echo -e "${YELLOW}查询更新后的客服状态：${NC}"
docker exec -it sql-client-sgcc /opt/flink/bin/sql-client.sh embedded -e "
SELECT 
    agent_id,
    agent_name,
    department,
    status,
    updated_at
FROM fluss.fluss.ods_agents 
WHERE agent_id IN (1, 2, 5)
ORDER BY agent_id;
"

echo ""
echo -e "${YELLOW}按Enter键继续...${NC}"
read

echo ""
echo -e "${BLUE}5. 查看实时统计指标更新${NC}"
echo -e "${YELLOW}查询实时工单状态分布（体现实时性）：${NC}"
docker exec -it sql-client-sgcc /opt/flink/bin/sql-client.sh embedded -e "
SELECT 
    status,
    COUNT(*) as ticket_count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as percentage
FROM fluss.fluss.ods_tickets 
GROUP BY status
ORDER BY ticket_count DESC;
"

echo ""
echo -e "${YELLOW}按Enter键继续...${NC}"
read

echo ""
echo -e "${BLUE}6. 查看客服工作负载实时变化${NC}"
echo -e "${YELLOW}查询客服工作负载实时统计：${NC}"
docker exec -it sql-client-sgcc /opt/flink/bin/sql-client.sh embedded -e "
SELECT 
    a.agent_name,
    a.department,
    a.status as agent_status,
    COUNT(t.ticket_id) as total_tickets,
    COUNT(CASE WHEN t.status = 'resolved' THEN 1 END) as resolved_tickets,
    COUNT(CASE WHEN t.status IN ('pending', 'in_progress') THEN 1 END) as active_tickets,
    ROUND(
        COUNT(CASE WHEN t.status = 'resolved' THEN 1 END) * 100.0 / 
        NULLIF(COUNT(t.ticket_id), 0), 2
    ) as resolution_rate
FROM fluss.fluss.ods_agents a
LEFT JOIN fluss.fluss.ods_tickets t ON a.agent_id = t.agent_id
GROUP BY a.agent_id, a.agent_name, a.department, a.status
ORDER BY resolved_tickets DESC;
"

echo ""
echo -e "${YELLOW}按Enter键继续...${NC}"
read

echo ""
echo -e "${BLUE}7. 添加操作日志记录${NC}"
echo -e "${YELLOW}模拟添加操作日志（append-only表）：${NC}"
docker exec -it postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "
INSERT INTO ticket_logs (log_id, ticket_id, agent_id, action, old_value, new_value, comment) VALUES
(35, 1009, 1, 'status_change', 'pending', 'resolved', '支付异常问题已解决'),
(36, 1010, 2, 'status_change', 'pending', 'in_progress', '开始处理退款申请'),
(37, 1011, 5, 'priority_change', 'medium', 'high', '账户冻结问题升级为高优先级'),
(38, 1013, NULL, 'create', 'NULL', 'pending', '新功能请求工单已创建');
"

echo ""
echo -e "${YELLOW}等待同步...${NC}"
sleep 3

echo ""
echo -e "${BLUE}8. 查看操作日志实时更新${NC}"
echo -e "${YELLOW}查询最新的操作日志：${NC}"
docker exec -it sql-client-sgcc /opt/flink/bin/sql-client.sh embedded -e "
SELECT 
    tl.ticket_id,
    tl.action,
    tl.old_value,
    tl.new_value,
    tl.comment,
    tl.created_at
FROM fluss.fluss.ods_ticket_logs tl
ORDER BY tl.created_at DESC
LIMIT 10;
"

echo ""
echo -e "${YELLOW}按Enter键继续...${NC}"
read

echo ""
echo -e "${BLUE}9. 测试连续更新场景${NC}"
echo -e "${YELLOW}模拟工单状态连续变化：${NC}"

# 连续更新工单1012
echo -e "${PURPLE}第1次更新：分配给客服${NC}"
docker exec -it postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "
UPDATE tickets SET 
    status = 'in_progress', 
    agent_id = 3 
WHERE ticket_id = 1012;
"

sleep 2

echo -e "${PURPLE}第2次更新：提升优先级${NC}"
docker exec -it postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "
UPDATE tickets SET 
    priority = 'high'
WHERE ticket_id = 1012;
"

sleep 2

echo -e "${PURPLE}第3次更新：解决问题${NC}"
docker exec -it postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "
UPDATE tickets SET 
    status = 'resolved',
    closed_at = CURRENT_TIMESTAMP
WHERE ticket_id = 1012;
"

echo ""
echo -e "${YELLOW}等待同步...${NC}"
sleep 3

echo ""
echo -e "${BLUE}10. 查看连续更新结果${NC}"
echo -e "${YELLOW}查询工单1012的最终状态：${NC}"
docker exec -it sql-client-sgcc /opt/flink/bin/sql-client.sh embedded -e "
SELECT 
    ticket_id,
    title,
    status,
    priority,
    agent_id,
    created_at,
    updated_at,
    closed_at
FROM fluss.fluss.ods_tickets 
WHERE ticket_id = 1012;
"

echo ""
echo -e "${CYAN}=================== 演示总结 ===================${NC}"
echo ""
echo -e "${GREEN}✅ Fluss实时更新特性展示完成！${NC}"
echo ""
echo -e "${YELLOW}🎯 关键优势总结：${NC}"
echo "1. 🔄 原生UPSERT：无需复杂的changelog处理"
echo "2. ⚡ 实时更新：数据变更立即可见"
echo "3. 🎯 状态管理：自动处理数据状态变化"
echo "4. 🔒 数据一致性：保证强一致性"
echo "5. 📊 实时统计：指标实时更新"
echo "6. 🏗️ 简化架构：无需外部状态存储"
echo ""
echo -e "${BLUE}🆚 与Kafka对比：${NC}"
echo "• Kafka：需要complex changelog + 外部存储 + 状态管理"
echo "• Fluss：原生支持UPSERT，自动处理状态变化"
echo ""
echo -e "${PURPLE}📈 更新操作验证：${NC}"
echo "✅ 工单1009：pending -> resolved"
echo "✅ 工单1010：pending -> in_progress"
echo "✅ 工单1011：priority升级为high"
echo "✅ 工单1012：连续更新最终resolved"
echo "✅ 工单1013：新增成功"
echo "✅ 客服状态：实时更新"
echo "✅ 操作日志：实时追加"
echo ""
echo -e "${CYAN}📋 接下来可以测试：${NC}"
echo "1. 运行 ./batch_stream_demo.sh 演示流批一体"
echo "2. 测试与Paimon的集成"
echo "3. 验证历史数据查询能力" 