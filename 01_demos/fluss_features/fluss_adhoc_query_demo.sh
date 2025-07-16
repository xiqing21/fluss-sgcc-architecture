#!/bin/bash

echo "🔍 Fluss即席查询演示 - 直接查询实时数据"
echo "展示Fluss相较于Kafka的核心优势：无需外部存储即可查询"
echo "=================================================="

# 设置颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo ""
echo -e "${BLUE}🎯 Fluss即席查询特性展示${NC}"
echo -e "${YELLOW}传统Kafka架构痛点：${NC}"
echo "• 无法直接查询数据，需要外部存储（如HBase、ES）"
echo "• 状态查询需要重放日志或维护额外的状态存储"
echo "• 复杂的架构，多个组件协同工作"
echo ""
echo -e "${GREEN}Fluss解决方案：${NC}"
echo "• 直接查询Fluss表，无需外部存储"
echo "• 支持列式存储，查询性能提升10倍"
echo "• 投影下推优化，只读取需要的字段"
echo "• 一体化架构，简化运维"

echo ""
echo -e "${CYAN}=================== 即席查询演示 ===================${NC}"

# 等待用户确认
echo -e "${YELLOW}按Enter键开始演示...${NC}"
read

echo ""
echo -e "${BLUE}1. 查询当前工单实时状态分布${NC}"
echo -e "${YELLOW}SQL: SELECT status, COUNT(*) FROM fluss.fluss.ods_tickets GROUP BY status;${NC}"
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
echo -e "${BLUE}2. 查询客服实时工作负载${NC}"
echo -e "${YELLOW}SQL: 实时JOIN查询，展示Fluss的JOIN能力${NC}"
docker exec -it sql-client-sgcc /opt/flink/bin/sql-client.sh embedded -e "
SELECT 
    a.agent_name,
    a.department,
    a.status as agent_status,
    COUNT(t.ticket_id) as total_tickets,
    COUNT(CASE WHEN t.status = 'resolved' THEN 1 END) as resolved_tickets,
    COUNT(CASE WHEN t.status IN ('pending', 'in_progress') THEN 1 END) as active_tickets
FROM fluss.fluss.ods_agents a
LEFT JOIN fluss.fluss.ods_tickets t ON a.agent_id = t.agent_id
GROUP BY a.agent_id, a.agent_name, a.department, a.status
ORDER BY resolved_tickets DESC;
"

echo ""
echo -e "${YELLOW}按Enter键继续...${NC}"
read

echo ""
echo -e "${BLUE}3. 查询高优先级工单实时状态${NC}"
echo -e "${YELLOW}SQL: 带WHERE条件的查询，展示过滤下推优化${NC}"
docker exec -it sql-client-sgcc /opt/flink/bin/sql-client.sh embedded -e "
SELECT 
    ticket_id,
    title,
    priority,
    status,
    agent_id,
    created_at,
    updated_at
FROM fluss.fluss.ods_tickets 
WHERE priority = 'high'
ORDER BY created_at DESC;
"

echo ""
echo -e "${YELLOW}按Enter键继续...${NC}"
read

echo ""
echo -e "${BLUE}4. 查询今日工单创建趋势${NC}"
echo -e "${YELLOW}SQL: 时间窗口查询，展示实时分析能力${NC}"
docker exec -it sql-client-sgcc /opt/flink/bin/sql-client.sh embedded -e "
SELECT 
    DATE_FORMAT(created_at, 'yyyy-MM-dd HH:00:00') as hour_window,
    COUNT(*) as tickets_created,
    COUNT(CASE WHEN priority = 'high' THEN 1 END) as high_priority_tickets,
    COUNT(CASE WHEN status = 'resolved' THEN 1 END) as resolved_tickets
FROM fluss.fluss.ods_tickets 
WHERE DATE(created_at) = CURRENT_DATE
GROUP BY DATE_FORMAT(created_at, 'yyyy-MM-dd HH:00:00')
ORDER BY hour_window;
"

echo ""
echo -e "${YELLOW}按Enter键继续...${NC}"
read

echo ""
echo -e "${BLUE}5. 查询工单操作日志（append-only表）${NC}"
echo -e "${YELLOW}SQL: 查询操作日志，展示append-only表特性${NC}"
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
echo -e "${BLUE}6. 复杂分析查询 - 客服绩效分析${NC}"
echo -e "${YELLOW}SQL: 复杂的聚合查询，展示分析能力${NC}"
docker exec -it sql-client-sgcc /opt/flink/bin/sql-client.sh embedded -e "
SELECT 
    a.agent_name,
    a.department,
    COUNT(t.ticket_id) as total_handled,
    COUNT(CASE WHEN t.status = 'resolved' THEN 1 END) as resolved_count,
    ROUND(
        COUNT(CASE WHEN t.status = 'resolved' THEN 1 END) * 100.0 / 
        NULLIF(COUNT(t.ticket_id), 0), 2
    ) as resolution_rate,
    AVG(
        CASE 
            WHEN t.closed_at IS NOT NULL AND t.created_at IS NOT NULL 
            THEN TIMESTAMPDIFF(HOUR, t.created_at, t.closed_at)
            ELSE NULL 
        END
    ) as avg_resolution_hours
FROM fluss.fluss.ods_agents a
LEFT JOIN fluss.fluss.ods_tickets t ON a.agent_id = t.agent_id
GROUP BY a.agent_id, a.agent_name, a.department
HAVING COUNT(t.ticket_id) > 0
ORDER BY resolution_rate DESC, resolved_count DESC;
"

echo ""
echo -e "${YELLOW}按Enter键继续...${NC}"
read

echo ""
echo -e "${BLUE}7. 投影下推优化演示${NC}"
echo -e "${YELLOW}SQL: 只选择需要的字段，展示列式存储优势${NC}"
docker exec -it sql-client-sgcc /opt/flink/bin/sql-client.sh embedded -e "
SELECT 
    ticket_id,
    title,
    status,
    priority
FROM fluss.fluss.ods_tickets 
WHERE status IN ('pending', 'in_progress')
ORDER BY priority DESC, created_at DESC;
"

echo ""
echo -e "${CYAN}=================== 演示总结 ===================${NC}"
echo ""
echo -e "${GREEN}✅ Fluss即席查询特性展示完成！${NC}"
echo ""
echo -e "${YELLOW}🎯 关键优势总结：${NC}"
echo "1. 🚀 直接查询：无需外部存储，直接查询Fluss表"
echo "2. 📊 列式存储：查询性能提升10倍，特别适合分析查询"
echo "3. 🔍 投影下推：只读取需要的字段，优化网络传输"
echo "4. 🔄 实时更新：支持UPSERT，数据实时可见"
echo "5. 📈 复杂分析：支持复杂的聚合和JOIN查询"
echo "6. 🏗️ 一体化：简化架构，降低运维复杂度"
echo ""
echo -e "${BLUE}🆚 与Kafka对比：${NC}"
echo "• Kafka需要：Kafka + 外部存储(HBase/ES) + 复杂ETL"
echo "• Fluss只需：Fluss一体化存储，直接查询"
echo ""
echo -e "${CYAN}📋 接下来可以测试：${NC}"
echo "1. 运行 ./realtime_update_demo.sh 演示实时更新"
echo "2. 测试流批一体特性"
echo "3. 验证与Paimon的集成" 