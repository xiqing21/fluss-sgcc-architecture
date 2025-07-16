#!/bin/bash

echo "🎯 智能客服工单分析系统 - 数据验证"
echo "展示Fluss流批一体、UPSERT、即席查询等特性"
echo "=================================================="

# 设置颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo ""
echo -e "${BLUE}📊 验证PostgreSQL源数据库...${NC}"

# 检查客服信息
echo -e "${YELLOW}👨‍💼 客服信息:${NC}"
docker exec -it postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "
SELECT 
    agent_id,
    agent_name,
    department,
    skill_level,
    status,
    created_at
FROM agents 
ORDER BY agent_id;
"

echo ""
echo -e "${YELLOW}🎫 工单状态分布:${NC}"
docker exec -it postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "
SELECT 
    status,
    COUNT(*) as ticket_count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as percentage
FROM tickets 
GROUP BY status
ORDER BY ticket_count DESC;
"

echo ""
echo -e "${YELLOW}📈 工单按优先级分布:${NC}"
docker exec -it postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "
SELECT 
    priority,
    COUNT(*) as ticket_count,
    AVG(EXTRACT(EPOCH FROM (closed_at - created_at))/3600) as avg_resolution_hours
FROM tickets 
GROUP BY priority
ORDER BY 
    CASE priority 
        WHEN 'high' THEN 1 
        WHEN 'medium' THEN 2 
        WHEN 'low' THEN 3 
    END;
"

echo ""
echo -e "${YELLOW}🏆 客服工作负载统计:${NC}"
docker exec -it postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "
SELECT 
    a.agent_name,
    a.department,
    COUNT(t.ticket_id) as total_tickets,
    COUNT(CASE WHEN t.status = 'resolved' THEN 1 END) as resolved_tickets,
    COUNT(CASE WHEN t.status IN ('pending', 'in_progress') THEN 1 END) as active_tickets,
    ROUND(
        COUNT(CASE WHEN t.status = 'resolved' THEN 1 END) * 100.0 / 
        NULLIF(COUNT(t.ticket_id), 0), 2
    ) as resolution_rate_percent
FROM agents a
LEFT JOIN tickets t ON a.agent_id = t.agent_id
GROUP BY a.agent_id, a.agent_name, a.department
ORDER BY resolved_tickets DESC;
"

echo ""
echo -e "${YELLOW}📅 每日工单统计:${NC}"
docker exec -it postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "
SELECT 
    DATE(created_at) as date,
    COUNT(*) as total_created,
    COUNT(CASE WHEN status = 'resolved' THEN 1 END) as resolved,
    COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending,
    COUNT(CASE WHEN status = 'in_progress' THEN 1 END) as in_progress,
    AVG(EXTRACT(EPOCH FROM (closed_at - created_at))/3600) as avg_resolution_hours
FROM tickets 
GROUP BY DATE(created_at)
ORDER BY date DESC;
"

echo ""
echo -e "${YELLOW}🔍 工单操作日志样例:${NC}"
docker exec -it postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "
SELECT 
    tl.ticket_id,
    tl.action,
    tl.old_value,
    tl.new_value,
    tl.comment,
    tl.created_at
FROM ticket_logs tl
ORDER BY tl.created_at DESC
LIMIT 10;
"

echo ""
echo -e "${BLUE}🎯 验证PostgreSQL目标数据库...${NC}"

echo -e "${YELLOW}📊 检查目标数据库表结构:${NC}"
docker exec -it postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns 
WHERE table_schema = 'public' 
  AND table_name IN ('ticket_dashboard', 'agent_ranking', 'agent_workload', 'daily_ticket_stats', 'agent_performance')
ORDER BY table_name, ordinal_position;
"

echo ""
echo -e "${GREEN}✅ 数据验证完成！${NC}"
echo ""
echo -e "${BLUE}🚀 接下来可以执行以下操作：${NC}"
echo "1. 运行 ./smart_sql_execution.sh 执行智能客服分析系统SQL"
echo "2. 执行实时数据更新测试"
echo "3. 验证流批一体特性"
echo "4. 测试即席查询功能"
echo ""
echo -e "${YELLOW}💡 Fluss特性展示要点：${NC}"
echo "• 流批一体：同时支持实时流处理和历史批处理"
echo "• 原生UPSERT：工单状态更新无需复杂changelog处理"
echo "• 即席查询：直接查询Fluss表获取实时状态"
echo "• 列式存储：投影下推优化，提升查询性能"
echo "• 与Paimon集成：分层存储，冷热数据分离" 