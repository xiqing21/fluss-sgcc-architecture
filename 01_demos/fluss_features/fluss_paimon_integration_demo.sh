#!/bin/bash

echo "🔗 Fluss与Paimon集成演示 - 分层存储与流批一体"
echo "展示Fluss作为实时数据层，Paimon作为湖仓存储层的完整架构"
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
echo -e "${BLUE}🎯 Fluss与Paimon集成架构${NC}"
echo -e "${YELLOW}架构层次：${NC}"
echo "• 🔥 Fluss：实时数据层，提供亚秒级查询"
echo "• 🏔️ Paimon：湖仓存储层，支持历史数据和批处理"
echo "• 🔄 双向通信：实时与历史数据无缝互通"
echo "• 📊 统一查询：同一SQL接口访问不同存储层"
echo ""
echo -e "${GREEN}核心特性：${NC}"
echo "• 流批一体：统一的数据处理范式"
echo "• 分层存储：热数据在Fluss，冷数据在Paimon"
echo "• 状态初始化：从Paimon加载历史状态到Fluss"
echo "• 时间旅行：支持历史数据查询"

echo ""
echo -e "${CYAN}=================== 集成演示 ===================${NC}"

# 等待用户确认
echo -e "${YELLOW}按Enter键开始演示...${NC}"
read

echo ""
echo -e "${BLUE}1. 创建Paimon湖仓存储表${NC}"
echo -e "${YELLOW}创建Paimon表用于长期存储：${NC}"

# 注意：这里假设Paimon已经配置好，实际环境中需要配置Paimon catalog
echo -e "${PURPLE}创建Paimon历史工单表（用于长期存储）${NC}"
docker exec -it sql-client-sgcc /opt/flink/bin/sql-client.sh embedded -e "
-- 创建Paimon catalog（示例配置）
CREATE CATALOG paimon_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 'file:///tmp/paimon'
);

USE CATALOG paimon_catalog;

-- 创建Paimon历史工单表
CREATE TABLE IF NOT EXISTS historical_tickets (
    ticket_id BIGINT,
    customer_id INT,
    agent_id INT,
    title STRING,
    description STRING,
    priority STRING,
    status STRING,
    category STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    closed_at TIMESTAMP(3),
    archive_date DATE,
    PRIMARY KEY (ticket_id, archive_date) NOT ENFORCED
) PARTITIONED BY (archive_date);
"

echo ""
echo -e "${YELLOW}按Enter键继续...${NC}"
read

echo ""
echo -e "${BLUE}2. 数据分层存储策略${NC}"
echo -e "${YELLOW}配置数据分层规则：${NC}"

echo -e "${PURPLE}策略说明：${NC}"
echo "• 近7天数据：保留在Fluss（实时查询）"
echo "• 7天以上数据：归档到Paimon（历史分析）"
echo "• 冷数据：定期压缩和清理"

# 创建数据归档作业
echo -e "${PURPLE}创建数据归档作业：${NC}"
docker exec -it sql-client-sgcc /opt/flink/bin/sql-client.sh embedded -e "
-- 切换到default catalog进行归档操作
USE CATALOG default_catalog;

-- 将7天前的数据归档到Paimon
INSERT INTO paimon_catalog.default.historical_tickets
SELECT 
    ticket_id,
    customer_id,
    agent_id,
    title,
    description,
    priority,
    status,
    category,
    created_at,
    updated_at,
    closed_at,
    CAST(created_at AS DATE) as archive_date
FROM fluss.fluss.ods_tickets
WHERE CAST(created_at AS DATE) < CURRENT_DATE - INTERVAL '7' DAY;
"

echo ""
echo -e "${YELLOW}按Enter键继续...${NC}"
read

echo ""
echo -e "${BLUE}3. 流批一体查询演示${NC}"
echo -e "${YELLOW}同一SQL查询Fluss和Paimon数据：${NC}"

echo -e "${PURPLE}查询最近3天的实时数据（Fluss）：${NC}"
docker exec -it sql-client-sgcc /opt/flink/bin/sql-client.sh embedded -e "
SELECT 
    'Fluss实时数据' as data_source,
    COUNT(*) as ticket_count,
    COUNT(CASE WHEN status = 'resolved' THEN 1 END) as resolved_count,
    AVG(TIMESTAMPDIFF(HOUR, created_at, COALESCE(closed_at, CURRENT_TIMESTAMP))) as avg_resolution_hours
FROM fluss.fluss.ods_tickets
WHERE CAST(created_at AS DATE) >= CURRENT_DATE - INTERVAL '3' DAY;
"

echo ""
echo -e "${PURPLE}查询历史数据（Paimon）：${NC}"
docker exec -it sql-client-sgcc /opt/flink/bin/sql-client.sh embedded -e "
SELECT 
    'Paimon历史数据' as data_source,
    COUNT(*) as ticket_count,
    COUNT(CASE WHEN status = 'resolved' THEN 1 END) as resolved_count,
    AVG(TIMESTAMPDIFF(HOUR, created_at, COALESCE(closed_at, updated_at))) as avg_resolution_hours
FROM paimon_catalog.default.historical_tickets
WHERE archive_date < CURRENT_DATE - INTERVAL '3' DAY;
"

echo ""
echo -e "${YELLOW}按Enter键继续...${NC}"
read

echo ""
echo -e "${BLUE}4. 统一查询接口（Union All）${NC}"
echo -e "${YELLOW}统一查询实时和历史数据：${NC}"

docker exec -it sql-client-sgcc /opt/flink/bin/sql-client.sh embedded -e "
-- 统一查询：实时数据 + 历史数据
SELECT 
    data_source,
    ticket_count,
    resolved_count,
    avg_resolution_hours
FROM (
    SELECT 
        'Fluss实时层' as data_source,
        COUNT(*) as ticket_count,
        COUNT(CASE WHEN status = 'resolved' THEN 1 END) as resolved_count,
        AVG(TIMESTAMPDIFF(HOUR, created_at, COALESCE(closed_at, CURRENT_TIMESTAMP))) as avg_resolution_hours
    FROM fluss.fluss.ods_tickets
    WHERE CAST(created_at AS DATE) >= CURRENT_DATE - INTERVAL '7' DAY
    
    UNION ALL
    
    SELECT 
        'Paimon存储层' as data_source,
        COUNT(*) as ticket_count,
        COUNT(CASE WHEN status = 'resolved' THEN 1 END) as resolved_count,
        AVG(TIMESTAMPDIFF(HOUR, created_at, COALESCE(closed_at, updated_at))) as avg_resolution_hours
    FROM paimon_catalog.default.historical_tickets
    WHERE archive_date < CURRENT_DATE - INTERVAL '7' DAY
) unified_data;
"

echo ""
echo -e "${YELLOW}按Enter键继续...${NC}"
read

echo ""
echo -e "${BLUE}5. 状态初始化演示${NC}"
echo -e "${YELLOW}从Paimon加载历史状态到Fluss：${NC}"

echo -e "${PURPLE}场景：系统重启后需要从历史数据初始化状态${NC}"

# 模拟状态初始化
docker exec -it sql-client-sgcc /opt/flink/bin/sql-client.sh embedded -e "
-- 从Paimon加载客服历史绩效数据到Fluss
INSERT INTO fluss.fluss.dws_agent_performance
SELECT 
    agent_id,
    '历史客服' as agent_name,
    archive_date as stat_date,
    COUNT(*) as tickets_handled,
    COUNT(CASE WHEN status = 'resolved' THEN 1 END) as tickets_resolved,
    COUNT(CASE WHEN status = 'resolved' THEN 1 END) * 100.0 / COUNT(*) as resolution_rate,
    AVG(TIMESTAMPDIFF(HOUR, created_at, COALESCE(closed_at, updated_at))) as avg_resolution_time_hours
FROM paimon_catalog.default.historical_tickets
WHERE agent_id IS NOT NULL
  AND archive_date >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY agent_id, archive_date;
"

echo ""
echo -e "${YELLOW}按Enter键继续...${NC}"
read

echo ""
echo -e "${BLUE}6. 时间旅行查询${NC}"
echo -e "${YELLOW}Paimon支持的时间旅行特性：${NC}"

echo -e "${PURPLE}查询特定时间点的数据快照：${NC}"
docker exec -it sql-client-sgcc /opt/flink/bin/sql-client.sh embedded -e "
-- 查询昨天的数据快照
SELECT 
    archive_date,
    COUNT(*) as daily_tickets,
    COUNT(CASE WHEN status = 'resolved' THEN 1 END) as resolved_tickets,
    COUNT(CASE WHEN priority = 'high' THEN 1 END) as high_priority_tickets
FROM paimon_catalog.default.historical_tickets
WHERE archive_date = CURRENT_DATE - INTERVAL '1' DAY
GROUP BY archive_date;
"

echo ""
echo -e "${YELLOW}按Enter键继续...${NC}"
read

echo ""
echo -e "${BLUE}7. 增量数据同步${NC}"
echo -e "${YELLOW}Fluss到Paimon的增量同步：${NC}"

echo -e "${PURPLE}设置定时同步任务：${NC}"
docker exec -it sql-client-sgcc /opt/flink/bin/sql-client.sh embedded -e "
-- 增量同步今天的数据到Paimon
INSERT INTO paimon_catalog.default.historical_tickets
SELECT 
    ticket_id,
    customer_id,
    agent_id,
    title,
    description,
    priority,
    status,
    category,
    created_at,
    updated_at,
    closed_at,
    CURRENT_DATE as archive_date
FROM fluss.fluss.ods_tickets
WHERE CAST(created_at AS DATE) = CURRENT_DATE
  AND ticket_id NOT IN (
    SELECT ticket_id FROM paimon_catalog.default.historical_tickets 
    WHERE archive_date = CURRENT_DATE
  );
"

echo ""
echo -e "${YELLOW}按Enter键继续...${NC}"
read

echo ""
echo -e "${BLUE}8. 性能对比测试${NC}"
echo -e "${YELLOW}对比Fluss和Paimon的查询性能：${NC}"

echo -e "${PURPLE}Fluss实时查询性能（亚秒级）：${NC}"
echo "开始时间: $(date)"
docker exec -it sql-client-sgcc /opt/flink/bin/sql-client.sh embedded -e "
SELECT 
    COUNT(*) as total_tickets,
    COUNT(CASE WHEN status = 'resolved' THEN 1 END) as resolved_tickets
FROM fluss.fluss.ods_tickets;
"
echo "结束时间: $(date)"

echo ""
echo -e "${PURPLE}Paimon批处理查询性能：${NC}"
echo "开始时间: $(date)"
docker exec -it sql-client-sgcc /opt/flink/bin/sql-client.sh embedded -e "
SELECT 
    COUNT(*) as total_tickets,
    COUNT(CASE WHEN status = 'resolved' THEN 1 END) as resolved_tickets
FROM paimon_catalog.default.historical_tickets;
"
echo "结束时间: $(date)"

echo ""
echo -e "${YELLOW}按Enter键继续...${NC}"
read

echo ""
echo -e "${BLUE}9. 数据生命周期管理${NC}"
echo -e "${YELLOW}配置数据保留策略：${NC}"

echo -e "${PURPLE}数据生命周期策略：${NC}"
echo "• 实时数据（Fluss）：保留7天"
echo "• 温数据（Paimon）：保留1年"
echo "• 冷数据：压缩存储或删除"

# 示例清理策略
echo -e "${PURPLE}清理过期数据：${NC}"
docker exec -it sql-client-sgcc /opt/flink/bin/sql-client.sh embedded -e "
-- 清理Fluss中超过7天的数据
DELETE FROM fluss.fluss.ods_tickets
WHERE CAST(created_at AS DATE) < CURRENT_DATE - INTERVAL '7' DAY;
"

echo ""
echo -e "${CYAN}=================== 演示总结 ===================${NC}"
echo ""
echo -e "${GREEN}✅ Fluss与Paimon集成特性展示完成！${NC}"
echo ""
echo -e "${YELLOW}🎯 集成架构优势：${NC}"
echo "1. 🔥 热数据查询：Fluss提供亚秒级实时查询"
echo "2. 🏔️ 冷数据存储：Paimon提供高效的历史数据存储"
echo "3. 🔄 无缝互通：实时与历史数据统一查询"
echo "4. 📊 流批一体：统一的数据处理范式"
echo "5. 🕐 时间旅行：支持历史数据快照查询"
echo "6. 🔧 状态初始化：从历史数据初始化实时状态"
echo "7. 📈 性能优化：分层存储，各取所长"
echo "8. 🗂️ 生命周期：自动化数据管理"
echo ""
echo -e "${BLUE}🏗️ 完整架构：${NC}"
echo "┌─────────────────┐    ┌─────────────────┐"
echo "│   Fluss实时层   │◄──►│  Paimon存储层   │"
echo "│  (热数据查询)   │    │  (冷数据批处理)  │"
echo "└─────────────────┘    └─────────────────┘"
echo "        │                      │"
echo "        ▼                      ▼"
echo "   亚秒级查询            历史数据分析"
echo ""
echo -e "${PURPLE}📋 应用场景：${NC}"
echo "• 实时监控：使用Fluss查询当前状态"
echo "• 趋势分析：使用Paimon分析历史趋势"
echo "• 报表生成：结合两者生成综合报表"
echo "• 状态恢复：从Paimon恢复实时状态"
echo ""
echo -e "${CYAN}🚀 技术价值：${NC}"
echo "• 降低成本：分层存储，按需分配资源"
echo "• 提升性能：实时查询 + 批处理分析"
echo "• 简化架构：统一SQL接口，减少组件"
echo "• 增强可靠性：数据备份与恢复" 