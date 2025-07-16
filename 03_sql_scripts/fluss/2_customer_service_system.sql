-- ============================================
-- 智能客服工单分析系统 - Fluss特性展示
-- 展示：流批一体、即席查询、UPSERT支持、列式存储优化
-- ============================================

-- 设置执行环境
SET 'execution.checkpointing.interval' = '30s';
SET 'table.exec.source.idle-timeout' = '30s';

-- ============================================
-- 1. 创建PostgreSQL CDC源表
-- ============================================

-- 客服信息表（相对稳定的维表）
CREATE TABLE source_agents (
    agent_id INT,
    agent_name STRING,
    department STRING,
    skill_level STRING,
    status STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (agent_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres-sgcc-source',
    'port' = '5432',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'database-name' = 'sgcc_source_db',
    'schema-name' = 'public',
    'table-name' = 'agents',
    'decoding.plugin.name' = 'pgoutput'
);

-- 工单表（频繁更新的事实表）
CREATE TABLE source_tickets (
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
    PRIMARY KEY (ticket_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres-sgcc-source',
    'port' = '5432',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'database-name' = 'sgcc_source_db',
    'schema-name' = 'public',
    'table-name' = 'tickets',
    'decoding.plugin.name' = 'pgoutput'
);

-- 工单操作日志表（只追加）
CREATE TABLE source_ticket_logs (
    log_id BIGINT,
    ticket_id BIGINT,
    agent_id INT,
    action STRING,
    old_value STRING,
    new_value STRING,
    comment STRING,
    created_at TIMESTAMP(3),
    PRIMARY KEY (log_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres-sgcc-source',
    'port' = '5432',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'database-name' = 'sgcc_source_db',
    'schema-name' = 'public',
    'table-name' = 'ticket_logs',
    'decoding.plugin.name' = 'pgoutput'
);

-- ============================================
-- 2. 创建Fluss存储层（ODS层）
-- ============================================

-- 客服信息表 - 支持UPSERT
CREATE TABLE fluss.fluss.ods_agents (
    agent_id INT,
    agent_name STRING,
    department STRING,
    skill_level STRING,
    status STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (agent_id) NOT ENFORCED
) WITH (
    'bucket' = '4',
    'write.compaction.rescale-bucket' = 'true'
);

-- 工单表 - 支持UPSERT，体现Fluss优势
CREATE TABLE fluss.fluss.ods_tickets (
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
    PRIMARY KEY (ticket_id) NOT ENFORCED
) WITH (
    'bucket' = '4',
    'write.compaction.rescale-bucket' = 'true'
);

-- 工单操作日志表 - Append-only模式
CREATE TABLE fluss.fluss.ods_ticket_logs (
    log_id BIGINT,
    ticket_id BIGINT,
    agent_id INT,
    action STRING,
    old_value STRING,
    new_value STRING,
    comment STRING,
    created_at TIMESTAMP(3),
    PRIMARY KEY (log_id) NOT ENFORCED
) WITH (
    'bucket' = '4',
    'write.compaction.rescale-bucket' = 'true'
);

-- ============================================
-- 3. 数据同步（CDC到Fluss）
-- ============================================

-- 同步客服信息
INSERT INTO fluss.fluss.ods_agents
SELECT 
    agent_id,
    agent_name,
    department,
    skill_level,
    status,
    created_at,
    updated_at
FROM source_agents;

-- 同步工单信息
INSERT INTO fluss.fluss.ods_tickets
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
    closed_at
FROM source_tickets;

-- 同步工单日志
INSERT INTO fluss.fluss.ods_ticket_logs
SELECT 
    log_id,
    ticket_id,
    agent_id,
    action,
    old_value,
    new_value,
    comment,
    created_at
FROM source_ticket_logs;

-- ============================================
-- 4. 实时指标层（DWD）- 流处理特性展示
-- ============================================

-- 实时工单状态统计
CREATE TABLE fluss.fluss.dwd_ticket_metrics (
    metric_time TIMESTAMP(3),
    total_tickets BIGINT,
    pending_tickets BIGINT,
    in_progress_tickets BIGINT,
    resolved_tickets BIGINT,
    closed_tickets BIGINT,
    avg_resolution_time_minutes DOUBLE,
    PRIMARY KEY (metric_time) NOT ENFORCED
) WITH (
    'bucket' = '4',
    'write.compaction.rescale-bucket' = 'true'
);

-- 实时客服工作负载统计
CREATE TABLE fluss.fluss.dwd_agent_workload (
    agent_id INT,
    agent_name STRING,
    department STRING,
    active_tickets BIGINT,
    resolved_today BIGINT,
    avg_response_time_minutes DOUBLE,
    last_updated TIMESTAMP(3),
    PRIMARY KEY (agent_id) NOT ENFORCED
) WITH (
    'bucket' = '4',
    'write.compaction.rescale-bucket' = 'true'
);

-- ============================================
-- 5. 实时流处理作业
-- ============================================

-- 实时工单状态统计（流处理）
INSERT INTO fluss.fluss.dwd_ticket_metrics
SELECT 
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) as metric_time,
    COUNT(*) as total_tickets,
    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending_tickets,
    SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) as in_progress_tickets,
    SUM(CASE WHEN status = 'resolved' THEN 1 ELSE 0 END) as resolved_tickets,
    SUM(CASE WHEN status = 'closed' THEN 1 ELSE 0 END) as closed_tickets,
    AVG(
        CASE 
            WHEN closed_at IS NOT NULL AND created_at IS NOT NULL 
            THEN TIMESTAMPDIFF(MINUTE, created_at, closed_at)
            ELSE NULL 
        END
    ) as avg_resolution_time_minutes
FROM fluss.fluss.ods_tickets;

-- 实时客服工作负载统计（流处理）
INSERT INTO fluss.fluss.dwd_agent_workload
SELECT 
    a.agent_id,
    a.agent_name,
    a.department,
    COUNT(CASE WHEN t.status IN ('pending', 'in_progress') THEN 1 END) as active_tickets,
    COUNT(CASE WHEN t.status = 'resolved' 
                 AND CAST(t.updated_at AS DATE) = CURRENT_DATE 
                 THEN 1 END) as resolved_today,
    AVG(
        CASE 
            WHEN t.updated_at IS NOT NULL AND t.created_at IS NOT NULL 
            THEN TIMESTAMPDIFF(MINUTE, t.created_at, t.updated_at)
            ELSE NULL 
        END
    ) as avg_response_time_minutes,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) as last_updated
FROM fluss.fluss.ods_agents a
LEFT JOIN fluss.fluss.ods_tickets t ON a.agent_id = t.agent_id
GROUP BY a.agent_id, a.agent_name, a.department;

-- ============================================
-- 6. 历史分析层（DWS）- 批处理特性展示
-- ============================================

-- 每日工单统计（批处理）
CREATE TABLE fluss.fluss.dws_daily_ticket_stats (
    stat_date DATE,
    total_created BIGINT,
    total_resolved BIGINT,
    total_closed BIGINT,
    avg_resolution_time_hours DOUBLE,
    PRIMARY KEY (stat_date) NOT ENFORCED
) WITH (
    'bucket' = '4',
    'write.compaction.rescale-bucket' = 'true'
);

-- 客服绩效分析（批处理）
CREATE TABLE fluss.fluss.dws_agent_performance (
    agent_id INT,
    agent_name STRING,
    stat_date DATE,
    tickets_handled BIGINT,
    tickets_resolved BIGINT,
    resolution_rate DOUBLE,
    avg_resolution_time_hours DOUBLE,
    PRIMARY KEY (agent_id, stat_date) NOT ENFORCED
) WITH (
    'bucket' = '4',
    'write.compaction.rescale-bucket' = 'true'
);

-- ============================================
-- 7. 批处理作业（历史数据分析）
-- ============================================

-- 每日工单统计（批处理模式）
INSERT INTO fluss.fluss.dws_daily_ticket_stats
SELECT 
    CAST(created_at AS DATE) as stat_date,
    COUNT(*) as total_created,
    SUM(CASE WHEN status = 'resolved' THEN 1 ELSE 0 END) as total_resolved,
    SUM(CASE WHEN status = 'closed' THEN 1 ELSE 0 END) as total_closed,
    AVG(
        CASE 
            WHEN closed_at IS NOT NULL AND created_at IS NOT NULL 
            THEN TIMESTAMPDIFF(HOUR, created_at, closed_at)
            ELSE NULL 
        END
    ) as avg_resolution_time_hours
FROM fluss.fluss.ods_tickets
GROUP BY CAST(created_at AS DATE);

-- 客服绩效分析（批处理模式）
INSERT INTO fluss.fluss.dws_agent_performance
SELECT 
    a.agent_id,
    a.agent_name,
    CAST(t.created_at AS DATE) as stat_date,
    COUNT(t.ticket_id) as tickets_handled,
    SUM(CASE WHEN t.status = 'resolved' THEN 1 ELSE 0 END) as tickets_resolved,
    CAST(SUM(CASE WHEN t.status = 'resolved' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(t.ticket_id) as resolution_rate,
    AVG(
        CASE 
            WHEN t.closed_at IS NOT NULL AND t.created_at IS NOT NULL 
            THEN TIMESTAMPDIFF(HOUR, t.created_at, t.closed_at)
            ELSE NULL 
        END
    ) as avg_resolution_time_hours
FROM fluss.fluss.ods_agents a
INNER JOIN fluss.fluss.ods_tickets t ON a.agent_id = t.agent_id
GROUP BY a.agent_id, a.agent_name, CAST(t.created_at AS DATE);

-- ============================================
-- 8. 应用层（ADS）- 即席查询展示
-- ============================================

-- 实时工单仪表板
CREATE TABLE fluss.fluss.ads_ticket_dashboard (
    dashboard_id INT,
    metric_name STRING,
    metric_value DOUBLE,
    metric_desc STRING,
    update_time TIMESTAMP(3),
    PRIMARY KEY (dashboard_id) NOT ENFORCED
) WITH (
    'bucket' = '4',
    'write.compaction.rescale-bucket' = 'true'
);

-- 客服排行榜
CREATE TABLE fluss.fluss.ads_agent_ranking (
    agent_id INT,
    agent_name STRING,
    department STRING,
    rank_type STRING,
    rank_value DOUBLE,
    ranking INT,
    update_time TIMESTAMP(3),
    PRIMARY KEY (agent_id, rank_type) NOT ENFORCED
) WITH (
    'bucket' = '4',
    'write.compaction.rescale-bucket' = 'true'
);

-- ============================================
-- 9. 应用层数据聚合
-- ============================================

-- 实时仪表板数据
INSERT INTO fluss.fluss.ads_ticket_dashboard
SELECT 
    1 as dashboard_id,
    'total_tickets' as metric_name,
    CAST(COUNT(*) AS DOUBLE) as metric_value,
    '总工单数' as metric_desc,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) as update_time
FROM fluss.fluss.ods_tickets
UNION ALL
SELECT 
    2 as dashboard_id,
    'pending_tickets' as metric_name,
    CAST(COUNT(*) AS DOUBLE) as metric_value,
    '待处理工单数' as metric_desc,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) as update_time
FROM fluss.fluss.ods_tickets
WHERE status = 'pending'
UNION ALL
SELECT 
    3 as dashboard_id,
    'avg_resolution_time' as metric_name,
    AVG(TIMESTAMPDIFF(HOUR, created_at, closed_at)) as metric_value,
    '平均解决时间(小时)' as metric_desc,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) as update_time
FROM fluss.fluss.ods_tickets
WHERE closed_at IS NOT NULL;

-- 客服排行榜数据
INSERT INTO fluss.fluss.ads_agent_ranking
SELECT 
    a.agent_id,
    a.agent_name,
    a.department,
    'resolution_count' as rank_type,
    CAST(COUNT(CASE WHEN t.status = 'resolved' THEN 1 END) AS DOUBLE) as rank_value,
    ROW_NUMBER() OVER (ORDER BY COUNT(CASE WHEN t.status = 'resolved' THEN 1 END) DESC) as ranking,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) as update_time
FROM fluss.fluss.ods_agents a
LEFT JOIN fluss.fluss.ods_tickets t ON a.agent_id = t.agent_id
GROUP BY a.agent_id, a.agent_name, a.department;

-- ============================================
-- 10. PostgreSQL Sink层 - 支持外部查询
-- ============================================

-- 创建仪表板sink表
CREATE TABLE sink_ticket_dashboard (
    dashboard_id INT,
    metric_name STRING,
    metric_value DOUBLE,
    metric_desc STRING,
    update_time TIMESTAMP(3),
    PRIMARY KEY (dashboard_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'ticket_dashboard',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s',
    'sink.max-retries' = '3'
);

-- 创建客服排行榜sink表
CREATE TABLE sink_agent_ranking (
    agent_id INT,
    agent_name STRING,
    department STRING,
    rank_type STRING,
    rank_value DOUBLE,
    ranking INT,
    update_time TIMESTAMP(3),
    PRIMARY KEY (agent_id, rank_type) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'agent_ranking',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s',
    'sink.max-retries' = '3'
);

-- ============================================
-- 11. 数据输出到PostgreSQL
-- ============================================

-- 输出仪表板数据
INSERT INTO sink_ticket_dashboard
SELECT 
    dashboard_id,
    metric_name,
    metric_value,
    metric_desc,
    update_time
FROM fluss.fluss.ads_ticket_dashboard;

-- 输出客服排行榜数据
INSERT INTO sink_agent_ranking
SELECT 
    agent_id,
    agent_name,
    department,
    rank_type,
    rank_value,
    ranking,
    update_time
FROM fluss.fluss.ads_agent_ranking;

-- ============================================
-- 特性展示总结：
-- 1. 流批一体：同时支持实时流处理和历史批处理
-- 2. 原生UPSERT：工单状态更新无需复杂changelog处理
-- 3. 即席查询：直接查询Fluss表获取实时状态
-- 4. 列式存储：投影下推优化，提升查询性能
-- 5. 分层存储：可与Paimon集成实现冷热分离
-- ============================================ 