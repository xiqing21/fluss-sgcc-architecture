-- ============================================
-- 智能客服工单分析系统测试数据
-- 展示Fluss流批一体、UPSERT、即席查询等特性
-- ============================================

-- 创建客服信息表
CREATE TABLE agents (
    agent_id INT PRIMARY KEY,
    agent_name VARCHAR(100),
    department VARCHAR(50),
    skill_level VARCHAR(20),
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建工单表
CREATE TABLE tickets (
    ticket_id BIGINT PRIMARY KEY,
    customer_id INT,
    agent_id INT,
    title VARCHAR(200),
    description TEXT,
    priority VARCHAR(20),
    status VARCHAR(20),
    category VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    closed_at TIMESTAMP
);

-- 创建工单操作日志表
CREATE TABLE ticket_logs (
    log_id BIGINT PRIMARY KEY,
    ticket_id BIGINT,
    agent_id INT,
    action VARCHAR(50),
    old_value VARCHAR(200),
    new_value VARCHAR(200),
    comment TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- 插入客服信息测试数据
-- ============================================

INSERT INTO agents (agent_id, agent_name, department, skill_level, status) VALUES
(1, '张小明', '技术支持', 'senior', 'online'),
(2, '李小红', '技术支持', 'junior', 'online'),
(3, '王大力', '销售支持', 'senior', 'online'),
(4, '刘小花', '销售支持', 'intermediate', 'offline'),
(5, '陈建国', '技术支持', 'senior', 'online'),
(6, '赵小丽', '客户服务', 'intermediate', 'online'),
(7, '孙大海', '客户服务', 'junior', 'online'),
(8, '周小月', '技术支持', 'intermediate', 'offline');

-- ============================================
-- 插入工单测试数据
-- ============================================

-- 已解决的工单
INSERT INTO tickets (ticket_id, customer_id, agent_id, title, description, priority, status, category, created_at, updated_at, closed_at) VALUES
(1001, 101, 1, '登录问题', '用户无法登录系统', 'high', 'resolved', '技术问题', '2025-01-16 09:00:00', '2025-01-16 10:30:00', '2025-01-16 10:30:00'),
(1002, 102, 3, '订单查询', '查询历史订单记录', 'medium', 'resolved', '业务咨询', '2025-01-16 09:15:00', '2025-01-16 09:45:00', '2025-01-16 09:45:00'),
(1003, 103, 2, '密码重置', '忘记密码需要重置', 'low', 'resolved', '账户问题', '2025-01-16 10:00:00', '2025-01-16 10:20:00', '2025-01-16 10:20:00'),
(1004, 104, 5, '功能异常', '报表功能显示异常', 'high', 'resolved', '技术问题', '2025-01-16 11:00:00', '2025-01-16 12:00:00', '2025-01-16 12:00:00'),
(1005, 105, 6, '账单问题', '账单金额显示错误', 'medium', 'resolved', '账户问题', '2025-01-16 13:00:00', '2025-01-16 14:00:00', '2025-01-16 14:00:00'),

-- 处理中的工单
(1006, 106, 1, '系统卡顿', '系统响应速度很慢', 'high', 'in_progress', '技术问题', '2025-01-16 14:00:00', '2025-01-16 14:30:00', NULL),
(1007, 107, 2, '数据同步', '数据同步失败', 'medium', 'in_progress', '技术问题', '2025-01-16 14:30:00', '2025-01-16 15:00:00', NULL),
(1008, 108, 3, '产品咨询', '新产品功能介绍', 'low', 'in_progress', '业务咨询', '2025-01-16 15:00:00', '2025-01-16 15:30:00', NULL),

-- 待处理的工单
(1009, 109, NULL, '支付异常', '支付过程中出现错误', 'high', 'pending', '技术问题', '2025-01-16 15:30:00', '2025-01-16 15:30:00', NULL),
(1010, 110, NULL, '退款申请', '申请订单退款', 'medium', 'pending', '业务咨询', '2025-01-16 16:00:00', '2025-01-16 16:00:00', NULL),
(1011, 111, NULL, '账户冻结', '账户被意外冻结', 'high', 'pending', '账户问题', '2025-01-16 16:30:00', '2025-01-16 16:30:00', NULL),
(1012, 112, NULL, '接口调用', 'API接口调用失败', 'medium', 'pending', '技术问题', '2025-01-16 17:00:00', '2025-01-16 17:00:00', NULL);

-- ============================================
-- 插入工单操作日志数据
-- ============================================

INSERT INTO ticket_logs (log_id, ticket_id, agent_id, action, old_value, new_value, comment) VALUES
-- 工单1001的操作日志
(1, 1001, 1, 'assign', 'NULL', 'agent_1', '分配给技术支持张小明'),
(2, 1001, 1, 'status_change', 'pending', 'in_progress', '开始处理登录问题'),
(3, 1001, 1, 'status_change', 'in_progress', 'resolved', '问题已解决，用户可以正常登录'),

-- 工单1002的操作日志
(4, 1002, 3, 'assign', 'NULL', 'agent_3', '分配给销售支持王大力'),
(5, 1002, 3, 'status_change', 'pending', 'in_progress', '开始处理订单查询'),
(6, 1002, 3, 'status_change', 'in_progress', 'resolved', '已为用户查询到历史订单'),

-- 工单1003的操作日志
(7, 1003, 2, 'assign', 'NULL', 'agent_2', '分配给技术支持李小红'),
(8, 1003, 2, 'status_change', 'pending', 'in_progress', '开始处理密码重置'),
(9, 1003, 2, 'status_change', 'in_progress', 'resolved', '密码已重置，已发送邮件通知'),

-- 工单1004的操作日志
(10, 1004, 5, 'assign', 'NULL', 'agent_5', '分配给技术支持陈建国'),
(11, 1004, 5, 'status_change', 'pending', 'in_progress', '开始处理报表功能异常'),
(12, 1004, 5, 'priority_change', 'medium', 'high', '发现影响多个用户，提升优先级'),
(13, 1004, 5, 'status_change', 'in_progress', 'resolved', '报表功能已修复'),

-- 工单1005的操作日志
(14, 1005, 6, 'assign', 'NULL', 'agent_6', '分配给客户服务赵小丽'),
(15, 1005, 6, 'status_change', 'pending', 'in_progress', '开始处理账单问题'),
(16, 1005, 6, 'status_change', 'in_progress', 'resolved', '账单金额已更正'),

-- 工单1006的操作日志（处理中）
(17, 1006, 1, 'assign', 'NULL', 'agent_1', '分配给技术支持张小明'),
(18, 1006, 1, 'status_change', 'pending', 'in_progress', '开始处理系统卡顿问题'),
(19, 1006, 1, 'comment', 'NULL', 'NULL', '正在检查服务器性能指标'),

-- 工单1007的操作日志（处理中）
(20, 1007, 2, 'assign', 'NULL', 'agent_2', '分配给技术支持李小红'),
(21, 1007, 2, 'status_change', 'pending', 'in_progress', '开始处理数据同步问题'),
(22, 1007, 2, 'comment', 'NULL', 'NULL', '正在检查数据同步配置'),

-- 工单1008的操作日志（处理中）
(23, 1008, 3, 'assign', 'NULL', 'agent_3', '分配给销售支持王大力'),
(24, 1008, 3, 'status_change', 'pending', 'in_progress', '开始处理产品咨询');

-- ============================================
-- 创建目标数据库表结构
-- ============================================

-- 切换到目标数据库
\c sgcc_dw_db;

-- 创建工单仪表板表
CREATE TABLE IF NOT EXISTS ticket_dashboard (
    dashboard_id INT PRIMARY KEY,
    metric_name VARCHAR(100),
    metric_value DOUBLE PRECISION,
    metric_desc VARCHAR(200),
    update_time TIMESTAMP
);

-- 创建客服排行榜表
CREATE TABLE IF NOT EXISTS agent_ranking (
    agent_id INT,
    agent_name VARCHAR(100),
    department VARCHAR(50),
    rank_type VARCHAR(50),
    rank_value DOUBLE PRECISION,
    ranking INT,
    update_time TIMESTAMP,
    PRIMARY KEY (agent_id, rank_type)
);

-- 创建客服工作负载表
CREATE TABLE IF NOT EXISTS agent_workload (
    agent_id INT PRIMARY KEY,
    agent_name VARCHAR(100),
    department VARCHAR(50),
    active_tickets BIGINT,
    resolved_today BIGINT,
    avg_response_time_minutes DOUBLE PRECISION,
    last_updated TIMESTAMP
);

-- 创建日统计表
CREATE TABLE IF NOT EXISTS daily_ticket_stats (
    stat_date DATE PRIMARY KEY,
    total_created BIGINT,
    total_resolved BIGINT,
    total_closed BIGINT,
    avg_resolution_time_hours DOUBLE PRECISION
);

-- 创建客服绩效表
CREATE TABLE IF NOT EXISTS agent_performance (
    agent_id INT,
    agent_name VARCHAR(100),
    stat_date DATE,
    tickets_handled BIGINT,
    tickets_resolved BIGINT,
    resolution_rate DOUBLE PRECISION,
    avg_resolution_time_hours DOUBLE PRECISION,
    PRIMARY KEY (agent_id, stat_date)
);

-- ============================================
-- 切换回源数据库，添加更多测试数据
-- ============================================

\c sgcc_source_db;

-- 添加历史工单数据（昨天的数据）
INSERT INTO tickets (ticket_id, customer_id, agent_id, title, description, priority, status, category, created_at, updated_at, closed_at) VALUES
(2001, 201, 1, '系统升级', '系统升级后无法使用', 'high', 'resolved', '技术问题', '2025-01-15 09:00:00', '2025-01-15 11:00:00', '2025-01-15 11:00:00'),
(2002, 202, 2, '数据丢失', '用户数据丢失', 'high', 'resolved', '技术问题', '2025-01-15 10:00:00', '2025-01-15 12:00:00', '2025-01-15 12:00:00'),
(2003, 203, 3, '商品咨询', '新商品信息咨询', 'low', 'resolved', '业务咨询', '2025-01-15 11:00:00', '2025-01-15 11:30:00', '2025-01-15 11:30:00'),
(2004, 204, 5, '权限问题', '用户权限配置错误', 'medium', 'resolved', '账户问题', '2025-01-15 13:00:00', '2025-01-15 14:00:00', '2025-01-15 14:00:00'),
(2005, 205, 6, '发票申请', '需要开具发票', 'low', 'resolved', '业务咨询', '2025-01-15 14:00:00', '2025-01-15 14:30:00', '2025-01-15 14:30:00');

-- 添加对应的操作日志
INSERT INTO ticket_logs (log_id, ticket_id, agent_id, action, old_value, new_value, comment) VALUES
(25, 2001, 1, 'assign', 'NULL', 'agent_1', '分配给技术支持张小明'),
(26, 2001, 1, 'status_change', 'pending', 'resolved', '系统升级问题已解决'),
(27, 2002, 2, 'assign', 'NULL', 'agent_2', '分配给技术支持李小红'),
(28, 2002, 2, 'status_change', 'pending', 'resolved', '数据已恢复'),
(29, 2003, 3, 'assign', 'NULL', 'agent_3', '分配给销售支持王大力'),
(30, 2003, 3, 'status_change', 'pending', 'resolved', '已提供商品详细信息'),
(31, 2004, 5, 'assign', 'NULL', 'agent_5', '分配给技术支持陈建国'),
(32, 2004, 5, 'status_change', 'pending', 'resolved', '权限已重新配置'),
(33, 2005, 6, 'assign', 'NULL', 'agent_6', '分配给客户服务赵小丽'),
(34, 2005, 6, 'status_change', 'pending', 'resolved', '发票已开具并邮寄');

-- ============================================
-- 创建更新触发器（模拟实时数据变化）
-- ============================================

-- 创建更新tickets表的updated_at字段的触发器
CREATE OR REPLACE FUNCTION update_ticket_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_ticket_timestamp_trigger
    BEFORE UPDATE ON tickets
    FOR EACH ROW
    EXECUTE FUNCTION update_ticket_timestamp();

-- 创建更新agents表的updated_at字段的触发器
CREATE OR REPLACE FUNCTION update_agent_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_agent_timestamp_trigger
    BEFORE UPDATE ON agents
    FOR EACH ROW
    EXECUTE FUNCTION update_agent_timestamp();

-- ============================================
-- 示例：模拟实时数据变化的SQL命令
-- ============================================

/*
-- 模拟工单状态更新（体现UPSERT特性）
UPDATE tickets SET status = 'resolved', closed_at = CURRENT_TIMESTAMP WHERE ticket_id = 1009;

-- 模拟新工单创建
INSERT INTO tickets (ticket_id, customer_id, title, description, priority, status, category) 
VALUES (1013, 113, '新功能请求', '希望增加新的功能', 'low', 'pending', '功能建议');

-- 模拟客服状态变更
UPDATE agents SET status = 'busy' WHERE agent_id = 1;

-- 模拟添加操作日志
INSERT INTO ticket_logs (log_id, ticket_id, agent_id, action, old_value, new_value, comment) 
VALUES (35, 1009, 1, 'status_change', 'pending', 'resolved', '支付异常问题已解决');
*/ 