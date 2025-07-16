-- ===========================
-- 第四步：ADS层 - 应用数据层
-- ===========================

USE CATALOG fluss;

-- ===========================
-- 创建ADS层表：应用数据层
-- ===========================

-- 实时运营看板数据（ADS）
CREATE TABLE IF NOT EXISTS ads_realtime_dashboard (
    metric_name STRING,
    metric_value DECIMAL(15,2),
    metric_desc STRING,
    update_time TIMESTAMP_LTZ(3),
    PRIMARY KEY (metric_name) NOT ENFORCED
) WITH (
    'bucket.num' = '3'
);

-- 用户价值分层表（ADS）
CREATE TABLE IF NOT EXISTS ads_user_segments (
    user_id INT,
    username STRING,
    segment_type STRING, -- high_value, medium_value, low_value
    total_amount DECIMAL(12,2),
    order_count BIGINT,
    avg_order_amount DECIMAL(10,2),
    last_order_days INT,
    update_time TIMESTAMP_LTZ(3),
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'bucket.num' = '3'
);

-- 实时销售趋势表（ADS）
CREATE TABLE IF NOT EXISTS ads_sales_trend (
    time_window STRING,
    total_sales DECIMAL(12,2),
    order_count BIGINT,
    avg_order_value DECIMAL(10,2),
    update_time TIMESTAMP_LTZ(3),
    PRIMARY KEY (time_window) NOT ENFORCED
) WITH (
    'bucket.num' = '3'
);

-- ===========================
-- ADS层业务指标计算
-- ===========================

-- 实时运营看板数据更新
INSERT INTO ads_realtime_dashboard
SELECT 
    'total_users' as metric_name,
    COUNT(DISTINCT user_id) as metric_value,
    '总用户数' as metric_desc,
    CURRENT_TIMESTAMP as update_time
FROM dwd_users

UNION ALL

SELECT 
    'total_orders' as metric_name,
    COUNT(*) as metric_value,
    '总订单数' as metric_desc,
    CURRENT_TIMESTAMP as update_time
FROM dwd_orders

UNION ALL

SELECT 
    'total_sales' as metric_name,
    COALESCE(SUM(order_amount), 0) as metric_value,
    '总销售额' as metric_desc,
    CURRENT_TIMESTAMP as update_time
FROM dwd_orders 
WHERE order_status = 'completed'

UNION ALL

SELECT 
    'avg_order_value' as metric_name,
    COALESCE(AVG(order_amount), 0) as metric_value,
    '平均订单金额' as metric_desc,
    CURRENT_TIMESTAMP as update_time
FROM dwd_orders 
WHERE order_status = 'completed'

UNION ALL

SELECT 
    'today_orders' as metric_name,
    COUNT(*) as metric_value,
    '今日订单数' as metric_desc,
    CURRENT_TIMESTAMP as update_time
FROM dwd_orders 
WHERE DATE(created_at) = CURRENT_DATE

UNION ALL

SELECT 
    'today_sales' as metric_name,
    COALESCE(SUM(order_amount), 0) as metric_value,
    '今日销售额' as metric_desc,
    CURRENT_TIMESTAMP as update_time
FROM dwd_orders 
WHERE DATE(created_at) = CURRENT_DATE AND order_status = 'completed';

-- 用户价值分层计算
INSERT INTO ads_user_segments
SELECT 
    user_id,
    username,
    CASE 
        WHEN total_amount >= 500 THEN 'high_value'
        WHEN total_amount >= 100 THEN 'medium_value'
        ELSE 'low_value'
    END as segment_type,
    total_amount,
    total_orders as order_count,
    avg_order_amount,
    DATEDIFF(CURRENT_DATE, DATE(last_order_date)) as last_order_days,
    CURRENT_TIMESTAMP as update_time
FROM dws_user_stats;

-- 实时销售趋势计算（按小时）
INSERT INTO ads_sales_trend
SELECT 
    CONCAT(DATE(created_at), ' ', HOUR(created_at), ':00') as time_window,
    SUM(order_amount) as total_sales,
    COUNT(*) as order_count,
    AVG(order_amount) as avg_order_value,
    CURRENT_TIMESTAMP as update_time
FROM dwd_orders
WHERE order_status = 'completed'
GROUP BY DATE(created_at), HOUR(created_at);

-- 创建实时告警视图（高价值用户监控）
CREATE VIEW IF NOT EXISTS ads_high_value_alerts AS
SELECT 
    user_id,
    username,
    total_amount,
    last_order_days,
    CASE 
        WHEN last_order_days > 30 THEN '流失风险'
        WHEN last_order_days > 7 THEN '需要关注'
        ELSE '正常'
    END as alert_type,
    CURRENT_TIMESTAMP as alert_time
FROM ads_user_segments
WHERE segment_type = 'high_value'
AND last_order_days > 7;

-- 创建实时业务监控视图
CREATE VIEW IF NOT EXISTS ads_business_monitor AS
SELECT 
    'order_completion_rate' as metric_name,
    (COUNT(CASE WHEN order_status = 'completed' THEN 1 END) * 100.0 / COUNT(*)) as metric_value,
    '订单完成率(%)' as metric_desc,
    CURRENT_TIMESTAMP as update_time
FROM dwd_orders

UNION ALL

SELECT 
    'user_activity_rate' as metric_name,
    (COUNT(DISTINCT CASE WHEN DATE(updated_at) = CURRENT_DATE THEN user_id END) * 100.0 / COUNT(DISTINCT user_id)) as metric_value,
    '用户活跃率(%)' as metric_desc,
    CURRENT_TIMESTAMP as update_time
FROM dwd_users u
LEFT JOIN dwd_orders o ON u.user_id = o.user_id;