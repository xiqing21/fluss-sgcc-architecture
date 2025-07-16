-- ===========================
-- 第三步：DWS层 - 汇总数据层
-- ===========================

USE CATALOG fluss;

-- ===========================
-- 创建DWS层表：汇总数据层
-- ===========================

-- 用户订单统计表（DWS）
CREATE TABLE IF NOT EXISTS dws_user_stats (
    user_id INT,
    username STRING,
    total_orders BIGINT,
    total_amount DECIMAL(12,2),
    avg_order_amount DECIMAL(10,2),
    last_order_date TIMESTAMP_LTZ(3),
    etl_time TIMESTAMP_LTZ(3),
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'bucket.num' = '3'
);

-- 商品销售统计表（DWS）
CREATE TABLE IF NOT EXISTS dws_product_stats (
    product_name STRING,
    total_quantity BIGINT,
    total_sales DECIMAL(12,2),
    total_orders BIGINT,
    avg_price DECIMAL(10,2),
    etl_time TIMESTAMP_LTZ(3),
    PRIMARY KEY (product_name) NOT ENFORCED
) WITH (
    'bucket.num' = '3'
);

-- 每日订单统计表（DWS）
CREATE TABLE IF NOT EXISTS dws_daily_stats (
    stat_date DATE,
    total_orders BIGINT,
    total_amount DECIMAL(12,2),
    completed_orders BIGINT,
    pending_orders BIGINT,
    etl_time TIMESTAMP_LTZ(3),
    PRIMARY KEY (stat_date) NOT ENFORCED
) WITH (
    'bucket.num' = '3'
);

-- ===========================
-- DWS层实时聚合逻辑
-- ===========================

-- 用户订单统计 - 实时聚合
INSERT INTO dws_user_stats
SELECT 
    user_id,
    username,
    COUNT(*) as total_orders,
    SUM(order_amount) as total_amount,
    AVG(order_amount) as avg_order_amount,
    MAX(created_at) as last_order_date,
    CURRENT_TIMESTAMP as etl_time
FROM dwd_orders
GROUP BY user_id, username;

-- 商品销售统计 - 实时聚合
INSERT INTO dws_product_stats
SELECT 
    product_name,
    SUM(quantity) as total_quantity,
    SUM(total_price) as total_sales,
    COUNT(DISTINCT order_id) as total_orders,
    AVG(unit_price) as avg_price,
    CURRENT_TIMESTAMP as etl_time
FROM dwd_order_items
GROUP BY product_name;

-- 每日订单统计 - 实时聚合
INSERT INTO dws_daily_stats
SELECT 
    DATE(created_at) as stat_date,
    COUNT(*) as total_orders,
    SUM(order_amount) as total_amount,
    COUNT(CASE WHEN order_status = 'completed' THEN 1 END) as completed_orders,
    COUNT(CASE WHEN order_status = 'pending' THEN 1 END) as pending_orders,
    CURRENT_TIMESTAMP as etl_time
FROM dwd_orders
GROUP BY DATE(created_at);

-- 创建实时统计视图
CREATE VIEW IF NOT EXISTS dws_realtime_stats AS
SELECT 
    'total_users' as metric_name,
    COUNT(DISTINCT user_id) as metric_value,
    CURRENT_TIMESTAMP as update_time
FROM dwd_users

UNION ALL

SELECT 
    'total_orders' as metric_name,
    COUNT(*) as metric_value,
    CURRENT_TIMESTAMP as update_time
FROM dwd_orders

UNION ALL

SELECT 
    'total_sales' as metric_name,
    SUM(order_amount) as metric_value,
    CURRENT_TIMESTAMP as update_time
FROM dwd_orders
WHERE order_status = 'completed'

UNION ALL

SELECT 
    'avg_order_value' as metric_name,
    AVG(order_amount) as metric_value,
    CURRENT_TIMESTAMP as update_time
FROM dwd_orders
WHERE order_status = 'completed';

-- 创建实时商品销售排行视图
CREATE VIEW IF NOT EXISTS dws_product_ranking AS
SELECT 
    product_name,
    total_sales,
    total_quantity,
    total_orders,
    RANK() OVER (ORDER BY total_sales DESC) as sales_rank,
    CURRENT_TIMESTAMP as update_time
FROM dws_product_stats;

-- 创建实时用户价值分层视图
CREATE VIEW IF NOT EXISTS dws_user_segments AS
SELECT 
    user_id,
    username,
    total_amount,
    total_orders,
    avg_order_amount,
    CASE 
        WHEN total_amount >= 500 THEN 'high_value'
        WHEN total_amount >= 100 THEN 'medium_value'
        ELSE 'low_value'
    END as segment_type,
    last_order_date,
    CURRENT_TIMESTAMP as update_time
FROM dws_user_stats;