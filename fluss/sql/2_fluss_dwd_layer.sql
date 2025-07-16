-- ===========================
-- 第二步：DWD层 - 明细数据层
-- ===========================

USE CATALOG fluss;

-- ===========================
-- 创建DWD层表：明细数据层
-- ===========================

-- 用户明细表（DWD）
CREATE TABLE IF NOT EXISTS dwd_users (
    user_id INT,
    username STRING,
    email STRING,
    created_at TIMESTAMP_LTZ(3),
    updated_at TIMESTAMP_LTZ(3),
    etl_time TIMESTAMP_LTZ(3),
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'bucket.num' = '3'
);

-- 订单明细表（DWD）- 包含用户信息的宽表
CREATE TABLE IF NOT EXISTS dwd_orders (
    order_id INT,
    user_id INT,
    username STRING,
    email STRING,
    order_amount DECIMAL(10,2),
    order_status STRING,
    created_at TIMESTAMP_LTZ(3),
    updated_at TIMESTAMP_LTZ(3),
    etl_time TIMESTAMP_LTZ(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'bucket.num' = '3'
);

-- 订单商品明细表（DWD）- 包含用户和订单信息的宽表
CREATE TABLE IF NOT EXISTS dwd_order_items (
    item_id INT,
    order_id INT,
    user_id INT,
    username STRING,
    product_name STRING,
    quantity INT,
    unit_price DECIMAL(10,2),
    total_price DECIMAL(10,2),
    created_at TIMESTAMP_LTZ(3),
    etl_time TIMESTAMP_LTZ(3),
    PRIMARY KEY (item_id) NOT ENFORCED
) WITH (
    'bucket.num' = '3'
);

-- ===========================
-- DWD层数据加工逻辑
-- ===========================

-- 用户明细数据加工
INSERT INTO dwd_users
SELECT 
    id as user_id,
    username,
    email,
    created_at,
    updated_at,
    CURRENT_TIMESTAMP as etl_time
FROM ods_users;

-- 订单明细数据加工 - 关联用户信息
INSERT INTO dwd_orders
SELECT 
    o.id as order_id,
    o.user_id,
    u.username,
    u.email,
    o.order_amount,
    o.order_status,
    o.created_at,
    o.updated_at,
    CURRENT_TIMESTAMP as etl_time
FROM ods_orders o
LEFT JOIN ods_users u ON o.user_id = u.id;

-- 订单商品明细数据加工 - 关联用户和订单信息
INSERT INTO dwd_order_items
SELECT 
    oi.id as item_id,
    oi.order_id,
    o.user_id,
    u.username,
    oi.product_name,
    oi.quantity,
    oi.unit_price,
    (oi.quantity * oi.unit_price) as total_price,
    oi.created_at,
    CURRENT_TIMESTAMP as etl_time
FROM ods_order_items oi
LEFT JOIN ods_orders o ON oi.order_id = o.id
LEFT JOIN ods_users u ON o.user_id = u.id;

-- 创建实时DWD数据视图
CREATE VIEW IF NOT EXISTS dwd_user_orders AS
SELECT 
    u.user_id,
    u.username,
    u.email,
    o.order_id,
    o.order_amount,
    o.order_status,
    o.created_at as order_time,
    CURRENT_TIMESTAMP as etl_time
FROM dwd_users u
JOIN dwd_orders o ON u.user_id = o.user_id;

CREATE VIEW IF NOT EXISTS dwd_order_details AS
SELECT 
    o.order_id,
    o.user_id,
    o.username,
    o.order_amount,
    o.order_status,
    oi.item_id,
    oi.product_name,
    oi.quantity,
    oi.unit_price,
    oi.total_price,
    o.created_at as order_time,
    CURRENT_TIMESTAMP as etl_time
FROM dwd_orders o
JOIN dwd_order_items oi ON o.order_id = oi.order_id;


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
