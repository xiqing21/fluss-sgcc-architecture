-- 设置结果模式为 tableau
SET 'sql-client.execution.result-mode' = 'tableau';
-- ===========================
-- 第一步：创建CDC Source表
-- ===========================
--首先将 catalog 切换回 default
use catalog default_catalog;

-- PostgreSQL CDC Source表：用户数据
CREATE TABLE IF NOT EXISTS source_users (
    id INT,
    username STRING,
    email STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres-sgcc-source',
    'port' = '5432',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'database-name' = 'sgcc_source_db',
    'schema-name' = 'public',
    'table-name' = 'users',
    'decoding.plugin.name' = 'pgoutput',
    'slot.name' = 'fluss_users_slot'
);

-- PostgreSQL CDC Source表：订单数据
CREATE TABLE IF NOT EXISTS source_orders (
    id INT,
    user_id INT,
    order_amount DECIMAL(10,2),
    order_status STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres-sgcc-source',
    'port' = '5432',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'database-name' = 'sgcc_source_db',
    'schema-name' = 'public',
    'table-name' = 'orders',
    'decoding.plugin.name' = 'pgoutput',
    'slot.name' = 'fluss_orders_slot'
);

-- PostgreSQL CDC Source表：订单明细数据
CREATE TABLE IF NOT EXISTS source_order_items (
    id INT,
    order_id INT,
    product_name STRING,
    quantity INT,
    unit_price DECIMAL(10,2),
    created_at TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres-sgcc-source',
    'port' = '5432',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'database-name' = 'sgcc_source_db',
    'schema-name' = 'public',
    'table-name' = 'order_items',
    'decoding.plugin.name' = 'pgoutput',
    'slot.name' = 'fluss_order_items_slot'
);

-- 第二步：CDC数据接入到Fluss
-- ===========================

-- 创建Fluss Catalog
CREATE CATALOG fluss WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);

USE CATALOG fluss;

-- ===========================
-- 创建ODS层表：原始数据层
-- ===========================

-- 用户表（ODS）
CREATE TABLE IF NOT EXISTS ods_users (
    id INT,
    username STRING,
    email STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'bucket.num' = '3'
);

-- 订单表（ODS）
CREATE TABLE IF NOT EXISTS ods_orders (
    id INT,
    user_id INT,
    order_amount DECIMAL(10,2),
    order_status STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'bucket.num' = '3'
);

-- 订单明细表（ODS）
CREATE TABLE IF NOT EXISTS ods_order_items (
    id INT,
    order_id INT,
    product_name STRING,
    quantity INT,
    unit_price DECIMAL(10,2),
    created_at TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'bucket.num' = '3'
);

-- ===========================
-- CDC数据同步到ODS层
-- ===========================

-- 同步用户数据到ODS
EXECUTE STATEMENT SET
BEGIN
INSERT INTO fluss.ods_users SELECT * FROM default_catalog.default_database.source_users;
INSERT INTO fluss.ods_orders SELECT * FROM default_catalog.default_database.source_orders;
INSERT INTO fluss.ods_order_items SELECT * FROM default_catalog.default_database.source_order_items;
END;


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

-- 启动DWD层数据加工任务
EXECUTE STATEMENT SET
BEGIN
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
END;

-- 注意：Fluss catalog 不支持创建视图，所以跳过视图创建

-- ===========================
-- 第三步：DWS层 - 数据汇总层
-- ===========================

-- 创建DWS层表：数据汇总层
CREATE TABLE IF NOT EXISTS dws_user_stats (
    user_id INT,
    username STRING,
    total_amount DECIMAL(12,2),
    total_orders BIGINT,
    avg_order_amount DECIMAL(10,2),
    last_order_date TIMESTAMP_LTZ(3),
    update_time TIMESTAMP_LTZ(3),
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'bucket.num' = '3'
);

-- 启动DWS层数据汇总任务
EXECUTE STATEMENT SET
BEGIN
-- 计算用户统计信息
INSERT INTO dws_user_stats
SELECT 
    u.user_id,
    u.username,
    COALESCE(SUM(o.order_amount), 0) as total_amount,
    COUNT(o.order_id) as total_orders,
    COALESCE(AVG(o.order_amount), 0) as avg_order_amount,
    MAX(o.created_at) as last_order_date,
    CURRENT_TIMESTAMP as update_time
FROM dwd_users u
LEFT JOIN dwd_orders o ON u.user_id = o.user_id
WHERE o.order_status = 'completed' OR o.order_status IS NULL
GROUP BY u.user_id, u.username;
END;

-- ===========================
-- 第四步：ADS层 - 应用数据层
-- ===========================

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
WHERE CAST(created_at AS DATE) = CURRENT_DATE

UNION ALL

SELECT 
    'today_sales' as metric_name,
    COALESCE(SUM(order_amount), 0) as metric_value,
    '今日销售额' as metric_desc,
    CURRENT_TIMESTAMP as update_time
FROM dwd_orders 
WHERE CAST(created_at AS DATE) = CURRENT_DATE AND order_status = 'completed';

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
    TIMESTAMPDIFF(DAY, CAST(last_order_date AS DATE), CURRENT_DATE) as last_order_days,
    CURRENT_TIMESTAMP as update_time
FROM dws_user_stats;

-- 实时销售趋势计算（按小时）
INSERT INTO ads_sales_trend
SELECT 
    CONCAT(CAST(CAST(created_at AS DATE) AS STRING), ' ', CAST(HOUR(created_at) AS STRING), ':00') as time_window,
    SUM(order_amount) as total_sales,
    COUNT(*) as order_count,
    AVG(order_amount) as avg_order_value,
    CURRENT_TIMESTAMP as update_time
FROM dwd_orders
WHERE order_status = 'completed'
GROUP BY CAST(created_at AS DATE), HOUR(created_at);

-- 注意：Fluss catalog 不支持创建视图，已跳过告警和监控视图创建





-- ===========================
-- 第五步：数据导出到PostgreSQL Sink
-- ===========================

use catalog default_catalog;

-- 创建PostgreSQL Sink表,注意需要容器内部 postgres端口 5432
-- postsink 只需要创建 ads 表来存储结果层
CREATE TABLE IF NOT EXISTS sink_ads_realtime_dashboard (
    metric_name STRING,
    metric_value DECIMAL(15,2),
    metric_desc STRING,
    update_time TIMESTAMP(3),
    PRIMARY KEY (metric_name) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'ads_realtime_dashboard',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE IF NOT EXISTS sink_ads_user_segments (
    user_id INT,
    username STRING,
    segment_type STRING,
    total_amount DECIMAL(12,2),
    order_count BIGINT,
    avg_order_amount DECIMAL(10,2),
    last_order_days INT,
    update_time TIMESTAMP(3),
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'ads_user_segments',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE IF NOT EXISTS sink_ads_sales_trend (
    time_window STRING,
    total_sales DECIMAL(12,2),
    order_count BIGINT,
    avg_order_value DECIMAL(10,2),
    update_time TIMESTAMP(3),
    PRIMARY KEY (time_window) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'ads_sales_trend',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s'
);

-- ===========================
-- 启动实时数据同步任务
-- ===========================

-- 启动ADS层数据同步到PostgreSQL
EXECUTE STATEMENT SET
BEGIN
INSERT INTO sink_ads_realtime_dashboard SELECT * FROM fluss.fluss.ads_realtime_dashboard;
INSERT INTO sink_ads_user_segments SELECT * FROM fluss.fluss.ads_user_segments;
INSERT INTO sink_ads_sales_trend SELECT * FROM fluss.fluss.ads_sales_trend;
END;


-- 验证数据同步状态
SELECT *  FROM sink_ads_realtime_dashboard;

-- 持续监控数据变化 (保持session不退出)
-- 如果需要保持session，可以在交互模式下运行以下查询
-- SELECT COUNT(*) FROM sink_ads_realtime_dashboard;