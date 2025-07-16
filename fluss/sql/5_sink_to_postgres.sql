-- ===========================
-- 第五步：数据导出到PostgreSQL Sink
-- ===========================

-- 创建PostgreSQL Sink表
CREATE TABLE IF NOT EXISTS sink_dwd_users (
    user_id INT,
    username STRING,
    email STRING,
    created_at TIMESTAMP_LTZ(3),
    updated_at TIMESTAMP_LTZ(3),
    etl_time TIMESTAMP_LTZ(3),
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'dwd_users',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE IF NOT EXISTS sink_dwd_orders (
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
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'dwd_orders',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE IF NOT EXISTS sink_dwd_order_items (
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
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'dwd_order_items',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE IF NOT EXISTS sink_dws_user_stats (
    user_id INT,
    username STRING,
    total_orders BIGINT,
    total_amount DECIMAL(12,2),
    avg_order_amount DECIMAL(10,2),
    last_order_date TIMESTAMP_LTZ(3),
    etl_time TIMESTAMP_LTZ(3),
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'dws_user_stats',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE IF NOT EXISTS sink_dws_product_stats (
    product_name STRING,
    total_quantity BIGINT,
    total_sales DECIMAL(12,2),
    total_orders BIGINT,
    avg_price DECIMAL(10,2),
    etl_time TIMESTAMP_LTZ(3),
    PRIMARY KEY (product_name) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'dws_product_stats',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE IF NOT EXISTS sink_dws_daily_stats (
    stat_date DATE,
    total_orders BIGINT,
    total_amount DECIMAL(12,2),
    completed_orders BIGINT,
    pending_orders BIGINT,
    etl_time TIMESTAMP_LTZ(3),
    PRIMARY KEY (stat_date) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'dws_daily_stats',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE IF NOT EXISTS sink_ads_realtime_dashboard (
    metric_name STRING,
    metric_value DECIMAL(15,2),
    metric_desc STRING,
    update_time TIMESTAMP_LTZ(3),
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
    update_time TIMESTAMP_LTZ(3),
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

-- ===========================
-- 启动实时数据同步任务
-- ===========================

-- 启动DWD层数据同步到PostgreSQL
EXECUTE STATEMENT SET
BEGIN
INSERT INTO sink_dwd_users SELECT * FROM dwd_users;
INSERT INTO sink_dwd_orders SELECT * FROM dwd_orders;
INSERT INTO sink_dwd_order_items SELECT * FROM dwd_order_items;
INSERT INTO sink_dws_user_stats SELECT * FROM dws_user_stats;
INSERT INTO sink_dws_product_stats SELECT * FROM dws_product_stats;
INSERT INTO sink_dws_daily_stats SELECT * FROM dws_daily_stats;
INSERT INTO sink_ads_realtime_dashboard SELECT * FROM ads_realtime_dashboard;
INSERT INTO sink_ads_user_segments SELECT * FROM ads_user_segments;
END;

-- 创建持续运行的数据同步JOB
-- 这些作业会持续监控Fluss中的数据变化并同步到PostgreSQL

CREATE JOB IF NOT EXISTS sync_dwd_to_postgres AS
INSERT INTO sink_dwd_users SELECT * FROM dwd_users;

CREATE JOB IF NOT EXISTS sync_dws_to_postgres AS
INSERT INTO sink_dws_user_stats SELECT * FROM dws_user_stats;

CREATE JOB IF NOT EXISTS sync_ads_to_postgres AS
INSERT INTO sink_ads_realtime_dashboard SELECT * FROM ads_realtime_dashboard;

-- 验证数据同步状态
SELECT 'DWD同步状态' as layer, COUNT(*) as record_count FROM sink_dwd_users
UNION ALL
SELECT 'DWS同步状态' as layer, COUNT(*) as record_count FROM sink_dws_user_stats
UNION ALL
SELECT 'ADS同步状态' as layer, COUNT(*) as record_count FROM sink_ads_realtime_dashboard;