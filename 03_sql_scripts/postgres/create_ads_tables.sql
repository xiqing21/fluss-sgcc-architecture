-- 创建ADS层对应的PostgreSQL表结构
-- 用于接收从Fluss同步的数据

-- 实时运营看板数据表
CREATE TABLE IF NOT EXISTS ads_realtime_dashboard (
    metric_name VARCHAR(255) PRIMARY KEY,
    metric_value DECIMAL(15,2),
    metric_desc VARCHAR(255),
    update_time TIMESTAMP WITH TIME ZONE
);

-- 用户价值分层表
CREATE TABLE IF NOT EXISTS ads_user_segments (
    user_id INTEGER PRIMARY KEY,
    username VARCHAR(255),
    segment_type VARCHAR(50),
    total_amount DECIMAL(12,2),
    order_count BIGINT,
    avg_order_amount DECIMAL(10,2),
    last_order_days INTEGER,
    update_time TIMESTAMP WITH TIME ZONE
);

-- 实时销售趋势表
CREATE TABLE IF NOT EXISTS ads_sales_trend (
    time_window VARCHAR(50) PRIMARY KEY,
    total_sales DECIMAL(12,2),
    order_count BIGINT,
    avg_order_value DECIMAL(10,2),
    update_time TIMESTAMP WITH TIME ZONE
);

-- 创建索引提高查询性能
CREATE INDEX IF NOT EXISTS idx_ads_user_segments_segment_type ON ads_user_segments(segment_type);
CREATE INDEX IF NOT EXISTS idx_ads_user_segments_update_time ON ads_user_segments(update_time);
CREATE INDEX IF NOT EXISTS idx_ads_sales_trend_update_time ON ads_sales_trend(update_time);

-- 插入初始数据（可选）
INSERT INTO ads_realtime_dashboard (metric_name, metric_value, metric_desc, update_time) VALUES
('total_users', 0, '总用户数', NOW()),
('total_orders', 0, '总订单数', NOW()),
('total_sales', 0, '总销售额', NOW()),
('avg_order_value', 0, '平均订单金额', NOW()),
('today_orders', 0, '今日订单数', NOW()),
('today_sales', 0, '今日销售额', NOW())
ON CONFLICT (metric_name) DO NOTHING; 