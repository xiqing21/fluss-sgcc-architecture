#!/bin/bash

# 🔍 快速验证数据脚本
# 检查所有表的数据状态

echo "🔍 正在检查数据状态..."
echo ""

echo "📊 PostgreSQL 源数据库状态:"
docker exec postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "
SELECT 'users' as table_name, count(*) as records FROM users 
UNION SELECT 'orders' as table_name, count(*) as records FROM orders 
UNION SELECT 'order_items' as table_name, count(*) as records FROM order_items
ORDER BY table_name;
"

echo ""
echo "📊 PostgreSQL 目标数据库状态:"
docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
SELECT 'ads_realtime_dashboard' as table_name, count(*) as records FROM ads_realtime_dashboard 
UNION SELECT 'ads_user_segments' as table_name, count(*) as records FROM ads_user_segments 
UNION SELECT 'ads_sales_trend' as table_name, count(*) as records FROM ads_sales_trend
ORDER BY table_name;
"

echo ""
echo "📊 Flink 任务状态:"
docker exec jobmanager-sgcc /opt/flink/bin/flink list

echo ""
echo "📊 ads_realtime_dashboard 最新数据:"
docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
SELECT * FROM ads_realtime_dashboard ORDER BY update_time DESC LIMIT 10;
"

echo ""
echo "📊 ads_user_segments 数据:"
docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
SELECT * FROM ads_user_segments LIMIT 5;
"

echo ""
echo "📊 ads_sales_trend 数据:"
docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
SELECT * FROM ads_sales_trend LIMIT 5;
"

echo ""
echo "✅ 数据检查完成" 