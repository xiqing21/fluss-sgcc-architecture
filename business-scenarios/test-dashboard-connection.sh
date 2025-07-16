#!/bin/bash

# 测试Grafana Dashboard数据连接
echo "🔍 测试Grafana Dashboard数据连接..."
echo "=========================================="

# 1. 测试Grafana API健康状态
echo "1. 检查Grafana API健康状态..."
if curl -s -o /dev/null -w "%{http_code}" http://localhost:3000/api/health | grep -q "200"; then
    echo "✅ Grafana API健康状态正常"
else
    echo "❌ Grafana API连接失败"
    exit 1
fi

# 2. 测试PostgreSQL数据库连接
echo "2. 检查PostgreSQL数据库连接..."
if docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "SELECT 1;" > /dev/null 2>&1; then
    echo "✅ PostgreSQL数据库连接正常"
else
    echo "❌ PostgreSQL数据库连接失败"
    exit 1
fi

# 3. 检查fluss_sink_table表是否存在
echo "3. 检查fluss_sink_table表..."
if docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "SELECT COUNT(*) FROM fluss_sink_table;" > /dev/null 2>&1; then
    echo "✅ fluss_sink_table表存在"
    # 显示表中的数据行数
    ROW_COUNT=$(docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -t -c "SELECT COUNT(*) FROM fluss_sink_table;" | tr -d ' ')
    echo "   📊 表中数据行数: $ROW_COUNT"
else
    echo "❌ fluss_sink_table表不存在或无法访问"
    echo "   🔧 正在创建测试数据表..."
    
    # 创建测试表和数据
    docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
    CREATE TABLE IF NOT EXISTS fluss_sink_table (
        device_id VARCHAR(100),
        device_type VARCHAR(50),
        location VARCHAR(100),
        status VARCHAR(20),
        load_factor DOUBLE PRECISION,
        efficiency DOUBLE PRECISION,
        temperature INTEGER,
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- 插入测试数据
    INSERT INTO fluss_sink_table (device_id, device_type, location, status, load_factor, efficiency, temperature) 
    VALUES 
        ('DEV_001', 'transformer', '北京', 'online', 75.5, 96.2, 45),
        ('DEV_002', 'generator', '上海', 'online', 82.1, 94.8, 52),
        ('DEV_003', 'switch', '广州', 'offline', 0.0, 0.0, 25),
        ('DEV_004', 'transformer', '深圳', 'online', 68.9, 97.1, 41),
        ('DEV_005', 'generator', '杭州', 'maintenance', 45.2, 85.3, 38),
        ('DEV_006', 'switch', '成都', 'online', 91.3, 98.5, 35),
        ('DEV_007', 'transformer', '重庆', 'online', 77.8, 95.9, 48),
        ('DEV_008', 'generator', '武汉', 'online', 84.6, 96.7, 50),
        ('DEV_009', 'switch', '西安', 'online', 73.2, 94.1, 42),
        ('DEV_010', 'transformer', '南京', 'online', 79.5, 97.3, 44);
    " > /dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        echo "✅ 测试数据表创建成功"
        NEW_ROW_COUNT=$(docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -t -c "SELECT COUNT(*) FROM fluss_sink_table;" | tr -d ' ')
        echo "   📊 新增数据行数: $NEW_ROW_COUNT"
    else
        echo "❌ 测试数据表创建失败"
        exit 1
    fi
fi

# 4. 测试关键SQL查询
echo "4. 测试Dashboard SQL查询..."

# 测试设备状态查询
echo "   📊 设备状态分布:"
docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
SELECT 
    status as \"状态\",
    COUNT(*) as \"数量\",
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fluss_sink_table), 2) as \"占比(%)\",
    string_agg(device_id, ', ') as \"设备列表\"
FROM fluss_sink_table 
GROUP BY status 
ORDER BY COUNT(*) DESC;
" 2>/dev/null | grep -E "(状态|online|offline|maintenance)"

# 测试设备类型查询
echo "   🔧 设备类型分布:"
docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
SELECT 
    device_type as \"设备类型\",
    COUNT(*) as \"数量\",
    ROUND(AVG(efficiency), 2) as \"平均效率(%)\",
    ROUND(AVG(load_factor), 2) as \"平均负荷(%)\"
FROM fluss_sink_table 
WHERE status = 'online'
GROUP BY device_type 
ORDER BY COUNT(*) DESC;
" 2>/dev/null | grep -E "(设备类型|transformer|generator|switch)"

# 测试地区分布查询
echo "   🗺️ 地区分布:"
docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
SELECT 
    location as \"地区\",
    COUNT(*) as \"设备数量\",
    ROUND(AVG(CASE WHEN status = 'online' THEN efficiency END), 2) as \"平均效率(%)\"
FROM fluss_sink_table 
GROUP BY location 
ORDER BY COUNT(*) DESC;
" 2>/dev/null | grep -E "(地区|北京|上海|广州|深圳|杭州|成都|重庆|武汉|西安|南京)"

# 5. 测试数据源连接
echo "5. 测试Grafana数据源连接..."
sleep 5  # 等待Grafana完全启动

# 使用Grafana API测试数据源
if curl -s -u admin:admin123 http://localhost:3000/api/datasources | grep -q "PostgreSQL-SGCC"; then
    echo "✅ PostgreSQL-SGCC数据源已配置"
else
    echo "❌ PostgreSQL-SGCC数据源配置失败"
fi

# 6. 检查Dashboard是否已加载
echo "6. 检查Dashboard配置..."
sleep 5  # 等待Dashboard加载

if curl -s -u admin:admin123 http://localhost:3000/api/search | grep -q "sgcc-fluss-dashboard"; then
    echo "✅ 国网智能调度大屏Dashboard已加载"
else
    echo "❌ Dashboard加载失败"
fi

# 7. 显示访问信息
echo ""
echo "🎉 测试完成! 现在可以访问以下地址:"
echo "=========================================="
echo "🔋 Grafana 国网智能调度大屏:"
echo "   URL: http://localhost:3000"
echo "   用户名: admin"
echo "   密码: admin123"
echo ""
echo "📊 直接访问Dashboard:"
echo "   URL: http://localhost:3000/d/sgcc-fluss-dashboard"
echo ""
echo "🔧 如果遇到问题，请检查:"
echo "   1. PostgreSQL数据库是否运行: docker ps | grep postgres"
echo "   2. Grafana日志: docker logs grafana-sgcc"
echo "   3. 数据源配置: curl -u admin:admin123 http://localhost:3000/api/datasources"
echo ""
echo "🚀 接下来可以在Grafana中查看实时大屏效果!" 