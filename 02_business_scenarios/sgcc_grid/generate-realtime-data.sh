#!/bin/bash

# 国网智能调度大屏 - 实时数据生成器
# 模拟电网设备状态的动态变化

echo "🚀 开始生成实时数据..."
echo "模拟国网智能调度系统的设备状态变化"
echo "按 Ctrl+C 停止数据生成"
echo "=========================================="

# 设备类型数组
DEVICE_TYPES=("transformer" "generator" "switch" "relay" "breaker" "capacitor")

# 地区数组
LOCATIONS=("北京" "上海" "广州" "深圳" "杭州" "成都" "重庆" "武汉" "西安" "南京" "天津" "苏州" "青岛" "大连" "宁波")

# 状态数组
STATUSES=("online" "offline" "maintenance")

# 数据生成函数
generate_device_data() {
    local device_id=$1
    local device_type=${DEVICE_TYPES[$RANDOM % ${#DEVICE_TYPES[@]}]}
    local location=${LOCATIONS[$RANDOM % ${#LOCATIONS[@]}]}
    local status=${STATUSES[$RANDOM % ${#STATUSES[@]}]}
    
    # 根据状态生成对应的数据
    if [ "$status" == "online" ]; then
        # 在线设备：负荷率60-95%，效率85-99%，温度25-75°C
        local load_factor=$(echo "scale=2; $RANDOM/327.67 * 35 + 60" | bc)
        local efficiency=$(echo "scale=2; $RANDOM/327.67 * 14 + 85" | bc)
        local temperature=$(echo "$RANDOM % 50 + 25" | bc)
    elif [ "$status" == "offline" ]; then
        # 离线设备：负荷率0%，效率0%，温度室温
        local load_factor=0.0
        local efficiency=0.0
        local temperature=25
    else
        # 维护状态：负荷率20-50%，效率50-80%，温度25-45°C
        local load_factor=$(echo "scale=2; $RANDOM/327.67 * 30 + 20" | bc)
        local efficiency=$(echo "scale=2; $RANDOM/327.67 * 30 + 50" | bc)
        local temperature=$(echo "$RANDOM % 20 + 25" | bc)
    fi
    
    echo "INSERT INTO fluss_sink_table (device_id, device_type, location, status, load_factor, efficiency, temperature) VALUES ('$device_id', '$device_type', '$location', '$status', $load_factor, $efficiency, $temperature) ON CONFLICT (device_id) DO UPDATE SET device_type = EXCLUDED.device_type, location = EXCLUDED.location, status = EXCLUDED.status, load_factor = EXCLUDED.load_factor, efficiency = EXCLUDED.efficiency, temperature = EXCLUDED.temperature, update_time = CURRENT_TIMESTAMP;"
}

# 初始化数据库约束
echo "🔧 初始化数据库约束..."
docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
-- 添加主键约束
ALTER TABLE fluss_sink_table ADD CONSTRAINT pk_fluss_sink_table PRIMARY KEY (device_id);
" 2>/dev/null || true

# 创建索引以提高查询性能
docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
-- 创建索引
CREATE INDEX IF NOT EXISTS idx_fluss_sink_table_update_time ON fluss_sink_table(update_time);
CREATE INDEX IF NOT EXISTS idx_fluss_sink_table_status ON fluss_sink_table(status);
CREATE INDEX IF NOT EXISTS idx_fluss_sink_table_device_type ON fluss_sink_table(device_type);
CREATE INDEX IF NOT EXISTS idx_fluss_sink_table_location ON fluss_sink_table(location);
" 2>/dev/null || true

# 主循环 - 持续生成数据
COUNTER=0
while true; do
    COUNTER=$((COUNTER + 1))
    BATCH_SIZE=5
    
    echo "📊 生成第 $COUNTER 批数据 ($(date '+%Y-%m-%d %H:%M:%S'))..."
    
    # 生成一批设备数据
    SQL_BATCH=""
    for i in $(seq 1 $BATCH_SIZE); do
        DEVICE_ID="SGCC_$(printf "%04d" $((RANDOM % 9999 + 1)))"
        SQL_BATCH="$SQL_BATCH $(generate_device_data $DEVICE_ID)"
    done
    
    # 批量插入数据
    docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
    BEGIN;
    $SQL_BATCH
    COMMIT;
    " > /dev/null 2>&1
    
    # 显示当前数据库状态
    if [ $((COUNTER % 5)) -eq 0 ]; then
        echo "📈 当前数据库状态:"
        docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
        SELECT 
            '设备总数' as 指标, COUNT(*)::text as 数值
        FROM fluss_sink_table
        UNION ALL
        SELECT 
            '在线设备' as 指标, COUNT(*)::text as 数值
        FROM fluss_sink_table WHERE status = 'online'
        UNION ALL
        SELECT 
            '离线设备' as 指标, COUNT(*)::text as 数值
        FROM fluss_sink_table WHERE status = 'offline'
        UNION ALL
        SELECT 
            '维护设备' as 指标, COUNT(*)::text as 数值
        FROM fluss_sink_table WHERE status = 'maintenance'
        UNION ALL
        SELECT 
            '平均效率' as 指标, ROUND(AVG(efficiency), 2)::text || '%' as 数值
        FROM fluss_sink_table WHERE status = 'online'
        UNION ALL
        SELECT 
            '平均负荷' as 指标, ROUND(AVG(load_factor), 2)::text || '%' as 数值
        FROM fluss_sink_table WHERE status = 'online';
        " 2>/dev/null | grep -E "(指标|设备|效率|负荷)"
        
        echo "🔗 Grafana Dashboard: http://localhost:3000/d/sgcc-fluss-dashboard"
        echo "---"
    fi
    
    # 随机等待3-8秒，模拟实际数据到达间隔
    SLEEP_TIME=$((RANDOM % 6 + 3))
    sleep $SLEEP_TIME
done 