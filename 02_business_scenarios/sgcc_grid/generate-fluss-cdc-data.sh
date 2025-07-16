#!/bin/bash

# 国网智能调度大屏 - Fluss CDC数据生成器
# 数据流：PostgreSQL源 → CDC → Fluss → PostgreSQL sink
# 完全体现Fluss流批一体架构的优势

echo "🚀 开始生成Fluss CDC数据流..."
echo "数据流：PostgreSQL源 → CDC → Fluss → PostgreSQL sink"
echo "按 Ctrl+C 停止数据生成"
echo "=========================================="

# 1. 检查必要的服务
echo "🔍 检查服务状态..."
if ! docker exec postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "SELECT 1;" > /dev/null 2>&1; then
    echo "❌ PostgreSQL源数据库连接失败"
    exit 1
fi

if ! docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "SELECT 1;" > /dev/null 2>&1; then
    echo "❌ PostgreSQL sink数据库连接失败"
    exit 1
fi

echo "✅ 数据库连接正常"

# 2. 数据生成配置
GRID_REGIONS=("华北电网" "华东电网" "华南电网" "西北电网" "东北电网")
DEVICE_TYPES=("变压器" "发电机" "输电线路" "配电设备" "开关设备" "继电保护")
LOCATIONS=("北京" "上海" "广州" "深圳" "杭州" "成都" "重庆" "武汉" "西安" "南京" "天津" "苏州" "青岛" "大连" "宁波")
MANUFACTURERS=("国电南瑞" "许继电气" "平高电气" "特变电工" "中国西电" "宝光股份")
EMERGENCY_LEVELS=("NORMAL" "LOW" "MEDIUM" "HIGH" "CRITICAL")
LOAD_BALANCE_STATUSES=("BALANCED" "STRESSED" "SURPLUS" "IMBALANCED")
MAINTENANCE_STATUSES=("NORMAL" "WARNING" "MAINTENANCE" "CRITICAL")

# 3. 数据生成函数
generate_dispatch_data() {
    local dispatch_id=$1
    local grid_region=${GRID_REGIONS[$RANDOM % ${#GRID_REGIONS[@]}]}
    local load_demand=$(echo "scale=2; $RANDOM/327.67 * 20000 + 15000" | bc)
    local supply_capacity=$(echo "scale=2; $RANDOM/327.67 * 25000 + 15000" | bc)
    local emergency_level=${EMERGENCY_LEVELS[$RANDOM % ${#EMERGENCY_LEVELS[@]}]}
    local load_balance_status=${LOAD_BALANCE_STATUSES[$RANDOM % ${#LOAD_BALANCE_STATUSES[@]}]}
    local grid_frequency=$(echo "scale=3; $RANDOM/327.67 * 0.4 + 49.8" | bc)
    local voltage_stability=$(echo "scale=1; $RANDOM/327.67 * 40 + 200" | bc)
    
    echo "INSERT INTO power_dispatch_data (dispatch_id, grid_region, load_demand_mw, supply_capacity_mw, emergency_level, load_balance_status, grid_frequency_hz, voltage_stability) VALUES ('$dispatch_id', '$grid_region', $load_demand, $supply_capacity, '$emergency_level', '$load_balance_status', $grid_frequency, $voltage_stability) ON CONFLICT (dispatch_id) DO UPDATE SET grid_region = EXCLUDED.grid_region, load_demand_mw = EXCLUDED.load_demand_mw, supply_capacity_mw = EXCLUDED.supply_capacity_mw, emergency_level = EXCLUDED.emergency_level, load_balance_status = EXCLUDED.load_balance_status, grid_frequency_hz = EXCLUDED.grid_frequency_hz, voltage_stability = EXCLUDED.voltage_stability, dispatch_time = CURRENT_TIMESTAMP;"
}

generate_device_data() {
    local device_id=$1
    local device_name="设备_$device_id"
    local device_type=${DEVICE_TYPES[$RANDOM % ${#DEVICE_TYPES[@]}]}
    local location=${LOCATIONS[$RANDOM % ${#LOCATIONS[@]}]}
    local capacity_mw=$(echo "scale=2; $RANDOM/327.67 * 750 + 50" | bc)
    local manufacturer=${MANUFACTURERS[$RANDOM % ${#MANUFACTURERS[@]}]}
    local model="MODEL_$(printf "%04d" $((RANDOM % 9999 + 1)))"
    local efficiency_rate=$(echo "scale=3; $RANDOM/327.67 * 0.18 + 0.80" | bc)
    local maintenance_status=${MAINTENANCE_STATUSES[$RANDOM % ${#MAINTENANCE_STATUSES[@]}]}
    local real_time_voltage=$(echo "scale=2; $RANDOM/327.67 * 40 + 210" | bc)
    local real_time_current=$(echo "scale=2; $RANDOM/327.67 * 150 + 50" | bc)
    local real_time_temperature=$(echo "$RANDOM % 60 + 20" | bc)
    
    echo "INSERT INTO device_dimension_data (device_id, device_name, device_type, location, capacity_mw, manufacturer, model, efficiency_rate, maintenance_status, real_time_voltage, real_time_current, real_time_temperature) VALUES ('$device_id', '$device_name', '$device_type', '$location', $capacity_mw, '$manufacturer', '$model', $efficiency_rate, '$maintenance_status', $real_time_voltage, $real_time_current, $real_time_temperature) ON CONFLICT (device_id) DO UPDATE SET device_name = EXCLUDED.device_name, device_type = EXCLUDED.device_type, location = EXCLUDED.location, capacity_mw = EXCLUDED.capacity_mw, manufacturer = EXCLUDED.manufacturer, model = EXCLUDED.model, efficiency_rate = EXCLUDED.efficiency_rate, maintenance_status = EXCLUDED.maintenance_status, real_time_voltage = EXCLUDED.real_time_voltage, real_time_current = EXCLUDED.real_time_current, real_time_temperature = EXCLUDED.real_time_temperature, installation_date = CURRENT_TIMESTAMP;"
}

# 4. 主循环 - 持续生成数据到PostgreSQL源
COUNTER=0
while true; do
    COUNTER=$((COUNTER + 1))
    BATCH_SIZE=8
    
    echo "📊 生成第 $COUNTER 批数据 ($(date '+%Y-%m-%d %H:%M:%S'))..."
    
    # 生成一批电力调度数据
    DISPATCH_SQL=""
    for i in $(seq 1 $BATCH_SIZE); do
        DISPATCH_ID="DISPATCH_$(printf "%06d" $((RANDOM % 999999 + 1)))"
        DISPATCH_SQL="$DISPATCH_SQL $(generate_dispatch_data $DISPATCH_ID)"
    done
    
    # 生成一批设备维度数据
    DEVICE_SQL=""
    for i in $(seq 1 $BATCH_SIZE); do
        DEVICE_ID="DEVICE_$(printf "%06d" $((RANDOM % 999999 + 1)))"
        DEVICE_SQL="$DEVICE_SQL $(generate_device_data $DEVICE_ID)"
    done
    
    # 批量插入到PostgreSQL源数据库
    docker exec postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "
    BEGIN;
    $DISPATCH_SQL
    $DEVICE_SQL
    COMMIT;
    " > /dev/null 2>&1
    
    # 每5批显示数据库状态
    if [ $((COUNTER % 5)) -eq 0 ]; then
        echo "📈 PostgreSQL源数据库状态:"
        docker exec postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "
        SELECT 
            '电力调度数据' as 数据类型, COUNT(*)::text as 记录数
        FROM power_dispatch_data
        UNION ALL
        SELECT 
            '设备维度数据' as 数据类型, COUNT(*)::text as 记录数
        FROM device_dimension_data;
        " 2>/dev/null | grep -E "(数据类型|电力调度|设备维度)"
        
        echo "📊 PostgreSQL sink数据库状态:"
        docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
        SELECT 
            '智能电网综合报表' as 数据类型, COUNT(*)::text as 记录数
        FROM smart_grid_comprehensive_result
        UNION ALL
        SELECT 
            '设备状态汇总' as 数据类型, COUNT(*)::text as 记录数
        FROM device_status_summary
        UNION ALL
        SELECT 
            '电网监控指标' as 数据类型, COUNT(*)::text as 记录数
        FROM grid_monitoring_metrics;
        " 2>/dev/null | grep -E "(数据类型|智能电网|设备状态|电网监控)"
        
        echo "🔗 数据流状态: PostgreSQL源 → CDC → Fluss → PostgreSQL sink"
        echo "🔗 Grafana Dashboard: http://localhost:3000/d/sgcc-fluss-cdc-dashboard"
        echo "---"
    fi
    
    # 随机等待2-6秒，模拟真实数据到达间隔
    SLEEP_TIME=$((RANDOM % 5 + 2))
    sleep $SLEEP_TIME
done 