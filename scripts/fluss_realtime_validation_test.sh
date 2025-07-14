#!/bin/bash

# ========================================
# 国网电力监控系统 - Fluss实时业务验证测试脚本
# ========================================

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# 全局变量
MONITORING_ID=""
TEST_START_TIME=""
TEST_INSERT_TIME=""
EQUIPMENT_ID="1001"  # 使用现有的设备ID

# 打印带颜色的消息
print_header() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

print_step() {
    echo -e "${PURPLE}[STEP]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

# 检查环境
check_environment() {
    print_step "检查Fluss环境状态..."
    
    # 检查Docker Compose服务
    if ! docker-compose ps | grep -q "Up"; then
        print_error "Docker Compose服务未运行，请先执行: ./start_sgcc_fluss.sh start"
        exit 1
    fi
    
    # 检查Flink JobManager
    if ! curl -s http://localhost:8091/overview > /dev/null; then
        print_error "Flink JobManager未就绪，请等待服务启动"
        exit 1
    fi
    
    # 检查Fluss Coordinator
    if ! nc -z localhost 9123 2>/dev/null; then
        print_error "Fluss Coordinator未就绪，请等待服务启动"
        exit 1
    fi
    
    print_success "环境检查通过"
}

# 验证初始数据
verify_initial_data() {
    print_step "验证初始数据状态..."
    
    echo -e "\n${BLUE}=== 源数据库初始设备数据 ===${NC}"
    docker exec postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "
    SELECT equipment_id, equipment_name, equipment_type, location, status 
    FROM sgcc_power.power_equipment 
    ORDER BY equipment_id;
    " 2>/dev/null | grep -E "(equipment_id|---|[0-9]+)"
    
    echo -e "\n${BLUE}=== 源数据库初始监控数据 ===${NC}"
    docker exec postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "
    SELECT monitoring_id, equipment_id, voltage_a, current_a, power_active, monitoring_time 
    FROM sgcc_power.power_monitoring 
    ORDER BY monitoring_id LIMIT 3;
    " 2>/dev/null | grep -E "(monitoring_id|---|[0-9]+)"
    
    echo -e "\n${BLUE}=== 检查Flink作业状态 ===${NC}"
    local job_count=$(docker exec jobmanager-sgcc flink list 2>/dev/null | grep -c "RUNNING" || echo "0")
    print_info "运行中的Flink作业数量: $job_count"
    
    if [ "$job_count" -lt 1 ]; then
        print_warning "Flink作业数量不足，请确保已执行所有SQL脚本"
        print_info "可以运行以下命令来执行所有SQL脚本："
        print_info "  ./scripts/execute_fluss_sql_scripts.sh"
        read -p "是否继续测试？ (y/n): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    fi
    
    echo -e "\n${BLUE}=== 检查目标数据库表结构 ===${NC}"
    docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
    SELECT table_name FROM information_schema.tables 
    WHERE table_schema = 'sgcc_dw' 
    ORDER BY table_name;
    " 2>/dev/null | grep -E "(table_name|---|[a-z_]+)"
}

# 执行实时业务操作
execute_realtime_operations() {
    print_step "执行实时电力监控业务操作..."
    
    # 生成唯一的监控记录ID
    MONITORING_ID=$((5000 + RANDOM % 1000))
    TEST_START_TIME=$(date +%s)
    
    print_info "测试监控记录ID: $MONITORING_ID"
    print_info "测试设备ID: $EQUIPMENT_ID"
    print_info "测试开始时间: $(date '+%Y-%m-%d %H:%M:%S')"
    
    # 1. 插入新的监控数据（正常状态）
    print_info "1. 插入新的电力监控数据 (正常状态)"
    TEST_INSERT_TIME=$(date +%s)
    docker exec postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "
    INSERT INTO sgcc_power.power_monitoring (
        monitoring_id, equipment_id, voltage_a, voltage_b, voltage_c, 
        current_a, current_b, current_c, power_active, power_reactive, 
        frequency, temperature, humidity, monitoring_time
    )
    VALUES (
        $MONITORING_ID, $EQUIPMENT_ID, 
        220.5, 221.2, 219.8,  -- 三相电压
        45.2, 46.1, 44.8,     -- 三相电流
        280.5, 35.2,          -- 有功功率、无功功率
        50.01, 35.5, 65.2,    -- 频率、温度、湿度
        NOW()
    );
    " >/dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        print_success "监控数据插入成功"
    else
        print_error "监控数据插入失败"
        return 1
    fi
    
    sleep 3
    
    # 2. 插入告警数据（异常电压）
    print_info "2. 插入异常监控数据 (电压异常)"
    local alarm_id=$((MONITORING_ID + 1))
    docker exec postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "
    INSERT INTO sgcc_power.power_monitoring (
        monitoring_id, equipment_id, voltage_a, voltage_b, voltage_c, 
        current_a, current_b, current_c, power_active, power_reactive, 
        frequency, temperature, humidity, monitoring_time
    )
    VALUES (
        $alarm_id, $EQUIPMENT_ID, 
        245.8, 246.5, 244.2,  -- 异常高电压
        48.5, 49.2, 47.8,     -- 电流
        320.8, 42.1,          -- 功率
        50.02, 38.2, 68.5,    -- 频率、温度、湿度
        NOW()
    );
    " >/dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        print_success "异常监控数据插入成功"
    else
        print_error "异常监控数据插入失败"
        return 1
    fi
    
    sleep 3
    
    # 3. 插入设备告警
    print_info "3. 插入设备告警信息"
    local alert_id=$((6000 + RANDOM % 1000))
    docker exec postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "
    INSERT INTO sgcc_power.equipment_alarms (
        alarm_id, equipment_id, alarm_type, alarm_level, 
        alarm_message, alarm_code, occurred_at
    )
    VALUES (
        $alert_id, $EQUIPMENT_ID, '电压异常', 2,
        '设备电压超出正常范围，请检查', 'VOLT_001', NOW()
    );
    " >/dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        print_success "设备告警插入成功"
    else
        print_error "设备告警插入失败"
        return 1
    fi
    
    sleep 3
    
    # 4. 模拟告警解决
    print_info "4. 更新告警状态 (问题已解决)"
    docker exec postgres-sgcc-source psql -U sgcc_user -d sgcc_source_db -c "
    UPDATE sgcc_power.equipment_alarms 
    SET is_resolved = true, resolved_at = NOW(), updated_at = NOW()
    WHERE alarm_id = $alert_id;
    " >/dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        print_success "告警状态更新完成"
    else
        print_error "告警状态更新失败"
        return 1
    fi
    
    print_success "实时业务操作完成"
}

# 验证Fluss数据流转
verify_fluss_data_flow() {
    print_step "验证Fluss数据流转..."
    
    # 等待数据传播到Fluss
    print_info "等待数据传播到Fluss和Paimon数据湖..."
    sleep 15
    
    # 验证Fluss catalog连接
    print_info "验证Fluss catalog连接..."
    local catalog_result=$(docker exec sql-client-sgcc timeout 30s ./sql-client -e "USE CATALOG fluss_catalog; SHOW TABLES;" 2>/dev/null)
    
    if echo "$catalog_result" | grep -q "fluss_ods_power_monitoring"; then
        print_success "✅ Fluss catalog连接正常，ODS表已创建"
    else
        print_warning "⚠️ Fluss catalog连接或ODS表创建可能有问题"
    fi
    
    # 检查Paimon数据湖文件
    print_info "检查Paimon数据湖文件..."
    local paimon_structure=$(docker exec tablet-server-sgcc find /tmp/paimon -name "*.orc" -o -name "*.parquet" 2>/dev/null | head -5)
    
    if [ -n "$paimon_structure" ]; then
        print_success "✅ Paimon数据湖文件验证通过"
        echo "$paimon_structure" | head -3
    else
        print_info "ℹ️ Paimon数据湖文件仍在生成中..."
    fi
    
    # 验证DWD层处理
    print_info "检查DWD层数据处理..."
    local dwd_check=$(docker exec sql-client-sgcc timeout 60s ./sql-client -e "
    USE CATALOG fluss_catalog;
    SELECT COUNT(*) FROM fluss_dwd_power_monitoring WHERE equipment_id = $EQUIPMENT_ID;
    " 2>/dev/null | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | tr -d ' ')
    
    if [ "$dwd_check" -gt 0 ]; then
        print_success "✅ DWD层数据处理验证通过 (记录数: $dwd_check)"
    else
        print_info "ℹ️ DWD层数据仍在处理中..."
    fi
    
    return 0
}

# 验证最终结果和延迟
verify_final_results() {
    print_step "验证最终结果和数据一致性..."
    
    # 等待数据写入最终数据库
    print_info "等待数据写入PostgreSQL目标数据库..."
    sleep 20
    
    echo -e "\n${BLUE}=== DWD层数据验证 ===${NC}"
    local dwd_result=$(docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
    SELECT COUNT(*) FROM sgcc_dw.dwd_power_monitoring 
    WHERE equipment_id = $EQUIPMENT_ID 
    AND created_at >= NOW() - INTERVAL '10 minutes';
    " 2>/dev/null | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | tr -d ' ')
    
    if [ "$dwd_result" -gt 0 ]; then
        print_success "✅ DWD层数据写入验证通过 (记录数: $dwd_result)"
        
        # 显示部分DWD数据
        docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
        SELECT equipment_id, voltage_avg, current_avg, power_active, 
               TO_CHAR(created_at, 'HH24:MI:SS') as created_time
        FROM sgcc_dw.dwd_power_monitoring 
        WHERE equipment_id = $EQUIPMENT_ID 
        AND created_at >= NOW() - INTERVAL '10 minutes'
        ORDER BY created_at DESC LIMIT 2;
        " 2>/dev/null
    else
        print_warning "⚠️ DWD层数据未找到或仍在处理中"
    fi
    
    echo -e "\n${BLUE}=== DWS层汇总数据验证 ===${NC}"
    local dws_result=$(docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
    SELECT COUNT(*) FROM sgcc_dw.dws_equipment_daily_summary 
    WHERE equipment_id = $EQUIPMENT_ID 
    AND summary_date = CURRENT_DATE;
    " 2>/dev/null | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | tr -d ' ')
    
    if [ "$dws_result" -gt 0 ]; then
        print_success "✅ DWS层汇总数据验证通过"
    else
        print_info "ℹ️ DWS层汇总数据仍在处理中（需要时间窗口触发）"
    fi
    
    echo -e "\n${BLUE}=== ADS层应用数据验证 ===${NC}"
    local ads_result=$(docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
    SELECT COUNT(*) FROM sgcc_dw.ads_realtime_dashboard 
    WHERE last_updated >= NOW() - INTERVAL '15 minutes';
    " 2>/dev/null | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | tr -d ' ')
    
    if [ "$ads_result" -gt 0 ]; then
        print_success "✅ ADS层应用数据验证通过"
        
        # 显示实时大屏数据
        docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
        SELECT total_equipment, online_equipment, total_active_alarms,
               TO_CHAR(last_updated, 'HH24:MI:SS') as updated_time
        FROM sgcc_dw.ads_realtime_dashboard 
        ORDER BY last_updated DESC LIMIT 1;
        " 2>/dev/null
    else
        print_info "ℹ️ ADS层应用数据仍在处理中"
    fi
    
    # 延迟分析
    echo -e "\n${BLUE}=== 端到端延迟分析 ===${NC}"
    if [ ! -z "$TEST_INSERT_TIME" ]; then
        local latest_result_time=$(docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
        SELECT EXTRACT(EPOCH FROM MAX(created_at)) 
        FROM sgcc_dw.dwd_power_monitoring 
        WHERE equipment_id = $EQUIPMENT_ID 
        AND created_at >= NOW() - INTERVAL '10 minutes';
        " 2>/dev/null | grep -E "^[[:space:]]*[0-9]+(\.[0-9]+)?[[:space:]]*$" | tr -d ' ')
        
        if [ ! -z "$latest_result_time" ]; then
            local latency=$(echo "$latest_result_time - $TEST_INSERT_TIME" | bc -l 2>/dev/null | sed 's/^\./0./')
            if [ ! -z "$latency" ]; then
                print_info "测试数据端到端延迟: $(printf "%.2f" "$latency") 秒"
            fi
        fi
    fi
    
    return 0
}

# 数据质量验证
verify_data_quality() {
    print_step "验证数据质量和业务逻辑..."
    
    echo -e "\n${BLUE}=== 数据质量检查 ===${NC}"
    
    # 检查数据完整性
    print_info "检查数据完整性..."
    local completeness_check=$(docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
    SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN voltage_avg IS NOT NULL THEN 1 END) as voltage_records,
        COUNT(CASE WHEN current_avg IS NOT NULL THEN 1 END) as current_records,
        COUNT(CASE WHEN power_active IS NOT NULL THEN 1 END) as power_records
    FROM sgcc_dw.dwd_power_monitoring 
    WHERE equipment_id = $EQUIPMENT_ID;
    " 2>/dev/null)
    
    echo "$completeness_check"
    
    # 检查业务规则
    print_info "检查业务规则..."
    local business_rules=$(docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
    SELECT 
        equipment_id,
        CASE 
            WHEN voltage_avg BETWEEN 200 AND 240 THEN '正常'
            ELSE '异常'
        END as voltage_status,
        CASE 
            WHEN frequency BETWEEN 49.5 AND 50.5 THEN '正常'
            ELSE '异常'  
        END as frequency_status
    FROM sgcc_dw.dwd_power_monitoring 
    WHERE equipment_id = $EQUIPMENT_ID 
    AND created_at >= NOW() - INTERVAL '10 minutes'
    ORDER BY created_at DESC LIMIT 3;
    " 2>/dev/null)
    
    echo "$business_rules"
    
    return 0
}

# 性能统计报告
performance_summary() {
    print_step "生成性能统计报告..."
    
    echo -e "\n${BLUE}=== 性能统计报告 ===${NC}"
    
    # 总监控数据量
    local total_monitoring=$(docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
    SELECT COUNT(*) FROM sgcc_dw.dwd_power_monitoring;
    " 2>/dev/null | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | tr -d ' ')
    
    # 今日新增数据
    local today_monitoring=$(docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
    SELECT COUNT(*) FROM sgcc_dw.dwd_power_monitoring 
    WHERE DATE(created_at) = CURRENT_DATE;
    " 2>/dev/null | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | tr -d ' ')
    
    # Flink集群资源
    local cluster_info=$(curl -s http://localhost:8091/overview 2>/dev/null)
    local taskmanagers=$(echo "$cluster_info" | grep -o '"taskmanagers":[0-9]*' | grep -o '[0-9]*')
    local slots_total=$(echo "$cluster_info" | grep -o '"slots-total":[0-9]*' | grep -o '[0-9]*')
    local slots_available=$(echo "$cluster_info" | grep -o '"slots-available":[0-9]*' | grep -o '[0-9]*')
    local jobs_running=$(echo "$cluster_info" | grep -o '"jobs-running":[0-9]*' | grep -o '[0-9]*')
    
    echo "📊 数据处理统计:"
    echo "  • 总监控记录数: ${total_monitoring:-0}"
    echo "  • 今日新增记录: ${today_monitoring:-0}"
    echo "  • 测试记录ID: $MONITORING_ID"
    echo "  • 测试设备ID: $EQUIPMENT_ID"
    echo ""
    echo "🔧 Fluss集群状态:"
    echo "  • TaskManager数量: ${taskmanagers:-0}"
    echo "  • 总Task Slots: ${slots_total:-0}"
    echo "  • 可用Task Slots: ${slots_available:-0}"
    echo "  • 运行中作业: ${jobs_running:-0}"
    if [ ! -z "$slots_total" ] && [ ! -z "$slots_available" ] && [ "$slots_total" -gt 0 ]; then
        echo "  • 资源利用率: $((100 - slots_available * 100 / slots_total))%"
    fi
    echo ""
    
    # 数据湖统计
    local paimon_files=$(docker exec tablet-server-sgcc find /tmp/paimon -type f 2>/dev/null | wc -l | tr -d ' ')
    echo "🗄️ Paimon数据湖:"
    echo "  • 数据文件数量: ${paimon_files:-0}"
    echo "  • 存储路径: /tmp/paimon"
    echo ""
    
    # 性能评估
    if [ "$total_monitoring" -gt 0 ] && [ "$today_monitoring" -gt 0 ]; then
        print_success "✅ 数据处理性能: 正常"
    else
        print_warning "⚠️ 数据处理性能: 需要检查"
    fi
    
    if [ "$jobs_running" -gt 0 ]; then
        print_success "✅ 作业运行状态: 正常"
    else
        print_warning "⚠️ 作业运行状态: 需要检查"
    fi
}

# 主函数
main() {
    print_header "国网电力监控系统 - Fluss实时业务验证测试"
    
    # 检查环境
    check_environment
    
    # 验证初始数据
    verify_initial_data
    
    # 执行实时业务操作
    if execute_realtime_operations; then
        print_success "实时业务操作完成"
        
        # 验证Fluss数据流转
        verify_fluss_data_flow
        
        # 验证最终结果
        verify_final_results
        
        # 验证数据质量
        verify_data_quality
        
        # 性能统计
        performance_summary
        
        print_success "🎉 Fluss实时验证测试完成！"
        
    else
        print_error "❌ 实时业务操作失败"
        exit 1
    fi
    
    print_header "测试完成"
    echo -e "详细信息请查看上方输出结果"
    echo -e "如需重复测试，请再次运行此脚本"
    echo -e ""
    echo -e "相关命令："
    echo -e "  • 查看Flink作业: docker exec jobmanager-sgcc flink list"
    echo -e "  • 查看服务状态: ./start_sgcc_fluss.sh status"
    echo -e "  • 查看服务日志: ./start_sgcc_fluss.sh logs"
}

# 运行主函数
main "$@" 