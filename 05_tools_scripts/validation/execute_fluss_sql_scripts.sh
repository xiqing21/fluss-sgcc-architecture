#!/bin/bash

# ========================================
# 国网电力监控系统 - Fluss SQL脚本批量执行器
# ========================================

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

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
    
    # 检查Docker容器状态
    if ! docker ps | grep -q "sql-client-sgcc"; then
        print_error "sql-client-sgcc容器未运行，请先执行: ./start_sgcc_fluss.sh start"
        exit 1
    fi
    
    # 检查Flink集群状态
    if ! curl -s http://localhost:8091/overview > /dev/null; then
        print_error "Flink集群未就绪，请等待服务启动"
        exit 1
    fi
    
    # 检查Fluss Coordinator状态
    if ! nc -z localhost 9123 2>/dev/null; then
        print_error "Fluss Coordinator未就绪，请等待服务启动"
        exit 1
    fi
    
    print_success "环境检查通过"
}

# 启动Lakehouse Tiering Service
start_tiering_service() {
    print_step "检查Lakehouse Tiering Service状态..."
    
    # 检查是否已有tiering任务在运行
    local tiering_jobs=$(docker exec jobmanager-sgcc flink list 2>/dev/null | grep -i "tiering\|lakehouse" | wc -l | tr -d ' ')
    
    if [ "$tiering_jobs" -gt 0 ]; then
        print_success "Lakehouse Tiering Service已在运行"
        return 0
    fi
    
    print_info "正在启动Lakehouse Tiering Service..."
    
    # 根据官方文档启动tiering服务
    docker exec jobmanager-sgcc /opt/flink/bin/flink run \
        /opt/flink/opt/fluss-flink-tiering-0.7.0.jar \
        --fluss.bootstrap.servers coordinator-server-sgcc:9123 \
        --datalake.format paimon \
        --datalake.paimon.metastore filesystem \
        --datalake.paimon.warehouse /tmp/paimon &
    
    sleep 5
    
    # 验证tiering服务是否启动成功
    local new_tiering_jobs=$(docker exec jobmanager-sgcc flink list 2>/dev/null | grep -i "tiering\|lakehouse" | wc -l | tr -d ' ')
    
    if [ "$new_tiering_jobs" -gt 0 ]; then
        print_success "Lakehouse Tiering Service启动成功"
    else
        print_warning "Lakehouse Tiering Service启动可能失败，但继续执行"
    fi
}

# 执行单个SQL文件并验证
execute_sql_file() {
    local sql_file=$1
    local step_name=$2
    
    print_step "执行 $step_name: $sql_file"
    
    if [ ! -f "./fluss/sql/$sql_file" ]; then
        print_error "SQL文件不存在: ./fluss/sql/$sql_file"
        return 1
    fi
    
    print_info "开始执行SQL脚本..."
    
    # 使用交互式执行，获取详细输出
    local sql_output=$(docker exec sql-client-sgcc bash -c "
        timeout 120s ./sql-client -f /opt/sql/$sql_file 2>&1
    ")
    
    local exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        print_success "$step_name 执行完成"
        
        # 检查输出中是否有错误
        if echo "$sql_output" | grep -qi "error\|exception\|failed"; then
            print_warning "SQL执行可能有问题，输出包含错误信息："
            echo "$sql_output" | grep -i "error\|exception\|failed" | head -3
        fi
        
        # 等待一段时间让作业完全启动
        sleep 8
        
        # 验证作业是否成功启动
        local job_count=$(docker exec jobmanager-sgcc flink list 2>/dev/null | grep -c "RUNNING" || echo "0")
        print_info "当前运行的作业数量: $job_count"
        
        return 0
    elif [ $exit_code -eq 124 ]; then
        print_error "$step_name 执行超时"
        return 1
    else
        print_error "$step_name 执行失败 (退出代码: $exit_code)"
        
        # 显示错误详情
        if [ -n "$sql_output" ]; then
            print_info "错误详情："
            echo "$sql_output" | tail -10
        fi
        
        return 1
    fi
}

# 批量执行所有SQL脚本
execute_all_sql_scripts() {
    print_header "开始执行所有Fluss SQL脚本"
    
    # 定义SQL文件执行顺序
    declare -a sql_files=(
        "1_cdc_source_to_fluss.sql:第一阶段-CDC数据捕获到Fluss"
        "2_fluss_dwd_layer.sql:第二阶段-DWD数据清洗"
        "3_fluss_dws_layer.sql:第三阶段-DWS数据汇总"
        "4_fluss_ads_layer.sql:第四阶段-ADS应用服务"
        "5_sink_to_postgres.sql:第五阶段-写入PostgreSQL"
    )
    
    local total_steps=${#sql_files[@]}
    local current_step=1
    local failed_count=0
    
    # 执行每个SQL文件
    for item in "${sql_files[@]}"; do
        local sql_file="${item%%:*}"
        local step_name="${item##*:}"
        
        print_info "进度: $current_step/$total_steps"
        
        if execute_sql_file "$sql_file" "$step_name"; then
            print_success "✅ $step_name 完成"
        else
            print_error "❌ $step_name 失败"
            failed_count=$((failed_count + 1))
            
            # 显示详细错误信息
            print_info "检查最近的Flink任务状态："
            docker exec jobmanager-sgcc flink list 2>/dev/null | head -10
            
            # 不退出，继续执行下一个
            print_info "继续执行下一个脚本..."
        fi
        
        current_step=$((current_step + 1))
        
        # 在步骤之间加入短暂延迟
        if [ $current_step -le $total_steps ]; then
            sleep 5
        fi
    done
    
    if [ $failed_count -eq 0 ]; then
        print_success "🎉 所有Fluss SQL脚本执行完成！"
        return 0
    else
        print_warning "⚠️ 有 $failed_count 个脚本执行失败，但其他脚本已完成"
        return 1
    fi
}

# 验证执行结果
verify_execution() {
    print_step "验证执行结果..."
    
    # 检查Flink作业状态
    local job_count=$(docker exec jobmanager-sgcc flink list 2>/dev/null | grep -c "RUNNING" || echo "0")
    print_info "运行中的Flink作业数量: $job_count"
    
    if [ "$job_count" -gt 1 ]; then
        print_success "✅ 有多个作业在运行"
        
        # 显示作业详情
        print_info "运行中的作业："
        docker exec jobmanager-sgcc flink list 2>/dev/null | grep "RUNNING"
        
    else
        print_warning "⚠️ 作业数量较少，可能需要检查"
    fi
    
    # 检查PostgreSQL目标数据库
    print_info "检查PostgreSQL目标数据库..."
    local target_tables=$(docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
    SELECT COUNT(*) FROM information_schema.tables 
    WHERE table_schema = 'sgcc_dw';
    " 2>/dev/null | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | tr -d ' ')
    
    print_info "目标数据库表数量: ${target_tables:-0}"
    
    # 显示集群资源使用情况
    print_info "集群资源使用情况:"
    local cluster_info=$(curl -s http://localhost:8091/overview 2>/dev/null)
    local slots_total=$(echo "$cluster_info" | grep -o '"slots-total":[0-9]*' | grep -o '[0-9]*')
    local slots_available=$(echo "$cluster_info" | grep -o '"slots-available":[0-9]*' | grep -o '[0-9]*')
    
    echo "  • 总Task Slots: ${slots_total:-0}"
    echo "  • 可用Task Slots: ${slots_available:-0}"
    if [ ! -z "$slots_total" ] && [ ! -z "$slots_available" ] && [ "$slots_total" -gt 0 ]; then
        echo "  • 使用率: $((100 - slots_available * 100 / slots_total))%"
    fi
}

# 测试Fluss连接
test_fluss_connection() {
    print_step "测试Fluss连接..."
    
    # 测试catalog连接
    local catalog_test=$(docker exec sql-client-sgcc timeout 30s ./sql-client -e "
    CREATE CATALOG fluss_catalog WITH (
        'type' = 'fluss',
        'bootstrap.servers' = 'coordinator-server-sgcc:9123'
    );
    USE CATALOG fluss_catalog;
    SHOW TABLES;
    " 2>/dev/null)
    
    if echo "$catalog_test" | grep -q "fluss_ods"; then
        print_success "✅ Fluss catalog连接成功"
        print_info "发现的Fluss表:"
        echo "$catalog_test" | grep "fluss_" | head -5
    else
        print_warning "⚠️ Fluss catalog连接可能有问题"
        print_info "尝试手动连接测试:"
        print_info "  docker exec -it sql-client-sgcc ./sql-client"
    fi
}

# 主函数
main() {
    print_header "国网电力监控系统 - Fluss SQL脚本批量执行器"
    
    # 检查环境
    check_environment
    
    # 启动Lakehouse Tiering Service
    start_tiering_service
    
    # 测试Fluss连接
    test_fluss_connection
    
    # 执行SQL脚本
    if execute_all_sql_scripts; then
        print_success "🎉 所有Fluss SQL脚本执行成功！"
    else
        print_warning "⚠️ 部分SQL脚本执行失败，请检查详细日志"
    fi
    
    # 验证执行结果
    verify_execution
    
    echo ""
    print_info "接下来可以运行实时验证测试："
    print_info "  ./scripts/fluss_realtime_validation_test.sh"
    
    print_info "或者手动测试SQL连接："
    print_info "  docker exec -it sql-client-sgcc ./sql-client"
    
    print_header "执行完成"
}

# 运行主函数
main "$@" 