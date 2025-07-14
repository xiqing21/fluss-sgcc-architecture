#!/bin/bash

# 🚀 SGCC Fluss 一键启动全链路验证测试脚本
# 功能：环境启动 + 业务SQL执行 + 全链路数据验证 + 增删改测试 + 性能指标统计
# 作者：AI助手 & 用户协作开发
# 版本：v1.0
# 日期：$(date +%Y-%m-%d)

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 测试配置
TEST_START_TIME=$(date +%s)
TEST_REPORT_DIR="test-reports"
TEST_REPORT_FILE="$TEST_REPORT_DIR/full_test_report_$(date +%Y%m%d_%H%M%S).md"

# 创建测试报告目录
mkdir -p "$TEST_REPORT_DIR"

# 日志函数
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
    echo "[INFO] $1" >> "$TEST_REPORT_FILE"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
    echo "[SUCCESS] $1" >> "$TEST_REPORT_FILE"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
    echo "[WARNING] $1" >> "$TEST_REPORT_FILE"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
    echo "[ERROR] $1" >> "$TEST_REPORT_FILE"
}

# 性能指标统计
declare -A metrics
metrics[total_records_processed]=0
metrics[total_jobs_created]=0
metrics[total_test_scenarios]=0
metrics[successful_scenarios]=0
metrics[failed_scenarios]=0

# 初始化测试报告
init_test_report() {
    cat > "$TEST_REPORT_FILE" << EOF
# 🚀 SGCC Fluss 全链路验证测试报告

**测试开始时间**: $(date)  
**测试环境**: Docker Compose + Fluss 0.7.0 + Flink 1.20 + PostgreSQL  
**测试版本**: v1.0  

---

## 📊 测试概览

EOF
}

# 环境启动函数
start_environment() {
    log_info "🌟 步骤1: 启动测试环境"
    
    # 停止现有环境
    log_info "停止现有环境..."
    docker-compose down > /dev/null 2>&1
    
    # 清理Docker卷
    log_info "清理Docker卷..."
    docker volume prune -f > /dev/null 2>&1
    
    # 启动环境
    log_info "启动Docker Compose环境..."
    if docker-compose up -d; then
        log_success "环境启动成功"
    else
        log_error "环境启动失败"
        exit 1
    fi
    
    # 等待服务就绪
    log_info "等待服务就绪..."
    sleep 45
    
    # 验证服务状态
    log_info "验证服务状态..."
    local services=("coordinator-server-sgcc" "tablet-server-sgcc" "jobmanager-sgcc" "taskmanager-sgcc-1" "postgres-sgcc-source" "postgres-sgcc-sink")
    for service in "${services[@]}"; do
        if docker ps --format "table {{.Names}}" | grep -q "$service"; then
            log_success "✅ $service 服务正常"
        else
            log_error "❌ $service 服务异常"
        fi
    done
}

# 执行SQL脚本函数
execute_sql_script() {
    local script_file="$1"
    local description="$2"
    
    log_info "执行: $description"
    
    local start_time=$(date +%s)
    
    if docker exec -i sql-client-sgcc /opt/flink/bin/sql-client.sh embedded < "$script_file"; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        log_success "✅ $description 完成 (耗时: ${duration}秒)"
        metrics[total_jobs_created]=$((metrics[total_jobs_created] + 1))
        return 0
    else
        log_error "❌ $description 失败"
        return 1
    fi
}

# 数据验证函数
validate_data() {
    log_info "🔍 步骤2: 数据验证"
    
    # 验证PostgreSQL源数据
    log_info "验证PostgreSQL源数据..."
    local source_count=$(docker exec postgres-sgcc-source psql -U sgcc_user -d sgcc_source -t -c "SELECT COUNT(*) FROM electrical_data;" 2>/dev/null | tr -d ' ')
    if [[ "$source_count" -gt 0 ]]; then
        log_success "✅ 源数据表记录数: $source_count"
        metrics[total_records_processed]=$((metrics[total_records_processed] + source_count))
    else
        log_warning "⚠️ 源数据表无数据"
    fi
    
    # 验证Fluss各层数据
    log_info "验证Fluss数据湖各层数据..."
    
    # 创建验证SQL脚本
    cat > /tmp/validate_fluss_data.sql << 'EOF'
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

USE CATALOG fluss_catalog;

-- 验证ODS层
SELECT 'ODS层记录数' as layer, COUNT(*) as count FROM sgcc_ods.electrical_data_ods;

-- 验证DWD层
SELECT 'DWD层记录数' as layer, COUNT(*) as count FROM sgcc_dwd.electrical_data_dwd;

-- 验证DWS层
SELECT 'DWS层记录数' as layer, COUNT(*) as count FROM sgcc_dws.electrical_data_dws;

-- 验证ADS层
SELECT 'ADS层记录数' as layer, COUNT(*) as count FROM sgcc_ads.alarm_intelligence_report;

-- 验证最新数据
SELECT 'DWS最新数据' as info, device_id, avg_current, max_power, record_time 
FROM sgcc_dws.electrical_data_dws 
ORDER BY record_time DESC LIMIT 5;
EOF
    
    # 执行验证
    if execute_sql_script "/tmp/validate_fluss_data.sql" "Fluss数据湖验证"; then
        log_success "✅ Fluss数据湖验证完成"
    else
        log_error "❌ Fluss数据湖验证失败"
    fi
    
    # 验证PostgreSQL目标数据
    log_info "验证PostgreSQL目标数据..."
    local target_count=$(docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_target -t -c "SELECT COUNT(*) FROM alarm_intelligence_report;" 2>/dev/null | tr -d ' ')
    if [[ "$target_count" -gt 0 ]]; then
        log_success "✅ 目标数据表记录数: $target_count"
        metrics[total_records_processed]=$((metrics[total_records_processed] + target_count))
    else
        log_warning "⚠️ 目标数据表无数据"
    fi
}

# 业务场景测试函数
run_business_scenarios() {
    log_info "🎯 步骤3: 业务场景测试"
    
    local scenarios=(
        "business-scenarios/场景1_高频维度表服务.sql:场景1_高频维度表服务"
        "business-scenarios/场景2_智能双流JOIN.sql:场景2_智能双流JOIN"
        "business-scenarios/场景3_时间旅行查询.sql:场景3_时间旅行查询"
        "business-scenarios/场景4_柱状流优化.sql:场景4_柱状流优化"
    )
    
    for scenario in "${scenarios[@]}"; do
        local script_file="${scenario%:*}"
        local description="${scenario#*:}"
        
        metrics[total_test_scenarios]=$((metrics[total_test_scenarios] + 1))
        
        if execute_sql_script "$script_file" "$description"; then
            metrics[successful_scenarios]=$((metrics[successful_scenarios] + 1))
        else
            metrics[failed_scenarios]=$((metrics[failed_scenarios] + 1))
        fi
        
        # 场景间暂停
        sleep 10
    done
}

# 增删改测试函数
run_crud_operations() {
    log_info "🔄 步骤4: 增删改操作测试"
    
    # 创建增删改测试脚本
    cat > /tmp/crud_operations.sql << 'EOF'
-- 插入测试数据
INSERT INTO electrical_data (device_id, voltage, current, power, temperature, humidity, location, status, record_time) 
VALUES 
  ('TEST_DEVICE_001', 220.5, 15.2, 3350.0, 25.0, 60.0, 'TEST_LOCATION', 'NORMAL', NOW()),
  ('TEST_DEVICE_002', 218.3, 14.8, 3230.0, 26.5, 58.0, 'TEST_LOCATION', 'NORMAL', NOW()),
  ('TEST_DEVICE_003', 225.1, 16.1, 3625.0, 24.0, 62.0, 'TEST_LOCATION', 'NORMAL', NOW());

-- 更新测试数据
UPDATE electrical_data SET status = 'ALERT', temperature = 35.0 WHERE device_id = 'TEST_DEVICE_001';

-- 删除测试数据
DELETE FROM electrical_data WHERE device_id = 'TEST_DEVICE_003';
EOF
    
    # 执行增删改操作
    log_info "执行增删改操作..."
    if docker exec -i postgres-sgcc-source psql -U sgcc_user -d sgcc_source < /tmp/crud_operations.sql; then
        log_success "✅ 增删改操作执行成功"
        
        # 等待CDC同步
        log_info "等待CDC同步..."
        sleep 30
        
        # 验证同步结果
        log_info "验证CDC同步结果..."
        validate_data
        
    else
        log_error "❌ 增删改操作执行失败"
    fi
}

# 性能指标统计函数
calculate_metrics() {
    log_info "📈 步骤5: 性能指标统计"
    
    local test_end_time=$(date +%s)
    local total_duration=$((test_end_time - TEST_START_TIME))
    
    # 获取Flink作业信息
    log_info "获取Flink作业信息..."
    local running_jobs=$(docker exec jobmanager-sgcc /opt/flink/bin/flink list | grep -c "RUNNING" || echo "0")
    
    # 计算吞吐量
    local throughput=0
    if [[ $total_duration -gt 0 ]]; then
        throughput=$((metrics[total_records_processed] / total_duration))
    fi
    
    # 输出性能指标
    cat >> "$TEST_REPORT_FILE" << EOF

## 📈 性能指标统计

| 指标 | 数值 |
|------|------|
| 总耗时 | ${total_duration}秒 |
| 处理记录数 | ${metrics[total_records_processed]} |
| 创建作业数 | ${metrics[total_jobs_created]} |
| 当前运行作业数 | ${running_jobs} |
| 测试场景数 | ${metrics[total_test_scenarios]} |
| 成功场景数 | ${metrics[successful_scenarios]} |
| 失败场景数 | ${metrics[failed_scenarios]} |
| 数据吞吐量 | ${throughput} 记录/秒 |
| 成功率 | $(( metrics[successful_scenarios] * 100 / metrics[total_test_scenarios] ))% |

## 🔧 系统资源使用情况

EOF
    
    # 获取容器资源使用情况
    log_info "获取容器资源使用情况..."
    docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}" >> "$TEST_REPORT_FILE"
    
    # 输出到终端
    log_success "📊 测试完成统计："
    log_success "  - 总耗时: ${total_duration}秒"
    log_success "  - 处理记录数: ${metrics[total_records_processed]}"
    log_success "  - 创建作业数: ${metrics[total_jobs_created]}"
    log_success "  - 成功场景数: ${metrics[successful_scenarios]}/${metrics[total_test_scenarios]}"
    log_success "  - 数据吞吐量: ${throughput} 记录/秒"
}

# 清理函数
cleanup() {
    log_info "🧹 清理临时文件..."
    rm -f /tmp/validate_fluss_data.sql /tmp/crud_operations.sql
}

# 主函数
main() {
    echo -e "${BLUE}🚀 SGCC Fluss 一键启动全链路验证测试${NC}"
    echo -e "${BLUE}=================================================${NC}"
    
    # 初始化测试报告
    init_test_report
    
    # 执行测试流程
    start_environment
    validate_data
    run_business_scenarios
    run_crud_operations
    calculate_metrics
    cleanup
    
    # 完成信息
    log_success "🎉 全链路验证测试完成！"
    log_success "📄 详细报告: $TEST_REPORT_FILE"
    
    echo -e "${GREEN}🎉 测试完成！详细报告已保存到: $TEST_REPORT_FILE${NC}"
}

# 捕获中断信号
trap cleanup EXIT

# 执行主函数
main "$@" 