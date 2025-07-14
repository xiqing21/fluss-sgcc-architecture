#!/bin/bash

# ========================================
# 国网电力监控系统 - Fluss架构一键启动脚本
# ========================================

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_header() {
    echo -e "${PURPLE}$1${NC}"
}

# 检查先决条件
check_prerequisites() {
    log_header "========================================="
    log_header "🔍 检查系统先决条件"
    log_header "========================================="
    
    # 检查Docker
    if ! command -v docker &> /dev/null; then
        log_error "Docker未安装，请先安装Docker"
        exit 1
    fi
    log_success "Docker已安装: $(docker --version)"
    
    # 检查Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose未安装，请先安装Docker Compose"
        exit 1
    fi
    log_success "Docker Compose已安装: $(docker-compose --version)"
    
    # 检查端口占用
    local ports=(5442 5443 2181 9123 9124 8091)
    for port in "${ports[@]}"; do
        if lsof -i :$port &> /dev/null; then
            log_warning "端口 $port 被占用，可能会导致冲突"
        fi
    done
    
    log_success "先决条件检查完成"
    echo
}

# 启动服务
start_services() {
    log_header "========================================="
    log_header "🚀 启动Fluss架构服务"
    log_header "========================================="
    
    log_info "清理可能存在的旧容器..."
    docker-compose down -v 2>/dev/null || true
    
    log_info "启动所有服务..."
    docker-compose up -d
    
    if [ $? -eq 0 ]; then
        log_success "所有服务启动成功"
    else
        log_error "服务启动失败"
        exit 1
    fi
    echo
}

# 等待服务就绪
wait_for_services() {
    log_header "========================================="
    log_header "⏳ 等待服务就绪"
    log_header "========================================="
    
    log_info "等待PostgreSQL数据库初始化..."
    sleep 10
    
    log_info "等待Fluss集群启动..."
    sleep 20
    
    log_info "等待Flink集群就绪..."
    sleep 30
    
    log_success "服务等待完成"
    echo
}

# 运行验证测试
run_validation() {
    log_header "========================================="
    log_header "🧪 运行系统验证测试"
    log_header "========================================="
    
    if [ -f "./scripts/sgcc_validation_test.sh" ]; then
        ./scripts/sgcc_validation_test.sh
    else
        log_warning "验证脚本不存在，跳过验证"
    fi
    echo
}

# 显示访问信息
show_access_info() {
    log_header "========================================="
    log_header "🌐 服务访问信息"
    log_header "========================================="
    
    echo -e "${GREEN}Web界面:${NC}"
    echo "  🎛️  Flink Dashboard: http://localhost:8091"
    echo
    
    echo -e "${GREEN}数据库连接:${NC}"
    echo "  📊 PostgreSQL源数据库: localhost:5442 (sgcc_source_db)"
    echo "  🏗️  PostgreSQL目标数据库: localhost:5443 (sgcc_dw_db)"
    echo "  👤 用户名/密码: sgcc_user/sgcc_pass_2024"
    echo
    
    echo -e "${GREEN}Fluss服务:${NC}"
    echo "  🎯 Fluss Coordinator: localhost:9123"
    echo "  💾 Fluss Tablet Server: localhost:9124"
    echo "  🗄️  ZooKeeper: localhost:2181"
    echo
    
    echo -e "${GREEN}下一步操作:${NC}"
    echo "  1️⃣  连接Flink SQL客户端:"
    echo "     docker exec -it sql-client-sgcc ./sql-client"
    echo
    echo "  2️⃣  执行Fluss SQL脚本:"
    echo "     -- 在Flink SQL客户端中依次执行 /opt/sql/ 目录下的SQL文件"
    echo
    echo "  3️⃣  重新验证数据流:"
    echo "     ./scripts/sgcc_validation_test.sh"
    echo
    
    log_header "========================================="
    log_header "🎉 国网电力监控系统 - Fluss架构启动完成！"
    log_header "========================================="
}

# 清理函数
cleanup() {
    if [ "$1" == "stop" ]; then
        log_header "========================================="
        log_header "🛑 停止Fluss架构服务"
        log_header "========================================="
        
        docker-compose down
        log_success "所有服务已停止"
    elif [ "$1" == "clean" ]; then
        log_header "========================================="
        log_header "🧹 清理Fluss架构环境"
        log_header "========================================="
        
        docker-compose down -v --remove-orphans
        docker system prune -f
        log_success "环境清理完成"
    fi
}

# 主函数
main() {
    local command=${1:-start}
    
    case $command in
        "start")
            check_prerequisites
            start_services
            wait_for_services
            run_validation
            show_access_info
            ;;
        "stop")
            cleanup "stop"
            ;;
        "clean")
            cleanup "clean"
            ;;
        "restart")
            cleanup "stop"
            sleep 5
            main "start"
            ;;
        "status")
            log_header "📊 服务状态检查"
            docker-compose ps
            ;;
        "logs")
            log_header "📜 服务日志"
            docker-compose logs -f
            ;;
        "help"|*)
            echo "用法: $0 [command]"
            echo
            echo "命令:"
            echo "  start   - 启动Fluss架构 (默认)"
            echo "  stop    - 停止所有服务"
            echo "  clean   - 清理环境和数据"
            echo "  restart - 重启服务"
            echo "  status  - 查看服务状态"
            echo "  logs    - 查看服务日志"
            echo "  help    - 显示帮助信息"
            echo
            echo "示例:"
            echo "  $0 start     # 启动服务"
            echo "  $0 stop      # 停止服务"
            echo "  $0 clean     # 完全清理"
            ;;
    esac
}

# 执行主函数
main "$@" 