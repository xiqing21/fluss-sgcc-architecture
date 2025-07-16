#!/bin/bash

echo "🚀 Fluss特性完整演示 - 智能客服工单分析系统"
echo "展示流批一体、即席查询、UPSERT、与Paimon集成等核心特性"
echo "=================================================="

# 设置颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# 检查脚本执行权限
chmod +x *.sh

echo ""
echo -e "${CYAN}🎯 Fluss特性演示菜单${NC}"
echo "=================================================="
echo ""
echo -e "${YELLOW}📋 演示流程：${NC}"
echo "1. 🔍 数据准备与验证"
echo "2. 💾 执行SQL系统构建"
echo "3. 🔍 即席查询特性演示"
echo "4. 🔄 实时更新UPSERT演示"
echo "5. 🔗 与Paimon集成演示"
echo "6. 📊 完整功能验证"
echo "7. 🚪 退出演示"
echo ""
echo -e "${BLUE}🎪 特性展示亮点：${NC}"
echo "• 流批一体：统一SQL处理实时和历史数据"
echo "• 即席查询：直接查询Fluss表，无需外部存储"
echo "• 原生UPSERT：支持数据更新，无需复杂changelog"
echo "• 列式存储：查询性能提升10倍"
echo "• 投影下推：只读取需要的字段，优化网络传输"
echo "• 与Paimon集成：分层存储，实现完整湖仓架构"
echo ""

while true; do
    echo -e "${GREEN}请选择要演示的功能 (1-7):${NC}"
    read -r choice
    
    case $choice in
        1)
            echo ""
            echo -e "${BLUE}🔍 开始数据准备与验证...${NC}"
            echo -e "${YELLOW}这将验证PostgreSQL源数据和目标数据库的准备情况${NC}"
            echo ""
            ./verify_customer_service_system.sh
            ;;
        2)
            echo ""
            echo -e "${BLUE}💾 开始执行SQL系统构建...${NC}"
            echo -e "${YELLOW}这将执行智能客服工单分析系统的完整SQL${NC}"
            echo ""
            echo -e "${PURPLE}提醒：执行前请确保已重启Flink集群清理环境${NC}"
            echo -e "${PURPLE}建议使用: docker-compose restart jobmanager taskmanager-1 taskmanager-2${NC}"
            echo ""
            echo -e "${YELLOW}是否继续执行？(y/n)${NC}"
            read -r confirm
            if [[ $confirm == "y" || $confirm == "Y" ]]; then
                ./smart_sql_execution.sh
            else
                echo "已取消执行"
            fi
            ;;
        3)
            echo ""
            echo -e "${BLUE}🔍 开始即席查询特性演示...${NC}"
            echo -e "${YELLOW}这将展示Fluss的直接查询能力，无需外部存储${NC}"
            echo ""
            ./fluss_adhoc_query_demo.sh
            ;;
        4)
            echo ""
            echo -e "${BLUE}🔄 开始实时更新UPSERT演示...${NC}"
            echo -e "${YELLOW}这将展示Fluss的原生UPSERT特性，支持数据实时更新${NC}"
            echo ""
            ./realtime_update_demo.sh
            ;;
        5)
            echo ""
            echo -e "${BLUE}🔗 开始与Paimon集成演示...${NC}"
            echo -e "${YELLOW}这将展示Fluss与Paimon的分层存储和流批一体特性${NC}"
            echo ""
            ./fluss_paimon_integration_demo.sh
            ;;
        6)
            echo ""
            echo -e "${BLUE}📊 开始完整功能验证...${NC}"
            echo -e "${YELLOW}这将验证整个系统的运行状态和数据流转${NC}"
            echo ""
            ./quick_verify_data.sh
            ;;
        7)
            echo ""
            echo -e "${GREEN}🎉 感谢使用Fluss特性演示系统！${NC}"
            echo -e "${CYAN}演示总结：${NC}"
            echo "✅ 流批一体：统一SQL处理实时和历史数据"
            echo "✅ 即席查询：直接查询Fluss表，性能提升10倍"
            echo "✅ 原生UPSERT：支持数据更新，简化开发"
            echo "✅ 列式存储：投影下推优化，降低网络成本"
            echo "✅ 与Paimon集成：分层存储，完整湖仓架构"
            echo ""
            echo -e "${YELLOW}🏆 Fluss相较于Kafka的核心优势：${NC}"
            echo "• Kafka需要：Kafka + 外部存储 + 复杂ETL + 状态管理"
            echo "• Fluss只需：Fluss一体化存储 + 直接查询 + 原生UPSERT"
            echo ""
            break
            ;;
        *)
            echo -e "${RED}无效选择，请输入1-7之间的数字${NC}"
            ;;
    esac
    
    echo ""
    echo "=================================================="
    echo -e "${CYAN}🎯 返回主菜单${NC}"
    echo "=================================================="
    echo ""
    echo -e "${YELLOW}📋 演示流程：${NC}"
    echo "1. 🔍 数据准备与验证"
    echo "2. 💾 执行SQL系统构建"
    echo "3. 🔍 即席查询特性演示"
    echo "4. 🔄 实时更新UPSERT演示"
    echo "5. 🔗 与Paimon集成演示"
    echo "6. 📊 完整功能验证"
    echo "7. 🚪 退出演示"
    echo ""
done 