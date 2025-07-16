#!/bin/bash

# 国网智能调度大屏 - Grafana监控栈一键部署脚本
# Powered by Fluss流批一体架构

echo "🔋 开始部署国网智能调度大屏 - Grafana监控栈..."
echo "=========================================="

# 检查Docker是否运行
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker 未运行或未安装，请先启动Docker"
    exit 1
fi

# 检查Docker Compose是否安装
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose 未安装，请先安装Docker Compose"
    exit 1
fi

# 设置项目目录
PROJECT_DIR="business-scenarios"
cd "${PROJECT_DIR}" || exit 1

echo "📁 当前工作目录: $(pwd)"

# 创建必要的目录
echo "📂 创建配置目录..."
mkdir -p grafana/dashboards/system

# 检查配置文件是否存在
echo "🔍 检查配置文件..."
REQUIRED_FILES=(
    "grafana-stack-deploy.yml"
    "grafana/provisioning/datasources/datasources.yml"
    "grafana/provisioning/dashboards/dashboards.yml"
    "grafana/dashboards/国网智能调度大屏.json"
    "prometheus/prometheus.yml"
)

for file in "${REQUIRED_FILES[@]}"; do
    if [[ ! -f "$file" ]]; then
        echo "❌ 缺少配置文件: $file"
        exit 1
    fi
    echo "✅ 找到配置文件: $file"
done

# 停止可能存在的旧容器
echo "🛑 停止旧的容器..."
docker-compose -f grafana-stack-deploy.yml down --remove-orphans

# 清理旧的卷（可选）
read -p "🗑️  是否清理旧的数据卷? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🧹 清理旧的数据卷..."
    docker volume rm business-scenarios_grafana-storage business-scenarios_prometheus-storage 2>/dev/null || true
fi

# 更新Docker镜像
echo "📥 拉取最新Docker镜像..."
docker-compose -f grafana-stack-deploy.yml pull

# 启动监控栈
echo "🚀 启动Grafana监控栈..."
docker-compose -f grafana-stack-deploy.yml up -d

# 等待服务启动
echo "⏳ 等待服务启动..."
sleep 30

# 检查服务状态
echo "🔍 检查服务状态..."
docker-compose -f grafana-stack-deploy.yml ps

# 检查Grafana是否就绪
echo "🌐 等待Grafana就绪..."
timeout=60
while [ $timeout -gt 0 ]; do
    if curl -s http://localhost:3000/api/health > /dev/null 2>&1; then
        echo "✅ Grafana已就绪!"
        break
    fi
    echo "⏳ 等待Grafana启动... ($timeout 秒)"
    sleep 5
    ((timeout-=5))
done

if [ $timeout -eq 0 ]; then
    echo "❌ Grafana启动超时，请检查日志: docker logs grafana-sgcc"
    exit 1
fi

# 显示访问信息
echo ""
echo "🎉 部署完成! 访问信息如下:"
echo "=========================================="
echo "🔋 Grafana 国网智能调度大屏:"
echo "   URL: http://localhost:3000"
echo "   用户名: admin"
echo "   密码: admin123"
echo ""
echo "📊 Prometheus 监控:"
echo "   URL: http://localhost:9090"
echo ""
echo "🔧 PostgreSQL Exporter:"
echo "   URL: http://localhost:9187"
echo ""
echo "💻 Node Exporter:"
echo "   URL: http://localhost:9100"
echo ""
echo "🎯 重要提示:"
echo "1. 请确保你的PostgreSQL (端口5442/5443) 正在运行"
echo "2. 大屏数据每5秒自动刷新"
echo "3. 可以在Grafana中手动修改SQL查询来适配你的表结构"
echo "4. 如需停止: cd ${PROJECT_DIR} && docker-compose -f grafana-stack-deploy.yml down"
echo ""
echo "🚀 Fluss流批一体架构 + Grafana大屏 部署完成!"

# 可选：自动打开浏览器（macOS）
if [[ "$OSTYPE" == "darwin"* ]]; then
    read -p "🌐 是否自动打开Grafana? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        open http://localhost:3000
    fi
fi

echo "✨ 部署脚本执行完成!" 