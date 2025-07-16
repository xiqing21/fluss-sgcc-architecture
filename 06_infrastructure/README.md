# 06_infrastructure - 基础设施配置

本目录包含项目运行所需的基础设施配置，包括容器化部署、数据库配置等。

## 目录结构

### docker - Docker容器配置
- `docker-compose.yml` - Docker编排文件
- `docker/` - Docker相关配置
- `jars/` - Fluss运行时依赖
- `conf/` - Fluss配置文件

### postgres - PostgreSQL配置
- `postgres_source/` - 源数据库配置
- `postgres_sink/` - 目标数据库配置
- 数据库初始化脚本
- 连接配置文件

## 主要组件

### 1. Docker Compose 服务
```yaml
services:
  - zookeeper: 协调服务
  - kafka: 消息队列
  - fluss: 核心存储服务
  - postgres_source: 源数据库
  - postgres_sink: 目标数据库
  - flink: 流处理引擎
```

### 2. Fluss 配置
- 集群配置
- 存储配置
- 网络配置
- 性能调优

### 3. PostgreSQL 配置
- 数据库实例配置
- CDC配置
- 权限配置
- 性能优化

## 部署架构

### 网络架构
```
┌─────────────────────────────────────────────────────────────┐
│                        Docker Network                       │
├─────────────────────────────────────────────────────────────│
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │   Zookeeper │  │    Kafka    │  │   Fluss     │        │
│  │    :2181    │  │    :9092    │  │    :9090    │        │
│  └─────────────┘  └─────────────┘  └─────────────┘        │
│                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │  Postgres   │  │  Postgres   │  │    Flink    │        │
│  │   Source    │  │    Sink     │  │  JobManager │        │
│  │    :5432    │  │    :5433    │  │    :8081    │        │
│  └─────────────┘  └─────────────┘  └─────────────┘        │
└─────────────────────────────────────────────────────────────┘
```

### 数据流架构
```
PostgreSQL Source → Fluss → Flink → PostgreSQL Sink
      ↓              ↓       ↓            ↓
   CDC Config    ODS Layer  Processing  ADS Layer
```

## 配置文件说明

### Docker Compose
```yaml
version: '3.8'
services:
  fluss:
    image: fluss/fluss:latest
    ports:
      - "9090:9090"
    volumes:
      - ./conf:/opt/fluss/conf
      - ./jars:/opt/fluss/lib
    environment:
      - FLUSS_CONF_DIR=/opt/fluss/conf
```

### Fluss 配置
```properties
# 集群配置
fluss.cluster.node-id=1
fluss.cluster.coordinators=localhost:9090

# 存储配置
fluss.storage.type=local
fluss.storage.path=/data/fluss

# 网络配置
fluss.network.host=localhost
fluss.network.port=9090
```

## 部署步骤

### 1. 环境准备
```bash
# 检查Docker环境
docker --version
docker-compose --version

# 创建数据目录
mkdir -p data/fluss data/postgres_source data/postgres_sink
```

### 2. 启动服务
```bash
# 启动所有服务
docker-compose up -d

# 检查服务状态
docker-compose ps

# 查看日志
docker-compose logs -f fluss
```

### 3. 初始化数据库
```bash
# 连接到源数据库
docker-compose exec postgres_source psql -U postgres -d fluss_source

# 执行初始化脚本
\i /docker-entrypoint-initdb.d/01_create_tables.sql
```

### 4. 验证部署
```bash
# 检查Fluss状态
curl http://localhost:9090/api/v1/status

# 检查Flink状态
curl http://localhost:8081/overview

# 验证数据库连接
docker-compose exec postgres_source psql -U postgres -c "SELECT version();"
```

## 监控和运维

### 1. 健康检查
```bash
# 检查服务状态
docker-compose ps

# 检查资源使用
docker stats

# 检查日志
docker-compose logs --tail=100 fluss
```

### 2. 性能监控
- CPU和内存使用率
- 磁盘IO性能
- 网络吞吐量
- 数据库连接数

### 3. 备份和恢复
```bash
# 备份数据库
docker-compose exec postgres_source pg_dump -U postgres fluss_source > backup.sql

# 恢复数据库
docker-compose exec postgres_source psql -U postgres fluss_source < backup.sql
```

## 故障排除

### 常见问题
1. **服务启动失败**
   - 检查端口占用
   - 查看服务日志
   - 验证配置文件

2. **数据库连接失败**
   - 检查网络配置
   - 验证用户权限
   - 确认服务状态

3. **性能问题**
   - 调整资源限制
   - 优化配置参数
   - 监控系统指标

### 调试命令
```bash
# 进入容器调试
docker-compose exec fluss bash

# 查看配置
docker-compose exec fluss cat /opt/fluss/conf/fluss.conf

# 检查网络连接
docker-compose exec fluss ping postgres_source
```

## 扩展和优化

### 1. 集群扩展
- 添加更多Fluss节点
- 配置负载均衡
- 实现高可用部署

### 2. 性能优化
- 调整JVM参数
- 优化存储配置
- 配置缓存策略

### 3. 安全配置
- 启用认证授权
- 配置SSL/TLS
- 网络安全配置

## 最佳实践

1. **资源分配**：合理分配CPU和内存
2. **存储优化**：使用SSD存储提升性能
3. **网络配置**：优化网络带宽和延迟
4. **监控告警**：建立完善的监控体系
5. **备份策略**：定期备份重要数据 