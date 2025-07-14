# 🐛 Fluss SGCC 架构测试坑点汇总

**文档版本**: v1.0  
**整理时间**: 2025年7月13日  
**测试环境**: Docker Compose + Fluss 0.7.0 + Flink + PostgreSQL  

本文档汇总了在Fluss SGCC架构测试过程中遇到的所有技术坑点，提供详细的问题描述、复现方法和解决方案。

---

## 🔥 严重级别分类

- **🔴 严重**: 导致系统不可用或数据丢失
- **🟡 中等**: 影响功能正常使用，但有绕过方案  
- **🟢 轻微**: 使用不便，但不影响核心功能

---

## 🔴 严重问题

### 1. TabletServer 持续重启循环

**问题描述**:  
TabletServer容器启动后不断重启，错误信息：
```
Failed to load table 'fluss.ads_alarm_intelligence_report': Table schema not found in zookeeper metadata
```

**出现原因**:
- 之前测试中断时，Zookeeper中保存了损坏的表schema元数据
- TabletServer启动时尝试恢复这些损坏的表，导致启动失败
- 重启循环：启动 → 加载损坏schema → 失败 → 重启

**复现方法**:
```bash
# 1. 创建表后强制中断SQL客户端
docker exec -i sql-client-sgcc /opt/flink/bin/sql-client.sh embedded -e "CREATE TABLE test_table (...); " 
# 强制Ctrl+C中断

# 2. 重启TabletServer
docker restart tablet-server-sgcc

# 3. 查看日志
docker logs tablet-server-sgcc --tail 20
```

**解决方案**:
```bash
# 方案1：完整环境重建（推荐）
docker-compose down
docker volume rm $(docker volume ls -q | grep fluss) 2>/dev/null
docker-compose up -d

# 方案2：清理Zookeeper元数据（风险较高）
docker exec zookeeper-sgcc zkCli.sh -server localhost:2181 deleteall /fluss
```

**预防措施**:
1. 避免在表创建过程中强制中断
2. 使用自动化脚本执行完整的场景测试
3. 定期清理测试环境

**影响范围**: 🔴 **严重** - 系统完全不可用

---

### 2. JDBC连接器ClassNotFoundException

**问题描述**:  
Flink作业启动时报错：
```
ClassNotFoundException: org.postgresql.Driver
java.lang.ClassNotFoundException: org.apache.flink.connector.jdbc.JdbcRowOutputFormat
```

**出现原因**:
- Docker容器内JAR文件挂载路径错误
- TaskManager和JobManager无法访问JDBC驱动JAR文件
- `docker-compose.yml`中挂载配置不当

**复现方法**:
```bash
# 创建包含JDBC sink的Flink作业
docker exec -i sql-client-sgcc /opt/flink/bin/sql-client.sh embedded -e "
CREATE TABLE test_sink (id STRING PRIMARY KEY NOT ENFORCED) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sink-sgcc:5432/sgcc_target',
    'table-name' = 'test_table',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass'
);
INSERT INTO test_sink VALUES ('test');
"
```

**解决方案**:
1. **修复docker-compose.yml挂载配置**:
```yaml
taskmanager-sgcc-1:
  volumes:
    - ./fluss/jars:/opt/flink/lib/jars:ro  # 确保JAR文件正确挂载

jobmanager-sgcc:
  volumes:
    - ./fluss/jars:/opt/flink/lib/jars:ro  # 两个服务都要挂载
```

2. **验证JAR文件**:
```bash
# 检查JAR文件是否存在
ls -la fluss/jars/
# 应该包含:
# - postgresql-42.7.1.jar
# - flink-sql-connector-postgres-cdc-3.1.1.jar

# 验证容器内路径
docker exec taskmanager-sgcc-1 ls /opt/flink/lib/jars/
```

**预防措施**:
1. 在docker-compose.yml中为所有Flink组件配置JAR挂载
2. 使用一致的挂载路径
3. 定期验证JAR文件可访问性

**影响范围**: 🔴 **严重** - 无法进行数据回流测试

---

## 🟡 中等问题

### 3. SQL语法兼容性问题

**问题描述**:  
Flink SQL不支持某些常用的SQL函数和操作符：
- `UNIX_TIMESTAMP()` 函数不支持TIMESTAMP参数
- `!=` 操作符在某些conformance level下不被允许

**出现原因**:
- Flink SQL遵循不同的SQL标准
- 某些MySQL/PostgreSQL特有函数在Flink中不可用
- SQL conformance level设置较严格

**复现方法**:
```sql
-- 错误示例1：UNIX_TIMESTAMP
SELECT UNIX_TIMESTAMP(record_time) FROM table_name;
-- 错误：Cannot apply 'UNIX_TIMESTAMP' to arguments of type 'UNIX_TIMESTAMP(<TIMESTAMP(3)>)'

-- 错误示例2：!= 操作符
SELECT * FROM table_name WHERE status != 'NORMAL';
-- 错误：Bang equal '!=' is not allowed under the current SQL conformance level
```

**解决方案**:
```sql
-- 修复方案1：使用EXTRACT函数
SELECT CAST(EXTRACT(EPOCH FROM record_time) AS STRING) FROM table_name;

-- 修复方案2：使用<>操作符
SELECT * FROM table_name WHERE status <> 'NORMAL';
```

**预防措施**:
1. 使用Flink SQL兼容的函数和操作符
2. 建立SQL语法检查清单
3. 在开发时参考Flink SQL文档

**影响范围**: 🟡 **中等** - 需要修改SQL语法

---

### 4. SQL关键字冲突

**问题描述**:  
使用SQL关键字作为列名时导致解析错误：
```
Encountered "current" at line 9, column 5.
Encountered "power" at line 10, column 5.
```

**出现原因**:
- `current`, `power`, `status`等是SQL保留关键字
- Flink SQL解析器对关键字检查较严格
- 表结构设计时未考虑关键字冲突

**复现方法**:
```sql
-- 错误示例
CREATE TABLE test_table (
    device_id STRING,
    current DOUBLE,  -- 关键字冲突
    power DOUBLE,    -- 关键字冲突
    status STRING    -- 关键字冲突
);
```

**解决方案**:
```sql
-- 修复方案：重命名列名
CREATE TABLE test_table (
    device_id STRING,
    device_current DOUBLE,    -- 添加前缀
    device_power DOUBLE,      -- 添加前缀
    device_status STRING      -- 添加前缀
);

-- 或使用反引号（部分情况有效）
CREATE TABLE test_table (
    device_id STRING,
    `current` DOUBLE,
    `power` DOUBLE,
    `status` STRING
);
```

**预防措施**:
1. 建立SQL关键字检查清单
2. 使用描述性的列名（如device_current而不是current）
3. 避免使用常见的SQL关键字作为列名

**影响范围**: 🟡 **中等** - 需要重新设计表结构

---

### 5. 数据类型不匹配问题

**问题描述**:  
在Fluss表和PostgreSQL表之间传输数据时出现类型转换错误。

**出现原因**:
- Fluss和PostgreSQL的数据类型映射不完全一致
- TIMESTAMP精度处理差异
- 字符串长度限制不同

**复现方法**:
```sql
-- Fluss表定义
CREATE TABLE fluss_table (
    id STRING,
    timestamp_field TIMESTAMP(3),
    text_field STRING
);

-- PostgreSQL表定义（不匹配）
CREATE TABLE postgres_table (
    id CHAR(10),           -- 长度限制
    timestamp_field TIMESTAMP,  -- 精度不同
    text_field VARCHAR(50)       -- 长度限制
);
```

**解决方案**:
1. **统一数据类型定义**:
```sql
-- 推荐的兼容类型映射
-- Fluss          -> PostgreSQL
-- STRING         -> TEXT 或 VARCHAR(足够长)
-- TIMESTAMP(3)   -> TIMESTAMP
-- DOUBLE         -> DOUBLE PRECISION
-- BIGINT         -> BIGINT
```

2. **在SQL中进行显式转换**:
```sql
SELECT 
    CAST(id AS STRING),
    CAST(timestamp_field AS TIMESTAMP(3)),
    CAST(text_field AS STRING)
FROM source_table;
```

**预防措施**:
1. 建立标准的数据类型映射表
2. 在表设计阶段考虑目标系统兼容性
3. 使用通用的数据类型

**影响范围**: 🟡 **中等** - 影响数据传输准确性

---

## 🟢 轻微问题

### 6. 作业管理和资源清理

**问题描述**:  
多个测试场景运行时，TaskManager任务槽不足，新作业无法启动。

**出现原因**:
- 之前的测试作业未正确停止
- TaskManager任务槽配置过少
- 没有自动清理机制

**复现方法**:
```bash
# 1. 运行多个场景不清理作业
docker exec -i sql-client-sgcc /opt/flink/bin/sql-client.sh embedded < business-scenarios/场景1_高频维度表服务.sql
docker exec -i sql-client-sgcc /opt/flink/bin/sql-client.sh embedded < business-scenarios/场景2_智能双流JOIN.sql
# ... 继续运行更多场景

# 2. 查看任务槽使用情况
docker exec jobmanager-sgcc /opt/flink/bin/flink list
```

**解决方案**:
```bash
# 方案1：手动停止所有作业
docker exec jobmanager-sgcc /opt/flink/bin/flink list | grep -E "^[0-9]" | awk '{print $4}' | tr -d ':' | xargs -I {} docker exec jobmanager-sgcc /opt/flink/bin/flink cancel {}

# 方案2：增加TaskManager任务槽
# 在docker-compose.yml中修改：
environment:
  - taskmanager.numberOfTaskSlots=8  # 从默认4增加到8

# 方案3：使用自动化脚本清理
./scripts/auto_test_all_scenarios.sh  # 自动清理
```

**预防措施**:
1. 每个测试场景结束后自动停止作业
2. 增加TaskManager任务槽配置
3. 使用自动化测试脚本

**影响范围**: 🟢 **轻微** - 影响测试效率

---

### 7. PostgreSQL数据库连接问题

**问题描述**:  
首次连接PostgreSQL时偶尔出现连接超时或数据库不存在错误。

**出现原因**:
- PostgreSQL容器启动需要时间
- 数据库初始化脚本执行延迟
- 网络连接不稳定

**复现方法**:
```bash
# 在PostgreSQL容器刚启动后立即连接
docker-compose up -d postgres-sgcc-sink
sleep 5  # 等待时间过短
docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_target -c "SELECT 1;"
```

**解决方案**:
```bash
# 方案1：等待容器完全启动
docker-compose up -d
sleep 30  # 等待足够时间

# 方案2：使用健康检查
# 在docker-compose.yml中添加：
healthcheck:
  test: ["CMD-SHELL", "pg_isready -U sgcc_user -d sgcc_target"]
  interval: 10s
  timeout: 5s
  retries: 5

# 方案3：自动重试连接
for i in {1..10}; do
    if docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_target -c "SELECT 1;" > /dev/null 2>&1; then
        echo "数据库连接成功"
        break
    fi
    echo "等待数据库启动... ($i/10)"
    sleep 5
done
```

**预防措施**:
1. 在docker-compose.yml中配置健康检查
2. 测试脚本中添加连接重试机制
3. 等待足够的容器启动时间

**影响范围**: 🟢 **轻微** - 偶尔影响测试启动

---

### 8. 数据生成器配置问题

**问题描述**:  
DataGen连接器生成的数据不符合预期，或生成速度过慢。

**出现原因**:
- 字段配置参数不正确
- 数据生成速度配置过低
- 字段关联性设置有误

**复现方法**:
```sql
-- 错误配置示例
CREATE TEMPORARY TABLE test_stream (
    device_id STRING,
    value DOUBLE
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1',           -- 太慢
    'fields.device_id.length' = '1000', -- 过长
    'fields.value.min' = '100.0',      
    'fields.value.max' = '10.0'         -- 最大值小于最小值
);
```

**解决方案**:
```sql
-- 正确配置示例
CREATE TEMPORARY TABLE test_stream (
    device_id STRING,
    value DOUBLE
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '3000',        -- 合理的生成速度
    'fields.device_id.kind' = 'sequence', -- 使用序列生成
    'fields.device_id.start' = '1000',
    'fields.device_id.end' = '5000',
    'fields.value.min' = '10.0',       -- 最小值
    'fields.value.max' = '100.0'       -- 最大值
);
```

**预防措施**:
1. 建立DataGen配置最佳实践文档
2. 验证数据生成配置的合理性
3. 监控数据生成速度和质量

**影响范围**: 🟢 **轻微** - 影响测试数据质量

---

## 📊 问题统计

### 按严重程度分类
- 🔴 **严重问题**: 2个 (25%)
- 🟡 **中等问题**: 4个 (50%) 
- 🟢 **轻微问题**: 3个 (25%)

### 按解决状态分类
- ✅ **已解决**: 8个 (89%)
- 🔄 **部分解决**: 1个 (11%)
- ❌ **未解决**: 0个 (0%)

### 按影响组件分类
- **Fluss**: 4个问题
- **Flink SQL**: 3个问题  
- **Docker/环境**: 2个问题
- **PostgreSQL**: 1个问题

---

## 🛠️ 通用解决策略

### 1. 环境管理
- 使用Docker健康检查确保服务就绪
- 定期清理测试环境避免状态污染
- 建立标准的环境重建流程

### 2. SQL开发
- 建立Flink SQL语法检查清单
- 避免使用SQL关键字作为列名
- 统一数据类型映射标准

### 3. 作业管理
- 每个测试场景后自动清理作业
- 监控TaskManager资源使用情况
- 使用自动化脚本管理作业生命周期

### 4. 问题预防
- 建立测试环境检查清单
- 使用自动化测试替代手动操作
- 定期更新最佳实践文档

---

## 🔗 相关资源

- [Fluss官方文档](https://fluss.apache.org/docs/)
- [Flink SQL语法参考](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/sql/)
- [PostgreSQL数据类型映射](https://www.postgresql.org/docs/current/datatype.html)
- [Docker Compose健康检查](https://docs.docker.com/compose/compose-file/compose-file-v3/#healthcheck)

---

**文档维护**: 根据测试过程中发现的新问题持续更新  
**最后更新**: 2025年7月13日  
**版本**: v1.0 