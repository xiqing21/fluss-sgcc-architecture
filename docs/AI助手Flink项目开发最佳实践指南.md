# 🤖 AI助手 + Flink项目开发最佳实践指南

> **适用场景**：AI助手协助进行Flink、Fluss、CDC等流处理项目开发  
> **目标**：避免常见坑点，优化开发工作流，提高开发效率  
> **复用性**：可直接应用于其他类似项目或平台

---

## 📋 **核心原则**

### 🔴 **原则1：项目文件边界严格控制**
```bash
# ✅ 正确：所有文件都在项目内
project-root/
├── docs/           # 文档
├── scripts/        # 脚本
├── sql/           # SQL文件
└── config/        # 配置文件

# ❌ 错误：在项目外创建文件
../some-file.md     # 会导致找不到文件
/tmp/temp-file.sql  # 不便于版本控制
```

**AI助手执行规则：**
- 🚫 **禁止**在项目根目录之外创建任何文件
- ✅ **必须**始终使用相对路径在项目内创建文件
- 🔍 **验证**每次操作前确认当前工作目录

### 🔴 **原则2：Docker配置精准控制**
```yaml
# ✅ 正确：精准挂载连接器到子目录
volumes:
  - ./external-jars:/opt/flink/lib/connectors

# ❌ 错误：覆盖整个lib目录导致容器无法启动
volumes:
  - ./external-jars:/opt/flink/lib  # 会覆盖Flink核心JAR包
```

**关键要点：**
- Docker容器的`/opt/flink/lib`包含Flink核心JAR包
- 外部连接器应挂载到子目录，避免覆盖
- 修改Docker配置后必须完全重启容器群

### 🔴 **原则3：聪明的测试策略**
```sql
-- ✅ 高效：使用COUNT和LIMIT
SELECT COUNT(*) as total FROM large_table;
SELECT * FROM large_table ORDER BY event_time DESC LIMIT 5;

-- ❌ 低效：全表扫描消耗大量token
SELECT * FROM large_table;  -- 可能返回百万行数据
```

---

## 🛠️ **技术层面最佳实践**

### 📦 **连接器管理**

#### JAR包管理策略
```bash
# 1. 创建专用连接器目录
mkdir -p external-jars/

# 2. 精准Docker挂载配置
cat >> docker-compose.yml << 'EOF'
    volumes:
      - ./external-jars:/opt/flink/lib/connectors  # 子目录挂载
EOF

# 3. 验证连接器加载
docker exec container-name ls -la /opt/flink/lib/connectors/
```

#### 常用连接器清单
```bash
# PostgreSQL相关
flink-connector-jdbc-3.2.0-1.18.jar
flink-sql-connector-postgres-cdc-3.1.1.jar  
postgresql-42.7.1.jar

# Kafka相关
flink-sql-connector-kafka-3.2.0-1.18.jar

# 其他数据源
flink-connector-elasticsearch7-3.0.1-1.18.jar
```

### 🗄️ **SQL语法最佳实践**

#### 保留字处理
```sql
-- ✅ 正确：使用反引号处理保留字
CREATE TABLE device_data (
    device_id STRING,
    `current` DOUBLE,  -- current是保留字，需要反引号
    `timestamp` TIMESTAMP(3),  -- timestamp是保留字
    `order` INT  -- order是保留字
);
```

#### CONCAT函数类型转换
```sql
-- ✅ 正确：所有参数转换为STRING
CONCAT('RPT_', location, '_', CAST(DATE_FORMAT(time, 'yyyyMMddHH') AS STRING))

-- ❌ 错误：类型不匹配
CONCAT('RPT_', location, '_', DATE_FORMAT(time, 'yyyyMMddHH'))  -- 类型错误
```

#### 跨Catalog查询
```sql
-- ✅ 正确：使用完整路径
INSERT INTO fluss_catalog.database.target_table 
SELECT * FROM default_catalog.default_database.source_table;

-- ❌ 错误：路径不完整
INSERT INTO target_table SELECT * FROM source_table;  -- 找不到表
```

### 🔄 **CDC配置最佳实践**

#### PostgreSQL CDC必备配置
```sql
CREATE TABLE cdc_source (...) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres-host',
    'port' = '5432',
    'username' = 'user',
    'password' = 'pass',
    'database-name' = 'dbname',
    'schema-name' = 'schema',
    'table-name' = 'table',
    'slot.name' = 'unique_slot_name',  -- 必须唯一
    'decoding.plugin.name' = 'pgoutput'  -- 使用内置解码器，避免插件依赖
);
```

---

## 🚫 **严禁操作清单**

### ❌ **文件操作禁忌**
1. **在项目根目录外创建文件**
   - `../config.yml` ❌  
   - `/tmp/test.sql` ❌
   - `~/.local/script.sh` ❌

2. **覆盖关键系统目录**
   - `-v ./jars:/opt/flink/lib` ❌ (会覆盖Flink核心JAR)
   - `-v ./config:/opt/flink/conf` ❌ (会覆盖Flink配置)

### ❌ **SQL操作禁忌**
1. **保留字未处理**
   ```sql
   CREATE TABLE test (current DOUBLE);  ❌
   CREATE TABLE test (`current` DOUBLE); ✅
   ```

2. **类型转换缺失**
   ```sql
   CONCAT('ID_', some_number)  ❌
   CONCAT('ID_', CAST(some_number AS STRING))  ✅
   ```

### ⚠️ **版本兼容性禁忌**
1. **使用过时的JAR包**
   ```bash
   # ❌ 错误：旧版本JDBC连接器
   flink-connector-jdbc_2.11-1.14.4.jar
   
   # ✅ 正确：Flink 1.20兼容版本
   flink-connector-jdbc-3.3.0-1.20.jar
   ```

2. **忽略安全漏洞**
   ```bash
   # ❌ 错误：存在安全漏洞的驱动
   postgresql-42.6.x.jar
   
   # ✅ 正确：最新安全版本
   postgresql-42.7.7.jar
   ```

---

## ✅ **快速检查清单**

### 🔧 **项目启动前检查**
- [ ] Docker Compose配置正确（JAR挂载到子目录）
- [ ] 所有必需的JAR包已放置到fluss/jars/
- [ ] PostgreSQL WAL配置正确（logical level）
- [ ] 网络和端口配置无冲突
- [ ] 文件目录结构清晰完整
- [ ] **关键：验证JAR包版本兼容性**
  - [ ] `flink-connector-jdbc-3.3.0-1.20.jar` (440KB)
  - [ ] `postgresql-42.7.7.jar` (1.1MB)

### 🧪 **测试执行前检查**  
- [ ] 确认当前工作目录在项目根目录
- [ ] 所有SQL语句保留字已用反引号处理
- [ ] CONCAT函数参数已进行类型转换
- [ ] 跨Catalog查询使用完整路径
- [ ] 流式查询使用timeout避免卡住
- [ ] **关键：验证连接器加载状态**
  - [ ] 检查TaskManager日志无JAR包错误
  - [ ] 验证JdbcDynamicTableFactory可用

### 🔍 **问题诊断检查**
- [ ] 查看知识库是否有相同问题
- [ ] 检查版本兼容性矩阵
- [ ] 验证环境配置是否正确
- [ ] 确认所有服务正常运行

---

## 🎯 **AI助手提示词模板**

### 通用工作提示词
```
在接下来的Flink项目开发中，请严格遵循以下规则：

1. 文件操作规则：
   - 所有文件必须创建在项目根目录内
   - 使用相对路径，禁止使用绝对路径或../向上路径  
   - 每次操作前用pwd确认工作目录

2. Docker配置规则：
   - JAR包挂载到/opt/flink/lib/jars，不要覆盖/opt/flink/lib
   - 修改docker-compose.yml后必须完全重启容器群

3. 版本兼容性规则（⚠️ 极其重要）：
   - Flink 1.20必须使用flink-connector-jdbc-3.3.0-1.20.jar
   - 使用最新安全的PostgreSQL驱动42.7.7
   - 遇到JdbcFactory错误立即检查JAR包版本
   - 参考知识库的版本兼容性矩阵

4. SQL编写规则：
   - current、timestamp、order等保留字必须使用反引号
   - CONCAT函数的所有参数必须CAST为STRING类型
   - 跨Catalog查询必须使用完整路径

5. 测试策略规则：
   - 使用COUNT()和LIMIT代替全表查询
   - 流式查询必须使用timeout避免卡住
   - 优先验证数据量，再查看样例数据

6. 错误处理规则：
   - PostgreSQL CDC必须使用'decoding.plugin.name' = 'pgoutput'
   - 每个SQL会话都要重新创建fluss_catalog
   - 遇到容器重启问题先检查JAR包挂载配置
   - 遇到问题先查看knowledge-base/目录的解决方案

7. 知识库更新规则：
   - 发现新问题时必须更新知识库文档
   - 解决问题后要记录详细的解决过程
   - 保持文档的时效性和准确性

请在每次操作前检查这些规则，确保项目开发的稳定性和效率。
```

---

**📝 使用建议：**
1. 将此文档作为AI助手的参考基准
2. 在新项目开始前先确立这些规则
3. 遇到问题时参考对应章节的解决方案
4. 根据项目特点调整和补充规则
5. 定期更新最佳实践内容 