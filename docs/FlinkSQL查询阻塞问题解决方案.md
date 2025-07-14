# FlinkSQL查询阻塞问题解决方案

## 问题现象
执行FlinkSQL查询时会阻塞窗口，导致：
1. 终端卡住，无法输入新命令
2. 对话会话阻塞，AI助手无法继续操作
3. 查询结果显示在一个新的FlinkSQL交互终端中

## 根本原因分析
**默认的FlinkSQL execution-result-mode是`table`模式**，这会：
- 启动一个交互式的查询结果浏览器
- 需要用户手动按`q`键退出
- 阻塞命令行直到用户手动退出

## 解决方案

### 方案1：使用`tableau`模式（推荐）
```bash
# 在每次查询前设置result-mode
docker exec -i jobmanager-sgcc /opt/flink/bin/sql-client.sh <<EOF
SET 'sql-client.execution.result-mode' = 'tableau';
SHOW TABLES;
EOF
```

### 方案2：使用文件方式执行
```bash
# 创建临时SQL文件，包含设置和查询
docker exec jobmanager-sgcc /opt/flink/bin/sql-client.sh -f /opt/flink/script.sql
```

### 方案3：组合命令（最佳实践）
```bash
# 一次性执行多个语句，确保非阻塞
docker exec -i jobmanager-sgcc /opt/flink/bin/sql-client.sh <<EOF
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'sql-client.execution.max-table-result.rows' = '100';
-- 你的查询语句
SELECT * FROM fluss_ods_device_raw LIMIT 10;
EOF
```

## execution-result-mode详解

| 模式 | 行为 | 适用场景 |
|------|------|----------|
| `table` | 交互式表格浏览器（默认） | 手动交互查询 |
| `tableau` | 直接在终端显示结果 | 脚本自动化查询 |
| `changelog` | 显示变更日志格式 | 流式数据监控 |

## 推荐的查询模板

### 检查表数据
```bash
docker exec -i jobmanager-sgcc /opt/flink/bin/sql-client.sh <<EOF
SET 'sql-client.execution.result-mode' = 'tableau';
SELECT COUNT(*) as record_count FROM fluss_ods_device_raw;
EOF
```

### 监控流式数据
```bash
docker exec -i jobmanager-sgcc /opt/flink/bin/sql-client.sh <<EOF
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'sql-client.execution.max-table-result.rows' = '20';
SELECT device_id, voltage, current, NOW() as check_time 
FROM fluss_ods_device_raw 
LIMIT 20;
EOF
```

## 最佳实践建议

1. **永远在查询前设置tableau模式**
2. **限制结果行数避免输出过多**
3. **使用Here Document方式避免临时文件**
4. **组合多个设置语句一次性执行**

## 更新所有脚本
所有的PowerShell和Bash脚本都需要更新，采用上述非阻塞的查询方式。 