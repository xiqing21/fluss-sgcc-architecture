# 03_sql_scripts - SQL脚本集合

本目录包含项目中使用的所有SQL脚本，按照数据库类型和功能分类。

## 目录结构

### fluss - Fluss SQL脚本
包含所有Fluss相关的SQL脚本：
- 表创建和管理
- 数据处理作业
- 流批一体查询
- 特性演示脚本

### postgres - PostgreSQL脚本
包含PostgreSQL相关的SQL脚本：
- 数据库初始化
- 表结构创建
- 测试数据生成
- 数据验证查询

## 主要脚本

### Fluss SQL脚本
- `2_customer_service_system.sql` - 客户服务系统完整SQL实现
  - 展示流批一体特性
  - 实时数据处理
  - 批量历史分析
  - 即席查询演示

### PostgreSQL脚本
- `01_create_tables.sql` - 创建基础表结构
- `02_insert_test_data.sql` - 插入测试数据
- `03_create_cdc_config.sql` - CDC配置
- `04_create_dws_tables.sql` - 数据仓库表创建
- `05_customer_service_data.sql` - 客户服务系统测试数据

## 脚本分类

### 1. 数据定义脚本 (DDL)
- 表结构创建
- 索引创建
- 约束定义
- 分区配置

### 2. 数据处理脚本 (DML)
- 数据插入
- 数据更新
- 数据删除
- 数据查询

### 3. 流处理脚本
- 实时数据处理
- 窗口函数
- 聚合计算
- 状态管理

### 4. 批处理脚本
- 历史数据分析
- 批量计算
- 报表生成
- 数据导入导出

## 使用方法

### 执行Fluss SQL脚本
```bash
# 使用智能SQL执行脚本
../05_tools_scripts/core/smart_sql_execution.sh fluss/2_customer_service_system.sql

# 或者手动执行
sql-client.sh -f fluss/2_customer_service_system.sql
```

### 执行PostgreSQL脚本
```bash
# 连接到PostgreSQL
psql -h localhost -U postgres -d fluss_source

# 执行脚本
\i postgres/01_create_tables.sql
```

## 脚本特点

### 1. 标准化
- 统一的命名规范
- 一致的代码风格
- 清晰的注释说明

### 2. 模块化
- 功能独立
- 可重用组件
- 易于维护

### 3. 参数化
- 支持参数配置
- 环境适配
- 灵活部署

### 4. 错误处理
- 异常捕获
- 回滚机制
- 状态检查

## 最佳实践

1. **执行顺序**：按照编号顺序执行脚本
2. **环境检查**：执行前确保环境正常
3. **数据备份**：重要操作前进行备份
4. **测试验证**：执行后验证结果
5. **监控日志**：关注执行日志和性能指标

## 注意事项

- 确保数据库连接正常
- 检查权限和配置
- 注意脚本依赖关系
- 监控资源使用情况
- 及时清理临时数据 