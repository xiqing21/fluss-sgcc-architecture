# 🔋 国网智能调度大屏 - Grafana部署指南

## 📖 概述

这是基于**Grafana + PostgreSQL + Prometheus**的国网智能调度大屏解决方案，专为**Fluss流批一体架构**设计。

### 🎯 核心特性

- ✅ **一键部署**：Docker Compose一键启动所有服务
- ✅ **实时数据**：直接连接PostgreSQL sink表，5秒自动刷新
- ✅ **专业大屏**：针对电网业务定制的Dashboard
- ✅ **流批一体**：完美展示Fluss架构优势
- ✅ **可扩展性**：支持自定义SQL查询和图表

## 🚀 快速开始

### 前置条件

1. **Docker** 和 **Docker Compose** 已安装
2. **PostgreSQL** 数据库运行在端口 5442/5443
3. 确保有 `fluss_sink_table` 表的访问权限

### 一键部署

```bash
# 进入项目目录
cd fluss-sgcc-architecture

# 赋予执行权限
chmod +x business-scenarios/deploy-grafana-stack.sh

# 一键部署
./business-scenarios/deploy-grafana-stack.sh
```

### 访问大屏

部署完成后，访问：http://localhost:3000

- **用户名**: `admin`
- **密码**: `admin123`

## 📊 大屏功能详解

### 1. 设备状态概览
- 实时显示设备总数、在线设备、离线设备
- 计算设备在线率

### 2. 实时负荷趋势
- 时序图展示平均负荷率、效率趋势
- 按分钟聚合数据

### 3. 实时设备监控表
- 表格展示最近10分钟的设备状态
- 支持状态颜色映射（🟢在线 🔴离线 🟡维护）

### 4. 设备类型分布
- 饼图展示不同设备类型的数量分布

### 5. 地区设备分布
- 环形图展示各地区设备分布情况

### 6. 核心指标仪表盘
- ⚡ 系统效率
- 📊 负荷率  
- 🌡️ 平均温度
- 🚀 Fluss实时处理记录数

## 🔧 数据源配置

### PostgreSQL 连接配置

数据源已自动配置，连接信息：

```yaml
Host: host.docker.internal:5442
Database: test_scenario
Username: test_user
Password: test_password
```

### 自定义SQL查询

所有Panel都使用SQL查询，你可以根据实际表结构修改：

```sql
-- 示例：设备状态概览
SELECT 
  COUNT(*) as "设备总数",
  COUNT(CASE WHEN status = 'online' THEN 1 END) as "在线设备",
  COUNT(CASE WHEN status = 'offline' THEN 1 END) as "离线设备"
FROM fluss_sink_table 
WHERE $__timeFilter(update_time);
```

## 📈 性能监控

### Prometheus指标

访问：http://localhost:9090

- **PostgreSQL指标**: http://localhost:9187/metrics
- **系统指标**: http://localhost:9100/metrics
- **Grafana指标**: http://localhost:3000/metrics

### 关键指标

1. **数据处理延迟**：从PostgreSQL到Grafana的端到端延迟
2. **查询性能**：SQL查询执行时间
3. **系统资源**：CPU、内存、网络使用率
4. **数据库连接**：活跃连接数、连接池状态

## 🛠️ 自定义配置

### 修改数据库连接

编辑 `grafana/provisioning/datasources/datasources.yml`：

```yaml
datasources:
  - name: PostgreSQL-SGCC
    type: postgres
    url: 你的数据库地址:端口
    database: 你的数据库名
    user: 你的用户名
    secureJsonData:
      password: 你的密码
```

### 添加新的Panel

1. 登录Grafana（http://localhost:3000）
2. 进入 "国网智能调度大屏" Dashboard
3. 点击右上角 "Add Panel"
4. 选择 "PostgreSQL-SGCC" 数据源
5. 编写SQL查询
6. 配置图表样式
7. 保存Dashboard

### 创建新的Dashboard

1. 点击左侧菜单 "+"
2. 选择 "Dashboard"
3. 配置数据源为 "PostgreSQL-SGCC"
4. 按需添加Panel

## 🔄 实时数据流

### 数据流架构

```
PostgreSQL CDC → Fluss ODS → Fluss DWD → Fluss DWS → fluss_sink_table → Grafana
```

### 刷新频率

- **Dashboard刷新**: 5秒（可在右上角修改）
- **数据收集频率**: 15秒（Prometheus配置）
- **查询缓存**: 自动管理

## 🎨 界面定制

### 主题修改

1. 进入 Settings → Preferences
2. 选择 Dark/Light 主题
3. 设置默认时区

### 颜色方案

当前使用的颜色主题：
- 🟢 正常状态：绿色系
- 🟡 警告状态：黄色系  
- 🔴 错误状态：红色系
- 🔵 信息状态：蓝色系

## 📱 移动端适配

Dashboard支持响应式设计，在手机/平板上也能正常显示。

建议移动端访问时：
1. 使用横屏模式
2. 启用全屏模式
3. 隐藏浏览器地址栏

## 🚨 告警配置

### 设置告警规则

1. 进入 Alerting → Alert Rules
2. 创建新规则
3. 配置查询条件（如设备离线率>10%）
4. 设置通知渠道

### 通知渠道

支持的通知方式：
- 📧 邮件通知
- 💬 Slack/企业微信
- 📱 手机短信
- 🌐 Webhook

## 🐛 故障排除

### 常见问题

1. **无法连接数据库**
   ```bash
   # 检查PostgreSQL是否运行
   docker ps | grep postgres
   # 检查网络连接
   telnet localhost 5442
   ```

2. **Dashboard显示无数据**
   ```bash
   # 检查数据源配置
   curl -u admin:admin123 http://localhost:3000/api/datasources
   # 测试SQL查询
   psql -h localhost -p 5442 -U test_user -d test_scenario
   ```

3. **Grafana启动失败**
   ```bash
   # 查看日志
   docker logs grafana-sgcc
   # 重新启动
   docker restart grafana-sgcc
   ```

### 性能优化

1. **数据库优化**
   ```sql
   -- 创建索引
   CREATE INDEX idx_fluss_sink_table_update_time ON fluss_sink_table(update_time);
   CREATE INDEX idx_fluss_sink_table_status ON fluss_sink_table(status);
   ```

2. **查询优化**
   - 使用时间范围过滤
   - 避免全表扫描
   - 合理使用聚合函数

## 📚 参考资料

- [Grafana官方文档](https://grafana.com/docs/)
- [PostgreSQL数据源文档](https://grafana.com/docs/grafana/latest/datasources/postgres/)
- [Prometheus监控指标](https://prometheus.io/docs/)
- [Fluss官方文档](https://fluss.apache.org/docs/)

## 🤝 技术支持

如有问题，请检查：

1. **日志文件**：`docker logs <container_name>`
2. **网络连通性**：确保各服务间可以正常通信
3. **配置文件**：检查YAML配置语法
4. **资源使用**：确保系统有足够的CPU和内存

---

## 🎉 总结

通过本方案，你已经拥有了一个完整的**国网智能调度大屏监控系统**：

- ✅ **专业级可视化**：企业级Grafana Dashboard
- ✅ **实时数据流**：PostgreSQL → Grafana 实时同步  
- ✅ **流批一体**：完美展示Fluss架构优势
- ✅ **易于扩展**：支持自定义查询和图表
- ✅ **一键部署**：Docker化部署，运维简单

相比自己开发HTML大屏，Grafana方案具有以下优势：

1. **开发效率**：无需编写前端代码，拖拽配置
2. **稳定可靠**：经过大规模生产环境验证
3. **功能丰富**：内置告警、用户管理、权限控制
4. **社区支持**：丰富的插件和模板库
5. **持续更新**：官方持续维护升级

🚀 **Fluss + Grafana = 完美的流批一体监控解决方案！** 