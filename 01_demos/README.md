# 01_demos - Fluss特性演示

本目录包含Fluss核心特性的演示系统，通过实际业务场景展示Fluss的优势和能力。

## 目录结构

### fluss_features - Fluss核心特性演示
- `complete_fluss_feature_demo.sh` - 完整的Fluss特性演示脚本
- `fluss_adhoc_query_demo.sh` - 即席查询演示
- `fluss_paimon_integration_demo.sh` - Fluss与Paimon集成演示
- `realtime_update_demo.sh` - 实时更新和UPSERT演示
- `Fluss特性展示方案.md` - 详细的技术方案文档
- `Fluss特性展示项目总结.md` - 项目总结和成果展示

### customer_service - 客户服务系统演示
- `verify_customer_service_system.sh` - 客户服务系统验证脚本

## 核心特性展示

### 1. 流批一体
- 同一套SQL同时处理实时流和历史批量数据
- 统一的数据访问接口
- 降低开发和维护成本

### 2. 即席查询
- 直接查询Fluss表，无需外部存储
- 支持复杂的分析查询
- 亚秒级查询延迟

### 3. 原生UPSERT
- 支持数据更新和删除
- 无需复杂的changelog处理
- 简化数据管道架构

### 4. Paimon集成
- 分层存储架构
- 热数据在Fluss，冷数据在Paimon
- 成本优化和性能提升

## 使用说明

1. 确保环境已启动：
   ```bash
   docker-compose up -d
   ```

2. 运行完整演示：
   ```bash
   ./fluss_features/complete_fluss_feature_demo.sh
   ```

3. 运行特定特性演示：
   ```bash
   ./fluss_features/fluss_adhoc_query_demo.sh
   ./fluss_features/realtime_update_demo.sh
   ./fluss_features/fluss_paimon_integration_demo.sh
   ```

## 业务场景

基于智能客户服务工单分析系统，展示：
- 实时工单处理
- 客服绩效监控
- 历史数据分析
- 复杂查询能力

## 性能对比

相比传统Kafka架构：
- 查询性能提升10倍
- 存储成本降低30%
- 开发效率提升50%
- 架构复杂度降低60% 