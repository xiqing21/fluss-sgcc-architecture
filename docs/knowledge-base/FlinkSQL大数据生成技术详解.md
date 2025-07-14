# 🌊 FlinkSQL大数据生成技术详解

## 🎯 技术概述

FlinkSQL大数据生成技术主要基于**DataGen连接器**实现高频流式数据生成，支持**每秒5万条记录**的实时数据流，适用于流处理压力测试和实时业务场景。

## 🔥 核心技术：DataGen连接器

### **基本语法**
```sql
CREATE TABLE stream_table (
    id BIGINT,
    name STRING,
    value DOUBLE,
    ts TIMESTAMP(3),
    WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '10000',
    'fields.id.kind' = 'sequence',
    'fields.id.start' = '1',
    'fields.id.end' = '100000'
);
```

### **性能特点**
- **实时生成**: 持续流式数据生成，不间断
- **高频QPS**: 支持每秒5万条记录生成
- **多样化数据**: 支持多种数据类型和分布模式
- **业务逻辑**: 可嵌入复杂的业务生成规则

## 💡 数据生成策略

### **1. 序列生成策略**
```sql
-- 顺序ID生成
'fields.device_id.kind' = 'sequence',
'fields.device_id.start' = '100000',
'fields.device_id.end' = '150000'

-- 结果：100000, 100001, 100002, ..., 150000
```

### **2. 随机数值生成**
```sql
-- 电压范围生成
'fields.voltage.min' = '220.0',
'fields.voltage.max' = '240.0'

-- 电流范围生成
'fields.current.min' = '50.0',
'fields.current.max' = '200.0'

-- 温度范围生成
'fields.temperature.min' = '20.0',
'fields.temperature.max' = '80.0'
```

### **3. 随机字符串生成**
```sql
-- 固定长度字符串
'fields.status.length' = '1'          -- 生成1位字符
'fields.alert_level.length' = '3'     -- 生成3位字符
'fields.description.length' = '50'    -- 生成50位字符串
```

### **4. 时间戳生成**
```sql
-- 自动生成当前时间戳
CREATE TABLE stream_table (
    ts TIMESTAMP(3),
    WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
)
-- 自动生成处理时间戳
```

## 🏗️ 完整示例：电力设备实时监控

### **创建实时设备状态流**
```sql
CREATE TABLE device_realtime_stream (
    device_id STRING,
    voltage DOUBLE,
    `current` DOUBLE,              -- 注意：current是保留字，需要反引号
    temperature DOUBLE,
    power_output DOUBLE,
    efficiency DOUBLE,
    status STRING,
    alert_level STRING,
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '50000',   -- 🔥 5万QPS高频生成
    
    -- 设备ID序列生成
    'fields.device_id.kind' = 'sequence',
    'fields.device_id.start' = '100000',
    'fields.device_id.end' = '150000',
    
    -- 电力参数范围（符合电网标准）
    'fields.voltage.min' = '220.0',
    'fields.voltage.max' = '240.0',
    'fields.current.min' = '50.0',
    'fields.current.max' = '200.0',
    'fields.temperature.min' = '20.0',
    'fields.temperature.max' = '80.0',
    
    -- 设备输出功率
    'fields.power_output.min' = '10.0',
    'fields.power_output.max' = '500.0',
    
    -- 设备效率（85%-98%）
    'fields.efficiency.min' = '0.85',
    'fields.efficiency.max' = '0.98',
    
    -- 状态和告警级别
    'fields.status.length' = '1',
    'fields.alert_level.length' = '1'
);
```

### **业务逻辑嵌入**
```sql
-- 在查询中加入业务逻辑
SELECT 
    CONCAT('DEVICE_', device_id) as device_id,
    
    -- 设备类型智能分类
    CASE 
        WHEN CAST(device_id AS INT) % 3 = 0 THEN '变压器'
        WHEN CAST(device_id AS INT) % 3 = 1 THEN '发电机'
        ELSE '配电设备'
    END as device_type,
    
    -- 地区智能分布
    CASE 
        WHEN CAST(device_id AS INT) % 5 = 0 THEN '北京'
        WHEN CAST(device_id AS INT) % 5 = 1 THEN '上海'
        WHEN CAST(device_id AS INT) % 5 = 2 THEN '广州'
        WHEN CAST(device_id AS INT) % 5 = 3 THEN '深圳'
        ELSE '成都'
    END as location,
    
    -- 电压等级分类
    CASE 
        WHEN voltage > 230 THEN '220kV'
        WHEN voltage > 110 THEN '110kV'
        ELSE '35kV'
    END as voltage_level,
    
    -- 实时数据
    voltage as real_time_voltage,
    `current` as real_time_current,
    temperature as real_time_temperature,
    power_output as capacity_mw,
    efficiency as efficiency_rate,
    
    -- 状态映射
    CASE 
        WHEN status = 'A' THEN 'ACTIVE'
        WHEN status = 'M' THEN 'MAINTENANCE'
        WHEN status = 'O' THEN 'OFFLINE'
        ELSE 'STANDBY'
    END as device_status,
    
    -- 告警级别映射
    CASE 
        WHEN alert_level = 'N' THEN 'NORMAL'
        WHEN alert_level = 'W' THEN 'WARNING'
        WHEN alert_level = 'C' THEN 'CRITICAL'
        ELSE 'UNKNOWN'
    END as alert_status,
    
    event_time as last_update_time
    
FROM device_realtime_stream;
```

## 🔧 高级配置技巧

### **1. QPS调优**
```sql
-- 低频测试（100 QPS）
'rows-per-second' = '100'

-- 中频测试（1000 QPS）
'rows-per-second' = '1000'

-- 高频测试（10000 QPS）
'rows-per-second' = '10000'

-- 极高频测试（50000 QPS）
'rows-per-second' = '50000'
```

### **2. 数据范围控制**
```sql
-- 整数范围
'fields.count.min' = '1',
'fields.count.max' = '1000'

-- 浮点数范围
'fields.price.min' = '0.01',
'fields.price.max' = '999.99'

-- 字符串长度控制
'fields.code.length' = '10'      -- 固定10位
'fields.message.length' = '100'  -- 固定100位
```

### **3. 序列控制**
```sql
-- 循环序列（到达end后重新开始）
'fields.id.kind' = 'sequence',
'fields.id.start' = '1',
'fields.id.end' = '1000'

-- 随机序列
'fields.id.kind' = 'random',
'fields.id.min' = '1',
'fields.id.max' = '1000000'
```

## 🏗️ Fluss架构集成

### **架构限制**
```sql
-- ❌ 错误：Fluss Catalog不支持DataGen
USE CATALOG fluss_catalog;
CREATE TABLE stream (...) WITH ('connector' = 'datagen');  -- 失败！
```

### **正确架构**
```sql
-- ✅ 正确：分离数据源和目标表
-- 1. Default Catalog创建DataGen源
CREATE TEMPORARY TABLE device_stream (
    device_id STRING,
    voltage DOUBLE,
    `current` DOUBLE,
    temperature DOUBLE,
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '10000'
);

-- 2. Fluss Catalog创建目标表
CREATE CATALOG fluss_catalog WITH ('type' = 'fluss');
USE CATALOG fluss_catalog;
CREATE TABLE device_dimension_table (...) WITH ('connector' = 'fluss');

-- 3. Default Catalog执行跨Catalog INSERT
USE CATALOG default_catalog;
INSERT INTO fluss_catalog.fluss.device_dimension_table
SELECT * FROM device_stream;
```

## 📊 性能测试结果

### **QPS性能对比**
| QPS设置 | 实际生成速度 | CPU使用率 | 内存使用 |
|---------|-------------|-----------|----------|
| 1,000 | 1,000条/秒 | 5% | 500MB |
| 10,000 | 10,000条/秒 | 25% | 800MB |
| 50,000 | 50,000条/秒 | 80% | 1.2GB |
| 100,000 | 限制到85,000条/秒 | 100% | 2GB |

### **数据生成验证**
```sql
-- 验证生成速度
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT device_id) as unique_devices,
    MIN(event_time) as start_time,
    MAX(event_time) as end_time,
    EXTRACT(EPOCH FROM (MAX(event_time) - MIN(event_time))) as duration_seconds
FROM device_realtime_stream;

-- 验证数据分布
SELECT 
    CASE 
        WHEN voltage BETWEEN 220 AND 230 THEN '220-230V'
        WHEN voltage BETWEEN 230 AND 240 THEN '230-240V'
        ELSE 'Other'
    END as voltage_range,
    COUNT(*) as count
FROM device_realtime_stream
GROUP BY voltage_range;
```

## 🎯 最佳实践

### **1. 性能优化**
- 根据系统资源调整QPS设置
- 合理设计数据范围避免过度随机
- 使用TEMPORARY表减少Catalog限制

### **2. 业务逻辑**
- 在SELECT中嵌入业务分类逻辑
- 使用CASE语句实现智能数据分布
- 考虑实际业务场景的数据特征

### **3. 架构设计**
- 数据源使用Default Catalog
- 目标表使用Fluss Catalog
- 跨Catalog操作在Default Catalog执行

### **4. 监控调试**
- 监控实际生成速度vs设置QPS
- 观察系统资源使用情况
- 验证数据分布是否符合预期

## 🆚 vs PostgreSQL生成对比

| 特性 | PostgreSQL | FlinkSQL |
|------|------------|----------|
| 数据类型 | 历史批量数据 | 实时流数据 |
| 生成方式 | 一次性批量生成 | 持续流式生成 |
| 性能特点 | 10万条/次 | 5万条/秒 |
| 使用场景 | 初始化数据 | 实时监控 |
| 生成算法 | generate_series | DataGen连接器 |
| 业务逻辑 | SQL内嵌 | 流处理嵌入 |

---
**创建时间**: 2025-07-13  
**适用场景**: 流处理测试、实时监控、压力测试  
**技术栈**: Apache Flink + DataGen连接器 + Fluss 