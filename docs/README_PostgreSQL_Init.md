# 🎯 Fluss SGCC PostgreSQL 初始化使用指南

## 📋 概述

此脚本为5个Fluss业务场景创建完整的PostgreSQL环境，包括：
- 🔌 **CDC源表**（7个表，72条测试数据）
- 🎯 **Sink目标表**（5个结果表）
- 🔧 **CDC复制槽配置**
- 👤 **用户权限设置**

## 🚀 快速开始

### 1. 执行初始化脚本

```bash
# 连接到PostgreSQL（以超级用户身份）
psql -U postgres -h your_postgres_host

# 执行初始化脚本
\i postgres_init_script.sql
```

### 2. 验证安装

```sql
-- 切换到源数据库查看数据
\c sgcc_source_db
\dt

-- 查看各表数据量
SELECT 'device_raw_data' as table_name, COUNT(*) as count FROM device_raw_data
UNION ALL SELECT 'device_alarms', COUNT(*) FROM device_alarms
UNION ALL SELECT 'device_status', COUNT(*) FROM device_status;
```

## 📊 数据库结构

### 源数据库 (sgcc_source_db)

| 表名 | 场景 | 记录数 | 用途 |
|------|------|--------|------|
| `device_raw_data` | 场景1 | 20条 | 高频维度表服务 |
| `device_alarms` | 场景2 | 10条 | 智能双流JOIN（告警流） |
| `device_status` | 场景2 | 20条 | 智能双流JOIN（状态流） |
| `device_historical_data` | 场景3 | 12条 | 时间旅行查询 |
| `large_scale_monitoring_data` | 场景4 | 5条 | 柱状流优化 |
| `power_dispatch_data` | 综合 | 5条 | 电力调度数据 |
| `device_dimension_data` | 综合 | 5条 | 设备维度数据 |

### 目标数据库 (sgcc_dw_db)

| 表名 | 场景 | 用途 |
|------|------|------|
| `device_final_report` | 场景1 | 设备最终报表 |
| `alarm_intelligence_result` | 场景2 | 告警智能分析结果 |
| `fault_analysis_result` | 场景3 | 故障分析结果 |

### 目标数据库 (sgcc_target)

| 表名 | 场景 | 用途 |
|------|------|------|
| `columnar_performance_result` | 场景4 | 柱状流性能结果 |
| `smart_grid_comprehensive_result` | 综合 | 智能电网综合分析 |

## 🧪 测试数据说明

### 设备数据分布
- **设备ID**: 100001-100020 (20个设备)
- **地区分布**: 北京、上海、广州、深圳、成都
- **设备类型**: 变压器、发电机、配电设备
- **状态分布**: NORMAL(15), WARNING(3), OFFLINE(2)

### 告警数据特点
- **告警级别**: HIGH, MEDIUM, LOW, CRITICAL
- **告警类型**: TEMPERATURE, EFFICIENCY, VOLTAGE, STATUS, POWER
- **时间跨度**: 最近3小时内的告警记录

### 历史数据时序
- **时间点**: 2小时前、1小时前、30分钟前、当前
- **监控指标**: 电压、电流、温度、功率、效率
- **运行模式**: AUTO(自动)、MANUAL(手动)

## 🔧 CDC配置

脚本已自动创建以下复制槽：
```sql
device_raw_slot         -- 对应 device_raw_data
device_alarms_slot      -- 对应 device_alarms  
device_status_slot      -- 对应 device_status
device_historical_slot  -- 对应 device_historical_data
```

## 🎮 手动测试示例

### 场景1：测试设备数据变化
```sql
-- 连接源数据库
\c sgcc_source_db

-- 插入新设备数据
INSERT INTO device_raw_data VALUES 
('TEST001', 240.5, 160.0, 48.5, 380.2, 0.97, 'A', 'L', NOW());

-- 更新设备状态
UPDATE device_raw_data 
SET efficiency = 0.98, alert_level = 'H' 
WHERE device_id = '100001';

-- 删除测试数据
DELETE FROM device_raw_data WHERE device_id = 'TEST001';
```

### 场景2：测试告警数据
```sql
-- 插入新告警
INSERT INTO device_alarms VALUES 
('ALARM_TEST', '100001', 'EMERGENCY', 'CRITICAL', '紧急测试告警', NOW(), 'TEST_SYS');

-- 更新设备状态
UPDATE device_status 
SET status = 'CRITICAL', efficiency = 0.85 
WHERE device_id = '100001';
```

### 场景3：测试历史数据
```sql
-- 插入历史记录
INSERT INTO device_historical_data VALUES 
('HIST_TEST', '100001', NOW() - INTERVAL '5 minutes', 238.5, 155.0, 46.8, 365.4, 0.965, 89.2, 'AUTO', '');
```

## 🔍 监控查询

### 数据分布检查
```sql
-- 设备状态分布
SELECT status, COUNT(*) as count 
FROM device_status 
GROUP BY status;

-- 告警级别分布  
SELECT alarm_level, COUNT(*) as count 
FROM device_alarms 
GROUP BY alarm_level;

-- 效率分析
SELECT 
    CASE 
        WHEN efficiency >= 0.95 THEN '优秀'
        WHEN efficiency >= 0.90 THEN '良好' 
        ELSE '需改进'
    END as grade,
    COUNT(*) as count
FROM device_raw_data 
GROUP BY 1;
```

### 实时监控
```sql
-- 最近告警
SELECT alarm_id, device_id, alarm_level, alarm_message, alarm_time
FROM device_alarms 
ORDER BY alarm_time DESC 
LIMIT 5;

-- 异常设备
SELECT device_id, voltage, temperature, efficiency, status
FROM device_raw_data 
WHERE efficiency < 0.90 OR status != 'A';
```

## 🎯 下一步

1. **启动Fluss作业**：使用对应场景的SQL脚本
2. **验证数据流**：检查Fluss catalog中的表数据
3. **测试CDC效果**：手动修改源表数据观察变化
4. **查看结果表**：检查PostgreSQL目标表中的处理结果

## ⚠️ 注意事项

- 确保PostgreSQL启用了逻辑复制：`wal_level = logical`
- 用户`sgcc_user`已获得所有必要权限
- 复制槽名称需与Fluss CDC配置匹配
- 建议在测试环境先验证完整流程

## 🔗 相关文件

- `postgres_init_script.sql` - 主初始化脚本
- `场景1_高频维度表服务.sql` - 场景1 Fluss作业
- `场景2_智能双流JOIN.sql` - 场景2 Fluss作业  
- `场景3_时间旅行查询.sql` - 场景3 Fluss作业
- `场景4_柱状流优化.sql` - 场景4 Fluss作业
- `综合业务场景测试.sql` - 综合场景 Fluss作业 