#!/bin/bash

# 🔋 国网智能调度大屏 - Fluss CDC数据流水线一键启动
# 完整数据流：PostgreSQL源 → CDC → Fluss → PostgreSQL sink → Grafana大屏
# 体现Fluss流批一体架构的真正价值

echo "🚀 启动Fluss CDC数据流水线..."
echo "数据流：PostgreSQL源 → CDC → Fluss → PostgreSQL sink → Grafana大屏"
echo "=========================================="

# 设置错误处理
set -e

# 1. 检查必要的服务
echo "🔍 检查服务状态..."
if ! docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "(coordinator|tablet|postgres|grafana)" | grep -q "Up"; then
    echo "❌ 部分必要服务未运行，请先启动基础服务"
    echo "需要运行的服务：coordinator-server-sgcc, tablet-server-sgcc, postgres-sgcc-source, postgres-sgcc-sink, grafana-sgcc"
    exit 1
fi

echo "✅ 基础服务运行正常"

# 2. 重启Grafana以加载新的Dashboard
echo "🔄 重启Grafana以加载新的Dashboard..."
cd business-scenarios
docker-compose -f grafana-stack-deploy.yml restart grafana > /dev/null 2>&1
cd ..

echo "✅ Grafana重启完成"

# 3. 启动用户的综合业务场景测试SQL
echo "🌊 启动用户的综合业务场景测试SQL..."
echo "这将创建Fluss数仓分层表和数据流处理逻辑..."

# 使用Here Document执行SQL
timeout 300 docker exec -i sql-client-sgcc ./sql-client <<'EOSQL'
-- 执行用户的综合业务场景测试SQL
-- 这里只执行表结构创建和数据流逻辑，不执行DataGen部分

SET 'sql-client.execution.result-mode' = 'tableau';

-- 创建Fluss Catalog和智能电网数仓分层表
CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

USE CATALOG fluss_catalog;
CREATE DATABASE IF NOT EXISTS fluss;
USE fluss;

-- ODS层：电力调度原始数据
CREATE TABLE IF NOT EXISTS ods_power_dispatch_raw (
    dispatch_id STRING PRIMARY KEY NOT ENFORCED,
    event_time TIMESTAMP(3),
    grid_region STRING,
    total_demand_mw DOUBLE,
    total_supply_mw DOUBLE,
    frequency_hz DOUBLE,
    voltage_level_kv DOUBLE,
    load_balance_status STRING,
    emergency_level STRING,
    dispatch_command STRING,
    response_time_ms BIGINT
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- ODS层：设备维度原始数据
CREATE TABLE IF NOT EXISTS ods_device_dimension_raw (
    device_id STRING PRIMARY KEY NOT ENFORCED,
    device_name STRING,
    device_type STRING,
    location STRING,
    capacity_mw DOUBLE,
    status STRING,
    real_time_voltage DOUBLE,
    real_time_current DOUBLE,
    real_time_temperature DOUBLE,
    efficiency_rate DOUBLE,
    event_time TIMESTAMP(3)
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- DWD层：智能电网运行明细
CREATE TABLE IF NOT EXISTS dwd_smart_grid_detail (
    grid_detail_id STRING PRIMARY KEY NOT ENFORCED,
    dispatch_id STRING,
    device_id STRING,
    grid_region STRING,
    device_location STRING,
    device_type STRING,
    dispatch_time TIMESTAMP(3),
    supply_demand_balance DOUBLE,
    frequency_stability STRING,
    voltage_quality STRING,
    device_health_score DOUBLE,
    grid_efficiency DOUBLE,
    risk_level STRING,
    operational_status STRING
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- DWS层：电网运行汇总
CREATE TABLE IF NOT EXISTS dws_grid_operation_summary (
    summary_id STRING PRIMARY KEY NOT ENFORCED,
    grid_region STRING,
    time_window STRING,
    total_dispatches BIGINT,
    total_devices BIGINT,
    avg_supply_demand_balance DOUBLE,
    frequency_stability_rate DOUBLE,
    voltage_quality_rate DOUBLE,
    avg_device_health DOUBLE,
    grid_efficiency_score DOUBLE,
    high_risk_incidents BIGINT,
    emergency_responses BIGINT
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- ADS层：智能电网综合报表
CREATE TABLE IF NOT EXISTS ads_smart_grid_comprehensive_report (
    report_id STRING PRIMARY KEY NOT ENFORCED,
    report_type STRING,
    analysis_period STRING,
    grid_region STRING,
    grid_stability_index DOUBLE,
    operational_efficiency DOUBLE,
    energy_optimization_score DOUBLE,
    reliability_rating STRING,
    risk_assessment STRING,
    performance_trends STRING,
    optimization_recommendations STRING,
    cost_benefit_analysis DOUBLE,
    report_time TIMESTAMP(3)
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server-sgcc:9123'
);

-- 创建PostgreSQL sink表
USE CATALOG default_catalog;

CREATE TABLE IF NOT EXISTS postgres_smart_grid_comprehensive_result (
    report_id STRING,
    report_type STRING,
    analysis_period STRING,
    grid_region STRING,
    grid_stability_index DOUBLE,
    operational_efficiency DOUBLE,
    energy_optimization_score DOUBLE,
    reliability_rating STRING,
    risk_assessment STRING,
    performance_trends STRING,
    optimization_recommendations STRING,
    cost_benefit_analysis DOUBLE,
    report_time TIMESTAMP(3),
    PRIMARY KEY (report_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sgcc-sink:5432/sgcc_dw_db',
    'table-name' = 'smart_grid_comprehensive_result',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024'
);

SHOW TABLES;
QUIT;
EOSQL

if [ $? -eq 0 ]; then
    echo "✅ Fluss数仓分层表创建完成"
else
    echo "❌ Fluss数仓分层表创建失败"
    exit 1
fi

# 4. 启动数据处理脚本
echo "🔧 启动数据处理脚本..."
timeout 180 docker exec -i sql-client-sgcc ./sql-client < business-scenarios/fluss-cdc-data-processing.sql > /dev/null 2>&1 &

echo "✅ 数据处理脚本启动完成"

# 5. 启动数据生成器（后台运行）
echo "📊 启动CDC数据生成器..."
chmod +x business-scenarios/generate-fluss-cdc-data.sh
./business-scenarios/generate-fluss-cdc-data.sh &
DATA_GENERATOR_PID=$!

echo "✅ CDC数据生成器启动完成 (PID: $DATA_GENERATOR_PID)"

# 6. 等待数据生成和处理
echo "⏳ 等待数据生成和处理..."
sleep 30

# 7. 启动数据流任务
echo "🌊 启动数据流任务..."
timeout 300 docker exec -i sql-client-sgcc ./sql-client <<'EOSQL'
SET 'sql-client.execution.result-mode' = 'tableau';

-- 创建PostgreSQL CDC源表
CREATE TABLE IF NOT EXISTS postgres_dispatch_source (
    dispatch_id STRING,
    grid_region STRING,
    dispatch_time TIMESTAMP(3),
    load_demand_mw DOUBLE,
    supply_capacity_mw DOUBLE,
    emergency_level STRING,
    load_balance_status STRING,
    grid_frequency_hz DOUBLE,
    voltage_stability DOUBLE,
    PRIMARY KEY (dispatch_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres-sgcc-source',
    'port' = '5432',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'database-name' = 'sgcc_source_db',
    'schema-name' = 'public',
    'table-name' = 'power_dispatch_data',
    'slot.name' = 'power_dispatch_slot',
    'decoding.plugin.name' = 'pgoutput'
);

CREATE TABLE IF NOT EXISTS postgres_device_source (
    device_id STRING,
    device_name STRING,
    device_type STRING,
    location STRING,
    capacity_mw DOUBLE,
    installation_date TIMESTAMP(3),
    manufacturer STRING,
    model STRING,
    efficiency_rate DOUBLE,
    maintenance_status STRING,
    real_time_voltage DOUBLE,
    real_time_current DOUBLE,
    real_time_temperature DOUBLE,
    PRIMARY KEY (device_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres-sgcc-source',
    'port' = '5432',
    'username' = 'sgcc_user',
    'password' = 'sgcc_pass_2024',
    'database-name' = 'sgcc_source_db',
    'schema-name' = 'public',
    'table-name' = 'device_dimension_data',
    'slot.name' = 'device_dimension_slot',
    'decoding.plugin.name' = 'pgoutput'
);

-- 启动ODS层数据采集
INSERT INTO fluss_catalog.fluss.ods_power_dispatch_raw
SELECT 
    dispatch_id,
    dispatch_time as event_time,
    grid_region,
    load_demand_mw as total_demand_mw,
    supply_capacity_mw as total_supply_mw,
    grid_frequency_hz as frequency_hz,
    voltage_stability as voltage_level_kv,
    load_balance_status,
    emergency_level,
    'AUTO_DISPATCH' as dispatch_command,
    50 as response_time_ms
FROM postgres_dispatch_source;

INSERT INTO fluss_catalog.fluss.ods_device_dimension_raw
SELECT 
    device_id,
    device_name,
    device_type,
    location,
    capacity_mw,
    CASE 
        WHEN maintenance_status = 'NORMAL' THEN 'ACTIVE'
        WHEN maintenance_status = 'WARNING' THEN 'ACTIVE'
        WHEN maintenance_status = 'MAINTENANCE' THEN 'MAINTENANCE'
        ELSE 'INACTIVE'
    END as status,
    real_time_voltage,
    real_time_current,
    real_time_temperature,
    efficiency_rate,
    installation_date as event_time
FROM postgres_device_source;

QUIT;
EOSQL

echo "✅ 数据流任务启动完成"

# 8. 等待数据流稳定
echo "⏳ 等待数据流稳定..."
sleep 30

# 9. 启动数据流水线处理
echo "🔄 启动数据流水线处理..."
timeout 300 docker exec -i sql-client-sgcc ./sql-client <<'EOSQL'
SET 'sql-client.execution.result-mode' = 'tableau';

-- 启动DWD层数据关联处理
INSERT INTO fluss_catalog.fluss.dwd_smart_grid_detail
SELECT 
    CONCAT(d.dispatch_id, '_', dev.device_id) as grid_detail_id,
    d.dispatch_id,
    dev.device_id,
    d.grid_region,
    dev.location as device_location,
    dev.device_type,
    d.event_time as dispatch_time,
    (d.total_supply_mw - d.total_demand_mw) as supply_demand_balance,
    CASE 
        WHEN d.frequency_hz BETWEEN 49.9 AND 50.1 THEN 'STABLE'
        WHEN d.frequency_hz BETWEEN 49.8 AND 50.2 THEN 'ACCEPTABLE'
        ELSE 'UNSTABLE'
    END as frequency_stability,
    CASE 
        WHEN d.voltage_level_kv > 750 AND dev.real_time_voltage > 240 THEN 'EXCELLENT'
        WHEN d.voltage_level_kv > 600 AND dev.real_time_voltage > 230 THEN 'GOOD'
        WHEN dev.real_time_voltage > 220 THEN 'ACCEPTABLE'
        ELSE 'POOR'
    END as voltage_quality,
    (dev.efficiency_rate * 0.4 + (250 - dev.real_time_temperature) * 0.003 + 
     CASE WHEN dev.status = 'ACTIVE' THEN 0.3 ELSE 0.1 END) * 100 as device_health_score,
    (d.total_supply_mw / d.total_demand_mw) * dev.efficiency_rate * 100 as grid_efficiency,
    CASE 
        WHEN d.emergency_level = 'CRITICAL' OR dev.real_time_temperature > 75 THEN 'HIGH_RISK'
        WHEN d.emergency_level = 'HIGH' OR dev.efficiency_rate < 0.85 THEN 'MEDIUM_RISK'
        WHEN d.load_balance_status <> 'BALANCED' THEN 'LOW_RISK'
        ELSE 'NORMAL'
    END as risk_level,
    CASE 
        WHEN d.frequency_hz BETWEEN 49.9 AND 50.1 AND dev.efficiency_rate > 0.95 THEN 'OPTIMAL'
        WHEN d.frequency_hz BETWEEN 49.8 AND 50.2 AND dev.efficiency_rate > 0.90 THEN 'GOOD'
        WHEN dev.efficiency_rate > 0.85 THEN 'ACCEPTABLE'
        ELSE 'NEEDS_ATTENTION'
    END as operational_status
FROM fluss_catalog.fluss.ods_power_dispatch_raw d
CROSS JOIN fluss_catalog.fluss.ods_device_dimension_raw dev
WHERE SUBSTRING(d.grid_region, 1, 2) = SUBSTRING(dev.location, 1, 2);

QUIT;
EOSQL

echo "✅ 数据流水线处理启动完成"

# 10. 显示启动结果
echo ""
echo "🎉 Fluss CDC数据流水线启动完成！"
echo "=========================================="
echo "🔋 数据流状态:"
echo "   📊 PostgreSQL源数据库: postgres-sgcc-source:5432"
echo "   🌊 Fluss流批一体处理: coordinator-server-sgcc:9123"
echo "   📋 PostgreSQL sink数据库: postgres-sgcc-sink:5432"
echo "   📈 Grafana大屏: http://localhost:3000"
echo ""
echo "🎯 访问地址:"
echo "   🔗 主大屏: http://localhost:3000/d/sgcc-fluss-cdc-dashboard"
echo "   🔗 登录信息: admin / admin123"
echo ""
echo "🚀 数据流优势:"
echo "   ✅ 流批一体: 统一存储计算架构"
echo "   ✅ 实时处理: 毫秒级数据延迟"
echo "   ✅ 数仓分层: ODS→DWD→DWS→ADS一体化"
echo "   ✅ 事务一致: ACID保证数据完整性"
echo "   ✅ 运维简化: 单一平台管理"
echo ""
echo "📊 后台进程:"
echo "   📡 数据生成器 PID: $DATA_GENERATOR_PID"
echo "   🔧 数据处理脚本: 后台运行中"
echo "   🌊 Fluss数据流: 持续处理中"
echo ""
echo "🔧 管理命令:"
echo "   停止数据生成器: kill $DATA_GENERATOR_PID"
echo "   查看数据流状态: docker exec sql-client-sgcc ./sql-client"
echo "   重启Grafana: docker restart grafana-sgcc"
echo ""
echo "🎉 完成！现在可以在Grafana中查看实时大屏效果！" 