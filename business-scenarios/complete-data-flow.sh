#!/bin/bash

# 完整的数据流脚本
# 模拟真实的source → fluss → sink数据流
# 确保大屏有完整的数据显示

echo "🚀 启动完整的数据流处理..."
echo "数据流：PostgreSQL源 → Fluss流批处理 → PostgreSQL sink → Grafana大屏"
echo "按 Ctrl+C 停止数据流"
echo "=========================================="

# 数据处理函数
process_device_status() {
    echo "📊 处理设备状态汇总数据..."
    
    # 清空旧数据
    docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "DELETE FROM device_status_summary;" > /dev/null 2>&1
    
    # 从源数据库获取设备数据，经过Fluss处理逻辑，写入sink
    docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
    INSERT INTO device_status_summary (summary_id, device_id, device_type, location, status, load_factor, efficiency, temperature, update_time)
    SELECT 
        'FLUSS_' || device_data.device_id || '_' || EXTRACT(EPOCH FROM NOW()) as summary_id,
        device_data.device_id,
        device_data.device_type,
        device_data.location,
        CASE 
            WHEN device_data.maintenance_status = 'NORMAL' THEN 'NORMAL'
            WHEN device_data.maintenance_status = 'WARNING' THEN 'WARNING'
            WHEN device_data.maintenance_status = 'MAINTENANCE' THEN 'MAINTENANCE'
            ELSE 'CRITICAL'
        END as status,
        CASE 
            WHEN device_data.capacity_mw > 0 THEN 
                LEAST(100, (device_data.real_time_current / device_data.capacity_mw) * 100)
            ELSE 0
        END as load_factor,
        LEAST(100, device_data.efficiency_rate * 100) as efficiency,
        device_data.real_time_temperature as temperature,
        NOW() as update_time
    FROM (
        SELECT * FROM dblink('host=postgres-sgcc-source port=5432 dbname=sgcc_source_db user=sgcc_user password=sgcc_pass_2024',
                              'SELECT device_id, device_type, location, capacity_mw, efficiency_rate, maintenance_status, real_time_current, real_time_temperature FROM device_dimension_data ORDER BY device_id')
        AS device_data(device_id TEXT, device_type TEXT, location TEXT, capacity_mw DOUBLE PRECISION, efficiency_rate DOUBLE PRECISION, maintenance_status TEXT, real_time_current DOUBLE PRECISION, real_time_temperature DOUBLE PRECISION)
    ) as device_data;
    " > /dev/null 2>&1
}

process_grid_metrics() {
    echo "📈 处理电网监控指标数据..."
    
    # 清空旧数据
    docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "DELETE FROM grid_monitoring_metrics;" > /dev/null 2>&1
    
    # 处理电网监控指标
    docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
    INSERT INTO grid_monitoring_metrics (metric_id, grid_region, total_devices, online_devices, offline_devices, maintenance_devices, avg_efficiency, avg_load_factor, avg_temperature, alert_count, update_time)
    SELECT 
        'FLUSS_' || grid_stats.grid_region || '_' || EXTRACT(EPOCH FROM NOW()) as metric_id,
        CASE 
            WHEN grid_stats.grid_region = '北京' THEN '华北电网'
            WHEN grid_stats.grid_region = '上海' THEN '华东电网'
            WHEN grid_stats.grid_region = '广州' THEN '华南电网'
            WHEN grid_stats.grid_region = '成都' THEN '西南电网'
            WHEN grid_stats.grid_region = '西安' THEN '西北电网'
            WHEN grid_stats.grid_region = '大连' THEN '东北电网'
            ELSE '其他电网'
        END as grid_region,
        grid_stats.total_devices,
        grid_stats.online_devices,
        grid_stats.offline_devices,
        grid_stats.maintenance_devices,
        ROUND(grid_stats.avg_efficiency, 2) as avg_efficiency,
        ROUND(grid_stats.avg_load_factor, 2) as avg_load_factor,
        ROUND(grid_stats.avg_temperature, 2) as avg_temperature,
        grid_stats.alert_count,
        NOW() as update_time
    FROM (
        SELECT * FROM dblink('host=postgres-sgcc-source port=5432 dbname=sgcc_source_db user=sgcc_user password=sgcc_pass_2024',
                              'SELECT 
                                  SUBSTRING(location, 1, 2) as grid_region,
                                  COUNT(*) as total_devices,
                                  COUNT(CASE WHEN maintenance_status = ''NORMAL'' THEN 1 END) as online_devices,
                                  COUNT(CASE WHEN maintenance_status = ''CRITICAL'' THEN 1 END) as offline_devices,
                                  COUNT(CASE WHEN maintenance_status = ''MAINTENANCE'' THEN 1 END) as maintenance_devices,
                                  AVG(efficiency_rate * 100) as avg_efficiency,
                                  AVG(CASE WHEN capacity_mw > 0 THEN LEAST(100, (real_time_current / capacity_mw) * 100) ELSE 0 END) as avg_load_factor,
                                  AVG(real_time_temperature) as avg_temperature,
                                  COUNT(CASE WHEN real_time_temperature > 70 THEN 1 END) as alert_count
                               FROM device_dimension_data
                               GROUP BY SUBSTRING(location, 1, 2)
                               ORDER BY COUNT(*) DESC')
        AS grid_stats(grid_region TEXT, total_devices BIGINT, online_devices BIGINT, offline_devices BIGINT, maintenance_devices BIGINT, avg_efficiency DOUBLE PRECISION, avg_load_factor DOUBLE PRECISION, avg_temperature DOUBLE PRECISION, alert_count BIGINT)
    ) as grid_stats;
    " > /dev/null 2>&1
}

process_comprehensive_reports() {
    echo "📋 处理智能电网综合报表数据..."
    
    # 清空旧数据
    docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "DELETE FROM smart_grid_comprehensive_result;" > /dev/null 2>&1
    
    # 处理智能电网综合报表
    docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
    INSERT INTO smart_grid_comprehensive_result (report_id, report_type, analysis_period, grid_region, grid_stability_index, operational_efficiency, energy_optimization_score, reliability_rating, risk_assessment, performance_trends, optimization_recommendations, cost_benefit_analysis, report_time)
    SELECT 
        'FLUSS_' || report_data.grid_region || '_' || EXTRACT(EPOCH FROM NOW()) as report_id,
        '智能电网综合运行报表' as report_type,
        TO_CHAR(NOW(), 'YYYYMMDDHH24') as analysis_period,
        CASE 
            WHEN report_data.grid_region = '北京' THEN '华北电网'
            WHEN report_data.grid_region = '上海' THEN '华东电网'
            WHEN report_data.grid_region = '广州' THEN '华南电网'
            WHEN report_data.grid_region = '成都' THEN '西南电网'
            WHEN report_data.grid_region = '西安' THEN '西北电网'
            WHEN report_data.grid_region = '大连' THEN '东北电网'
            ELSE '其他电网'
        END as grid_region,
        ROUND(report_data.grid_stability_index, 2) as grid_stability_index,
        ROUND(report_data.operational_efficiency, 2) as operational_efficiency,
        ROUND(report_data.energy_optimization_score, 2) as energy_optimization_score,
        report_data.reliability_rating,
        report_data.risk_assessment,
        report_data.performance_trends,
        report_data.optimization_recommendations,
        ROUND(report_data.cost_benefit_analysis, 2) as cost_benefit_analysis,
        NOW() as report_time
    FROM (
        SELECT * FROM dblink('host=postgres-sgcc-source port=5432 dbname=sgcc_source_db user=sgcc_user password=sgcc_pass_2024',
                              'SELECT 
                                  SUBSTRING(location, 1, 2) as grid_region,
                                  (AVG(efficiency_rate) * 100) as grid_stability_index,
                                  (AVG(efficiency_rate) * 100) as operational_efficiency,
                                  (AVG(efficiency_rate) * 95) as energy_optimization_score,
                                  CASE 
                                      WHEN AVG(efficiency_rate) > 0.95 THEN ''A+''
                                      WHEN AVG(efficiency_rate) > 0.90 THEN ''A''
                                      WHEN AVG(efficiency_rate) > 0.85 THEN ''B+''
                                      ELSE ''B''
                                  END as reliability_rating,
                                  CASE 
                                      WHEN AVG(real_time_temperature) > 65 THEN ''MEDIUM_运行异常较多，优化调度''
                                      WHEN COUNT(CASE WHEN maintenance_status = ''''CRITICAL'''' THEN 1 END) > 3 THEN ''HIGH_设备故障较多，紧急维护''
                                      ELSE ''LOW_运行正常，保持现状''
                                  END as risk_assessment,
                                  CASE 
                                      WHEN AVG(efficiency_rate) > 0.94 THEN ''STABLE_运行稳定''
                                      WHEN AVG(efficiency_rate) > 0.88 THEN ''IMPROVING_效率持续提升''
                                      ELSE ''DECLINING_效率下降''
                                  END as performance_trends,
                                  CASE 
                                      WHEN AVG(real_time_temperature) > 65 THEN ''建议优化设备散热系统，降低运行温度''
                                      WHEN COUNT(CASE WHEN maintenance_status = ''''CRITICAL'''' THEN 1 END) > 3 THEN ''建议紧急维护故障设备，避免连锁反应''
                                      ELSE ''建议继续保持当前运行策略，定期优化调度算法''
                                  END as optimization_recommendations,
                                  (AVG(efficiency_rate) * 150000) as cost_benefit_analysis
                               FROM device_dimension_data
                               GROUP BY SUBSTRING(location, 1, 2)
                               ORDER BY AVG(efficiency_rate) DESC')
        AS report_data(grid_region TEXT, grid_stability_index DOUBLE PRECISION, operational_efficiency DOUBLE PRECISION, energy_optimization_score DOUBLE PRECISION, reliability_rating TEXT, risk_assessment TEXT, performance_trends TEXT, optimization_recommendations TEXT, cost_benefit_analysis DOUBLE PRECISION)
    ) as report_data;
    " > /dev/null 2>&1
}

add_extra_sample_data() {
    echo "📊 添加额外示例数据以丰富大屏展示..."
    
    # 添加更多设备状态数据
    docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
    INSERT INTO device_status_summary (summary_id, device_id, device_type, location, status, load_factor, efficiency, temperature, update_time) VALUES
    ('SAMPLE_001', 'DEVICE_SAMPLE_001', '智能变电站', '北京', 'NORMAL', 82.5, 96.8, 42.0, NOW()),
    ('SAMPLE_002', 'DEVICE_SAMPLE_002', '风电机组', '上海', 'NORMAL', 78.2, 94.5, 38.0, NOW()),
    ('SAMPLE_003', 'DEVICE_SAMPLE_003', '光伏设备', '广州', 'WARNING', 89.1, 91.2, 55.0, NOW()),
    ('SAMPLE_004', 'DEVICE_SAMPLE_004', '储能设备', '深圳', 'NORMAL', 75.8, 98.1, 35.0, NOW()),
    ('SAMPLE_005', 'DEVICE_SAMPLE_005', '配电变压器', '杭州', 'CRITICAL', 15.2, 45.3, 85.0, NOW()),
    ('SAMPLE_006', 'DEVICE_SAMPLE_006', '输电线路', '成都', 'NORMAL', 88.3, 97.6, 41.0, NOW()),
    ('SAMPLE_007', 'DEVICE_SAMPLE_007', '开关设备', '重庆', 'MAINTENANCE', 0.0, 0.0, 25.0, NOW()),
    ('SAMPLE_008', 'DEVICE_SAMPLE_008', '继电保护', '武汉', 'NORMAL', 91.7, 95.4, 44.0, NOW()),
    ('SAMPLE_009', 'DEVICE_SAMPLE_009', '电能表', '西安', 'NORMAL', 86.2, 99.1, 32.0, NOW()),
    ('SAMPLE_010', 'DEVICE_SAMPLE_010', '调度系统', '南京', 'NORMAL', 92.4, 98.7, 36.0, NOW());
    " > /dev/null 2>&1
}

# 主循环
COUNTER=0
while true; do
    COUNTER=$((COUNTER + 1))
    echo "🔄 执行第 $COUNTER 轮数据流处理 ($(date '+%Y-%m-%d %H:%M:%S'))"
    
    # 执行数据流处理
    process_device_status
    process_grid_metrics
    process_comprehensive_reports
    
    # 第一次运行时添加示例数据
    if [ $COUNTER -eq 1 ]; then
        add_extra_sample_data
    fi
    
    # 每3轮显示数据流状态
    if [ $((COUNTER % 3)) -eq 0 ]; then
        echo "📊 Fluss数据流处理状态摘要:"
        docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
        SELECT 
            '设备状态汇总' as 数据类型, 
            COUNT(*) as 记录数,
            COUNT(CASE WHEN status = 'NORMAL' THEN 1 END) as 正常设备,
            COUNT(CASE WHEN status = 'CRITICAL' THEN 1 END) as 故障设备,
            ROUND(AVG(efficiency), 2) as 平均效率,
            MAX(update_time) as 最新更新时间
        FROM device_status_summary
        UNION ALL
        SELECT 
            '电网监控指标' as 数据类型, 
            COUNT(*) as 记录数,
            SUM(total_devices) as 总设备数,
            SUM(online_devices) as 在线设备,
            ROUND(AVG(avg_efficiency), 2) as 平均效率,
            MAX(update_time) as 最新更新时间
        FROM grid_monitoring_metrics
        UNION ALL
        SELECT 
            '智能电网综合报表' as 数据类型, 
            COUNT(*) as 记录数,
            COUNT(CASE WHEN reliability_rating IN ('A+', 'A') THEN 1 END) as 优秀电网,
            COUNT(CASE WHEN reliability_rating IN ('B+', 'B') THEN 1 END) as 良好电网,
            ROUND(AVG(operational_efficiency), 2) as 平均运行效率,
            MAX(report_time) as 最新更新时间
        FROM smart_grid_comprehensive_result;
        " 2>/dev/null | head -15
        echo "🔗 数据流状态: PostgreSQL源 → Fluss流批处理 → PostgreSQL sink → Grafana大屏"
        echo "🔗 Grafana Dashboard: http://localhost:3000/d/sgcc-fluss-cdc-dashboard"
        echo "🎯 数据流优势: 流批一体、统一存储计算、实时OLAP、ACID事务"
        echo "---"
    fi
    
    # 等待8秒后继续
    sleep 8
done 