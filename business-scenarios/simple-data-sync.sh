#!/bin/bash

# 简单的数据同步脚本 - 模拟CDC效果
# 从PostgreSQL源数据库同步数据到sink数据库

echo "🔄 启动数据同步脚本..."
echo "数据流：PostgreSQL源 → 数据处理 → PostgreSQL sink"
echo "按 Ctrl+C 停止同步"
echo "=========================================="

# 同步函数
sync_device_data() {
    echo "📊 同步设备状态数据..."
    
    # 清空旧数据
    docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "DELETE FROM device_status_summary WHERE summary_id LIKE 'SYNC_%';" > /dev/null 2>&1
    
    # 从源数据库获取设备数据并同步到sink
    docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
    INSERT INTO device_status_summary (summary_id, device_id, device_type, location, status, load_factor, efficiency, temperature, update_time)
    SELECT 
        'SYNC_' || device_data.device_id || '_' || EXTRACT(EPOCH FROM NOW()) as summary_id,
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
            WHEN device_data.capacity_mw > 0 THEN (device_data.real_time_current / device_data.capacity_mw) * 100
            ELSE 0
        END as load_factor,
        device_data.efficiency_rate * 100 as efficiency,
        device_data.real_time_temperature as temperature,
        NOW() as update_time
    FROM (
        SELECT * FROM dblink('host=postgres-sgcc-source port=5432 dbname=sgcc_source_db user=sgcc_user password=sgcc_pass_2024',
                              'SELECT device_id, device_type, location, capacity_mw, efficiency_rate, maintenance_status, real_time_current, real_time_temperature FROM device_dimension_data ORDER BY device_id LIMIT 20')
        AS device_data(device_id TEXT, device_type TEXT, location TEXT, capacity_mw DOUBLE PRECISION, efficiency_rate DOUBLE PRECISION, maintenance_status TEXT, real_time_current DOUBLE PRECISION, real_time_temperature DOUBLE PRECISION)
    ) as device_data
    ON CONFLICT (summary_id) DO UPDATE SET 
        device_type = EXCLUDED.device_type,
        location = EXCLUDED.location,
        status = EXCLUDED.status,
        load_factor = EXCLUDED.load_factor,
        efficiency = EXCLUDED.efficiency,
        temperature = EXCLUDED.temperature,
        update_time = EXCLUDED.update_time;
    " > /dev/null 2>&1
}

sync_grid_metrics() {
    echo "📈 同步电网监控指标..."
    
    # 清空旧数据
    docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "DELETE FROM grid_monitoring_metrics WHERE metric_id LIKE 'SYNC_%';" > /dev/null 2>&1
    
    # 同步电网监控指标
    docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
    INSERT INTO grid_monitoring_metrics (metric_id, grid_region, total_devices, online_devices, offline_devices, maintenance_devices, avg_efficiency, avg_load_factor, avg_temperature, alert_count, update_time)
    SELECT 
        'SYNC_' || grid_stats.grid_region || '_' || EXTRACT(EPOCH FROM NOW()) as metric_id,
        grid_stats.grid_region,
        grid_stats.total_devices,
        grid_stats.online_devices,
        grid_stats.offline_devices,
        grid_stats.maintenance_devices,
        grid_stats.avg_efficiency,
        grid_stats.avg_load_factor,
        grid_stats.avg_temperature,
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
                                  AVG(CASE WHEN capacity_mw > 0 THEN (real_time_current / capacity_mw) * 100 ELSE 0 END) as avg_load_factor,
                                  AVG(real_time_temperature) as avg_temperature,
                                  COUNT(CASE WHEN real_time_temperature > 70 THEN 1 END) as alert_count
                               FROM device_dimension_data
                               GROUP BY SUBSTRING(location, 1, 2)
                               ORDER BY COUNT(*) DESC
                               LIMIT 10')
        AS grid_stats(grid_region TEXT, total_devices BIGINT, online_devices BIGINT, offline_devices BIGINT, maintenance_devices BIGINT, avg_efficiency DOUBLE PRECISION, avg_load_factor DOUBLE PRECISION, avg_temperature DOUBLE PRECISION, alert_count BIGINT)
    ) as grid_stats
    ON CONFLICT (metric_id) DO UPDATE SET 
        total_devices = EXCLUDED.total_devices,
        online_devices = EXCLUDED.online_devices,
        offline_devices = EXCLUDED.offline_devices,
        maintenance_devices = EXCLUDED.maintenance_devices,
        avg_efficiency = EXCLUDED.avg_efficiency,
        avg_load_factor = EXCLUDED.avg_load_factor,
        avg_temperature = EXCLUDED.avg_temperature,
        alert_count = EXCLUDED.alert_count,
        update_time = EXCLUDED.update_time;
    " > /dev/null 2>&1
}

sync_comprehensive_reports() {
    echo "📋 同步综合分析报表..."
    
    # 清空旧数据
    docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "DELETE FROM smart_grid_comprehensive_result WHERE report_id LIKE 'SYNC_%';" > /dev/null 2>&1
    
    # 同步综合分析报表
    docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
    INSERT INTO smart_grid_comprehensive_result (report_id, report_type, analysis_period, grid_region, grid_stability_index, operational_efficiency, energy_optimization_score, reliability_rating, risk_assessment, performance_trends, optimization_recommendations, cost_benefit_analysis, report_time)
    SELECT 
        'SYNC_' || report_data.grid_region || '_' || EXTRACT(EPOCH FROM NOW()) as report_id,
        '智能电网综合运行报表' as report_type,
        TO_CHAR(NOW(), 'YYYYMMDDHH24') as analysis_period,
        report_data.grid_region,
        report_data.grid_stability_index,
        report_data.operational_efficiency,
        report_data.energy_optimization_score,
        report_data.reliability_rating,
        report_data.risk_assessment,
        report_data.performance_trends,
        report_data.optimization_recommendations,
        report_data.cost_benefit_analysis,
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
                                      ELSE ''LOW_运行正常，保持现状''
                                  END as risk_assessment,
                                  CASE 
                                      WHEN AVG(efficiency_rate) > 0.94 THEN ''STABLE_运行稳定''
                                      WHEN AVG(efficiency_rate) > 0.88 THEN ''IMPROVING_效率持续提升''
                                      ELSE ''DECLINING_效率下降''
                                  END as performance_trends,
                                  CASE 
                                      WHEN AVG(real_time_temperature) > 65 THEN ''建议优化设备散热系统，降低运行温度''
                                      ELSE ''建议继续保持当前运行策略，定期优化调度算法''
                                  END as optimization_recommendations,
                                  (AVG(efficiency_rate) * 150000) as cost_benefit_analysis
                               FROM device_dimension_data
                               GROUP BY SUBSTRING(location, 1, 2)
                               ORDER BY AVG(efficiency_rate) DESC
                               LIMIT 10')
        AS report_data(grid_region TEXT, grid_stability_index DOUBLE PRECISION, operational_efficiency DOUBLE PRECISION, energy_optimization_score DOUBLE PRECISION, reliability_rating TEXT, risk_assessment TEXT, performance_trends TEXT, optimization_recommendations TEXT, cost_benefit_analysis DOUBLE PRECISION)
    ) as report_data
    ON CONFLICT (report_id) DO UPDATE SET 
        grid_stability_index = EXCLUDED.grid_stability_index,
        operational_efficiency = EXCLUDED.operational_efficiency,
        energy_optimization_score = EXCLUDED.energy_optimization_score,
        reliability_rating = EXCLUDED.reliability_rating,
        risk_assessment = EXCLUDED.risk_assessment,
        performance_trends = EXCLUDED.performance_trends,
        optimization_recommendations = EXCLUDED.optimization_recommendations,
        cost_benefit_analysis = EXCLUDED.cost_benefit_analysis,
        report_time = EXCLUDED.report_time;
    " > /dev/null 2>&1
}

# 主循环
COUNTER=0
while true; do
    COUNTER=$((COUNTER + 1))
    echo "🔄 执行第 $COUNTER 轮数据同步 ($(date '+%Y-%m-%d %H:%M:%S'))"
    
    # 执行同步
    sync_device_data
    sync_grid_metrics
    sync_comprehensive_reports
    
    # 每5轮显示同步状态
    if [ $((COUNTER % 5)) -eq 0 ]; then
        echo "📊 数据同步状态摘要:"
        docker exec postgres-sgcc-sink psql -U sgcc_user -d sgcc_dw_db -c "
        SELECT 
            'SYNC设备状态' as 数据类型, 
            COUNT(*) as 记录数,
            MAX(update_time) as 最新更新时间
        FROM device_status_summary WHERE summary_id LIKE 'SYNC_%'
        UNION ALL
        SELECT 
            'SYNC电网监控' as 数据类型, 
            COUNT(*) as 记录数,
            MAX(update_time) as 最新更新时间
        FROM grid_monitoring_metrics WHERE metric_id LIKE 'SYNC_%'
        UNION ALL
        SELECT 
            'SYNC综合报表' as 数据类型, 
            COUNT(*) as 记录数,
            MAX(report_time) as 最新更新时间
        FROM smart_grid_comprehensive_result WHERE report_id LIKE 'SYNC_%';
        " 2>/dev/null | head -10
        echo "🔗 数据流状态: PostgreSQL源 → 数据处理 → PostgreSQL sink → Grafana"
        echo "🔗 Grafana Dashboard: http://localhost:3000/d/sgcc-fluss-cdc-dashboard"
        echo "---"
    fi
    
    # 等待10秒后继续
    sleep 10
done 