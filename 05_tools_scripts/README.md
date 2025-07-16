# 05_tools_scripts - 工具脚本集合

本目录包含项目中使用的各种工具脚本，按功能分类提供自动化支持。

## 目录结构

### core - 核心工具脚本
- 基础功能脚本
- 常用工具函数
- 核心业务逻辑

### validation - 验证和测试脚本
- 数据验证脚本
- 功能测试脚本
- 性能测试工具

### demos - 演示脚本
- 特性演示脚本
- 教学示例脚本
- 快速演示工具

## 主要脚本

### 核心工具脚本
- `smart_sql_execution.sh` - 智能SQL执行脚本
  - 解决会话终止问题
  - 支持交互式会话
  - 智能错误处理
  
- `start_sgcc_fluss.sh` - 国网Fluss启动脚本
  - 一键启动服务
  - 环境检查
  - 依赖管理

- `complete_test_cycle.sh` - 完整测试周期脚本
- `quick_verify_data.sh` - 快速数据验证脚本
- `interactive_sql_session.sh` - 交互式SQL会话脚本

### 验证和测试脚本
- `一键启动全链路验证测试.sh` - 全链路验证
- `execute_fluss_sql_scripts.sh` - Fluss SQL脚本执行
- `fluss_realtime_validation_test.sh` - 实时验证测试
- `sgcc_validation_test.sh` - 国网场景验证

## 脚本特点

### 1. 智能化
- 自动环境检测
- 智能错误处理
- 自适应配置

### 2. 模块化
- 功能独立
- 可重用组件
- 易于维护

### 3. 用户友好
- 清晰的日志输出
- 进度显示
- 错误提示

### 4. 健壮性
- 异常处理
- 回滚机制
- 状态检查

## 使用方法

### 快速启动
```bash
# 启动基础服务
./core/start_sgcc_fluss.sh

# 执行SQL脚本
./core/smart_sql_execution.sh ../03_sql_scripts/fluss/2_customer_service_system.sql

# 验证数据
./core/quick_verify_data.sh
```

### 验证测试
```bash
# 全链路验证
./validation/一键启动全链路验证测试.sh

# 实时验证
./validation/fluss_realtime_validation_test.sh

# 国网场景验证
./validation/sgcc_validation_test.sh
```

## 最佳实践

### 1. 脚本执行前检查
- 确保环境正常
- 检查依赖服务
- 验证权限配置

### 2. 参数配置
- 使用配置文件
- 支持环境变量
- 提供默认值

### 3. 日志记录
- 详细的执行日志
- 错误信息记录
- 性能指标监控

### 4. 错误处理
- 优雅的错误处理
- 清晰的错误提示
- 自动恢复机制

## 开发规范

### 脚本结构
```bash
#!/bin/bash
# 脚本说明
# 作者信息
# 使用方法

# 配置变量
CONFIG_FILE="config.conf"
LOG_FILE="script.log"

# 工具函数
function log_info() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [INFO] $1" | tee -a ${LOG_FILE}
}

function log_error() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [ERROR] $1" | tee -a ${LOG_FILE}
}

# 主要逻辑
main() {
    log_info "开始执行脚本"
    # 业务逻辑
    log_info "脚本执行完成"
}

# 入口点
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
```

### 命名规范
- 使用描述性名称
- 遵循下划线命名
- 添加适当前缀

### 文档要求
- 脚本头部注释
- 函数说明
- 使用示例
- 注意事项

## 故障排除

### 常见问题
1. **权限问题**：确保脚本有执行权限
2. **路径问题**：使用绝对路径或相对路径
3. **依赖问题**：检查所需服务是否启动
4. **配置问题**：验证配置文件和环境变量

### 调试技巧
- 使用 `set -x` 开启调试模式
- 检查脚本执行日志
- 分步执行定位问题
- 使用 `shellcheck` 检查语法

## 维护和更新

- 定期更新脚本功能
- 优化性能和稳定性
- 添加新的工具脚本
- 保持向后兼容性 