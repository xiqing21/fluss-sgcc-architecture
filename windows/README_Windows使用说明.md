# 🚀 SGCC Fluss Windows版本使用说明

## 📋 前提条件

在运行脚本之前，请确保您的Windows系统已安装：

1. **Docker Desktop** - 用于运行容器环境
2. **PowerShell 5.0+** - Windows 10/11默认已安装
3. **Git** (可选) - 用于克隆代码仓库

## 🛠️ 安装步骤

### 1. 安装Docker Desktop

1. 访问 [Docker Desktop官网](https://www.docker.com/products/docker-desktop/)
2. 下载并安装Docker Desktop for Windows
3. 启动Docker Desktop并等待其完全启动

### 2. 验证安装

在PowerShell中运行以下命令验证安装：

```powershell
# 验证Docker
docker --version
docker-compose --version

# 验证PowerShell版本
$PSVersionTable.PSVersion
```

## 🚀 运行测试

### 方法1: 直接运行PowerShell脚本

1. 打开PowerShell（以管理员身份运行推荐）
2. 导航到项目根目录：
   ```powershell
   cd path\to\fluss-sgcc-architecture
   ```
3. 执行脚本：
   ```powershell
   .\windows\一键启动全链路验证测试.ps1
   ```

### 方法2: 使用批处理文件

1. 双击运行 `windows\启动测试.bat`
2. 系统会自动以管理员权限运行PowerShell脚本

### 方法3: 从PowerShell ISE运行

1. 打开PowerShell ISE
2. 打开文件：`windows\一键启动全链路验证测试.ps1`
3. 按F5运行脚本

## 📊 执行流程

脚本将自动执行以下步骤：

1. **🌟 环境启动** - 启动Docker Compose环境
2. **🔍 数据验证** - 验证各层数据同步状态
3. **🎯 业务场景测试** - 运行4个核心业务场景
4. **🔄 增删改测试** - 验证CDC实时同步能力
5. **📈 性能统计** - 生成详细性能报告

## 📄 测试报告

- 测试报告将保存在 `test-reports` 目录下
- 文件名格式：`full_test_report_YYYYMMDD_HHMMSS.md`
- 脚本完成后会询问是否打开报告文件

## ⚠️ 注意事项

### PowerShell执行策略

如果遇到"禁止运行脚本"错误，请在管理员PowerShell中运行：

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### 防火墙设置

- 确保Docker Desktop能够正常访问网络
- 允许PowerShell通过防火墙
- 如有企业防火墙，请联系IT管理员

### 系统资源

建议系统配置：
- **内存**: 至少8GB RAM
- **CPU**: 4核心以上
- **存储**: 至少10GB可用空间

## 🐛 故障排除

### 常见问题

1. **Docker服务未启动**
   ```
   错误: Cannot connect to the Docker daemon
   解决: 启动Docker Desktop并等待其完全加载
   ```

2. **端口冲突**
   ```
   错误: Port already in use
   解决: 停止占用端口的其他服务，或修改docker-compose.yml中的端口配置
   ```

3. **PowerShell版本过低**
   ```
   错误: 语法错误或命令不识别
   解决: 升级到PowerShell 5.0或更高版本
   ```

4. **权限不足**
   ```
   错误: Access denied或权限被拒绝
   解决: 以管理员身份运行PowerShell
   ```

5. **Fluss LogStorageException错误**
   ```
   错误: Table schema not found in zookeeper metadata
   解决: 运行彻底清理脚本 .\windows\彻底清理重启脚本.ps1
   ```

### 日志查看

- 脚本执行过程中的详细日志会同时输出到控制台和测试报告
- 如遇到问题，请查看生成的测试报告中的错误信息

## 📞 技术支持

如遇到技术问题：

1. 查看生成的测试报告中的错误信息
2. 检查Docker容器状态：`docker ps -a`
3. 查看Docker日志：`docker-compose logs`
4. 参考项目主目录下的文档

## 🔧 端口配置说明

实际服务端口配置：
- **Flink Web UI**: http://localhost:8091 (不是8081)
- **PostgreSQL源数据库**: localhost:5442
- **PostgreSQL目标数据库**: localhost:5443
- **MySQL目标数据库**: localhost:3306
- **ZooKeeper**: localhost:2181

## 🔧 自定义配置

如需修改测试参数，可编辑以下配置：

- **等待时间**: 修改脚本中的 `Start-Sleep` 参数
- **测试场景**: 在 `business-scenarios` 目录下添加自定义SQL文件
- **报告路径**: 修改 `$TEST_REPORT_DIR` 变量

---

**版本**: v1.0  
**最后更新**: $(Get-Date -Format "yyyy-MM-dd")  
**兼容性**: Windows 10/11 + PowerShell 5.0+ + Docker Desktop 