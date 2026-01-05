# 数据库压测工具 (Database Benchmark Tool)

## 📖 项目简介
本项目是一个基于 Spring Boot 开发的轻量级数据库压测工具，旨在帮助开发者和DBA对MySQL和DB2等兼容性数据库进行持续的OLTP（在线事务处理）性能测试。它提供了一个直观的 Web UI，能够实时显示 TPS (Transactions Per Second) 和 QPS (Queries Per Second) 等关键指标，并提供详细的日志输出和压测总结报告。

## ✨ 主要特性
- **多数据库支持**：开箱即用支持 MySQL 和 DB2 数据库。
- **Web 用户界面**：直观、富有科技感的交互式 Web UI，方便配置和监控。
- **连接管理**：支持保存、加载、测试和删除数据库连接配置。
- **实时性能监控**：通过 Server-Sent Events (SSE) 实时展示 TPS 和 QPS。
- **可配置的压测参数**：可灵活配置压测频率和日志采样率。
- **详细日志输出**：实时显示事务日志，可开启/关闭日志抑制。
- **压测总结报告**：压测结束后提供包含总事务数、成功数、失败数及成功率的详细报告。
- **Git 版本管理**：项目已集成 Git 进行版本控制，便于代码管理和发布。

## 🛠️ 技术栈
- **后端**：
  - Java 17
  - Spring Boot 3.x
  - Druid Connection Pool
  - MySQL JDBC Driver
  - DB2 JDBC Driver (JCC)
- **前端**：
  - HTML5, CSS3, JavaScript
  - Bootstrap 5.x
  - Bootstrap Icons

## 🚀 本地运行指南

### 前提条件
- JDK 17 或更高版本
- Maven 3.6.3 或更高版本 (项目已包含 Maven Wrapper, 可直接使用 `./mvnw`)

### 1. 克隆仓库
```bash
git clone https://github.com/bsbforever/database_benchmark.git
cd database_benchmark
```

### 2. 构建项目
```bash
./mvnw clean package
```

### 3. 运行应用程序
```bash
./mvnw spring-boot:run
```
或者，您可以在 IDEA 等 IDE 中直接运行 `DatabaseBenchmarkApplication.java`。

### 4. 访问 Web UI
应用程序启动后，在浏览器中访问：
```
http://localhost:9090
```

## 💡 使用说明

1.  **配置数据库连接**：
    *   在左侧的“控制面板”中，展开“显示/编辑连接详情”。
    *   填写数据库连接信息（别名、地址、端口、数据库名、用户名、密码），并选择正确的“数据库类型”（MySQL 或 DB2）。
    *   点击“测试”按钮验证连接是否成功。
    *   点击“保存”按钮将连接配置持久化。
    *   您也可以通过顶部的下拉框选择已保存的连接。
2.  **设置压测参数**：在左侧“控制面板”中设置“压测频率 (ms)”和“日志采样率”。
3.  **开始/停止压测**：点击“开始压测”按钮启动，点击“停止压测”按钮结束。
4.  **监控与报告**：
    *   中间区域显示实时日志。
    *   右侧区域显示实时 QPS/TPS。
    *   压测停止后，右侧下方会显示详细的总结报告，包括总事务数、成功数、失败数和成功率。

## 🤝 贡献
欢迎提交 Issue 或 Pull Request 来改进本项目。

## 📄 许可证
本项目采用 MIT 许可证。
