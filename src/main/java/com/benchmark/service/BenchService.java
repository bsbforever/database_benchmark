package com.benchmark.service;

import com.alibaba.druid.pool.DruidDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map; // 新增
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.ConcurrentHashMap; // 新增

@Service
public class BenchService {

    private static final int SQLS_PER_TX = 10; // 每个事务10个SQL

    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private DruidDataSource dataSource;
    private String currentDbType;
    private String currentScenario;

    // Concurrency
    private ExecutorService benchmarkExecutor;
    private long benchmarkStartTime;

    // Performance Counters
    private final LongAdder successTxCount = new LongAdder();
    private final LongAdder failedTxCount = new LongAdder();
    private final LongAdder totalSqlCount = new LongAdder();
    private long lastSuccessTxCount = 0;
    private long lastFailedTxCount = 0;
    private long lastTotalSqlCount = 0;

    private ScheduledExecutorService statsScheduler;

    // Default configs from application.properties
    @Value("${ob.target.ip}") private String defaultIp;
    @Value("${ob.target.port}") private String defaultPort;
    @Value("${ob.target.dbname}") private String defaultDbName;
    @Value("${ob.target.username}") private String defaultUsername;
    @Value("${ob.target.password}") private String defaultPassword;
    @Value("${ob.target.connect-timeout:1000}") private String connectTimeout;
    @Value("${ob.target.socket-timeout:3000}") private String socketTimeout;
    @Value("${ob.target.interval:100}") private long defaultInterval;
    @Value("${ob.target.log-sample-rate:10}") private int defaultSampleRate;
    @Value("${ob.target.data-range:100000}") private int defaultDataRange;

    public void startBenchmark(Long intervalMs, Integer sampleRate, String scenario, Integer threadCount,
                               String ip, String port, String dbName, String user, String pass, String dbType) {
        if (isRunning.getAndSet(true)) return;
        this.benchmarkStartTime = System.currentTimeMillis();

        long actualInterval = (intervalMs != null && intervalMs >= 0) ? intervalMs : defaultInterval;
        int actualSampleRate = (sampleRate != null && sampleRate > 0) ? sampleRate : defaultSampleRate;
        int actualThreadCount = (threadCount != null && threadCount > 0) ? threadCount : 16;
        this.currentDbType = StringUtils.hasText(dbType) ? dbType : "MySQL";
        this.currentScenario = StringUtils.hasText(scenario) ? scenario : "READ_HEAVY";

        String targetIp = StringUtils.hasText(ip) ? ip : defaultIp;
        String targetPort = StringUtils.hasText(port) ? port : defaultPort;
        String targetDb = StringUtils.hasText(dbName) ? dbName : defaultDbName;
        String targetUser = StringUtils.hasText(user) ? user : defaultUsername;
        String targetPass = StringUtils.hasText(pass) ? pass : defaultPassword;

        successTxCount.reset();
        failedTxCount.reset();
        totalSqlCount.reset();
        lastSuccessTxCount = 0;
        lastFailedTxCount = 0;
        lastTotalSqlCount = 0;

        new Thread(() -> {
            try {
                sendLog("LOG", String.format("🔄 正在连接 %s 数据库 [%s:%s]...", this.currentDbType, targetIp, targetPort));
                if (this.dataSource != null && !this.dataSource.isClosed()) {
                    this.dataSource.close();
                }
                this.dataSource = createDataSource(targetIp, targetPort, targetDb, targetUser, targetPass, this.currentDbType, actualThreadCount);
                initSchema();

                sendLog("LOG", "✅ 连接成功，表结构已就绪。开始压测...");
                sendLog("LOG", String.format("🚀 目标: %s:%s | 场景: %s | 并发数: %d | 频率: %dms",
                        targetIp, targetPort, this.currentScenario, actualThreadCount, actualInterval));

                startMonitor();
                
                benchmarkExecutor = Executors.newFixedThreadPool(actualThreadCount);
                for (int i = 0; i < actualThreadCount; i++) {
                    benchmarkExecutor.submit(() -> {
                        long seq = 0;
                        while (isRunning.get()) {
                            seq++;
                            probeDatabase(seq, actualSampleRate);
                            if (actualInterval > 0) {
                                try {
                                    TimeUnit.MILLISECONDS.sleep(actualInterval);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                        }
                    });
                }
            } catch (Exception e) {
                isRunning.set(false);
                sendLog("LOG", "<span style='color:red'>❌ 启动失败: " + e.getMessage() + "</span>");
                if (dataSource != null) dataSource.close();
                if (statsScheduler != null) statsScheduler.shutdownNow();
            }
        }).start();
    }

    private void startMonitor() {
        statsScheduler = Executors.newSingleThreadScheduledExecutor();
        statsScheduler.scheduleAtFixedRate(this::calculateAndPushStats, 1, 1, TimeUnit.SECONDS);
    }

    public void stopBenchmark() {
        if (!isRunning.getAndSet(false)) return;

        if (benchmarkExecutor != null) benchmarkExecutor.shutdownNow();
        if (statsScheduler != null) statsScheduler.shutdownNow();

        new Thread(() -> {
            try {
                if (benchmarkExecutor != null) {
                    benchmarkExecutor.awaitTermination(5, TimeUnit.SECONDS);
                }
            } catch (InterruptedException ignored) {}
            
            if (dataSource != null && !dataSource.isClosed()) {
                dataSource.close();
            }
            generateReport();
            sendLog("LOG", "🛑 压测已停止，连接已释放。");

            for (SseEmitter emitter : emitters) {
                try {
                    emitter.complete();
                } catch (Exception ignored) {}
            }
            emitters.clear();
        }).start();
    }

    public SseEmitter subscribe() {
        SseEmitter emitter = new SseEmitter(0L);
        emitters.add(emitter);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError((e) -> emitters.remove(emitter));
        return emitter;
    }

    private void calculateAndPushStats() {
        long currentSuccessTx = successTxCount.sum();
        long currentFailedTx = failedTxCount.sum();
        long currentTotalSql = totalSqlCount.sum();

        long deltaSuccessTx = currentSuccessTx - lastSuccessTxCount;
        long deltaSql = currentTotalSql - lastTotalSqlCount;

        long tps = deltaSuccessTx;
        long qps = deltaSql;

        lastSuccessTxCount = currentSuccessTx;
        lastFailedTxCount = currentFailedTx;
        lastTotalSqlCount = currentTotalSql;

        String statsJson = String.format(
                "{\"tps\": %d, \"qps\": %d, \"totalSuccess\": %d, \"totalFail\": %d}",
                tps, qps, currentSuccessTx, currentFailedTx);
        sendLog("STATS", statsJson);
    }

    private void probeDatabase(long seq, int sampleRate) {
        long start = System.currentTimeMillis();
        String status = "OK", color = "#4caf50", errorMsg = "";
        boolean isSuccess = true;
        long currentId = ThreadLocalRandom.current().nextLong(defaultDataRange);
        if (currentId == 0) currentId = 1;

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                switch (this.currentScenario) {
                    case "READ_ONLY": // 10R, 0W on products
                        for (int i = 0; i < 10; i++) {
                            try (PreparedStatement stmt = conn.prepareStatement("SELECT name, stock FROM bench_products WHERE id = ?")) {
                                stmt.setLong(1, currentId);
                                stmt.executeQuery();
                                totalSqlCount.increment();
                            }
                        }
                        break;
                    case "RW_2_8": // 2R, 8W on orders and products
                        // Read product and user
                        try (PreparedStatement stmt = conn.prepareStatement("SELECT stock FROM bench_products WHERE id = ?")) { stmt.setLong(1, currentId); stmt.executeQuery(); totalSqlCount.increment(); }
                        try (PreparedStatement stmt = conn.prepareStatement("SELECT name FROM bench_users WHERE id = ?")) { stmt.setLong(1, currentId); stmt.executeQuery(); totalSqlCount.increment(); }
                        // Write orders and update stock
                        for (int i=0; i<4; i++) {
                            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO bench_orders (user_id, product_id, order_time) VALUES (?, ?, CURRENT_TIMESTAMP)")) {
                                stmt.setLong(1, currentId); stmt.setLong(2, currentId); stmt.executeUpdate(); totalSqlCount.increment();
                            }
                            try (PreparedStatement stmt = conn.prepareStatement("UPDATE bench_products SET stock = stock - 1 WHERE id = ?")) {
                                stmt.setLong(1, currentId);
                                stmt.executeUpdate();
                                totalSqlCount.increment();
                            }
                        }
                        break;
                    case "RW_5_5": // 5R, 5W mixed
                        // 5 reads
                        try (PreparedStatement stmt = conn.prepareStatement("SELECT * FROM bench_users WHERE id = ?")) { stmt.setLong(1, currentId); stmt.executeQuery(); totalSqlCount.increment(); }
                        try (PreparedStatement stmt = conn.prepareStatement("SELECT * FROM bench_products WHERE id = ?")) { stmt.setLong(1, currentId); stmt.executeQuery(); totalSqlCount.increment(); }
                        try (PreparedStatement stmt = conn.prepareStatement("SELECT * FROM bench_orders WHERE user_id = ?")) { stmt.setLong(1, currentId); stmt.executeQuery(); totalSqlCount.increment(); }
                        try (PreparedStatement stmt = conn.prepareStatement("SELECT name FROM bench_users WHERE id = ?")) { stmt.setLong(1, currentId); stmt.executeQuery(); totalSqlCount.increment(); }
                        try (PreparedStatement stmt = conn.prepareStatement("SELECT name FROM bench_products WHERE id = ?")) { stmt.setLong(1, currentId); stmt.executeQuery(); totalSqlCount.increment(); }
                        // 5 writes
                        try (PreparedStatement stmt = conn.prepareStatement("UPDATE bench_users SET name = 'user_' || ? WHERE id = ?")) { stmt.setLong(1, currentId); stmt.setLong(2, currentId); stmt.executeUpdate(); totalSqlCount.increment(); }
                        try (PreparedStatement stmt = conn.prepareStatement("UPDATE bench_products SET stock = stock + 1 WHERE id = ?")) { stmt.setLong(1, currentId); stmt.executeUpdate(); totalSqlCount.increment(); }
                        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO bench_orders (user_id, product_id, order_time) VALUES (?, ?, CURRENT_TIMESTAMP)")) { stmt.setLong(1, currentId); stmt.setLong(2, currentId); stmt.executeUpdate(); totalSqlCount.increment(); }
                        try (PreparedStatement stmt = conn.prepareStatement("UPDATE bench_products SET stock = stock - 1 WHERE id = ?")) { stmt.setLong(1, currentId); stmt.executeUpdate(); totalSqlCount.increment(); }
                        try (PreparedStatement stmt = conn.prepareStatement("UPDATE bench_users SET name = 'user_' || ? WHERE id = ?")) { stmt.setLong(1, currentId); stmt.setLong(2, currentId); stmt.executeUpdate(); totalSqlCount.increment(); }
                        break;
                    case "RW_8_2": // 8R, 2W on users
                    default:
                        for (int i = 0; i < 4; i++) {
                            try (PreparedStatement stmt = conn.prepareStatement("SELECT name, extra_info FROM bench_users WHERE id = ?")) {
                                stmt.setLong(1, currentId);
                                stmt.executeQuery();
                                totalSqlCount.increment();
                            }
                            try (PreparedStatement stmt = conn.prepareStatement("SELECT id, name FROM bench_users WHERE id = ?")) {
                                stmt.setLong(1, currentId);
                                stmt.executeQuery();
                                totalSqlCount.increment();
                            }
                        }
                        for (int i = 0; i < 2; i++) {
                             try (PreparedStatement stmt = conn.prepareStatement("UPDATE bench_users SET name = 'user_' || ? WHERE id = ?")) {
                                stmt.setLong(1, currentId);
                                stmt.setLong(2, currentId);
                                stmt.executeUpdate();
                                totalSqlCount.increment();
                            }
                        }
                        break;
                }
                conn.commit();
                successTxCount.increment();
            } catch (SQLException ex) {
                totalSqlCount.increment();
                try { conn.rollback(); } catch (SQLException ignored) {}
                throw ex;
            }
        } catch (Exception e) {
            isSuccess = false;
            failedTxCount.increment();
            status = "FAIL";
            color = "#ff5252";
            errorMsg = (e instanceof SQLException) ? "[" + ((SQLException)e).getErrorCode() + "] " + e.getMessage() : e.getMessage();
        }

        if (!isSuccess || seq % sampleRate == 0) {
            String logHtml = String.format(
                    "<div style='color:%s; font-family:monospace; border-bottom:1px solid #333; padding:2px; font-size:13px;'>" +
                            "<span style='display:inline-block; width:80px;'>[TX-%05d]</span> " +
                            "<span style='font-weight:bold;'>%s</span> " +
                            "耗时:<span style='display:inline-block; width:60px;'>%-4dms</span> %s</div>",
                    color, seq, status.equals("OK") ? "✅" : "❌", (System.currentTimeMillis() - start), errorMsg);
            sendLog("LOG", logHtml);
        }
    }

    private void sendLog(String eventName, String data) {
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event().name(eventName).data(data));
            } catch (IOException ignored) {
                emitters.remove(emitter);
            }
        }
    }

    private DruidDataSource createDataSource(String ip, String port, String dbName, String username, String password, String dbType, int threadCount) throws SQLException {
        DruidDataSource tempDataSource = new DruidDataSource();
        try {
            String url;
            if ("DB2".equals(dbType)) {
                url = String.format("jdbc:db2://%s:%s/%s", ip, port, dbName);
                tempDataSource.setDriverClassName("com.ibm.db2.jcc.DB2Driver");
            } else { // Default to MySQL
                url = String.format("jdbc:mysql://%s:%s/%s?useSSL=false&serverTimezone=Asia/Shanghai&allowMultiQueries=true",
                        ip, port, dbName);
            }
            tempDataSource.setUrl(url);
            tempDataSource.setUsername(username);
            tempDataSource.setPassword(password);
            tempDataSource.setInitialSize(threadCount);
            tempDataSource.setMaxActive(threadCount);
            tempDataSource.setMinIdle(threadCount);
            tempDataSource.setMaxWait(5000);

            if ("DB2".equals(dbType)) {
                tempDataSource.setValidationQuery("VALUES 1");
            } else {
                tempDataSource.setValidationQuery("SELECT 1");
            }
            
            tempDataSource.setTestOnBorrow(true);
            tempDataSource.init();
            try (Connection conn = tempDataSource.getConnection()) {
                // Connection successful
            }
            return tempDataSource;
        } catch (SQLException e) {
            if (tempDataSource != null) tempDataSource.close();
            throw e;
        }
    }

    public String testConnection(String ip, String port, String dbName, String user, String pass, String dbType) {
        String targetIp = StringUtils.hasText(ip) ? ip : defaultIp;
        String targetPort = StringUtils.hasText(port) ? port : defaultPort;
        String targetDb = StringUtils.hasText(dbName) ? dbName : defaultDbName;
        String targetUser = StringUtils.hasText(user) ? user : defaultUsername;
        String targetPass = StringUtils.hasText(pass) ? pass : defaultPassword;
        String targetDbType = StringUtils.hasText(dbType) ? dbType : "MySQL";

        DruidDataSource testDataSource = null;
        try {
            testDataSource = createDataSource(targetIp, targetPort, targetDb, targetUser, targetPass, targetDbType, 1);
            return "连接成功！";
        } catch (SQLException e) {
            return "连接失败: " + e.getMessage();
        } finally {
            if (testDataSource != null) testDataSource.close();
        }
    }

    private void initSchema() throws SQLException {
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            String timeType = "DB2".equals(currentDbType) ? "TIMESTAMP" : "DATETIME";
            String identityClause = "DB2".equals(currentDbType) ? "GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)" : "AUTO_INCREMENT";

            Map<String, String> tables = new LinkedHashMap<>();
            tables.put("bench_test", String.format("CREATE TABLE bench_test (id BIGINT PRIMARY KEY, create_time %s)", timeType));
            tables.put("bench_users", String.format("CREATE TABLE bench_users (id BIGINT PRIMARY KEY %s, name VARCHAR(255), extra_info CLOB)", identityClause));
            tables.put("bench_products", String.format("CREATE TABLE bench_products (id BIGINT PRIMARY KEY %s, name VARCHAR(255), stock INT)", identityClause));
            tables.put("bench_orders", String.format("CREATE TABLE bench_orders (id BIGINT PRIMARY KEY %s, user_id BIGINT, product_id BIGINT, order_time %s)", identityClause, timeType));

            for (Map.Entry<String, String> entry : tables.entrySet()) {
                String tableName = entry.getKey();
                String ddl = entry.getValue();

                boolean tableExists = false;
                // For DB2, table names are typically uppercase in catalog
                String catalogTableName = "DB2".equals(currentDbType) ? tableName.toUpperCase() : tableName;
                try (var rs = conn.getMetaData().getTables(null, null, catalogTableName, null)) {
                    if (rs.next()) {
                        tableExists = true;
                    }
                }

                if (!tableExists) {
                    stmt.execute(ddl);
                    sendLog("LOG", String.format("📄 表 '%s' 不存在，已自动创建。", tableName));
                }

                String truncateSql = "DB2".equals(currentDbType) ? "TRUNCATE TABLE " + tableName + " IMMEDIATE" : "TRUNCATE TABLE " + tableName;
                stmt.execute(truncateSql);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Schema 初始化失败: " + e.getMessage(), e);
        }
    }

    private void generateReport() {
        long durationMillis = System.currentTimeMillis() - benchmarkStartTime;
        double durationSeconds = durationMillis / 1000.0;
        if (durationSeconds == 0) durationSeconds = 1; // Avoid division by zero

        long s = successTxCount.sum();
        long f = failedTxCount.sum();
        long totalSql = totalSqlCount.sum();

        double avgTps = s / durationSeconds;
        double avgQps = totalSql / durationSeconds;

        String reportJson = String.format(
            "{\"total\": %d, \"success\": %d, \"fail\": %d, \"avgTps\": %.2f, \"avgQps\": %.2f}",
            s + f, s, f, avgTps, avgQps);
        sendLog("SUMMARY", reportJson);
    }
}