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
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

@Service
public class BenchService {

    private static final int SQLS_PER_TX = 10;

    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private DruidDataSource dataSource;
    private String currentDbType;
    private int currentWriteRatio;

    // Concurrency
    private ExecutorService benchmarkExecutor;

    // Performance Counters (upgraded to LongAdder for high-concurrency)
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

    public void startBenchmark(Long intervalMs, Integer sampleRate, Integer writeRatio, Integer threadCount,
                               String ip, String port, String dbName, String user, String pass, String dbType) {
        if (isRunning.getAndSet(true)) return;

        // Set benchmark parameters with defaults
        long actualInterval = (intervalMs != null && intervalMs >= 0) ? intervalMs : defaultInterval;
        int actualSampleRate = (sampleRate != null && sampleRate > 0) ? sampleRate : defaultSampleRate;
        int actualThreadCount = (threadCount != null && threadCount > 0) ? threadCount : 16;
        this.currentDbType = StringUtils.hasText(dbType) ? dbType : "MySQL";
        this.currentWriteRatio = (writeRatio != null && writeRatio >= 0 && writeRatio <= 10) ? writeRatio : 2;

        // Set DB connection parameters with defaults
        String targetIp = StringUtils.hasText(ip) ? ip : defaultIp;
        String targetPort = StringUtils.hasText(port) ? port : defaultPort;
        String targetDb = StringUtils.hasText(dbName) ? dbName : defaultDbName;
        String targetUser = StringUtils.hasText(user) ? user : defaultUsername;
        String targetPass = StringUtils.hasText(pass) ? pass : defaultPassword;

        // Reset all counters
        successTxCount.reset();
        failedTxCount.reset();
        totalSqlCount.reset();
        lastSuccessTxCount = 0;
        lastFailedTxCount = 0;
        lastTotalSqlCount = 0;

        // Main benchmark logic starts in a new thread to avoid blocking the web server
        new Thread(() -> {
            try {
                sendLog("LOG", String.format("🔄 正在连接 %s 数据库 [%s:%s]...", this.currentDbType, targetIp, targetPort));
                if (this.dataSource != null && !this.dataSource.isClosed()) {
                    this.dataSource.close();
                }
                this.dataSource = createDataSource(targetIp, targetPort, targetDb, targetUser, targetPass, this.currentDbType, actualThreadCount);
                initSchema();

                sendLog("LOG", "✅ 连接成功，表结构已就绪。开始压测...");
                sendLog("LOG", String.format("🚀 目标: %s:%s | 并发数: %d | 频率: %dms | 读写比: %d:%d",
                        targetIp, targetPort, actualThreadCount, actualInterval, (10 - this.currentWriteRatio), this.currentWriteRatio));

                // Start metric monitoring
                startMonitor();
                
                // Start benchmark worker threads
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
                                    Thread.currentThread().interrupt(); // Preserve interrupt status
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

        // Shutdown all services
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

            // Gracefully close client connections
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

        int numWrites = this.currentWriteRatio;
        int numReads = 10 - numWrites;

        String nowFunction = "DB2".equals(currentDbType) ? "CURRENT_TIMESTAMP" : "NOW()";
        String replaceSql, updateSql;

        if ("DB2".equals(currentDbType)) {
            replaceSql = "MERGE INTO bench_test AS t " +
                         "USING (VALUES (?, " + nowFunction + ")) AS s(id, create_time) " +
                         "ON t.id = s.id " +
                         "WHEN MATCHED THEN UPDATE SET t.create_time = s.create_time " +
                         "WHEN NOT MATCHED THEN INSERT (id, create_time) VALUES (s.id, s.create_time)";
            updateSql = "UPDATE bench_test SET create_time = " + nowFunction + " WHERE id = ?";
        } else {
            replaceSql = "REPLACE INTO bench_test (id, create_time) VALUES (?, " + nowFunction + ")";
            updateSql = "UPDATE bench_test SET create_time = " + nowFunction + " WHERE id = ?";
        }

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                for (int i = 0; i < numReads; i++) {
                    try (PreparedStatement stmt = conn.prepareStatement("SELECT id FROM bench_test WHERE id = ?")) {
                        stmt.setLong(1, currentId);
                        stmt.executeQuery();
                        totalSqlCount.increment();
                    }
                }

                for (int i = 0; i < numWrites; i++) {
                    if (i % 2 == 0) {
                        try (PreparedStatement stmt = conn.prepareStatement(replaceSql)) {
                            stmt.setLong(1, currentId);
                            stmt.executeUpdate();
                            totalSqlCount.increment();
                        }
                    } else {
                        try (PreparedStatement stmt = conn.prepareStatement(updateSql)) {
                            stmt.setLong(1, currentId);
                            stmt.executeUpdate();
                            totalSqlCount.increment();
                        }
                    }
                }
                conn.commit();
                successTxCount.increment();
            } catch (SQLException ex) {
                totalSqlCount.increment(); // Count the failed SQL
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
        String ddl;
        if ("DB2".equals(currentDbType)) {
            ddl = "CREATE TABLE bench_test (" +
                  "id BIGINT PRIMARY KEY NOT NULL, " +
                  "create_time TIMESTAMP" +
                  ")";
        } else {
            ddl = "CREATE TABLE IF NOT EXISTS bench_test (" +
                  "id BIGINT PRIMARY KEY, " +
                  "create_time DATETIME" +
                  ")";
        }

        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            boolean tableExists = false;
            if ("DB2".equals(currentDbType)) {
                try (var rs = conn.getMetaData().getTables(null, null, "BENCH_TEST", null)) {
                    if (rs.next()) {
                        tableExists = true;
                    }
                }
            }

            if (!tableExists) {
                stmt.execute(ddl);
            }

            String truncateSql = "DB2".equals(currentDbType) ? "TRUNCATE TABLE bench_test IMMEDIATE" : "TRUNCATE TABLE bench_test";
            stmt.execute(truncateSql);
        } catch (SQLException e) {
            throw new RuntimeException("Schema 初始化失败: " + e.getMessage(), e);
        }
    }

    private void generateReport() {
        long s = successTxCount.sum();
        long f = failedTxCount.sum();
        String reportJson = String.format("{\"total\": %d, \"success\": %d, \"fail\": %d}", s + f, s, f);
        sendLog("SUMMARY", reportJson);
    }
}