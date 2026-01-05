package com.benchmark.service;

import com.alibaba.druid.pool.DruidDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;



@Service

public class BenchService {



    private static final int SQLS_PER_TX = 5;



    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    private DruidDataSource dataSource;

    private String currentDbType; // 新增：记录当前数据库类型



    private ScheduledExecutorService statsScheduler;

    private final AtomicLong successCount = new AtomicLong(0);

    private final AtomicLong failCount = new AtomicLong(0);

    private long lastSuccessCount = 0;

    private long lastFailCount = 0;



    // 默认配置 (来自 application.properties)

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



    public void startBenchmark(Long intervalMs, Integer sampleRate,

                               String ip, String port, String dbName, String user, String pass, String dbType) {

        if (isRunning.get()) return;



        long actualInterval = (intervalMs != null) ? intervalMs : defaultInterval;

        int actualSampleRate = (sampleRate != null) ? sampleRate : defaultSampleRate;

        this.currentDbType = StringUtils.hasText(dbType) ? dbType : "MySQL";



        String targetIp = StringUtils.hasText(ip) ? ip : defaultIp;

        String targetPort = StringUtils.hasText(port) ? port : defaultPort;

        String targetDb = StringUtils.hasText(dbName) ? dbName : defaultDbName;

        String targetUser = StringUtils.hasText(user) ? user : defaultUsername;

        String targetPass = StringUtils.hasText(pass) ? pass : defaultPassword;



        successCount.set(0);

        failCount.set(0);

        lastSuccessCount = 0;

        lastFailCount = 0;

        isRunning.set(true);



        new Thread(() -> {

            try {

                sendLog("LOG", String.format("🔄 正在连接 %s 数据库 [%s:%s]...", this.currentDbType, targetIp, targetPort));



                if (this.dataSource != null && !this.dataSource.isClosed()) {

                    this.dataSource.close();

                }

                this.dataSource = createDataSource(targetIp, targetPort, targetDb, targetUser, targetPass, this.currentDbType);



                initSchema();



                sendLog("LOG", "✅ 连接成功，表结构已就绪。开始压测...");

                sendLog("LOG", "🚀 目标: " + targetIp + ":" + targetPort + " | 频率: " + actualInterval + "ms");



                startLoop(actualInterval, actualSampleRate);

                startMonitor();



            } catch (Exception e) {

                isRunning.set(false);

                sendLog("LOG", "<span style='color:red'>❌ 启动失败: " + e.getMessage() + "</span>");

                if (dataSource != null) dataSource.close();

            }

        }).start();

    }



    private void startLoop(long interval, int sampleRate) {

        new Thread(() -> {

            long seq = 0;

            while (isRunning.get()) {

                seq++;

                probeDatabase(seq, sampleRate);

                try {

                    TimeUnit.MILLISECONDS.sleep(interval);

                } catch (InterruptedException e) {

                    Thread.currentThread().interrupt();

                    break;

                }

            }

        }).start();

    }



    private void startMonitor() {

        statsScheduler = Executors.newSingleThreadScheduledExecutor();

        statsScheduler.scheduleAtFixedRate(this::calculateAndPushStats, 1, 1, TimeUnit.SECONDS);

    }



    public void stopBenchmark() {

        if (!isRunning.get()) return;

        isRunning.set(false);



        if (statsScheduler != null) statsScheduler.shutdownNow();



        new Thread(() -> {

            try { Thread.sleep(1000); } catch (InterruptedException ignored) {}

            if (dataSource != null && !dataSource.isClosed()) {

                dataSource.close();

            }

            generateReport();

            sendLog("LOG", "🛑 压测已停止，连接已释放。");

            for (SseEmitter emitter : emitters) {

                try {

                    emitter.complete();

                } catch (Exception e) {

                    System.err.println("Error completing SseEmitter: " + e.getMessage());

                }

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

        long currentSuccess = successCount.get();

        long currentFail = failCount.get();

        long deltaSuccess = currentSuccess - lastSuccessCount;

        long deltaFail = currentFail - lastFailCount;

        long tps = deltaSuccess;

        long sqlQps = (deltaSuccess * SQLS_PER_TX) + deltaFail;

        lastSuccessCount = currentSuccess;

        lastFailCount = currentFail;

        String statsJson = String.format(

                "{\"tps\": %d, \"qps\": %d, \"totalSuccess\": %d, \"totalFail\": %d}",

                tps, sqlQps, currentSuccess, currentFail);

        sendLog("STATS", statsJson);

    }



    private void probeDatabase(long seq, int sampleRate) {

        long start = System.currentTimeMillis();

        String status = "OK", color = "#4caf50", errorMsg = "";

        boolean isSuccess = true;

        long currentId = seq % defaultDataRange;



        String nowFunction = "DB2".equals(currentDbType) ? "CURRENT_TIMESTAMP" : "NOW()";

        String replaceSql;

        if ("DB2".equals(currentDbType)) {

            replaceSql = "MERGE INTO bench_test AS t " +

                         "USING (VALUES (?, " + nowFunction + ")) AS s(id, create_time) " +

                         "ON t.id = s.id " +

                         "WHEN MATCHED THEN UPDATE SET t.create_time = s.create_time " +

                         "WHEN NOT MATCHED THEN INSERT (id, create_time) VALUES (s.id, s.create_time)";

        } else {

            replaceSql = "REPLACE INTO bench_test (id, create_time) VALUES (?, " + nowFunction + ")";

        }



        try (Connection conn = dataSource.getConnection()) {

            conn.setAutoCommit(false);

            try {

                try (PreparedStatement stmt = conn.prepareStatement("SELECT create_time FROM bench_test WHERE id = ?")) {

                    stmt.setLong(1, currentId);

                    stmt.executeQuery();

                }

                try (PreparedStatement stmt = conn.prepareStatement(replaceSql)) {

                    stmt.setLong(1, currentId);

                    stmt.executeUpdate();

                }

                try (PreparedStatement stmt = conn.prepareStatement("SELECT id FROM bench_test WHERE id = ?")) {

                    stmt.setLong(1, currentId);

                    stmt.executeQuery();

                }

                try (PreparedStatement stmt = conn.prepareStatement("UPDATE bench_test SET create_time = " + nowFunction + " WHERE id = ?")) {

                    stmt.setLong(1, currentId);

                    stmt.executeUpdate();

                }

                try (PreparedStatement stmt = conn.prepareStatement("SELECT * FROM bench_test WHERE id = ?")) {

                    stmt.setLong(1, currentId);

                    stmt.executeQuery();

                }

                conn.commit();

                successCount.incrementAndGet();

            } catch (SQLException ex) {

                try { conn.rollback(); } catch (SQLException ignored) {}

                throw ex;

            }

        } catch (Exception e) {

            isSuccess = false;

            status = "FAIL";

            color = "#ff5252";

            errorMsg = (e instanceof SQLException) ? "[" + ((SQLException)e).getErrorCode() + "] " + e.getMessage() : e.getMessage();

            failCount.incrementAndGet();

        }



        if (!isSuccess || seq % sampleRate == 0) {

            long cost = System.currentTimeMillis() - start;

            String logHtml = String.format(

                    "<div style='color:%s; font-family:monospace; border-bottom:1px solid #333; padding:2px; font-size:13px;'>" +

                            "<span style='display:inline-block; width:80px;'>[TX-%05d]</span> " +

                            "<span style='font-weight:bold;'>%s</span> " +

                            "耗时:<span style='display:inline-block; width:60px;'>%-4dms</span> %s</div>",

                    color, seq, status.equals("OK") ? "✅" : "❌", cost, errorMsg);

            sendLog("LOG", logHtml);

        }

    }



    private void sendLog(String eventName, String data) {

        for (SseEmitter emitter : emitters) {

            try {

                emitter.send(SseEmitter.event().name(eventName).data(data));

            } catch (IOException e) {

                emitters.remove(emitter);

            }

        }

    }



    private DruidDataSource createDataSource(String ip, String port, String dbName, String username, String password, String dbType) throws SQLException {

        DruidDataSource tempDataSource = new DruidDataSource();

        try {

            String url;

            if ("DB2".equals(dbType)) {

                url = String.format("jdbc:db2://%s:%s/%s", ip, port, dbName);

                // DB2 JCC a driver-specific properties

                tempDataSource.setDriverClassName("com.ibm.db2.jcc.DB2Driver");

            } else { // Default to MySQL

                url = String.format("jdbc:mysql://%s:%s/%s?useSSL=false&serverTimezone=Asia/Shanghai&allowMultiQueries=true",

                        ip, port, dbName);

            }

            tempDataSource.setUrl(url);

            tempDataSource.setUsername(username);

            tempDataSource.setPassword(password);

            tempDataSource.setInitialSize(1);

            tempDataSource.setMaxActive(5);

            tempDataSource.setMaxWait(5000);

            tempDataSource.setValidationQuery("VALUES 1"); // Universal validation query

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

            testDataSource = createDataSource(targetIp, targetPort, targetDb, targetUser, targetPass, targetDbType);

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

        long s = successCount.get();

        long f = failCount.get();

        String reportJson = String.format("{\"total\": %d, \"success\": %d, \"fail\": %d}", s + f, s, f);

        sendLog("SUMMARY", reportJson);

    }

}



    