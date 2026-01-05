package com.benchmark.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ConfigService {

    private static final String CONFIG_FILE = "connections.json";
    private final ObjectMapper objectMapper = new ObjectMapper();
    // 内存缓存
    private Map<String, ConnectionInfo> connectionMap = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        loadConfig();
    }

    /**
     * 加载配置文件
     */
    public synchronized void loadConfig() {
        File file = new File(CONFIG_FILE);
        if (file.exists()) {
            try {
                connectionMap = objectMapper.readValue(file, new TypeReference<ConcurrentHashMap<String, ConnectionInfo>>() {});
            } catch (IOException e) {
                System.err.println("加载配置文件失败: " + e.getMessage());
            }
        }
    }

    /**
     * 保存配置文件
     */
    public synchronized void saveConfig() {
        try {
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(new File(CONFIG_FILE), connectionMap);
        } catch (IOException e) {
            throw new RuntimeException("保存配置文件失败", e);
        }
    }

    public Map<String, ConnectionInfo> getAllConnections() {
        return connectionMap;
    }

    public void saveConnection(String alias, ConnectionInfo info) {
        connectionMap.put(alias, info);
        saveConfig();
    }

    public void removeConnection(String alias) {
        connectionMap.remove(alias);
        saveConfig();
    }

    // 内部类 DTO
    public static class ConnectionInfo {
        public String ip;
        public String port;
        public String dbName;
        public String username;
        public String password;
        public String dbType; // 新增字段

        // 需要无参构造函数给 Jackson 使用
        public ConnectionInfo() {}

        public ConnectionInfo(String ip, String port, String dbName, String username, String password, String dbType) {
            this.ip = ip;
            this.port = port;
            this.dbName = dbName;
            this.username = username;
            this.password = password;
            this.dbType = dbType;
        }
    }
}