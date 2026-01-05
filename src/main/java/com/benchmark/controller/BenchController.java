package com.benchmark.controller;

import com.benchmark.service.BenchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;

@RestController
@RequestMapping("/api")
public class BenchController {

    @Autowired
    private BenchService benchService;

    /**
     * 启动压测 (升级版：支持动态传递数据库连接参数)
     */
    @GetMapping("/start")
    public String start(
            // 压测参数
            @RequestParam(required = false) Long interval,
            @RequestParam(required = false) Integer sampleRate,
            @RequestParam(required = false) Integer writeRatio,
            @RequestParam(required = false) Integer threadCount,

            // 数据库连接参数 (可选，不传则使用默认配置)
            @RequestParam(required = false) String ip,
            @RequestParam(required = false) String port,
            @RequestParam(required = false) String dbName,
            @RequestParam(required = false) String username,
            @RequestParam(required = false) String password,
            @RequestParam(required = false) String dbType
    ) {
        // 将参数传递给 Service 进行动态连接初始化
        benchService.startBenchmark(interval, sampleRate, writeRatio, threadCount, ip, port, dbName, username, password, dbType);
        return "Started with dynamic config";
    }

    @PostMapping("/test-connection")
    public String testConnection(@RequestBody Map<String, String> connectionParams) {
        return benchService.testConnection(
                connectionParams.get("ip"),
                connectionParams.get("port"),
                connectionParams.get("dbName"),
                connectionParams.get("username"),
                connectionParams.get("password"),
                connectionParams.get("dbType") // 新增 dbType 参数
        );
    }

    @GetMapping("/stop")
    public String stop() {
        benchService.stopBenchmark();
        return "Stopped";
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        return benchService.subscribe();
    }
}