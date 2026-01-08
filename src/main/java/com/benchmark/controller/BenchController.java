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

    @GetMapping("/start")
    public String start(
            @RequestParam(required = false) Long interval,
            @RequestParam(required = false) Integer sampleRate,
            @RequestParam(required = false) String scenario,
            @RequestParam(required = false) Integer threadCount,
            @RequestParam(required = false) String customSql,
            @RequestParam(required = false) String ip,
            @RequestParam(required = false) String port,
            @RequestParam(required = false) String dbName,
            @RequestParam(required = false) String username,
            @RequestParam(required = false) String password,
            @RequestParam(required = false) String dbType
    ) {
        benchService.startBenchmark(interval, sampleRate, scenario, threadCount, customSql, ip, port, dbName, username, password, dbType);
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
                connectionParams.get("dbType")
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
