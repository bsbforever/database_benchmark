package com.benchmark.controller;

import com.benchmark.service.ConfigService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/config")
public class ConfigController {

    @Autowired
    private ConfigService configService;

    @GetMapping("/connections")
    public Map<String, ConfigService.ConnectionInfo> listConnections() {
        return configService.getAllConnections();
    }

    @PostMapping("/connections")
    public String saveConnection(@RequestParam String alias, @RequestBody ConfigService.ConnectionInfo info) {
        configService.saveConnection(alias, info);
        return "Saved";
    }

    @DeleteMapping("/connections")
    public String deleteConnection(@RequestParam String alias) {
        configService.removeConnection(alias);
        return "Deleted";
    }
}