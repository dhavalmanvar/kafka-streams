package com.github.dhavalmanvar.kafka.controllers;

import com.github.dhavalmanvar.kafka.services.ConfigManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/config")
public class ConfigController {

    private ConfigManager configManager;

    @Autowired
    public ConfigController(ConfigManager configManager) {
        this.configManager = configManager;
    }

    @GetMapping(value="/brokers", produces = MediaType.TEXT_PLAIN_VALUE)
    public String getBootstrapServers() {
        return configManager.getBootstrapServers();
    }

    @PutMapping(value="/brokers", consumes = MediaType.APPLICATION_JSON_VALUE)
    public void addBroker() {

    }
}
