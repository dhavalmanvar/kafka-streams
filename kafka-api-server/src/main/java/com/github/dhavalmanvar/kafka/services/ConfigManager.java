package com.github.dhavalmanvar.kafka.services;

import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Set;

@Service
public class ConfigManager {

    private Set<String> brokers = new HashSet<>();

    public ConfigManager() {
        brokers.add("localhost:9092");
    }

    public void setBrokers(Set<String> brokers) {
        this.brokers.clear();
        this.brokers.addAll(brokers);
    }

    public Set<String> getBrokers() {
        return this.brokers;
    }

    public void addBroker(String broker) {
        this.brokers.add(broker);
    }

    public String getBootstrapServers() {
        return String.join(",", this.brokers);
    }
}
