package com.redis.server;

import java.util.concurrent.ConcurrentHashMap;

public class ConfigManager {
    private final ConcurrentHashMap<String, String> configMap;

    public ConfigManager() {
        configMap = new ConcurrentHashMap<>();
    }

    public void setConfig(String key, String value) {
        configMap.put(key, value);
    }

    public String getConfig(String key) {
        return configMap.get(key);
    }

    public ConcurrentHashMap<String, String> getConfigMap() {
        return configMap;
    }
}
