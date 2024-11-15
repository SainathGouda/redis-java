package com.redis.server;

import com.redis.data.Cache;
import com.redis.data.StreamCache;
import com.redis.data.RdbFileLoader;

import java.util.concurrent.ConcurrentHashMap;

public class Server {
    private final ConfigManager configManager;
    private final ConcurrentHashMap<String, Cache> setMap;
    private final ConcurrentHashMap<String, Cache> rdbMap;
    private final ConcurrentHashMap<String, StreamCache> streamMap;

    public Server(String dir, String dbFilename) {
        this.configManager = new ConfigManager();
        this.setMap = new ConcurrentHashMap<>();
        this.rdbMap = new ConcurrentHashMap<>();
        this.streamMap = new ConcurrentHashMap<>();
        
        configManager.setConfig("dir", dir);
        configManager.setConfig("dbfilename", dbFilename);
        
        RdbFileLoader rdbLoader = new RdbFileLoader(dir, dbFilename, rdbMap);
        rdbLoader.load();
    }

    public void start() {
        int port = 6379;
        ConnectionHandler connectionHandler = new ConnectionHandler(port, setMap, rdbMap, streamMap, configManager.getConfigMap());
        connectionHandler.start();
    }
}
