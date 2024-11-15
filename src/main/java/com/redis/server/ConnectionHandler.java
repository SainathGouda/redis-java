package com.redis.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import com.redis.data.Cache;
import com.redis.data.StreamCache;

public class ConnectionHandler {
    private final int port;
    private final ConcurrentHashMap<String, Cache> setMap;
    private final ConcurrentHashMap<String, Cache> rdbMap;
    private final ConcurrentHashMap<String, StreamCache> streamMap;
    private final ConcurrentHashMap<String, String> configMap;
    
    public ConnectionHandler(int port, 
                             ConcurrentHashMap<String, Cache> setMap, 
                             ConcurrentHashMap<String, Cache> rdbMap, 
                             ConcurrentHashMap<String, StreamCache> streamMap, 
                             ConcurrentHashMap<String, String> configMap) {
        this.port = port;
        this.setMap = setMap;
        this.rdbMap = rdbMap;
        this.streamMap = streamMap;
        this.configMap = configMap;
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                new ClientHandler(clientSocket, setMap, rdbMap, streamMap, configMap).start();
            }
        } catch (IOException e) {
            System.out.println("Error starting server: " + e.getMessage());
        }
    }
}
