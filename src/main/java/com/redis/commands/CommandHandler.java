package com.redis.commands;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import com.redis.commands.CommandProcessor.RedisParser;
import com.redis.data.Cache;
import com.redis.data.StreamCache;

public class CommandHandler {
    private final ConcurrentHashMap<String, Cache> setMap;
    private final ConcurrentHashMap<String, Cache> rdbMap;
    private final ConcurrentHashMap<String, StreamCache> streamMap;
    private final ConcurrentHashMap<String, String> configMap;

    public CommandHandler(ConcurrentHashMap<String, Cache> setMap, 
                          ConcurrentHashMap<String, Cache> rdbMap, 
                          ConcurrentHashMap<String, StreamCache> streamMap,
                          ConcurrentHashMap<String, String> configMap) {
        this.setMap = setMap;
        this.rdbMap = rdbMap;
        this.streamMap = streamMap;
        this.configMap = configMap;
    }

    public void handleSetCommand(RedisParser command, BufferedWriter outputStream) throws IOException {
      String key = command.getKey();
      String value = command.getValue();
      long ttl = command.getTTL() == -1 ? -1 : System.currentTimeMillis() + command.getTTL();

      if (key != null && value != null) {
          setMap.put(key, new Cache(value, ttl));
          outputStream.write("+OK\r\n");
      } else {
          outputStream.write("-ERR wrong number of arguments\r\n");
      }
    }

    public void handleGetCommand(RedisParser command, BufferedWriter outputStream) throws IOException {
      String key = command.getKey();
      Cache cacheItem = setMap.get(key);
      if(cacheItem == null){
        cacheItem = rdbMap.get(key);
      }

      if (cacheItem != null) {
        if (cacheItem.getTtl() != -1 && System.currentTimeMillis() > cacheItem.getTtl()) {
          setMap.remove(key);
          rdbMap.remove(key);
          outputStream.write("$-1\r\n");
        } else {
          outputStream.write("$" + cacheItem.getValue().length() + "\r\n" + cacheItem.getValue() + "\r\n");
        }
      } else {
        outputStream.write("$-1\r\n");
      }
    }

    public void handleConfigCommand(RedisParser command, BufferedWriter outputStream) throws IOException{
      if(command.getCommand() == null || !command.getArguments().get(0).equalsIgnoreCase("GET")){
        outputStream.write("-ERR invalid CONFIG command\r\n");
        return;
      }
      String param = command.getArguments().get(1);
      String value = configMap.get(param);
      if(value != null){
        outputStream.write("*2\r\n$"+param.length()+"\r\n"+param+"\r\n$"+value.length()+"\r\n"+value+"\r\n");
      } else {
        outputStream.write("-ERR unknown parameter for configuration");
      }
    }

    public void handleKeysCommand(BufferedWriter outputStream) throws IOException {
      outputStream.write("*"+rdbMap.size()+"\r\n");
      for(String key : rdbMap.keySet()){
        outputStream.write("$" + key.length() + "\r\n" + key + "\r\n");
        outputStream.flush();
      }
    }

    public void handleTypeCommand(RedisParser command, BufferedWriter outputStream) throws IOException {
      String key = command.getKey();

      if(setMap.containsKey(key) || rdbMap.containsKey(key)){
        outputStream.write("+string\r\n");
        outputStream.flush();
      }
      else if(streamMap.containsKey(key)){
        outputStream.write("+stream\r\n");
        outputStream.flush();
      }
      else{
        outputStream.write("+none\r\n");
        outputStream.flush();
      }
    }

    public synchronized void handleXADDCommand(RedisParser command, BufferedWriter outputStream) throws IOException {
      String streamKey = command.getKey();
      String entryId = command.getStreamEntryId();
      List<String> streamEntries = command.getStreamEntries();
      StreamCache streamCache = streamMap.getOrDefault(streamKey, new StreamCache());

      long millisecondsTime;
      int sequenceNumber;

      if (entryId.equals("*")) {
        millisecondsTime = System.currentTimeMillis();
        sequenceNumber = 0;

        if (Objects.equals(streamCache.getLastMillisecondsTime(), millisecondsTime)) {
          sequenceNumber = streamCache.getLastSequenceNumberForMs(millisecondsTime) + 1;
        }
      } else {
        String[] xaddId = entryId.split("-");
        millisecondsTime = Integer.parseInt(xaddId[0]);
        if(xaddId[1].equals("*")){
          int lastSequenceNumber = streamCache.getLastSequenceNumberForMs(millisecondsTime);

          if(lastSequenceNumber >= 0){
            sequenceNumber = lastSequenceNumber + 1;
          }
          else{
            sequenceNumber = (millisecondsTime == 0) ? 1 : 0;
          }
        }
        else{
          sequenceNumber = Integer.parseInt(xaddId[1]);
        }

        if(millisecondsTime <= 0 && sequenceNumber <=0){
          outputStream.write("-ERR The ID specified in XADD must be greater than 0-0\r\n");
          outputStream.flush();
          return;
        }

        if(millisecondsTime < streamCache.getLastMillisecondsTime() || millisecondsTime == streamCache.getLastMillisecondsTime() && sequenceNumber <= streamCache.getLastSequenceNumber()){
          outputStream.write("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
          outputStream.flush();
          return;
        }
      }

      entryId = millisecondsTime+"-"+sequenceNumber;
      streamCache.addEntry(entryId, streamEntries);
      streamMap.put(streamKey, streamCache);

      outputStream.write("$"+entryId.length()+"\r\n"+entryId+"\r\n");
      outputStream.flush();

      this.notifyAll();
    }

    public void handleXRANGECommand(RedisParser command, BufferedWriter outputStream) throws IOException {
      String streamKey = command.getKey();
      String startId = command.getArguments().get(1);
      String endId = command.getArguments().get(2);

      StreamCache streamCache = streamMap.get(streamKey);
      if (streamCache == null) {
        outputStream.write("*0\r\n");
        outputStream.flush();
        return;
      }

      TreeMap<String, List<String>> entries;
      startId = startId.contains("-") ? startId : startId + "-0";
      if(endId.equals("+")){
        // Get entries with IDs greater than the specified entry ID
        entries = new TreeMap<>(streamCache.getEntries().tailMap(startId, true));
      }
      else{
        endId = endId.contains("-") ? endId : endId + "-*";

        if(endId.endsWith("-*")){
            long endTimeMs = Long.parseLong(endId.split("-")[0]);
            int lastSequenceNumber = streamCache.getLastSequenceNumberForMs(endTimeMs);
            endId = endTimeMs + "-" + lastSequenceNumber;
          }

        entries = new TreeMap<>(streamCache.getEntries().subMap(startId, true, endId, true)); // boolean for including end boundaries
      }

      outputStream.write("*"+entries.size()+"\r\n");
      for(var entry : entries.entrySet()){
        outputStream.write("*2\r\n$" + entry.getKey().length() + "\r\n" + entry.getKey() + "\r\n");
        outputStream.write("*"+entry.getValue().size()+"\r\n");
        for (String value : entry.getValue()) {
          outputStream.write("$"+value.length()+"\r\n"+value+"\r\n");
        }
      }
      outputStream.flush();
    }

    public synchronized void handleXREADCommand(RedisParser command, BufferedWriter outputStream) throws IOException {
      List<String> arguments = command.getArguments();
      int blockIndex = arguments.indexOf("block");
      int streamsIndex = arguments.indexOf("streams");

      long blockTimeout = 0;
      boolean isBlocking = false;

      if (blockIndex != -1 && blockIndex + 1 < arguments.size()) {
        try {
            blockTimeout = Long.parseLong(arguments.get(blockIndex + 1));
            isBlocking = true;
        } catch (NumberFormatException e) {
            outputStream.write("-ERR Invalid block timeout value\r\n");
            outputStream.flush();
            return;
        }
      }
      
      if (streamsIndex == -1 || streamsIndex + 1 >= arguments.size()) {
          outputStream.write("-ERR Missing or invalid streams argument\r\n");
          outputStream.flush();
          return;
      }
      
      // Extract stream keys and entry IDs
      List<String> streamKeys = arguments.subList(streamsIndex + 1, streamsIndex + 1 + (arguments.size() - streamsIndex - 1) / 2);
      List<String> entryIds = arguments.subList(streamsIndex + 1 + streamKeys.size(), arguments.size());
      
      if (streamKeys.size() != entryIds.size()) {
          outputStream.write("-ERR Mismatched number of streams and IDs\r\n");
          outputStream.flush();
          return;
      }

      long endTime = System.currentTimeMillis() + blockTimeout;

      while(true){
        boolean hasData = false;
        StringBuilder response = new StringBuilder();
        response.append("*").append(streamKeys.size()).append("\r\n");

        for (int i = 0; i < streamKeys.size(); i++) {
          String streamKey = streamKeys.get(i);
          String entryId = entryIds.get(i);
          
          StreamCache streamCache = streamMap.get(streamKey);
          if (streamCache == null) {
              outputStream.write("*2\r\n");
              outputStream.write("$" + streamKey.length() + "\r\n" + streamKey + "\r\n");
              outputStream.write("*0\r\n");
              continue;
          }
          
          entryId = entryId.contains("-") ? entryId : entryId + "-0";
          
          // Get entries with IDs greater than the specified entry ID
          TreeMap<String, List<String>> entries = new TreeMap<>(streamCache.getEntries().tailMap(entryId, false));

          if (!entries.isEmpty()) {
            hasData = true;
            response.append("*2\r\n")
                    .append("$").append(streamKey.length()).append("\r\n").append(streamKey).append("\r\n")
                    .append("*").append(entries.size()).append("\r\n");

            for (var entry : entries.entrySet()) {
                response.append("*2\r\n")
                        .append("$").append(entry.getKey().length()).append("\r\n").append(entry.getKey()).append("\r\n")
                        .append("*").append(entry.getValue().size()).append("\r\n");
                for (String value : entry.getValue()) {
                    response.append("$").append(value.length()).append("\r\n").append(value).append("\r\n");
                }
            }
          } else {
              response.append("*2\r\n")
                      .append("$").append(streamKey.length()).append("\r\n").append(streamKey).append("\r\n")
                      .append("*0\r\n");
          }
        }

        if (hasData) {
          outputStream.write(response.toString());
          outputStream.flush();
          return;
        }

        if (!isBlocking || blockTimeout <= 0) {
          outputStream.write("$-1\r\n");
          outputStream.flush();
          return;
        }

        long currentTime = System.currentTimeMillis();
        if (currentTime >= endTime) {
          outputStream.write("$-1\r\n");
          outputStream.flush();
          return;
        }

        try {
          //This ensures that while one thread is in the synchronized block, no other thread can execute any synchronized code on the same object
          synchronized (this) {
            this.wait(endTime - currentTime);
          }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            outputStream.write("$-1\r\n");
            outputStream.flush();
            return;
        }
      }
    }
}