package com.redis.data;

import java.util.List;
import java.util.TreeMap;

public class StreamCache{
    private TreeMap<String, List<String>> entries = new TreeMap<>();

    public void addEntry(String entryId, List<String> entry){
        entries.put(entryId, entry);
    }

    public long getLastMillisecondsTime(){
        if(entries.isEmpty()){
        return -1;
        }
        return Long.parseLong(entries.lastKey().split("-")[0]);
    }

    public int getLastSequenceNumber(){
        if(entries.isEmpty()){
        return -1;
        }
        return Integer.parseInt(entries.lastKey().split("-")[1]);
    }

    public int getLastSequenceNumberForMs(long milliseconds){
        String preSequenceNumberPart = milliseconds + "-";
        return entries.descendingMap().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(preSequenceNumberPart))
                .findFirst()
                .map(entry -> Integer.parseInt(entry.getKey().split("-")[1]))
                .orElse(-1);
    }

    public TreeMap<String, List<String>> getEntries(){
        return entries;
    }
}