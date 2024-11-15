package com.redis.commands;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CommandProcessor {
    public static class RedisCommandParser{
        private BufferedReader inputStream;

        public RedisCommandParser(BufferedReader inputStream){
            this.inputStream = inputStream;
        }

        public RedisParser parseCommand() throws IOException{
        String firstLine = inputStream.readLine();
        if(firstLine == null || !firstLine.startsWith("*")){
            throw new IOException("Responding...");
        }

        //Parse number of arguments (*3)
        int numOfArgs = Integer.parseInt(firstLine.substring(1));
        List<String> arguments = new ArrayList<>();

        //Loading each argument
        for(int i=0;i<numOfArgs;i++){
            inputStream.readLine(); // ($3) length of argument
            String argument = inputStream.readLine();
            arguments.add(argument);
        } 

        if (arguments.isEmpty()) {
            throw new IOException("Command not found in input.");
        }

        return new RedisParser(arguments);
        }
    }

    public static class RedisParser{
        private List<String> arguments;

        public RedisParser(List<String> arguments){
        this.arguments = arguments;
        }

        public String getCommand(){
        return arguments.get(0).toUpperCase();
        }

        public List<String> getArguments(){
        return arguments.subList(1, arguments.size());
        }

        public String getKey(){
        return arguments.size() > 1 ? arguments.get(1) : null;
        }

        public String getValue(){
        return arguments.size() > 2 ? arguments.get(2) : null;
        }

        public long getTTL(){
        if(arguments.size()>4 && "PX".equalsIgnoreCase(arguments.get(3))){
            return Long.parseLong(arguments.get(4));
        }
        return -1;
        }

        public String getStreamEntryId(){
        return arguments.size()>2 ? arguments.get(2) : null;
        }

        public List<String> getStreamEntries(){
        return arguments.size()>3 ? arguments.subList(3, arguments.size()) : new ArrayList<>();
        }
    }
}
