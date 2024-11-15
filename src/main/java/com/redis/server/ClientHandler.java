package com.redis.server;

import com.redis.data.Cache;
import com.redis.data.StreamCache;
import com.redis.commands.CommandHandler;
import com.redis.commands.CommandProcessor.*;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

public class ClientHandler extends Thread{
    private final Socket clientSocket;
    private final CommandHandler commandHandler;

    public ClientHandler(Socket clientSocket, ConcurrentHashMap<String, Cache> setMap, 
                         ConcurrentHashMap<String, Cache> rdbMap, ConcurrentHashMap<String, StreamCache> streamMap,
                         ConcurrentHashMap<String, String> configMap) {
        this.clientSocket = clientSocket;
        this.commandHandler = new CommandHandler(setMap, rdbMap, streamMap, configMap);
    }

    public void run(){
      try(BufferedReader inputStream = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
      BufferedWriter outputStream = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));) {
        RedisCommandParser parser = new RedisCommandParser(inputStream);

        while(true){
            RedisParser command = parser.parseCommand();

            switch (command.getCommand()) {
              case "PING":
                outputStream.write("+PONG\r\n");
                // echo -e "*1\r\n$4\r\nPING\r\n" | nc localhost 6379
                break;
              case "SET":
                commandHandler.handleSetCommand(command, outputStream);
                // echo -e "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n" | nc localhost 6379
                // echo -e "*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$4\r\n1000\r\n" | nc localhost 6379
                break;
              case "GET":
                commandHandler.handleGetCommand(command, outputStream);
                // echo -e "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n" | nc localhost 6379
                // echo -e "*2\r\n$3\r\nGET\r\n$7\r\nunknown\r\n" | nc localhost 6379
                break;
              case "ECHO":
                String message = command.getArguments().get(0);
                outputStream.write("$" + message.length() + "\r\n" + message + "\r\n");
                // echo -e "*2\r\n$4\r\nECHO\r\n$13\r\nHello, Redis!\r\n" | nc localhost 6379
                break;
              case "CONFIG":
                commandHandler.handleConfigCommand(command, outputStream);
                // echo -e "*2\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$3\r\ndir\r\n" | nc localhost 6379
                // echo -e "*2\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$10\r\ndbfilename\r\n" | nc localhost 6379
                break;
              case "KEYS":
                commandHandler.handleKeysCommand(outputStream);
                // echo -e "*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n" | nc localhost 6379
                break;
              case "TYPE":
                commandHandler.handleTypeCommand(command, outputStream);
                // echo -e "*2\r\n$4\r\nTYPE\r\n$3\r\nkey\r\n" | nc localhost 6379
                break;
              case "XADD":
                commandHandler.handleXADDCommand(command, outputStream);
                break;
              case "XRANGE":
                commandHandler.handleXRANGECommand(command, outputStream);
                break;
              default:
                outputStream.write("-ERR unknown command\r\n");
                break;
            }
            //To send the data immediately instead of waiting to be filled
            outputStream.flush();
        }
      } catch (IOException e) {
        System.out.println("IOException: " + e.getMessage());
      } finally {
        try {
          if (clientSocket != null) {
            clientSocket.close();
          }
        } catch (IOException e) {
          System.out.println("IOException: " + e.getMessage());
        }
      }
    }
  }
