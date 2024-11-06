package com.redis;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

class Cache{
  String value;
  long ttl;

  public Cache(String value, long ttl){
    this.value = value;
    this.ttl = ttl;
  }
}

class RedisCommandParser{
  private BufferedReader inputStream;

  public RedisCommandParser(BufferedReader inputStream){
    this.inputStream = inputStream;
  }

  public RedisParser parseCommand() throws IOException{
    String firstLine = inputStream.readLine();
    if(firstLine == null || !firstLine.startsWith("*")){
      throw new IOException("Invalid Redis command format.");
    }

    //Parse number of arguments (*3)
    int numOfArgs = Integer.parseInt(firstLine.substring(1));
    List<String> arguments = new ArrayList<>();

    //Loading each argument
    for(int i=0;i<numOfArgs;i++){
      String argLen = inputStream.readLine(); // ($3)
      String argument = inputStream.readLine();
      arguments.add(argument);
    } 
    
    if (arguments.isEmpty()) {
      throw new IOException("Command not found in input.");
    }

    return new RedisParser(arguments);
  }
}

class RedisParser{
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
}

class Server{
  ServerSocket serverSocket = null;
  Socket clientSocket = null;
  int port = 6379;
  // Make same for each thread
  private static ConcurrentHashMap<String,Cache> cMap = new ConcurrentHashMap<>(); //Thread safe

  public void start(){
    try {
      serverSocket = new ServerSocket(port);
      // Since the tester restarts your program quite often, setting SO_REUSEADDR
      // ensures that we don't run into 'Address already in use' errors
      serverSocket.setReuseAddress(true);

      while (true) {
        // Wait for connection from client.
        clientSocket = serverSocket.accept();
        new ClientHandler(clientSocket).start();
      }
      
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }

  //To handle multiple client using Threads
  private static class ClientHandler extends Thread{
    private final Socket clientSocket;
    public ClientHandler(Socket clientSocket){
      this.clientSocket = clientSocket;
    }

    public void run(){
      try(BufferedReader inputStream = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
      BufferedWriter outputStream = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));) {
        RedisCommandParser parser = new RedisCommandParser(inputStream);

        while(true){ //(input = inputStream.readLine()) != null
            RedisParser command = parser.parseCommand();

            switch (command.getCommand()) {
              case "PING":
                outputStream.write("+PONG\r\n");
                break;
              case "SET":
                handleSetCommand(command, outputStream);
                break;
              case "GET":
                handleGetCommand(command, outputStream);
                break;
              case "ECHO":
                String message = command.getArguments().get(0);
                outputStream.write("$" + message.length() + "\r\n" + message + "\r\n");
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
    private void handleSetCommand(RedisParser command, BufferedWriter outputStream) throws IOException {
      String key = command.getKey();
      String value = command.getValue();
      long ttl = command.getTTL() == -1 ? -1 : System.currentTimeMillis() + command.getTTL();

      if (key != null && value != null) {
          cMap.put(key, new Cache(value, ttl));
          outputStream.write("+OK\r\n");
      } else {
          outputStream.write("-ERR wrong number of arguments\r\n");
      }
    }
    private void handleGetCommand(RedisParser command, BufferedWriter outputStream) throws IOException {
      String key = command.getKey();
      Cache cacheItem = cMap.get(key);

      if (cacheItem != null) {
          if (cacheItem.ttl != -1 && System.currentTimeMillis() > cacheItem.ttl) {
              cMap.remove(key);
              outputStream.write("$-1\r\n");
          } else {
              outputStream.write("$" + cacheItem.value.length() + "\r\n" + cacheItem.value + "\r\n");
          }
      } else {
          outputStream.write("$-1\r\n");
      }
    }
  }
}

public class App {
  public static void main(String[] args){
    System.out.println("Logs from your program will appear here!");

    Server server = new Server();
    server.start();
  }
}
