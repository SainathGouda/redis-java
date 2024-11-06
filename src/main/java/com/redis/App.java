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
      throw new IOException("Responding...");
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
  private static ConcurrentHashMap<String,Cache> setMap = new ConcurrentHashMap<>();
  private static ConcurrentHashMap<String,String> configMap = new ConcurrentHashMap<>();

  public Server(String dir, String dbfilename){
    configMap.put("dir", dir);
    configMap.put("dbfilename", dbfilename);
  }

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
              case "CONFIG":
                handleConfigCommand(command, outputStream);
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
          setMap.put(key, new Cache(value, ttl));
          outputStream.write("+OK\r\n");
      } else {
          outputStream.write("-ERR wrong number of arguments\r\n");
      }
    }

    private void handleGetCommand(RedisParser command, BufferedWriter outputStream) throws IOException {
      String key = command.getKey();
      Cache cacheItem = setMap.get(key);

      if (cacheItem != null) {
          if (cacheItem.ttl != -1 && System.currentTimeMillis() > cacheItem.ttl) {
              setMap.remove(key);
              outputStream.write("$-1\r\n");
          } else {
              outputStream.write("$" + cacheItem.value.length() + "\r\n" + cacheItem.value + "\r\n");
          }
      } else {
          outputStream.write("$-1\r\n");
      }
    }

    private void handleConfigCommand(RedisParser command, BufferedWriter outputStream) throws IOException{
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
  }
}

public class App {
  public static void main(String[] args){
    String dir = "/tmp/redis-file";
    String dbfilename = "rdbfile";

    for(int i=0;i<args.length;i++){
      if("--dir".equals(args[i]) && i+1<args.length){
        dir = args[i+1];
      }
      else if("--dbfilename".equals(args[i]) && i+1<args.length){
        dbfilename = args[i+1];
      }
    }

    Server server = new Server(dir, dbfilename);
    System.out.println("Starting server with dir=" + dir + " and dbfilename=" + dbfilename);
    server.start();
  }
}
