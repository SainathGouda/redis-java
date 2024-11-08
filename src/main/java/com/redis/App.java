package com.redis;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
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
                // echo -e "*1\r\n$4\r\nPING\r\n" | nc localhost 6379
                break;
              case "SET":
                handleSetCommand(command, outputStream);
                // echo -e "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n" | nc localhost 6379
                // echo -e "*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$4\r\n1000\r\n" | nc localhost 6379
                break;
              case "GET":
                handleGetCommand(command, outputStream);
                // echo -e "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n" | nc localhost 6379
                // echo -e "*2\r\n$3\r\nGET\r\n$7\r\nunknown\r\n" | nc localhost 6379
                break;
              case "ECHO":
                String message = command.getArguments().get(0);
                outputStream.write("$" + message.length() + "\r\n" + message + "\r\n");
                // echo -e "*2\r\n$4\r\nECHO\r\n$13\r\nHello, Redis!\r\n" | nc localhost 6379
                break;
              case "CONFIG":
                handleConfigCommand(command, outputStream);
                // echo -e "*2\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$3\r\ndir\r\n" | nc localhost 6379
                // echo -e "*2\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$10\r\ndbfilename\r\n" | nc localhost 6379
                break;
              case "KEYS":
                handleKeysCommand(outputStream);
                // echo -e "*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n" | nc localhost 6379
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

    private void handleKeysCommand(BufferedWriter outputStream) throws IOException {
      // Get the directory and dbfilename from config
      String dir = configMap.get("dir");
      String dbfilename = configMap.get("dbfilename");

      Path dbPath = Path.of(dir, dbfilename);
      File dbfile = new File(dbPath.toString());

      if (!dbfile.exists()) {
        outputStream.write("-ERR no such file\r\n");
        return;
      }

      try (InputStream inputStream = new FileInputStream(dbfile)) {
        int read;
        while ((read = inputStream.read()) != -1) {
          if (read == 0xFB) {  // Start of database section
            getLen(inputStream);  // Skip hash table size info
            getLen(inputStream);  // Skip expires size info
            break;
          }
        }

        int type = inputStream.read();  // Read the type (should be a valid type byte)
        int len = getLen(inputStream);  // Get the key length
        byte[] key_bytes = new byte[len];
        inputStream.read(key_bytes);  // Read the key bytes
        String parsed_key = new String(key_bytes, StandardCharsets.UTF_8);

        // Respond with the key in the format expected by Redis
        outputStream.write("*1\r\n$" + parsed_key.length() + "\r\n" + parsed_key + "\r\n");
        outputStream.flush();
      } catch (IOException e) {
        System.out.println("Error reading RDB file: " + e.getMessage());
        outputStream.write("-ERR error reading database file\r\n");
      }
    }

    private int getLen(InputStream inputStream) throws IOException {
      int read = inputStream.read();
      int len_encoding_bit = (read & 0b11000000) >> 6;
      int len = 0;
      
      if (len_encoding_bit == 0) {
        len = read & 0b00111111;
      } else if (len_encoding_bit == 1) {
        int extra_len = inputStream.read();
        len = ((read & 0b00111111) << 8) + extra_len;
      } else if (len_encoding_bit == 2) {
        byte[] extra_len = new byte[4];
        inputStream.read(extra_len);
        len = ByteBuffer.wrap(extra_len).getInt();
      }
      return len;
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
    // To make files for testing
    // mkdir -p /tmp/redis-file
    // java -cp . App --dir /tmp/redis-file --dbfilename rdbfile

    Server server = new Server(dir, dbfilename);
    System.out.println("Starting server with dir=" + dir + " and dbfilename=" + dbfilename);
    server.start();
  }
}