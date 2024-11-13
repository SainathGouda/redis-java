package com.redis;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

class Cache{
  String value;
  long ttl;

  public Cache(String value, long ttl){
    this.value = value;
    this.ttl = ttl;
  }
}

class StreamCache{
  private TreeMap<String, List<String>> entries = new TreeMap<>();

  public void addEntry(String entryId, List<String> entry){
    entries.put(entryId, entry);
  }

  public int getLastMillisecondsTime(){
    if(entries.isEmpty()){
      return -1;
    }
    return Integer.parseInt(entries.lastKey().split("-")[0]);
  }

  public int getLastSequenceNumber(){
    if(entries.isEmpty()){
      return -1;
    }
    return Integer.parseInt(entries.lastKey().split("-")[1]);
  }

  public int getLastSequenceNumberForMs(int milliseconds){
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

  public String getStreamEntryId(){
    return arguments.size()>2 ? arguments.get(2) : null;
  }

  public List<String> getStreamEntries(){
    return arguments.size()>3 ? arguments.subList(3, arguments.size()) : new ArrayList<>();
  }
}

class Server{
  ServerSocket serverSocket = null;
  Socket clientSocket = null;
  int port = 6379;
  // Make same for each thread
  private static ConcurrentHashMap<String,Cache> setMap = new ConcurrentHashMap<>();
  private static ConcurrentHashMap<String,String> configMap = new ConcurrentHashMap<>();
  private static ConcurrentHashMap<String,Cache> rdbMap = new ConcurrentHashMap<>();
  private static ConcurrentHashMap<String,StreamCache> streamMap = new ConcurrentHashMap<>();

  public Server(String dir, String dbfilename){
    configMap.put("dir", dir);
    configMap.put("dbfilename", dbfilename);
    loadRDBFile();
  }

  private void loadRDBFile(){
    // Get the directory and dbfilename from config
    String dir = configMap.get("dir");
    String dbfilename = configMap.get("dbfilename");

    Path dbPath = Path.of(dir, dbfilename);
    File dbfile = new File(dbPath.toString());

    if (!dbfile.exists()) {
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

      while ((read=inputStream.read()) != -1) {
        int type = read;
        if(type == 0xFF){
          break;
        }

        long ttl = -1;
        if (type == 0xFC) {
          ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
          buffer.put(inputStream.readNBytes(Long.BYTES));
          buffer.flip(); // From write to read mode
          ttl = buffer.getLong();
          
          // Read the next byte for the actual value type
          type = inputStream.read();
      }

        int keyLen = getLen(inputStream);
        byte[] keyBytes = new byte[keyLen];
        inputStream.read(keyBytes);
        String parsedKey = new String(keyBytes, StandardCharsets.UTF_8);

        int valueLen = getLen(inputStream);
        byte[] valueBytes = new byte[valueLen];
        inputStream.read(valueBytes);
        String parsedValue = new String(valueBytes, StandardCharsets.UTF_8);

        rdbMap.put(parsedKey, new Cache(parsedValue, ttl));
      }
    } catch (IOException e) {
      System.out.println("Error reading RDB file: " + e.getMessage());
    }
  }

  private int getLen(InputStream inputStream) throws IOException {
    int read = inputStream.read();
    int len_encoding_bit = (read & 0b11000000) >> 6;
    int len = 0;
    
    if (len_encoding_bit == 0) {// 6-bit length
      len = read & 0b00111111;
    } else if (len_encoding_bit == 1) {// 8-bit extra length (split across two bytes)
      int extra_len = inputStream.read();
      len = ((read & 0b00111111) << 8) + extra_len;
    } else if (len_encoding_bit == 2) {// 32-bit length (split across four bytes).
      byte[] extra_len = new byte[4];
      inputStream.read(extra_len);
      len = ByteBuffer.wrap(extra_len).getInt();
    }
    return len;
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
              case "TYPE":
                handleTypeCommand(command, outputStream);
                // echo -e "*2\r\n$4\r\nTYPE\r\n$3\r\nkey\r\n" | nc localhost 6379
                break;
              case "XADD":
                handleXADDCommand(command, outputStream);
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
      if(cacheItem == null){
        cacheItem = rdbMap.get(key);
      }

      if (cacheItem != null) {
        if (cacheItem.ttl != -1 && System.currentTimeMillis() > cacheItem.ttl) {
          setMap.remove(key);
          rdbMap.remove(key);
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
      outputStream.write("*"+rdbMap.size()+"\r\n");
      for(String key : rdbMap.keySet()){
        outputStream.write("$" + key.length() + "\r\n" + key + "\r\n");
        outputStream.flush();
      }
    }

    private void handleTypeCommand(RedisParser command, BufferedWriter outputStream) throws IOException {
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

    private void handleXADDCommand(RedisParser command, BufferedWriter outputStream) throws IOException {
      String streamKey = command.getKey();
      String entryId = command.getStreamEntryId();
      List<String> streamEntries = command.getStreamEntries();
      StreamCache streamCache = streamMap.getOrDefault(streamKey, new StreamCache());

      String[] xaddId = entryId.split("-");
      int millisecondsTime = Integer.parseInt(xaddId[0]);
      int sequenceNumber;

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

      entryId = millisecondsTime+"-"+sequenceNumber;
      streamCache.addEntry(entryId, streamEntries);
      streamMap.put(streamKey, streamCache);

      outputStream.write("$"+entryId.length()+"\r\n"+entryId+"\r\n");
      outputStream.flush();
    }
  }
}

public class Main {
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
    // java -cp . Main --dir /tmp/redis-file --dbfilename rdbfile

    Server server = new Server(dir, dbfilename);
    System.out.println("Starting server with dir=" + dir + " and dbfilename=" + dbfilename);
    server.start();
  }
}