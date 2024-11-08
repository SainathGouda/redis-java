package com.redis;
import java.io.*;
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
    loadRDBFile(dir, dbfilename);
  }

  private void loadRDBFile(String dir, String dbfilename){
    File rdbFile = new File(dir, dbfilename);
    if(!rdbFile.exists()){
      System.out.println("RDB file does not exist, starting with an empty dataset.");
      return;
    }

    try(DataInputStream inputStream = new DataInputStream(new FileInputStream(rdbFile))) {
      // Header 0x
      byte[] header = new byte[9];
      inputStream.readFully(header); //For safety (Corrupted bits)
      String headerStr = new String(header);
      if(!headerStr.equals("REDIS0011")){
        System.out.println("Invalid RDB file version");
        return;
      }

      // Metadata (Skip) 0xFA
      while (true) {
        int sectionType = inputStream.readUnsignedByte();
        if(sectionType == 0xFE){
          break;
        }
        else if(sectionType == 0xFA){
          readString(inputStream); // String encoded name
          readString(inputStream); // String encoded value
        }
      }

      // Database Section
      boolean continueReading = true;
      while (continueReading) {
        int sectionType = inputStream.readUnsignedByte();

        switch (sectionType) {
          case 0xFE: // Indicates the start of a database subsection.
            int dbIndex = decodeSize(inputStream);
            break;
          case 0xFB: // Indicates that hash table size information follows.
            int kvTableSize = decodeSize(inputStream);
            int expiryTableSize = decodeSize(inputStream);
            break;
          case 0x00:  // String key-value pair
            String key = readString(inputStream);
            String value = readString(inputStream);
            setMap.put(key, new Cache(value, -1));
            System.out.println("Loaded key from RDB: " + key);
            break;
          case 0xFD:  // Key with expiry (seconds)
            String expiringKey = readString(inputStream);
            String expiringValue = readString(inputStream);
            long expiryTTL = decodeExpiry(inputStream, 4);
            setMap.put(expiringKey, new Cache(expiringValue, expiryTTL * 1000));
            System.out.println("Loaded expiring key from RDB: " + expiringKey);
            break;
          case 0xFC:  // Key with expiry (milliseconds)
            String msExpiringKey = readString(inputStream);
            String msExpiringValue = readString(inputStream);
            long msExpiryTTL = decodeExpiry(inputStream, 8);
            setMap.put(msExpiringKey, new Cache(msExpiringValue, msExpiryTTL));
            System.out.println("Loaded expiring key from RDB: " + msExpiringKey);
            break;
          case 0xFF:  // End of file section
            continueReading = false;
            break;
          default:
            System.out.println("Unknown section type: "+sectionType);
            continueReading = false;
            break;
        }
      }

    } catch (IOException e) {
      System.out.println("Error in reading RDB file: " + e.getMessage());
    }
  }

  private int decodeSize(DataInputStream inputStream) throws IOException{
    int firstByte = inputStream.readUnsignedByte();
    int size;

    // C =>1100000 so anding gives first two bytes
    if ((firstByte & 0xC0) == 0x00) { // 0b00   
        size = firstByte & 0x3F;  // First two bits are 0b00, last 6 bits are the size
    } else if ((firstByte & 0xC0) == 0x40) { // 0x40 => 0b01
        int secondByte = inputStream.readUnsignedByte();
        size = ((firstByte & 0x3F) << 8) | secondByte;  // 14-bit size in big-endian (as last 6 bits are size so we need to move them by 8 bits to make space for second byte i.e. in total 14)
    } else if ((firstByte & 0xC0) == 0x80) { // 0b10
        size = inputStream.readInt();  // 32-bit size
    } else {
        throw new IOException("Unsupported encoding format.");
    }

    return size;
  }

  private String readString(DataInputStream inputStream) throws IOException{
    int size = decodeSize(inputStream);
    byte[] bytes = new byte[size];
    inputStream.readFully(bytes);
    return new String(bytes);
  }

  private long decodeExpiry(DataInputStream inputStream, int size) throws IOException{
    if (size==4) {
      return Integer.toUnsignedLong(inputStream.readInt());
    }
    else if(size==8){
      long partOne = Integer.toUnsignedLong(inputStream.readInt());
      long partTwo = Integer.toUnsignedLong(inputStream.readInt());
      return (partOne << 32) | partOne;
    }
    throw new IOException("Invalid expiry size");
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

    private void handleKeysCommand(BufferedWriter outputStream) throws IOException{
      outputStream.write("*"+setMap.size()+"\r\n");
      for (String key : setMap.keySet()) {
        outputStream.write("$"+key.length()+"\r\n"+key+"\r\n");
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
    // To make files for testing
    // mkdir -p /tmp/redis-file
    // java -cp . App --dir /tmp/redis-file --dbfilename rdbfile

    Server server = new Server(dir, dbfilename);
    System.out.println("Starting server with dir=" + dir + " and dbfilename=" + dbfilename);
    server.start();
  }
}