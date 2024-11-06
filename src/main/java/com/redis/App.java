package com.redis;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

class Server{
  ServerSocket serverSocket = null;
  Socket clientSocket = null;
  int port = 6379;
  // Make same for each thread
  private static ConcurrentHashMap<String,String> cMap = new ConcurrentHashMap<>(); //Thread safe

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
        String input;
        while((input = inputStream.readLine()) != null){
            if ("PING".equalsIgnoreCase(input)) { 
              outputStream.write("+PONG\r\n"); 
            }
            else if ("ECHO".equalsIgnoreCase(input)) {
              inputStream.readLine();
              String message = inputStream.readLine();
              outputStream.write(String.format("$"+message.length()+"\r\n"+message+"\r\n"));
            }
            else if ("SET".equalsIgnoreCase(input)) { 
              inputStream.readLine();
              String key = inputStream.readLine();
              inputStream.readLine();
              String value = inputStream.readLine();
              cMap.put(key, value);
              outputStream.write("+OK\r\n");
            }
            else if ("GET".equalsIgnoreCase(input)) { 
              inputStream.readLine();
              String key = inputStream.readLine();
              String value = cMap.get(key);
              if(value!=null){
                outputStream.write("$"+value.length()+"\r\n"+value+"\r\n");
              }
              else{
                outputStream.write("$-1\r\n");
              }
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
}

public class App {
  public static void main(String[] args){
    System.out.println("Logs from your program will appear here!");

    Server server = new Server();
    server.start();
  }
}
