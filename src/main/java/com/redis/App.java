package com.redis;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

class Server{
  ServerSocket serverSocket = null;
  Socket clientSocket = null;
  int port = 6379;

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
      try {
        BufferedReader inputStream = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        BufferedWriter outputStream = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));

        String input;
        while((input = inputStream.readLine()) != null){
        if("ping".equalsIgnoreCase(input)){
          outputStream.write("+PONG\r\n");
          //To send the data immediately instead of waiting to be filled
          outputStream.flush();
        }
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
