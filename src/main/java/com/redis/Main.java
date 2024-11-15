package com.redis;
import com.redis.server.Server;

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