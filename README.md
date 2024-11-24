# Redis Server Implementation in Java

This project is a custom implementation of a Redis server in Java, focusing on core Redis features such as streams, blocking reads, and persistence using the Redis Database (RDB) file format. The project demonstrates Redis functionality with attention to multithreading, command parsing, and data storage.

---

## Features

### General Functionality
- **Command Parsing**: Supports parsing and handling Redis commands using RESP (Redis Serialization Protocol).
- **Client Handling**: Allows multiple clients to connect and issue commands concurrently.
- **In-memory Storage**: Data is stored in `ConcurrentHashMap` and `TreeMap` for efficient key-value storage and ordered stream handling.

### Supported Redis Commands
1. **RDB File Loading**  
   - Preloads data from an RDB file into memory during server startup.  
   - Supports reading key-value pairs and timestamps from RDB files.  

2. **Basic Commands**  
   - **GET/SET**: Retrieve or store values associated with a key.
   - **DEL**: Delete keys from the in-memory store.
   - **EXPIRE**: Set time-to-live for a key.  

3. **Streams**  
   - **XADD**: Add entries to a stream. Supports both user-specified and auto-generated entry IDs.  
   - **XRANGE**: Retrieve a range of entries from a stream based on start and end IDs.  
   - **XREAD**: Retrieve entries from one or more streams.  
     - Supports blocking reads with indefinite (`block 0`) and timeout configurations.  
     - Synchronizes threads to wait for new entries when the requested data is unavailable.  

4. **Stream Entry ID Handling**  
   - Auto-incrementing IDs with millisecond precision and sequence numbers.  
   - Validates user-specified IDs to ensure they are greater than the current top entry.  

5. **Persistence**  
   - Uses RDB files to persist data and load it back into memory during startup.  

6. **Concurrency**  
   - Handles multiple client connections and commands using synchronized blocks and `notifyAll()` for thread communication.  

---

## Code Structure

### Key Classes
1. **`Main`**  
   - Initializes and starts the server with configurable RDB file paths and directory settings.  

2. **`Server`**  
   - Manages client connections and Redis command handling.  

3. **`ClientHandler`**  
   - Handles individual client requests and parses Redis commands.  

4. **`RedisParser`**  
   - Parses RESP input into structured commands.  

5. **`StreamCache`**  
   - Manages streams in memory, with support for ordered entries and range queries.  

6. **`RDBLoader`**  
   - Loads data from RDB files into memory.  

---

## How to Run

### Prerequisites
- Java Development Kit (JDK) 11 or above.
- Maven for project dependencies.
- Redis CLI for testing the server.

### Steps to Run
1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd <repository-directory>

2. Build the project using Maven:
   ```bash
   mvn clean install

If you have redis installed

3. Run the server:
   ```bash
   java -jar target/redis-server.jar --dir=<rdb-file-directory> --dbfilename=<rdb-file-name>

4. Connect using Redis CLI:
   ```bash
   redis-cli -h localhost -p 6379

Else

3. Run the server:
   ```bash
   java -cp target/classes/ com.test.Main

4. Run the code(example):
   ```bash
   echo -e "*2\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$3\r\ndir\r\n" | nc localhost 6379
