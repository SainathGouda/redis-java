package com.redis.data;

import com.redis.utils.StreamLength;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

public class RdbFileLoader {
    private final String dir;
    private final String dbFilename;
    private final ConcurrentHashMap<String, Cache> rdbMap;

    public RdbFileLoader(String dir, String dbFilename, ConcurrentHashMap<String, Cache> rdbMap) {
        this.dir = dir;
        this.dbFilename = dbFilename;
        this.rdbMap = rdbMap;
    }

    public void load() {
        StreamLength streamLength = new StreamLength();
        Path dbPath = Path.of(dir, dbFilename);
        File dbfile = new File(dbPath.toString());

        if (!dbfile.exists()) {
            return;
        }

        try (InputStream inputStream = new FileInputStream(dbfile)) {
            int read;
            while ((read = inputStream.read()) != -1) {
                if (read == 0xFB) {
                    streamLength.getLen(inputStream);
                    streamLength.getLen(inputStream);
                    break;
                }
            }

            while ((read = inputStream.read()) != -1) {
                int type = read;
                if (type == 0xFF) {
                    break;
                }

                long ttl = -1;
                if (type == 0xFC) {
                    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
                    buffer.put(inputStream.readNBytes(Long.BYTES));
                    buffer.flip();
                    ttl = buffer.getLong();
                    type = inputStream.read();
                }

                int keyLen = streamLength.getLen(inputStream);
                byte[] keyBytes = new byte[keyLen];
                inputStream.read(keyBytes);
                String parsedKey = new String(keyBytes, StandardCharsets.UTF_8);

                int valueLen = streamLength.getLen(inputStream);
                byte[] valueBytes = new byte[valueLen];
                inputStream.read(valueBytes);
                String parsedValue = new String(valueBytes, StandardCharsets.UTF_8);

                rdbMap.put(parsedKey, new Cache(parsedValue, ttl));
            }
        } catch (IOException e) {
            System.out.println("Error reading RDB file: " + e.getMessage());
        }
    }
}
