package com.redis.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class StreamLength {
    public int getLen(InputStream inputStream) throws IOException {
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
}
