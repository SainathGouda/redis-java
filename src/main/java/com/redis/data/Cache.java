package com.redis.data;
public class Cache{
    String value;
    long ttl;

    public Cache(String value, long ttl){
        this.value = value;
        this.ttl = ttl;
    }

    public long getTtl(){
        return ttl;
    }

    public String getValue(){
        return value;
    }
}
