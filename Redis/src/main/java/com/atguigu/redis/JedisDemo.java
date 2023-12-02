package com.atguigu.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class JedisDemo {

    public static void main(String[] args) {
        Jedis jedis = getJedis();

        String ping = jedis.ping();
        System.out.println(ping);

        /**
         * 作业：每种类型至少测试5个方法
         */
        testString();
        testList();
        testSet();
        testZSet();
        testHash();

    }


    public static void testString() {
        Jedis jedisFromPool = getJedisFromPool();

        jedisFromPool.set("k1","v1");
        jedisFromPool.append("k1","v22");
        Long k1 = jedisFromPool.strlen("k1");
        System.out.println("k1=" + k1);
        jedisFromPool.setnx("k2","v3");
        jedisFromPool.mset("k4","v5","k7","v8");

        jedisFromPool.close();
    }
    public static void testList() {

        Jedis jedisFromPool = getJedisFromPool();

        jedisFromPool.lpush("l1","v1","vc2");
        jedisFromPool.rpush("l1","v3","v4");

        jedisFromPool.llen("l1");
        jedisFromPool.lrem("l1",1L,"v3");
        jedisFromPool.lindex("l1",0);

        jedisFromPool.close();
    }
    public static void testSet() {
        Jedis jedisFromPool = getJedisFromPool();

        jedisFromPool.sadd("s1","v1","v3");
        Set<String> s1 = jedisFromPool.smembers("s1");
        System.out.println(s1);
        jedisFromPool.scard("s1");
        jedisFromPool.sismember("s1","v1");

        jedisFromPool.close();
    }
    public static void testZSet() {
        Jedis jedisFromPool = getJedisFromPool();

        jedisFromPool.zadd("z1",30,"v4");
        jedisFromPool.zincrby("z1",20,"v4");
        jedisFromPool.zcount("z1",0,100);
        jedisFromPool.zrevrank("z1","v4");
        jedisFromPool.zrange("z1",0,-1);
        Map<String,Double> sourceMembers = new HashMap<>();
        sourceMembers.put("ff",43d);
        sourceMembers.put("dd",23d);
        jedisFromPool.zadd("z1",sourceMembers);



        jedisFromPool.close();
    }
    public static void testHash() {
        Jedis jedisFromPool = getJedisFromPool();

        jedisFromPool.hset("h1","f1","v1");
        jedisFromPool.hexists("h1","f1");
        jedisFromPool.hget("h1","f1");
        jedisFromPool.hgetAll("h1");
        jedisFromPool.hdel("h1","f1");


        jedisFromPool.close();
    }

    private static Jedis getJedis() {
        Jedis jedis = new Jedis("hadoop102",6379,false);
        return jedis;
    }


    private static JedisPool jedisPool;
    static {
        getJedisFromPool();
    }

    /**
     * 创建jedis对象，基于连接池的方式
     */
    public static Jedis getJedisFromPool() {
        if (jedisPool == null) {
            // 创建连接池对象
            jedisPool =  new JedisPool("hadoop102",6379);

        }

        // 从连接池中获取Jedis
        return jedisPool.getResource();
    }

}
