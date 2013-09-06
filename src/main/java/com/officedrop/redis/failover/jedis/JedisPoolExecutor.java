package com.officedrop.redis.failover.jedis;

/**
 * User: Maurício Linhares
 * Date: 1/8/13
 * Time: 4:48 PM
 */
public interface JedisPoolExecutor {

    public void withJedis(JedisFunction action);

    public <T> T withJedis(JedisResultFunction<T> action);
    
    public <T> T withJedisPipeline(JedisPipelineResultFunction<T> action);

    public void close();

}
