package com.officedrop.redis.failover.jedis;

import redis.clients.jedis.Pipeline;

/**
 * User: Maurício Linhares
 * Date: 1/8/13
 * Time: 4:43 PM
 */
public interface JedisPipelineResultFunction<T> {

	 public T execute( Pipeline pipeline ) throws Exception ;

}
