package com.officedrop.redis.failover.zookeeper;

/**
 * User: Maurício Linhares
 * Date: 1/4/13
 * Time: 3:56 PM
 */
public class ZooKeeperException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

	public ZooKeeperException( Throwable t ) {
        super(t);
    }

}
