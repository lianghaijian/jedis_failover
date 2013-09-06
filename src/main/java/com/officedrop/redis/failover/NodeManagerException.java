package com.officedrop.redis.failover;

/**
 * User: Maurício Linhares
 * Date: 1/7/13
 * Time: 8:02 AM
 */
public class NodeManagerException extends IllegalStateException {

    private static final long serialVersionUID = 115839769854881868L;

	public NodeManagerException( String message ) {
        super(message);
    }

}
