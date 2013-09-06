package com.officedrop.redis.failover.strategy;

import com.officedrop.redis.failover.HostConfiguration;
import com.officedrop.redis.failover.NodeState;

import java.util.Collection;

/**
 * 节点不可用策略
 */
public interface FailureDetectionStrategy {

    public boolean isAvailable(HostConfiguration configuration, Collection<NodeState> states);

}
