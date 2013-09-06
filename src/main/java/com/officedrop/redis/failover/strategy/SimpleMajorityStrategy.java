package com.officedrop.redis.failover.strategy;

import com.officedrop.redis.failover.HostConfiguration;
import com.officedrop.redis.failover.NodeState;

import java.util.Collection;

/**
 * 节点不可用策略
 * 
 * 超过一半的client连接该node失败，则该node为unavailable
 */
public class SimpleMajorityStrategy implements FailureDetectionStrategy {

    public static final SimpleMajorityStrategy INSTANCE = new SimpleMajorityStrategy();

    @Override
    public boolean isAvailable(final HostConfiguration configuration, final Collection<NodeState> states) {

        int count = 0;

        for ( NodeState state : states ) {
            if ( state.isOffline() ) {
                count++;
            }
        }

        return !(count >= decideMajority(states.size()));
    }

    private int decideMajority( int size ) {
        if ( (size % 2) == 0 ) {
            return size / 2;
        } else {
            return (size / 2) + 1;
        }
    }

}
