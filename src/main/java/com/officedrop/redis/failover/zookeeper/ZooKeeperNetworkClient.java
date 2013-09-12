package com.officedrop.redis.failover.zookeeper;

import java.io.Closeable;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.leader.LeaderLatch;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.utils.EnsurePath;
import com.officedrop.redis.failover.ClusterStatus;
import com.officedrop.redis.failover.HostConfiguration;
import com.officedrop.redis.failover.NodeState;
import com.officedrop.redis.failover.ZooKeeperClient;
import com.officedrop.redis.failover.ZooKeeperEventListener;
import com.officedrop.redis.failover.utils.JacksonJsonBinder;
import com.officedrop.redis.failover.utils.JsonBinder;
import com.officedrop.redis.failover.utils.PathUtils;

/**
 * 消息订阅者
 * 消息：/reids_failover/nodes
 * 订阅方式：pull
 * 
 * 任务：每5s检查/reids_failover/nodes，监视redis cluster的变化
 */
public class ZooKeeperNetworkClient implements ZooKeeperClient {

    private static final Logger log = LoggerFactory.getLogger(ZooKeeperNetworkClient.class);

    public static final String BASE_PATH = "/redis_failover";
    public static final String NODE_STATES_PATH = PathUtils.toPath(BASE_PATH, "manager_node_state");
    public static final String LEADER_MUTEX = PathUtils.toPath(BASE_PATH, "leader");
    public static final String CLUSTER_PATH = PathUtils.toPath(BASE_PATH, "nodes");
    public static final String MANUAL_FAILOVER_PATH = PathUtils.toPath(BASE_PATH, "manual_failover");

    private final CuratorFramework curator;
    private final List<ZooKeeperEventListener> listeners = new CopyOnWriteArrayList<ZooKeeperEventListener>();
    private final JsonBinder jsonBinder = JacksonJsonBinder.BINDER;
    private final LeaderLatch leaderLatch;
    private volatile boolean closed = false;
    private final Timer timer;
    private volatile ClusterStatus lastClusterStatus;

    public ZooKeeperNetworkClient(String hosts) {

        try {
            int slashIndex;

            if ((slashIndex = hosts.indexOf('/')) != -1) {
                String namespace = hosts.substring(hosts.indexOf('/'), hosts.length());
                CuratorFramework namespaceCurator = CuratorFrameworkFactory
                        .builder()
                        .connectString(hosts.substring(0, slashIndex))
                        .retryPolicy(new RetryOneTime(1))
                        .build();
                namespaceCurator.start();

                EnsurePath namespaceEnsurePath = new EnsurePath(namespace);
                namespaceEnsurePath.ensure(namespaceCurator.getZookeeperClient());
                namespaceCurator.close();
            }

            this.curator = CuratorFrameworkFactory
                    .builder()
                    .connectString(hosts)
                    .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                    .build();
            this.curator.start();

            this.lastClusterStatus = new ClusterStatus(null, Collections.<HostConfiguration> emptyList(), Collections.<HostConfiguration> emptyList());

            EnsurePath ensurePath = new EnsurePath(NODE_STATES_PATH);
            ensurePath.ensure(this.curator.getZookeeperClient());

            ensurePath = new EnsurePath(CLUSTER_PATH);
            ensurePath.ensure(this.curator.getZookeeperClient());

            this.timer = new Timer(true);
            //没5s检查一次/redis_failover/nodes节点数据是否发生变化
            this.timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    ClusterStatus clusterStatus = getClusterData();

                    if (clusterStatus != null && !clusterStatus.equals(ZooKeeperNetworkClient.this.lastClusterStatus)) {
                        ZooKeeperNetworkClient.this.lastClusterStatus = clusterStatus;
                        clusterStatusChanged();
                    }

                }
            }, 0, 5000);

            this.leaderLatch = new LeaderLatch(this.curator, LEADER_MUTEX, UUID.randomUUID().toString());
            this.leaderLatch.start();
        } catch (Exception e) {
            throw new ZooKeeperException(e);
        }
    }

    public CuratorFramework getCurator() {
        return this.curator;
    }

    @Override
    public void waitUntilLeader(long timeout, TimeUnit unit) throws InterruptedException {
        this.leaderLatch.await(timeout, unit);
    }

    @Override
    public boolean hasLeadership() {
        return this.leaderLatch.hasLeadership();
    }

    /**
     * 对应zookeeper节点   /redis_failover/manager_node_state/&{hostname}
     */
    @Override
    public void setNodeData(final String hostName, final Map<HostConfiguration, NodeState> nodeStates) {
        String path = PathUtils.toPath(NODE_STATES_PATH, hostName);
        this.createOrSet(path, this.jsonBinder.toBytes(nodeStates), CreateMode.EPHEMERAL);
    }

    /**
     * 对应zookeeper节点    /redis_failover/nodes
     */
    @Override
    public void setClusterData(final ClusterStatus clusterStatus) {

        if (!clusterStatus.hasMaster()) {
            throw new IllegalArgumentException("You can't set a cluster status without a master");
        }

        this.createOrSet(CLUSTER_PATH, this.jsonBinder.toBytes(clusterStatus), CreateMode.PERSISTENT);
    }

    /**
     * 对应zookeeper的/redis_failover/nodes
     */
    @Override
    public ClusterStatus getClusterData() {
        try {
            byte[] data = this.curator.getData().forPath(CLUSTER_PATH);

            if (data != null && data.length != 0) {
                return this.jsonBinder.toClusterStatus(data);
            } else {
                return new ClusterStatus(null, Collections.<HostConfiguration> emptyList(), Collections.<HostConfiguration> emptyList());
            }
        } catch (Exception e) {
            throw new ZooKeeperException(e);
        }
    }

    @Override
    public void addEventListeners(final ZooKeeperEventListener... listeners) {
        this.listeners.addAll(Arrays.asList(listeners));
    }

    public void close() {
        log.info("Closing ZookeeperNetworkClient");
        if (!this.closed) {
            this.closed = true;
            this.timer.cancel();
            this.close(this.leaderLatch, this.curator);
        }
    }

    private void close(Closeable... closables) {

        for (Closeable closeable : closables) {
            try {
                closeable.close();
            } catch (Exception e) {
                log.error(String.format("Failed to close %s", closeable), e);
                throw new ZooKeeperException(e);
            }
        }

    }

    private void createOrSet(String path, byte[] data, CreateMode mode) {
        try {
            synchronized (this.curator) {
                if (this.curator.checkExists().forPath(path) != null) {
                    this.curator.setData().forPath(path, data);
                } else {
                    this.curator.create()
                            .creatingParentsIfNeeded()
                            .withMode(mode)
                            .forPath(path, data);
                }
            }

        } catch (Exception e) {
            throw new ZooKeeperException(e);
        }
    }

    private void clusterStatusChanged() {
        for (ZooKeeperEventListener listener : this.listeners) {
            listener.clusterDataChanged(this, this.lastClusterStatus);
        }
    }

    @Override
    public HostConfiguration getManualFailoverConfiguration() {
        try {
            if (this.curator.checkExists().forPath(MANUAL_FAILOVER_PATH) != null) {
                byte[] data = this.curator.getData().forPath(MANUAL_FAILOVER_PATH);
                String host = new String(data, Charset.forName("UTF-8"));

                if (host.contains(":")) {
                    String[] hostData = host.split(":");
                    return new HostConfiguration(hostData[0], Integer.valueOf(hostData[1]));
                } else {
                    this.deleteManualFailoverConfiguration();
                }
            }
        } catch (NumberFormatException e) {
            this.deleteManualFailoverConfiguration();
        } catch (Exception e) {
            throw new ZooKeeperException(e);
        }

        return null;
    }

    public void deleteManualFailoverConfiguration() {
        try {
            this.curator.delete().guaranteed().inBackground().forPath(MANUAL_FAILOVER_PATH);
        } catch (Exception e) {
            throw new ZooKeeperException(e);
        }
    }

    /**
     * 对应zookeeper的/redis_failover/manager_node_state
     */
    @Override
    public Map<String, Map<HostConfiguration, NodeState>> getNodeDatas() {
        Map<String, Map<HostConfiguration, NodeState>> states = new HashMap<String, Map<HostConfiguration, NodeState>>();

        try {
            List<String> children = this.curator.getChildren().forPath(NODE_STATES_PATH);

            for (String path : children) {
                byte[] nodeData = this.curator.getData().forPath(PathUtils.toPath(NODE_STATES_PATH, path));

                if (nodeData != null && nodeData.length > 0) {
                    states.put(path, this.jsonBinder.toNodeState(nodeData));
                }
            }
        } catch (Exception e) {
            log.error("Error trying to access node datas", e);
            throw new ZooKeeperException(e);
        }

        return states;
    }

}
