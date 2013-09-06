package com.officedrop.redis.failover;

import java.net.Inet4Address;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.officedrop.redis.failover.jedis.GenericJedisClientFactory;
import com.officedrop.redis.failover.jedis.JedisClientFactory;
import com.officedrop.redis.failover.strategy.FailoverSelectionStrategy;
import com.officedrop.redis.failover.strategy.FailureDetectionStrategy;
import com.officedrop.redis.failover.strategy.LatencyFailoverSelectionStrategy;
import com.officedrop.redis.failover.strategy.SimpleMajorityStrategy;
import com.officedrop.redis.failover.utils.DaemonThreadPoolFactory;
import com.officedrop.redis.failover.utils.Function;
import com.officedrop.redis.failover.utils.SleepUtils;
import com.officedrop.redis.failover.utils.TransformationUtils;
import com.officedrop.redis.failover.zookeeper.ZooKeeperNetworkClient;

/**
 * <pre>
 * 消息订阅者
 * 订阅：/redis_failover/manual_failover与/redis_failover/manager_node_state
 * 订阅方式：pull
 * 
 * 间接消息发布者
 * 发布：/redis_failover/nodes
 * 方式：订阅/redis_failover/manager_node_state消息改变，如果node改变则间接发布消息
 * 
 * Failover中的消息			
 * 			/redis_failover/nodes					master/slave信息
 * 			/redis_failover/manual_failover			手动指定的master
 * 			/redis_failover/manager_node_state		NodeManager收集的clients与cluster时时连接信息
 * 
 * 任务：nodeManager没5s
 * 				检查/redis_failover/manager_node_state，有变化则触发ClusterStatusChanged事件
 * 				检查/redis_failover/manual_failover
 * 				如果master/slave角色有变化则重新配置cluster
 * </pre>
 */
public class NodeManager implements NodeListener, ClusterChangeEventSource {

    private static final Logger log = LoggerFactory.getLogger(NodeManager.class);

    private ZooKeeperClient zooKeeperClient;
    
    //用户指定的redis server，该framework操作发生在这些server间
    private Collection<HostConfiguration> redisServers; 
    private JedisClientFactory factory;
    private ExecutorService threadPool;
    //对应redisServer
    private Set<Node> nodes = new HashSet<Node>();
    private FailoverSelectionStrategy failoverStrategy;
    private FailureDetectionStrategy failureDetectionStatery;
    private long nodeSleepTimeout;
    private int nodeRetries;
    private String nodeName/*NodeManager标识*/;
    private volatile boolean running;
    private volatile ClusterStatus lastClusterStatus;//对应/redis_failover/nodes
    private volatile Map<HostConfiguration, NodeState> currentNodesState;
    //对应 /redis_failover/manager_state_node
    private volatile Map<String, Map<HostConfiguration, NodeState>> lastNodesData;
    private final Object mutex = new Object();
    private final List<NodeManagerListener> listeners = new CopyOnWriteArrayList<NodeManagerListener>();
    //
    private final Set<HostConfiguration> reportedNodes = Collections.synchronizedSet(new HashSet<HostConfiguration>());
    private boolean closeZookeeper;

    public NodeManager(String zooKeeperUrl, String redisUrl){
    	if(redisUrl==null) 
    		throw new NullPointerException("Redis url can't be null.");
    	
    	Collection<HostConfiguration> redisCluster = new ArrayList<HostConfiguration>();
    	String[] redisServers = redisUrl.split(",");
    	for(String redis : redisServers){
    		String[] host = redis.split(":");
    		redisCluster.add(new HostConfiguration(host[0], Integer.valueOf(host[1])));
    	}
    	
    	init(
                new ZooKeeperNetworkClient(zooKeeperUrl),
                redisCluster,
                GenericJedisClientFactory.INSTANCE,
                DaemonThreadPoolFactory.newCachedPool(),
                LatencyFailoverSelectionStrategy.INSTANCE,
                SimpleMajorityStrategy.INSTANCE,
                5000,
                3,
                true
        );
    	
    }
    
    public NodeManager(String zooKeeperUrl, Collection<HostConfiguration> redisServers) {
        init(
                new ZooKeeperNetworkClient(zooKeeperUrl),
                redisServers,
                GenericJedisClientFactory.INSTANCE,
                DaemonThreadPoolFactory.newCachedPool(),
                LatencyFailoverSelectionStrategy.INSTANCE,
                SimpleMajorityStrategy.INSTANCE,
                5000,
                3,
                true
        );
    }
    
    private void init(
            ZooKeeperClient zooKeeperClient,
            Collection<HostConfiguration> redisServers,
            JedisClientFactory factory,
            ExecutorService threadPool,
            FailoverSelectionStrategy failoverStrategy,
            FailureDetectionStrategy failureDetectionStrategy,
            long nodeSleepTimeout,
            int nodeRetries,
            boolean closeZooKeeper
    ) {
        this.zooKeeperClient = zooKeeperClient;
        this.redisServers = new HashSet<HostConfiguration>(redisServers);
        this.factory = factory;
        this.threadPool = threadPool;
        this.nodeSleepTimeout = nodeSleepTimeout;
        this.nodeRetries = nodeRetries;
        this.failoverStrategy = failoverStrategy;
        this.failureDetectionStatery = failureDetectionStrategy;
        this.closeZookeeper = closeZooKeeper;

        String uniqueName = UUID.randomUUID().toString();

        try {
            String hostName = Inet4Address.getLocalHost().getHostName();
            uniqueName = hostName + "-" + uniqueName;
        } catch (Exception e) {
            log.error("Failed to get host name", e);
        }

        this.nodeName = uniqueName;

        this.zooKeeperClient.addEventListeners(new ZooKeeperEventListener() {

            @Override
            public void clusterDataChanged(final ZooKeeperClient client, final ClusterStatus clusterStatus) {
                if (NodeManager.this.running) {
                    NodeManager.this.threadPool.submit(new Runnable() {
                        @Override
                        public void run() {
                            log.info("Cluster status has changed - {}", clusterStatus);
                            clusterStatusChanged(clusterStatus);
                        }
                    });
                }
            }
        });
    }

    public ClusterStatus getLastClusterStatus() {
        return this.lastClusterStatus;
    }

    public void addListeners(NodeManagerListener... listeners) {
        this.listeners.addAll(Arrays.asList(listeners));
    }

    public void removeListeners(NodeManagerListener... listeners) {
        this.listeners.removeAll(Arrays.asList(listeners));
    }

    private void nodeStatusesChanged(Map<String, Map<HostConfiguration, NodeState>> nodesData) {

        synchronized (this.mutex) {

            if (this.lastNodesData == null || !nodesData.equals(this.lastNodesData)) {

                log.info("Nodes data has changed, checking if we have differences in the cluster \n{}\n{}", this.lastNodesData, nodesData);

                this.lastNodesData = nodesData;

                Collection<HostConfiguration> available = new ArrayList<HostConfiguration>();
                Collection<HostConfiguration> unavailable = new ArrayList<HostConfiguration>();

                Map<HostConfiguration, Collection<NodeState>> statusByNode = TransformationUtils.toStatusByNode(nodesData);

                for (Map.Entry<HostConfiguration, Collection<NodeState>> nodeStates : statusByNode.entrySet()) {
                    boolean isAvailable = this.failureDetectionStatery.isAvailable(nodeStates.getKey(), nodeStates.getValue());

                    if (!isAvailable) {
                        log.info("{} is not available", nodeStates.getKey());
                        unavailable.add(nodeStates.getKey());
                    } else {
                        log.info("{} is available", nodeStates.getKey());
                        available.add(nodeStates.getKey());
                    }
                }

                HostConfiguration newMaster = null;
                boolean slavesChanged = false;

                if (this.lastClusterStatus.hasMaster() && unavailable.contains(this.lastClusterStatus.getMaster())) {
                    newMaster = this.failoverStrategy.selectMaster(new HashSet<HostConfiguration>(available), nodesData);
                    available.remove(newMaster);
                } else {
                    available.remove(this.lastClusterStatus.getMaster());
                }

                if (!this.lastClusterStatus.getSlaves().equals(available)) {
                    slavesChanged = true;
                }

                if (newMaster != null || slavesChanged) {
                    ClusterStatus status = new ClusterStatus(newMaster != null ? newMaster : this.lastClusterStatus.getMaster(), available, unavailable);
                    this.fireClusterStatusChanged(status);
                }

            }


        }

    }

    /**
     * 回掉函数
     * 触发事件：master slave角色发生变化
     * @param clusterStatus
     */
    private void clusterStatusChanged(ClusterStatus clusterStatus) {
        synchronized (this.mutex) {
            if (!this.zooKeeperClient.hasLeadership()) {
                this.fireClusterStatusChanged(clusterStatus);
            }
        }
    }

    /**
     * 
     * @param clusterStatus
     */
    private void fireClusterStatusChanged(ClusterStatus clusterStatus) {

        ClusterStatus previousClusterStatus = this.lastClusterStatus;
        this.lastClusterStatus = clusterStatus;

        if (previousClusterStatus != null) {
            switch (previousClusterStatus.difference(this.lastClusterStatus)) {
                case BOTH:
                    log.info("Both master and slaves changed");
                    fireMasterChangedEvent(clusterStatus);
                    fireSlaveChangedEvent(clusterStatus);
                    break;
                case MASTER:
                    log.info("Master changed");
                    fireMasterChangedEvent(clusterStatus);
                    break;
                case SLAVES:
                    log.info("Slaves changed");
                    fireSlaveChangedEvent(clusterStatus);
                    break;
            }
        } else {
            fireMasterChangedEvent(clusterStatus);
            fireSlaveChangedEvent(clusterStatus);
        }

        if (this.zooKeeperClient.hasLeadership()) {
            if (!clusterStatus.hasMaster()) {
                throw new IllegalArgumentException("There has to be a master before setting the cluster configuration");
            }

            //发布消息/redis_failover/nodes
            //订阅/redis_failover/manager_node_state，触发事件时，如果node改变则间接发布消息
            this.zooKeeperClient.setClusterData(clusterStatus);
        }
    }

    private void fireMasterChangedEvent(ClusterStatus status) {
        for (NodeManagerListener listener : this.listeners) {
            try {
                listener.masterChanged(this, status);
            } catch (Exception e) {
                log.error("Failed to send event to listener", e);
            }
        }
    }

    private void fireSlaveChangedEvent(ClusterStatus status) {
        for (NodeManagerListener listener : this.listeners) {
            try {
                listener.slavesChanged(this, status);
            } catch (Exception e) {
                log.error("Failed to send event to listener", e);
            }
        }
    }

    /**
     * nodeManager没5s
     * 				检查/redis_failover/nodes，有变化则触发ClusterStatusChanged事件
     * 				检查/redis_failover/manual_failover
     * 				如果master/slave角色有变化则重新配置cluster
     */
    public void start() {

        synchronized (this.mutex) {
            for (final HostConfiguration configuration : this.redisServers) {
                final Node node = new Node(configuration, this.factory, this.nodeSleepTimeout, this.nodeRetries);
                node.addNodeListeners(this);
                this.nodes.add(node);

                this.threadPool.submit(new Runnable() {
                    @Override
                    public void run() {
                        node.start();
                    }
                });
            }

            this.lastClusterStatus = this.zooKeeperClient.getClusterData();

            this.running = true;

            while (this.running && this.reportedNodes.size() != this.nodes.size()) {
                log.info("Waiting for all nodes to report {} ({}) - {} - {}", this.reportedNodes.size(), this.nodes.size(), this.nodes, this.reportedNodes);
                SleepUtils.safeSleep(this.nodeSleepTimeout, TimeUnit.MILLISECONDS);
            }

            log.info("All nodes reported, starting leader election loop - {}", TransformationUtils.toNodeStates(this.nodes));
        }

        this.threadPool.submit(new Runnable() {
            @Override
            public void run() {
                masterLoop();
            }
        });
    }

    private void masterLoop() {
        while (this.running) {
            try {

                if (!this.zooKeeperClient.hasLeadership()) {
                    log.info("Trying to acquire redis failover cluster leadership");
                    this.zooKeeperClient.waitUntilLeader(5, TimeUnit.SECONDS);
                } else {
                    log.info("Last cluster status is {}", this.lastClusterStatus);
                    synchronized (this.mutex) {
                        if (this.lastClusterStatus.isEmpty() || !this.lastClusterStatus.hasMaster()) {
                            electMaster();
                        } else {
                            reconcile();
                        }
                    }
                }
            } catch (NodeManagerException e) {
                log.error("Failed to boot the NodeManager, giving up execution", e);
                throw new IllegalStateException(e);
            } catch (Exception e) {
                log.error("Wait for leader wait call raised an error", e);
            } finally {
                SleepUtils.safeSleep(5, TimeUnit.SECONDS);
            }
        }
    }

    private void electMaster() {

        log.info("Electing master on empty cluster configuration");

        Node master = null;

        for (Node node : this.nodes) {
            try {
                if (node.isMaster()) {
                    master = node;
                    break;
                }
            } catch (Exception e) {
                log.error("Failed to check if node is master", e);
            }
        }

        if (master == null) {
            throw new NodeManagerException("No node is configured as master and I have no information on who was the previous master");
        }

        log.info("Node {} is the master of the current cluster configuration", master.getHostConfiguration());

        List<HostConfiguration> slaves = new ArrayList<HostConfiguration>();
        List<HostConfiguration> unavailable = new ArrayList<HostConfiguration>();

        for (Node node : this.nodes) {
            if (!node.equals(master)) {
                try {
                    node.makeSlaveOf(master.getHostConfiguration().getHost(), master.getHostConfiguration().getPort());
                    slaves.add(node.getHostConfiguration());
                } catch (Exception e) {
                    unavailable.add(node.getHostConfiguration());
                    log.error("Failed to set node to be slave of current master");
                }
            }
        }

        ClusterStatus status = new ClusterStatus(master.getHostConfiguration(), slaves, unavailable);

        this.fireClusterStatusChanged(status);
    }

    private void reconcile() {
        log.info("Checking if we have to do a manual failover");
        handleManualFailover();

        log.info("Checking if node data has changed");
        nodeStatusesChanged(this.zooKeeperClient.getNodeDatas());

        log.info("Reconciling cluster configuration");
        this.assertMasterIsConfigured();
    }

    private void handleManualFailover() {
        HostConfiguration config = this.zooKeeperClient.getManualFailoverConfiguration();

        if ( config != null ) {
            log.info("Received new master from manual failover configuration {} - {} - {}",
                    config,
                    config.equals(this.lastClusterStatus.getMaster()),
                    this.reportedNodes.contains(config)
                    );
            if ( !config.equals(this.lastClusterStatus.getMaster()) && this.reportedNodes.contains(config) ) {
                Set<HostConfiguration> slaves = new HashSet<HostConfiguration>();
                slaves.add(this.lastClusterStatus.getMaster());
                slaves.addAll( this.lastClusterStatus.getSlaves() );
                slaves.remove(config);

                ClusterStatus status =  new ClusterStatus( config, slaves, this.lastClusterStatus.getUnavailables() );
                this.fireClusterStatusChanged(status);
            }
            this.zooKeeperClient.deleteManualFailoverConfiguration();
        } else {
            log.info("No manual failover configuration was available");
        }
    }

    /**
     * 重新配置master/slave
     */
    public void assertMasterIsConfigured() {
        List<Node> availableNodes = new ArrayList<Node>();

        for (Node node : this.nodes) {
            if (!node.getCurrentState().isOffline()) {
                availableNodes.add(node);
            }
        }

        for (Node node : availableNodes) {
            if (this.lastClusterStatus.getMaster().equals(node.getHostConfiguration())) {
                try {
                    if (!node.isMaster()) {
                        node.becomeMaster();
                    }
                } catch (Exception e) {
                    log.error("Failed to mark current master as master", e);
                }
                break;
            }
        }

        for (Node node : availableNodes) {
            try {
                log.info("Node {} master is {}", node.getHostConfiguration(), node.getMasterConfiguration());
                if (this.lastClusterStatus.getSlaves().contains(node.getHostConfiguration())) {
                    if (node.getMasterConfiguration() == null || !node.getMasterConfiguration().equals(this.lastClusterStatus.getMaster())) {
                        node.makeSlaveOf(this.lastClusterStatus.getMaster().getHost(), this.lastClusterStatus.getMaster().getPort());
                    }
                }
            } catch (Exception e) {
                log.error("Failed to configure slave to be slave of current correct master", e);
            }

        }
    }

    public void stop() {

        log.warn("Stopping node manager {}", this);

        this.running = false;

        for (Node node : this.nodes) {
            try {
                node.stop();
            } catch (Exception e) {
                log.error("Failed to stop this node", e);
            }
        }

        if ( this.closeZookeeper ) {
            this.zooKeeperClient.close();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        log.warn("Finalizing node manager {}", this);
        if (this.running) {
            this.stop();
        }
    }

    @Override
    public void nodeIsOnline(final Node node, final long latency) {
        this.publishNodeState();
        if (this.reportedNodes.size() != this.nodes.size()) {
            this.reportedNodes.add(node.getHostConfiguration());
        }
    }

    @Override
    public void nodeIsOffline(final Node node, final Exception e) {
        this.publishNodeState();
        if (this.reportedNodes.size() != this.nodes.size()) {
            this.reportedNodes.add(node.getHostConfiguration());
        }
    }

    /**
     * 写入redis cluster状态写入zookeeper
     * 
     * 节点：/redis_failover/manager_node_state/${hostname}
     */
    private void publishNodeState() {

        Map<HostConfiguration, NodeState> states = TransformationUtils.toNodeStates(this.nodes);

        if (this.currentNodesState == null || !this.currentNodesState.equals( states )) {
            this.currentNodesState = states;
            this.zooKeeperClient.setNodeData(this.nodeName, this.currentNodesState);
        }
    }

    public void waitUntilMasterIsAvailable(long millis) {
        SleepUtils.waitUntil(millis, new Function<Boolean>() {
            @Override
            public Boolean apply() {
                return NodeManager.this.getLastClusterStatus().hasMaster();
            }
        });
    }

}
