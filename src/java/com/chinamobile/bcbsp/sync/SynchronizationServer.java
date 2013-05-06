/**
 * CopyRight by Chinamobile
 * 
 * SychronizationServer.java
 */
package com.chinamobile.bcbsp.sync;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;

/**
 * SynchronizationServer
 * 
 * This is an implementation for ZooKeeper.
 * 
 * @version 1.0
 */
public class SynchronizationServer implements SynchronizationServerInterface,
        Watcher {
    private static final Log LOG = LogFactory.getLog(SynchronizationServer.class);
    
    private BSPConfiguration conf;
    private ZooKeeper zk = null;
    private final String zookeeperAddr;
    private final String bspZKRoot;
    private volatile Integer mutex = 0;

    public SynchronizationServer() {
        this.conf = new BSPConfiguration();
        this.zookeeperAddr = conf.get(Constants.ZOOKEEPER_QUORUM)
                + ":"
                + conf.getInt(Constants.ZOOKEPER_CLIENT_PORT,
                        Constants.DEFAULT_ZOOKEPER_CLIENT_PORT);
        this.bspZKRoot = Constants.BSPJOB_ZOOKEEPER_DIR_ROOT;
    }

    @Override
    public boolean startServer() {
        try {
            this.zk = new ZooKeeper(this.zookeeperAddr, 3000, this);
            if (this.zk != null) {
                Stat s = null;
                s = this.zk.exists(this.bspZKRoot, false);
                if (s != null) {
                    deleteZKNodes(this.bspZKRoot);
                }
                this.zk.create(this.bspZKRoot, new byte[0],
                        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            return true;
        } catch (Exception e) {
            LOG.error("[startServer]", e);
            return false;
        }
    }

    @Override
    public boolean stopServer() {
        try {
            deleteZKNodes(this.bspZKRoot);
            return true;
        } catch (Exception e) {
            LOG.error("[stopServer]", e);
            return false;
        }
    }
    
    private void deleteZKNodes(String node) throws Exception {
        Stat s = this.zk.exists(node, false);
        if (s != null) {
            List<String> children = this.zk.getChildren(node, false);
            if (children.size() > 0) {
                for (String child : children) {
                    deleteZKNodes(node + "/" + child);
                }
            }
            this.zk.delete(node, s.getVersion());
        }
    }

    @Override
    public void process(WatchedEvent event) {
        synchronized (mutex) {
            mutex.notify();
        }
    }
}
