/**
 * CopyRight by Chinamobile
 * 
 * WorkerSSController.java
 */
package com.chinamobile.bcbsp.sync;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.util.BSPJobID;

/**
 * WorkerSSController
 *
 * This is an implementation for ZooKeeper.
 * WorkerSSController for local synchronization and aggregation. This class is
 * connected to WorkerAgentForJob.
 * 
 * @author
 * @version
 */

public class WorkerSSController implements WorkerSSControllerInterface, Watcher {
    private BSPConfiguration conf;
    private final String zookeeperAddr;
    private final String bspZKRoot;
    private ZooKeeper zk = null;
    private volatile Integer mutex = 0;
    private BSPJobID jobId;
    private String workerName;
    public static final Log LOG = LogFactory.getLog(StaffSSController.class);
    private static final int SESSION_TIMEOUT = 5000;
    
    /**
     * Generate the WorkerSSController to control the local synchronization and
     * aggregation
     * 
     * @param jobId
     */
    public WorkerSSController(BSPJobID jobId, String workerName) {

        this.conf = new BSPConfiguration();
        this.zookeeperAddr = conf.get(Constants.ZOOKEEPER_QUORUM)
                + ":"
                + conf.getInt(Constants.ZOOKEPER_CLIENT_PORT,
                        Constants.DEFAULT_ZOOKEPER_CLIENT_PORT);
        this.bspZKRoot = Constants.BSPJOB_ZOOKEEPER_DIR_ROOT;

        this.jobId = jobId;
        this.workerName = workerName;
    }

    @Override
    public boolean firstStageSuperStepBarrier(int superStepCounter,
            SuperStepReportContainer ssrc) {
        LOG.info(this.jobId.toString() + " on " + this.workerName
                + " enter the firstStageSuperStepBarrier of "
                + Integer.toString(superStepCounter));

        try {
            this.zk = getZooKeeper();
            LOG.info(bspZKRoot + "/" + jobId.toString().substring(17) + "-ss"
                    + "/" + Integer.toString(superStepCounter) + "/"
                    + this.workerName + (ssrc.getDirFlag())[0]);
            zk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-ss"
                    + "/" + Integer.toString(superStepCounter) + "/"
                    + this.workerName + (ssrc.getDirFlag())[0], new byte[0],
                    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            closeZooKeeper();
            return true;
        } catch (KeeperException e) {
            closeZooKeeper();
            LOG.error("[firstStageSuperStepBarrier]", e);
            return false;
        } catch (InterruptedException e) {
            closeZooKeeper();
            LOG.error("[firstStageSuperStepBarrier]", e);
            return false;
        }
    }

    @Override
    public boolean secondStageSuperStepBarrier(int superStepCounter,
            SuperStepReportContainer ssrc) {
        LOG.info(this.jobId.toString() + " on " + this.workerName
                + " enter the secondStageSuperStepBarrier of "
                + Integer.toString(superStepCounter));

        try {
            this.zk = getZooKeeper();
            LOG.info(bspZKRoot + "/" + jobId.toString().substring(17) + "-sc"
                    + "/" + Integer.toString(superStepCounter) + "/"
                    + this.workerName);
            zk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-sc"
                    + "/" + Integer.toString(superStepCounter) + "/"
                    + this.workerName, ssrc.toString().getBytes(),
                    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            LOG.info(bspZKRoot + "/" + jobId.toString().substring(17) + "-sc"
                    + "/" + Integer.toString(superStepCounter) + "/"
                    + this.workerName + ":" + ssrc.toString());
            closeZooKeeper();
            return true;
        } catch (KeeperException e) {
            closeZooKeeper();
            LOG.error("[secondStageSuperStepBarrier]", e);
            return false;
        } catch (InterruptedException e) {
            closeZooKeeper();
            LOG.error("[secondStageSuperStepBarrier]", e);
            return false;
        }
    }

    @Override
    public boolean checkPointStageSuperStepBarrier(int superStepCounter,
            SuperStepReportContainer ssrc) {
        LOG.info(this.jobId.toString() + " on " + this.workerName
                + " enter the checkPointStageBarrier of "
                + Integer.toString(superStepCounter));

        try {
            this.zk = getZooKeeper();
            LOG.info("[checkPointStageSuperStepBarrier]" + this.bspZKRoot + "/" + jobId.toString().substring(17) + "-ss"
                    + "/" + Integer.toString(superStepCounter) + "/"
                    + this.workerName + (ssrc.getDirFlag())[0]);
            
            String send = "no information";
            if (ssrc.getStageFlag() == Constants.SUPERSTEP_STAGE.READ_CHECKPOINT_STAGE) {
                send = ssrc.getActiveMQWorkerNameAndPorts();
                LOG.info("Test send:" + send);
                LOG.info("Test path:" + bspZKRoot + "/" + jobId.toString().substring(17) + "-ss"
                        + "/" + Integer.toString(superStepCounter) + "/"
                        + this.workerName + (ssrc.getDirFlag())[0]);
            }
            zk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-ss"
                    + "/" + Integer.toString(superStepCounter) + "/"
                    + this.workerName + (ssrc.getDirFlag())[0], send.getBytes(),
                    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            closeZooKeeper();
            return true;
        } catch (KeeperException e) {
            closeZooKeeper();
            LOG.error("[checkPointStageSuperStepBarrier]", e);
            return false;
        } catch (InterruptedException e) {
            closeZooKeeper();
            LOG.error("[checkPointStageSuperStepBarrier]", e);
            return false;
        }
    }

    @Override
    public boolean saveResultStageSuperStepBarrier(int superStepCounter,
            SuperStepReportContainer ssrc) {
        try {
            this.zk = getZooKeeper();
            Stat s = null;
            String[] dir = ssrc.getDirFlag();
            for (String e : dir) {
                s = zk.exists(bspZKRoot + "/" + jobId.toString().substring(17)
                        + "-ss" + "/" + Integer.toString(superStepCounter)
                        + "/" + this.workerName + e, false);
                if (s != null) {
                    zk.delete(bspZKRoot + "/" + jobId.toString().substring(17)
                            + "-ss" + "/" + Integer.toString(superStepCounter)
                            + "/" + this.workerName + e, s.getAversion());
                }
            }
            closeZooKeeper();
            return true;
        } catch (KeeperException e) {
            closeZooKeeper();
            LOG.error("[saveResultStageSuperStepBarrier]", e);
            return false;
        } catch (InterruptedException e) {
            closeZooKeeper();
            LOG.error("[saveResultStageSuperStepBarrier]", e);
            return false;
        }
    }
    
    private ZooKeeper getZooKeeper() {
        try {
            if (this.zk == null) {
                return new ZooKeeper(this.zookeeperAddr, SESSION_TIMEOUT, this);
            } else {
                return this.zk;
            }
        } catch (Exception e) {
            LOG.error("[getZooKeeper]", e);
            return null;
        }
    }
    
    private void closeZooKeeper() {
        
        if (this.zk != null) {
            try {
                this.zk.close();
                this.zk = null;
            } catch (InterruptedException e) {
                LOG.error("[closeZooKeeper]", e);
            }
        }
    }
    
    @Override
    public void process(WatchedEvent event) {

        synchronized (mutex) {
            mutex.notify();
        }
    }
}
