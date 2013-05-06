/**
 * CopyRight by Chinamobile
 * 
 * StaffSSController.java
 */
package com.chinamobile.bcbsp.sync;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.workermanager.WorkerAgentProtocol;

/**
 * StaffSSController
 * 
 * StaffSSController for completing the staff SuperStep synchronization control.
 * This class is connected to BSPStaff.
 * 
 * @author
 * @version
 */
public class StaffSSController implements StaffSSControllerInterface, Watcher {

    private BSPConfiguration conf;
    private static final int SESSION_TIMEOUT = 5000;
    private final String zookeeperAddr;
    private final String bspZKRoot;
    private ZooKeeper zk = null;
    /**
     * Review comment:
     *      why is here volatile used
     * Review time: 2011-11-30
     * Reviewer: Hongxu Zhang.  
     * 
     * Fix log:
     *      we use volatile to specify mutex is to force the system not to read the value 
     *      from a cache everytime this mutex needs to be read, otherwise the system will read
     *      its value from a cache which may makes the mutex-user get a old value.
     * Fix time: 2011-12-1
     * Programmer: Hu Zheng
     */
    private volatile Integer mutex = 0;
    private BSPJobID jobId;
    private StaffAttemptID staffId;
    private WorkerAgentProtocol workerAgent;
    public static final Log LOG = LogFactory.getLog(StaffSSController.class);

    public static long rebuildTime = 0;/**Clock*/
    /**
     * Generate the StaffSSController to control the synchronization between
     * SuperSteps
     * 
     * @param jobId
     */
    public StaffSSController(BSPJobID jobId, StaffAttemptID staffId,
            WorkerAgentProtocol workerAgent) {

        this.conf = new BSPConfiguration();
        this.zookeeperAddr = conf.get(Constants.ZOOKEEPER_QUORUM)
                + ":"
                + conf.getInt(Constants.ZOOKEPER_CLIENT_PORT,
                        Constants.DEFAULT_ZOOKEPER_CLIENT_PORT);
        this.bspZKRoot = Constants.BSPJOB_ZOOKEEPER_DIR_ROOT;
        
        this.jobId = jobId;
        this.staffId = staffId;
        this.workerAgent = workerAgent;
    }

    @Override
    public HashMap<Integer, String> scheduleBarrier(
            SuperStepReportContainer ssrc) {
        LOG.info(staffId + " enter the scheduleBarrier");
        String send = ssrc.getPartitionId() + Constants.SPLIT_FLAG
                + this.workerAgent.getWorkerManagerName(jobId, staffId)+Constants.SPLIT_FLAG+ssrc.getPort1()
                + "-" + ssrc.getPort2();

        try {
            this.zk = getZooKeeper();
            zk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-s"
                    + "/" + staffId.toString(), send.getBytes(),
                    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            List<String> list = new ArrayList<String>();
            List<String> workerManagerNames = new ArrayList<String>();
            HashMap<Integer, String> partitionToWorkerManagerName = new HashMap<Integer, String>();
            Stat s = null;
            int checkNum = ssrc.getCheckNum();
            while (true) {
                synchronized (mutex) {
                    list.clear();
                    list = zk.getChildren(bspZKRoot + "/"
                            + jobId.toString().substring(17) + "-s", true);
                    if (list.size() < checkNum) {
                        LOG.info(list.size() + " of " + checkNum
                                + " ,waiting......");
                        mutex.wait();
                    } else {
                        for (String e : list) {
                            s = zk.exists(bspZKRoot + "/"
                                    + jobId.toString().substring(17) + "-s"
                                    + "/" + e, false);
                            byte[] b = zk.getData(bspZKRoot + "/"
                                    + jobId.toString().substring(17) + "-s"
                                    + "/" + e, false, s);

                            String[] receive = new String(b).split(Constants.SPLIT_FLAG);
                            String hostWithPort = receive[1] + ":" + receive[2];
                            LOG.info(hostWithPort);

                            partitionToWorkerManagerName.put(Integer.valueOf(receive[0]), hostWithPort);
                            workerAgent.setWorkerNametoPartitions(jobId,
                                    Integer.valueOf(receive[0]), receive[1]);
                            if (!workerManagerNames.contains(receive[1])) {
                                workerManagerNames.add(receive[1]);
                            }
                        }
                        this.workerAgent.setNumberWorkers(jobId, staffId, workerManagerNames.size());
                        list.clear();
                        workerManagerNames.clear();
                        break;
                    }
                }
            }
            closeZooKeeper();
            LOG.info(staffId + " leave the scheduleBarrier");
            return partitionToWorkerManagerName;
        } catch (KeeperException e) {
            closeZooKeeper();
            LOG.error("[schedulerBarrier]", e);
            return null;
        } catch (InterruptedException e) {
            closeZooKeeper();
            LOG.error("[schedulerBarrier]", e);
            return null;
        }
    }

    @Override
    public HashMap<Integer, List<Integer>> loadDataBarrier(
            SuperStepReportContainer ssrc) {
        LOG.info(staffId + " enter the loadDataBarrier");
        String send = ssrc.getPartitionId() + Constants.SPLIT_FLAG
                + ssrc.getMinRange() + Constants.SPLIT_FLAG
                + ssrc.getMaxRange();
        int checkNum = ssrc.getCheckNum();
        try {
            this.zk = getZooKeeper();
            zk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-d"
                    + "/" + staffId.toString(), send.getBytes(),
                    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            List<String> list = new ArrayList<String>();
            HashMap<Integer, List<Integer>> partitionToRange = new HashMap<Integer, List<Integer>>();
            Stat s = null;
            while (true) {
                synchronized (mutex) {
                    list = zk.getChildren(bspZKRoot + "/"
                            + jobId.toString().substring(17) + "-d", true);
                    if (list.size() < checkNum) {
                        LOG.info(list.size() + " of " + checkNum
                                + " ,waiting......");
                        mutex.wait();
                    } else {
                        for (String e : list) {
                            s = zk.exists(bspZKRoot + "/"
                                    + jobId.toString().substring(17) + "-d"
                                    + "/" + e, false);
                            byte[] b = zk.getData(bspZKRoot + "/"
                                    + jobId.toString().substring(17) + "-d"
                                    + "/" + e, false, s);
                            String[] receive = new String(b)
                                    .split(Constants.SPLIT_FLAG);
                            List<Integer> tmp_list = new ArrayList<Integer>();
                            tmp_list.add(Integer.parseInt(receive[1]));
                            tmp_list.add(Integer.parseInt(receive[2]));
                            partitionToRange.put(Integer.parseInt(receive[0]),
                                    tmp_list);
                        }// for
                        closeZooKeeper();
                        LOG.info(staffId + " leave the loadDataBarrier");
                        return partitionToRange;
                    }// if-else
                }// sync
            }// while
        } catch (KeeperException e) {
            closeZooKeeper();
            LOG.error("[loadDataBarrier]", e);
            return null;
        } catch (InterruptedException e) {
            closeZooKeeper();
            LOG.error("[loadDataBarrier]", e);
            return null;
        }
    }

    @Override
    public boolean loadDataBarrier(SuperStepReportContainer ssrc,
            String partitionType) {
        LOG.info(staffId + " enter the loadDataBarrier");

        int checkNum = ssrc.getCheckNum();
        try {
            this.zk = getZooKeeper();
            zk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-d"
                    + "/" + staffId.toString() + "-" + ssrc.getDirFlag()[0],
                    new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            List<String> list = new ArrayList<String>();
            ;
            while (true) {
                synchronized (mutex) {
                    list = zk.getChildren(bspZKRoot + "/"
                            + jobId.toString().substring(17) + "-d", true);
                    if (list.size() < checkNum) {
                        LOG.info(list.size() + " of " + checkNum
                                + " ,waiting......");
                        mutex.wait();
                    } else {
                        closeZooKeeper();
                        LOG.info(staffId + " leave the loadDataBarrier");
                        return true;
                    }// if-else
                }// sync
            }// while
        } catch (KeeperException e) {
            closeZooKeeper();
            LOG.error("[loadDataBarrier]", e);
            return false;
        } catch (InterruptedException e) {
            closeZooKeeper();
            LOG.error("[loadDataBarrier]", e);
            return false;
        }
    }

    @Override
    public HashMap<Integer, Integer> loadDataInBalancerBarrier(
            SuperStepReportContainer ssrc, String partitionType) {
        LOG.info(staffId + " enter the loadDataInBalancerBarrier");
        String counterInfo = "";
        for (Integer e : ssrc.getCounter().keySet()) {
            counterInfo += e + Constants.SPLIT_FLAG + ssrc.getCounter().get(e)
                    + Constants.SPLIT_FLAG;
        }
        counterInfo += staffId.toString().substring(26,32 ); 
        try {
            this.zk = getZooKeeper();
            zk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-d"
                    + "/" + staffId.toString() + "-" + ssrc.getDirFlag()[0],
                    counterInfo.getBytes(), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);

            List<String> list = new ArrayList<String>();
            HashMap<Integer, Integer> counter = new HashMap<Integer, Integer>();
            Stat s = null;
            int checkNum = ssrc.getCheckNum();
            int copyNum = ssrc.getNumCopy();
            int [][]bucketHasHeadNodeInStaff=new int[checkNum*copyNum][checkNum];
            while (true) {
                synchronized (mutex) {
                    list = zk.getChildren(bspZKRoot + "/"
                            + jobId.toString().substring(17) + "-d", true);
                    if (list.size() < checkNum) {
                        LOG.info(list.size() + " of " + checkNum
                                + " waiting......");
                        mutex.wait();
                    } else {
                        for (String e : list) {
                            s = zk.exists(bspZKRoot + "/"
                                    + jobId.toString().substring(17) + "-d"
                                    + "/" + e, false);
                            byte[] b = zk.getData(bspZKRoot + "/"
                                    + jobId.toString().substring(17) + "-d"
                                    + "/" + e, false, s);
                            String receive[] = new String(b)
                                    .split(Constants.SPLIT_FLAG);
                            int sid=Integer.parseInt(receive[receive.length-1]);
                            for (int i = 0; i < receive.length-1; i += 2) {    
                                int index = Integer.parseInt(receive[i]);
                                int value = Integer.parseInt(receive[i + 1]);
                                bucketHasHeadNodeInStaff[index][sid]+=value;
                                if (counter.containsKey(index)) {
                                    counter.put(index,
                                            (counter.get(index) + value));
                                } else {
                                    counter.put(index, value);
                                }
                            }
                        }

                        int numHeadNode = 0;
                        int index[] = new int[counter.keySet().size() + 1];
                        int value[] = new int[counter.keySet().size() + 1];
                        int k = 1;
                        for (Integer ix : counter.keySet()) {
                            index[k] = ix;
                            value[k] = counter.get(ix);
                            numHeadNode += value[k];
                            k++;
                        }
                        HashMap<Integer, Integer> hashBucketToPartition = new HashMap<Integer, Integer>();
                        HashMap<Integer, Integer> bucketIDToPartitionID = new HashMap<Integer, Integer>();
                        int pid[] = new int[checkNum];
                        boolean flag[] = new boolean[index.length];
                        int avg = numHeadNode / checkNum;
                        int[][][]partitionHasHeadNodeInStaff=new int[checkNum][checkNum][1];
                        
                        for (int n = 0; n < checkNum; n++) {                            
                            boolean stop = false;
                            while (!stop) {
                                int id = find(value, flag, avg - pid[n]);
                                if (id != 0) {
                                    pid[n] += value[id];
                                    flag[id] = true;
                                    for(int i=0;i<checkNum;i++){
                                        partitionHasHeadNodeInStaff[n][i][0]+=bucketHasHeadNodeInStaff[index[id]][i];
                                    }                                        
                                    hashBucketToPartition.put(index[id], n);
                                } else {
                                    stop = true;
                                }
                            }
                        }

                        int id = pid.length - 1;
                        for (int i = 1; i < flag.length; i++) {
                            if (!flag[i]) {
                                for(int j=0;j<checkNum;j++){
                                    partitionHasHeadNodeInStaff[id % pid.length][j][0]+=bucketHasHeadNodeInStaff[index[i]][j];
                                }  
                                hashBucketToPartition.put(index[i], id--
                                        % pid.length);
                                if (id == -1)
                                    id = pid.length - 1;
                            }
                        }
                        boolean [] hasBeenSelected = new boolean[checkNum];
                        for(int i=0;i<checkNum;i++){
                            int partitionID=0;
                            int max=Integer.MIN_VALUE;
                            for(int j=0;j<checkNum;j++){
                                if(partitionHasHeadNodeInStaff[i][j][0]>max && !hasBeenSelected[j]){
                                    max=partitionHasHeadNodeInStaff[i][j][0];
                                    partitionID=j;                                    
                                }
                            }
                            hasBeenSelected[partitionID] = true;
                            for(int e:hashBucketToPartition.keySet()){
                                if(hashBucketToPartition.get(e)==i){
                                    bucketIDToPartitionID.put(e, partitionID);
                                }
                            }
                        }
                        closeZooKeeper();
                        LOG.info(staffId
                                + " leave the loadDataInBalancerBarrier");
                        return bucketIDToPartitionID;
                    }// if-else
                }// sync
            }// while
        } catch (KeeperException e) {
            closeZooKeeper();
            LOG.error("[loadDataInBalancerBarrier]", e);
            return null;
        } catch (InterruptedException e) {
            closeZooKeeper();
            LOG.info("[loadDataInBalancerBarrier]", e);
            return null;
        }
    }

    private static int find(int value[], boolean flag[], int lack) {
        int id = 0;
        int min = Integer.MAX_VALUE;
        for (int i = value.length - 1; i > 0; i--) {
            if (!flag[i]) {
                if ((lack - value[i] >= 0) && (min > lack - value[i])) {
                    min = lack - value[i];
                    id = i;
                }
            }

        }
        return id;

    }

    @Override
    public boolean firstStageSuperStepBarrier(int superStepCounter,
            SuperStepReportContainer ssrc) {
        LOG.info(staffId.toString() + " enter the firstStageSuperStepBarrier of " + Integer.toString(superStepCounter));
        List<String> list = new ArrayList<String>();
        int checkNum = ssrc.getCheckNum();
        try {
            this.zk = getZooKeeper();
            workerAgent.localBarrier(jobId, staffId, superStepCounter, ssrc);

            while (true) {
                synchronized (mutex) {
                    list.clear();
                    list = zk.getChildren(bspZKRoot + "/"
                            + jobId.toString().substring(17) + "-ss" + "/"
                            + Integer.toString(superStepCounter), true);
                    if (list.size() < checkNum) {
                        LOG.info(list.size() + " of " + checkNum
                                + " ,waiting......");
                        mutex.wait();
                    } else {
                        closeZooKeeper();
                        LOG.info("[firstStageSuperStepBarrier]--[List]" + list.toString());
                        
                        LOG.info(staffId.toString()
                                + " leave the firstStageSuperStepBarrier of "
                                + Integer.toString(superStepCounter));
                        return true;
                    }
                }
            }
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
    public SuperStepCommand secondStageSuperStepBarrier(int superStepCounter,
            SuperStepReportContainer ssrc) {
        LOG.info(staffId.toString()
                + " enter the secondStageSuperStepBarrier of "
                + Integer.toString(superStepCounter));
        Stat s = null;
        List<String> list = new ArrayList<String>();
        int checkNum = ssrc.getCheckNum();
        try {
            this.zk = getZooKeeper();
            workerAgent.localBarrier(jobId, staffId, superStepCounter, ssrc);
            while (true) {
                synchronized (mutex) {
                    list.clear();
                    list = zk.getChildren(bspZKRoot + "/"
                            + jobId.toString().substring(17) + "-sc" + "/"
                            + Integer.toString(superStepCounter), true);
                    LOG.info("[secondStageSuperStepBarrier] ZooKeeper get children....");
                    if (list.size() < checkNum) {
                        mutex.wait();
                        LOG.info(list.size() + " of " + checkNum
                                + " ,waiting......");
                    } else {
                        
                        LOG.info("[secondStageSuperStepBarrier]--[List]" + list.toString());
                        
                        s = zk.exists(bspZKRoot + "/"
                                + jobId.toString().substring(17) + "-sc" + "/"
                                + superStepCounter + "/"
                                + Constants.COMMAND_NAME, false);
                        byte[] b = zk.getData(bspZKRoot + "/"
                                + jobId.toString().substring(17) + "-sc" + "/"
                                + superStepCounter + "/"
                                + Constants.COMMAND_NAME, false, s);
                        LOG.info("[Get Command Path]" + bspZKRoot + "/"
                                + jobId.toString().substring(17) + "-sc" + "/"
                                + superStepCounter + "/"
                                + Constants.COMMAND_NAME);
                        LOG.info("[Init Command String]" + new String(b));
                        SuperStepCommand ssc = new SuperStepCommand(new String(
                                b));
                        closeZooKeeper();
                        LOG.info(staffId.toString()
                                + " leave the secondStageSuperStepBarrier of "
                                + Integer.toString(superStepCounter));
                        return ssc;
                    }
                }
            }
        } catch (KeeperException e) {
            closeZooKeeper();
            LOG.error("[secondStageSuperStepBarrier]", e);
            return null;
        } catch (InterruptedException e) {
            closeZooKeeper();
            LOG.error("[secondStageSuperStepBarrier]", e);
            return null;
        } catch (Exception e) {
            closeZooKeeper();
            LOG.error("[secondStageSuperStepBarrier]", e);
            return null;
        }
    }
    
    @Override
    public SuperStepCommand secondStageSuperStepBarrierForRecovery(int superStepCounter) {
        LOG.info("secondStageSuperStepBarrierForRecovery(int superStepCounter) " + superStepCounter);
        SuperStepCommand ssc = null;
        Stat s = null;
        try {
            this.zk = getZooKeeper();
            
            while (true) {
                s = zk.exists(bspZKRoot + "/" + jobId.toString().substring(17) 
                        + "-sc" + "/" + superStepCounter + "/" + Constants.COMMAND_NAME, false);
                if (s != null) {
                    break;
                } else {
                    Thread.sleep(500);
                }
            }
            
            byte[] b = zk.getData(bspZKRoot + "/" + jobId.toString().substring(17) 
                    + "-sc" + "/" + superStepCounter + "/" + Constants.COMMAND_NAME, false, s);
            LOG.info("byte[] b = zk.getData size: " + b.length);
            ssc = new SuperStepCommand(new String(b));
            closeZooKeeper();
            LOG.info(staffId.toString() + " recovery: superStepCounter: "
                    + Integer.toString(superStepCounter));
            return ssc;
        } catch (KeeperException e) {
            closeZooKeeper();
            LOG.error("[secondStageSuperStepBarrierForRecovery]", e);
            return null;
        } catch (InterruptedException e) {
            closeZooKeeper();
            LOG.error("[secondStageSuperStepBarrierForRecovery]", e);
            return null;
        }
        
    }

    @Override
    public HashMap<Integer, String> checkPointStageSuperStepBarrier(int superStepCounter,
            SuperStepReportContainer ssrc) {
        LOG.info(staffId.toString()
                + " enter the checkPointStageSuperStepBarrier of "
                + Integer.toString(superStepCounter) + " : "
                + ssrc.getDirFlag()[0]);
        
        HashMap<Integer, String> workerNameAndPorts = new HashMap<Integer, String>();
        
        List<String> list = new ArrayList<String>();
        int checkNum = ssrc.getCheckNum();
        try {
            this.zk = getZooKeeper();
            workerAgent.localBarrier(jobId, staffId, superStepCounter, ssrc);

            while (true) {
                synchronized (mutex) {
                    list.clear();
                    list = zk.getChildren(bspZKRoot + "/"
                            + jobId.toString().substring(17) + "-ss" + "/"
                            + Integer.toString(superStepCounter), true);
                    if (list.size() < checkNum) {
                        LOG.info(list.size() + " of " + checkNum
                                + " ,waiting......");
                        mutex.wait();
                    } else {
                        if (ssrc.getStageFlag() != Constants.SUPERSTEP_STAGE.READ_CHECKPOINT_STAGE) {
                            return null;
                        }
                        
                        for (String e : list) {
                            LOG.info("Test ReadCK zookeeper:" + e);
                            Stat s = zk.exists(bspZKRoot + "/"
                                    + jobId.toString().substring(17) + "-ss" + "/"
                                    + Integer.toString(superStepCounter)
                                    + "/" + e, false);
                            if (!e.endsWith(ssrc.getDirFlag()[0])) {
                                continue;
                            }
                            
                            byte[] b = zk.getData(bspZKRoot + "/"
                                    + jobId.toString().substring(17) + "-ss" + "/"
                                    + Integer.toString(superStepCounter)
                                    + "/" + e, false, s);
                            LOG.info("Test path:" + bspZKRoot + "/"
                                    + jobId.toString().substring(17) + "-ss" + "/"
                                    + Integer.toString(superStepCounter)
                                    + "/" + e);
                            LOG.info("Test initBytes:" + new String(b));
                            String[] receive = new String(b).split(Constants.KV_SPLIT_FLAG);
                            for (String str: receive) {
                                LOG.info("Test firstSplit:" + str);
                                String[] firstSplit = str.split(":");
                                workerNameAndPorts.put(Integer.valueOf(firstSplit[0]), firstSplit[1] + ":" + firstSplit[2]);
                            }
                        }
                            
                        closeZooKeeper();
                        LOG.info(staffId.toString()
                                + " leave the checkPointStageSuperStepBarrier of "
                                + Integer.toString(superStepCounter) + " : "
                                + ssrc.getDirFlag()[0]);
                        return workerNameAndPorts;
                    }
                }
            }
        } catch (KeeperException e) {
            closeZooKeeper();
            LOG.error("[checkPointStageSuperStepBarrier]", e);
            return null;
        } catch (InterruptedException e) {
            closeZooKeeper();
            LOG.error("[checkPointStageSuperStepBarrier]", e);
            return null;
        }
    }

    @Override
    public boolean saveResultStageSuperStepBarrier(int superStepCounter,
            SuperStepReportContainer ssrc) {
        LOG.info(staffId.toString()
                + " enter the saveResultStageSuperStepBarrier of "
                + Integer.toString(superStepCounter));
        workerAgent.localBarrier(jobId, staffId, superStepCounter, ssrc);
        LOG.info(staffId.toString()
                + " leave the saveResultStageSuperStepBarrier of "
                + Integer.toString(superStepCounter));
        return true;
    }

    @Override
    public void process(WatchedEvent event) {
        synchronized (mutex) {
            mutex.notify();
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
}
