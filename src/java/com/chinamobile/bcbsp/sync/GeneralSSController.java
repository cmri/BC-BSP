/**
 * CopyRight by Chinamobile
 * 
 * GeneralSSController.java
 */
package com.chinamobile.bcbsp.sync;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.mortbay.log.Log;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.bspcontroller.JobInProgressControlInterface;
import com.chinamobile.bcbsp.util.BSPJobID;

/**
 * GeneralSSController
 * 
 * GeneralSSController for completing the general SuperStep synchronization
 * control. This class is connected to JobInProgress.
 * 
 * @author
 * @version
 */

public class GeneralSSController implements Watcher,
        GeneralSSControllerInterface {
    private static final org.apache.commons.logging.Log LOG = LogFactory.getLog(GeneralSSController.class);
    
    private BSPConfiguration conf;
    private JobInProgressControlInterface jip;
    private BSPJobID jobId;
    private int superStepCounter = 0;
    private int faultSuperStepCounter = 0;
    private int checkNumBase;

    private ZooKeeper zk = null;
    private final String zookeeperAddr;
    private final String bspZKRoot;
    private volatile Integer mutex = 0;
    
    private int stageFlag = 1;

    private ZooKeeperRun zkRun = new ZooKeeperRun();

    public class ZooKeeperRun extends Thread {

        public void startNextSuperStep(SuperStepCommand ssc) throws Exception {
            int nextSuperStep = ssc.getNextSuperStepNum();
            jip.reportLOG(jobId.toString()
                    + "the next superstepnum is : "
                    + nextSuperStep);
            Stat s = null;
            s = zk.exists(bspZKRoot + "/" + jobId.toString().substring(17)
                    + "-ss" + "/" + nextSuperStep, false);
            if (s == null) {
                zk.create(bspZKRoot + "/" + jobId.toString().substring(17)
                        + "-ss" + "/" + nextSuperStep, new byte[0],
                        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } else {
                jip.reportLOG("The node hash exists"
                        + bspZKRoot + "/" + jobId.toString().substring(17)
                        + "-ss" + "/" + nextSuperStep);
                List<String> tmpList = new ArrayList<String>();
                Stat tmpStat = null;
                tmpList = zk.getChildren(bspZKRoot + "/"
                        + jobId.toString().substring(17) + "-ss" + "/"
                        + nextSuperStep, false);
                for (String e : tmpList) {
                    tmpStat = zk.exists(bspZKRoot + "/"
                            + jobId.toString().substring(17) + "-ss" + "/"
                            + nextSuperStep + "/" + e, false);
                    zk.delete(bspZKRoot + "/" + jobId.toString().substring(17)
                            + "-ss" + "/" + nextSuperStep + "/" + e, tmpStat
                            .getAversion());
                }
            }

            s = zk.exists(bspZKRoot + "/" + jobId.toString().substring(17)
                    + "-sc" + "/" + nextSuperStep, false);
            if (s == null) {
                zk.create(bspZKRoot + "/" + jobId.toString().substring(17)
                        + "-sc" + "/" + nextSuperStep, new byte[0],
                        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } else {
                List<String> tmpList = new ArrayList<String>();
                Stat tmpStat = null;
                tmpList = zk.getChildren(bspZKRoot + "/"
                        + jobId.toString().substring(17) + "-sc" + "/"
                        + nextSuperStep, false);
                for (String e : tmpList) {
                    tmpStat = zk.exists(bspZKRoot + "/"
                            + jobId.toString().substring(17) + "-sc" + "/"
                            + nextSuperStep + "/" + e, false);
                    zk.delete(bspZKRoot + "/" + jobId.toString().substring(17)
                            + "-sc" + "/" + nextSuperStep + "/" + e, tmpStat
                            .getAversion());
                }
            }
            
            zk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-sc"
                    + "/" + superStepCounter + "/" + Constants.COMMAND_NAME,
                    ssc.toString().getBytes(), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            jip.reportLOG(jobId.toString() + " command of next is "
                    + ssc.toString());
            jip.reportLOG(jobId.toString() + " [Write Command Path] "
                    + bspZKRoot + "/" + jobId.toString().substring(17) + "-sc"
                    + "/" + superStepCounter + "/" + Constants.COMMAND_NAME);
            jip.reportLOG(jobId.toString() + " leave the barrier of "
                    + superStepCounter);
        }

        public void stopNextSuperStep(String command) throws Exception {
            zk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-sc"
                    + "/" + superStepCounter + "/" + Constants.COMMAND_NAME,
                    command.getBytes(), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            jip.reportLOG(jobId.toString() + " command of next is " + command);
            jip.reportLOG(jobId.toString() + " prepare to quit");
        }
        
        public void cleanReadHistory(int ableCheckPoint) {
            List<String> tmpList = new ArrayList<String>();
            Stat tmpStat = null;
            
            try {
                tmpList = zk.getChildren(bspZKRoot + "/"
                        + jobId.toString().substring(17) + "-ss" + "/"
                        + ableCheckPoint, false);
                for (String e : tmpList) {
                    tmpStat = zk.exists(bspZKRoot + "/"
                            + jobId.toString().substring(17) + "-ss" + "/"
                            + ableCheckPoint + "/" + e, false);
                    zk.delete(bspZKRoot + "/" + jobId.toString().substring(17)
                            + "-ss" + "/" + ableCheckPoint + "/" + e, tmpStat
                            .getAversion());
                    jip.reportLOG("The node hash exists"
                            + bspZKRoot + "/" + jobId.toString().substring(17)
                            + "-ss" + "/" + ableCheckPoint + "/" + e);
                }
            } catch (Exception exc) {
                jip.reportLOG(jobId.toString() + " [cleanReadHistory]" + exc.getMessage());
            }
        }

        /**
         * This is a thread and execute the logic control
         */
        public void run() {
            Stat s = null;
            boolean jobEndFlag = true;

            // create the directory for the 0th SuperStep
            try {
                s = zk.exists(bspZKRoot + "/" + jobId.toString().substring(17)
                        + "-ss" + "/" + superStepCounter, false);
                if (s == null) {
                    zk.create(bspZKRoot + "/" + jobId.toString().substring(17)
                            + "-ss" + "/" + superStepCounter, new byte[0],
                            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }

                s = zk.exists(bspZKRoot + "/" + jobId.toString().substring(17)
                        + "-sc" + "/" + superStepCounter, false);
                if (s == null) {
                    zk.create(bspZKRoot + "/" + jobId.toString().substring(17)
                            + "-sc" + "/" + superStepCounter, new byte[0],
                            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            } catch (Exception e) {
                jip.reportLOG(jobId.toString() + " [run]" + e.getMessage());
            }

            while (jobEndFlag) {
                try {
                    setStageFlag(Constants.SUPERSTEP_STAGE.FIRST_STAGE);
                    generalSuperStepBarrier(checkNumBase * 2);
                    setStageFlag(Constants.SUPERSTEP_STAGE.SECOND_STAGE);
                    SuperStepCommand ssc = getSuperStepCommand(checkNumBase);

                    switch (ssc.getCommandType()) {
                        case Constants.COMMAND_TYPE.START:
                            startNextSuperStep(ssc);
                            superStepCounter = ssc.getNextSuperStepNum();
                            jip.setSuperStepCounter(superStepCounter);
                            break;
                        case Constants.COMMAND_TYPE.START_AND_CHECKPOINT:
                            startNextSuperStep(ssc);
                            generalSuperStepBarrier(checkNumBase * 3);
                            jip.setAbleCheckPoint(superStepCounter);
                            LOG.info("ableCheckPoint: " + superStepCounter);
                            superStepCounter = ssc.getNextSuperStepNum();
                            jip.setSuperStepCounter(superStepCounter);
                            break;
                        case Constants.COMMAND_TYPE.START_AND_RECOVERY:
                            cleanReadHistory(ssc.getAbleCheckPoint());
                            startNextSuperStep(ssc);
                            
                            setCheckNumBase();
                            
                            superStepCounter = ssc.getAbleCheckPoint();
                            generalSuperStepBarrier(checkNumBase * 1);
                            superStepCounter = ssc.getNextSuperStepNum();
                            jip.setSuperStepCounter(superStepCounter);
                            break;
                        case Constants.COMMAND_TYPE.STOP:
                            stopNextSuperStep(ssc.toString());
                            jobEndFlag = quitBarrier();
                            break;
                        default:
                            jip.reportLOG(jobId.toString()
                                    + " Unkonwn command of "
                                    + ssc.getCommandType());
                    }
                } catch (Exception e) {
                    jip.reportLOG(jobId.toString() + "error: " + e.toString());
                }
            }// while(jobEndFlag)
        }// run
    }

    /**
     * Generate the GeneralSSController to control the synchronization between
     * SuperSteps
     * 
     * @param jobId
     */
    @SuppressWarnings("unused")
    public GeneralSSController(BSPJobID jobId) {

        this.jobId = jobId;
        this.conf = new BSPConfiguration();
        this.zookeeperAddr = conf.get(Constants.ZOOKEEPER_QUORUM)
                + ":"
                + conf.getInt(Constants.ZOOKEPER_CLIENT_PORT,
                        Constants.DEFAULT_ZOOKEPER_CLIENT_PORT);
        this.bspZKRoot = Constants.BSPJOB_ZOOKEEPER_DIR_ROOT;

        // adjust the location. This function must be located there.
        // If it is located in run(), may be crashed on ZooKeeper cluster.
        setup();
    }

    @Override
    public boolean isCommandBarrier() {
        try {
            List<String> list = new ArrayList<String>();
            list = zk.getChildren(bspZKRoot + "/"
                    + jobId.toString().substring(17) + "-sc" + "/"
                    + faultSuperStepCounter, false);
            jip.reportLOG("[isCommandBarrier] path: " + bspZKRoot + "/"
                    + jobId.toString().substring(17) + "-sc" + "/"
                    + faultSuperStepCounter);
            if (list.size() < checkNumBase + 1) {
                jip.reportLOG("[isCommandBarrier] " + list.size() + " instead of " + (checkNumBase + 1));
                jip.reportLOG("[isCommandBarrier] " + list.toString());
                return false;
            } else {
                jip.reportLOG("[isCommandBarrier] " + list.size());
                return true;
            }
        } catch (Exception e) {
            jip.reportLOG("[isCommandBarrier] " + e.getMessage());
            return false;
        }
    }
    
    @Override
    public void setJobInProgressControlInterface(
            JobInProgressControlInterface jip) {

        this.jip = jip;
        this.superStepCounter = jip.getSuperStepCounter();
    }

    @Override
    public void setCheckNumBase() {
        this.checkNumBase = jip.getCheckNum();
    }

    public int getStageFlag() {
        return stageFlag;
    }

    public void setStageFlag(int stageFlag) {
        this.stageFlag = stageFlag;
    }
    
    /**
     * Connect to ZooKeeper cluster and create the root directory for the job
     */
    @Override
    public void setup() {
        try {
            this.zk = new ZooKeeper(this.zookeeperAddr, 3000, this);
            if (zk != null) {
                Stat s = null;

                // create the directory for scheduler
                s = zk.exists(this.bspZKRoot + "/"
                        + this.jobId.toString().substring(17) + "-s", false);
                if (s == null) {
                    zk.create(this.bspZKRoot + "/"
                            + this.jobId.toString().substring(17) + "-s",
                            new byte[0], Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                }

                // create the directory for load data
                s = zk.exists(this.bspZKRoot + "/"
                        + this.jobId.toString().substring(17) + "-d", false);
                if (s == null) {
                    zk.create(this.bspZKRoot + "/"
                            + this.jobId.toString().substring(17) + "-d",
                            new byte[0], Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                }

                // create the directory for SuperStep
                s = zk.exists(this.bspZKRoot + "/"
                        + this.jobId.toString().substring(17) + "-ss", false);
                if (s == null) {
                    zk.create(this.bspZKRoot + "/"
                            + this.jobId.toString().substring(17) + "-ss",
                            new byte[0], Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                }

                // create the directory for SuperStep Command
                s = zk.exists(this.bspZKRoot + "/"
                        + this.jobId.toString().substring(17) + "-sc", false);
                if (s == null) {
                    zk.create(this.bspZKRoot + "/"
                            + this.jobId.toString().substring(17) + "-sc",
                            new byte[0], Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                }
            }
        } catch (Exception e) {
            jip.reportLOG(jobId.toString() + " [setup]" + e.getMessage());
        }
    }

    /**
     * Connect to ZooKeeper cluster and delete the directory for the job
     */
    @Override
    public void cleanup() {

        Stat statJob = null;
        Stat statStaff = null;
        Stat tmpStat = null;
        List<String> list = new ArrayList<String>();
        List<String> tmpList = new ArrayList<String>();

        try {
            // cleanup the directory of scheduler
            try {
                list.clear();
                list = zk.getChildren(this.bspZKRoot + "/"
                        + jobId.toString().substring(17) + "-s", false);
                for (String e : list) {
                    statStaff = zk.exists(this.bspZKRoot + "/"
                            + jobId.toString().substring(17) + "-s" + "/" + e,
                            false);
                    zk.delete(this.bspZKRoot + "/"
                            + jobId.toString().substring(17) + "-s" + "/" + e,
                            statStaff.getVersion());
                }
            } catch (Exception e) {
                // Undo
            } finally {
                statJob = zk.exists(this.bspZKRoot + "/"
                        + jobId.toString().substring(17) + "-s", false);
                zk.delete(this.bspZKRoot + "/" + jobId.toString().substring(17)
                        + "-s", statJob.getVersion());
            }
            jip.reportLOG(jobId.toString() + "delete the -s");

            // cleanup the directory of load data
            try {
                list.clear();
                list = zk.getChildren(this.bspZKRoot + "/"
                        + jobId.toString().substring(17) + "-d", false);
                for (String e : list) {
                    statStaff = zk.exists(this.bspZKRoot + "/"
                            + jobId.toString().substring(17) + "-d" + "/" + e,
                            false);
                    zk.delete(this.bspZKRoot + "/"
                            + jobId.toString().substring(17) + "-d" + "/" + e,
                            statStaff.getVersion());
                }
            } catch (Exception e) {
                // Undo
            } finally {
                statJob = zk.exists(this.bspZKRoot + "/"
                        + jobId.toString().substring(17) + "-d", false);
                zk.delete(this.bspZKRoot + "/" + jobId.toString().substring(17)
                        + "-d", statJob.getVersion());
            }
            jip.reportLOG(jobId.toString() + "delete the -d");

            // cleanup the directory of SuperStep control
            list.clear();
            list = zk.getChildren(this.bspZKRoot + "/"
                    + jobId.toString().substring(17) + "-ss", false);
            for (String e : list) {
                try {
                    tmpList.clear();
                    tmpList = zk.getChildren(this.bspZKRoot + "/"
                            + jobId.toString().substring(17) + "-ss" + "/" + e,
                            false);
                    for (String ee : tmpList) {
                        tmpStat = zk.exists(this.bspZKRoot + "/"
                                + jobId.toString().substring(17) + "-ss" + "/"
                                + e + "/" + ee, false);
                        zk.delete(this.bspZKRoot + "/"
                                + jobId.toString().substring(17) + "-ss" + "/"
                                + e + "/" + ee, tmpStat.getAversion());
                    }
                } catch (Exception exc) {
                    // Undo
                } finally {
                    statStaff = zk.exists(this.bspZKRoot + "/"
                            + jobId.toString().substring(17) + "-ss" + "/" + e,
                            false);
                    zk.delete(this.bspZKRoot + "/"
                            + jobId.toString().substring(17) + "-ss" + "/" + e,
                            statStaff.getVersion());
                }
            }
            statJob = zk.exists(this.bspZKRoot + "/"
                    + jobId.toString().substring(17) + "-ss", false);
            zk.delete(this.bspZKRoot + "/" + jobId.toString().substring(17)
                    + "-ss", statJob.getVersion());
            jip.reportLOG(jobId.toString() + "delete the -ss");

            // cleanup the directory of SuperStep command
            list.clear();
            list = zk.getChildren(this.bspZKRoot + "/"
                    + jobId.toString().substring(17) + "-sc", false);
            for (String e : list) {
                try {
                    tmpList.clear();
                    tmpList = zk.getChildren(this.bspZKRoot + "/"
                            + jobId.toString().substring(17) + "-sc" + "/" + e,
                            false);
                    for (String ee : tmpList) {
                        tmpStat = zk.exists(this.bspZKRoot + "/"
                                + jobId.toString().substring(17) + "-sc" + "/"
                                + e + "/" + ee, false);
                        zk.delete(this.bspZKRoot + "/"
                                + jobId.toString().substring(17) + "-sc" + "/"
                                + e + "/" + ee, tmpStat.getAversion());
                    }
                } catch (Exception exc) {
                    // Undo
                } finally {
                    statStaff = zk.exists(this.bspZKRoot + "/"
                            + jobId.toString().substring(17) + "-sc" + "/" + e,
                            false);
                    zk.delete(this.bspZKRoot + "/"
                            + jobId.toString().substring(17) + "-sc" + "/" + e,
                            statStaff.getVersion());
                }
            }
            statJob = zk.exists(this.bspZKRoot + "/"
                    + jobId.toString().substring(17) + "-sc", false);
            zk.delete(this.bspZKRoot + "/" + jobId.toString().substring(17)
                    + "-sc", statJob.getVersion());
            jip.reportLOG(jobId.toString() + "delete the -sc");
        } catch (KeeperException e) {
            jip.reportLOG(jobId.toString() + "delet error: " + e.toString());
        } catch (InterruptedException e) {;
            jip.reportLOG(jobId.toString() + "delet error: " + e.toString());
        }
    }

    @Override
    public void start() {
        this.zkRun.start();
    }

    @Override
    @SuppressWarnings("deprecation")
    public void stop() {
        this.zkRun.stop();
    }

    @Override
    public boolean generalSuperStepBarrier(int checkNum) {
        List<String> list = new ArrayList<String>();

        try {
            // make sure that all staffs complete the computation and
            // receiving-messages
            jip.reportLOG(jobId.toString() + " enter the barrier of "
                    + superStepCounter);
            while (true) {
                synchronized (mutex) {
                    list.clear();
                    list = zk.getChildren(bspZKRoot + "/"
                            + jobId.toString().substring(17) + "-ss" + "/"
                            + superStepCounter, true);
                    if (list.size() < checkNum) {
                        mutex.wait();
                    } else {
                        break;
                    }
                }
            }// while(true)
            return true;
        } catch (KeeperException e) {
            jip.reportLOG(jobId.toString() + "error: " + e.toString());
            return false;
        } catch (InterruptedException e) {
            jip.reportLOG(jobId.toString() + "error: " + e.toString());
            return false;
        }
    }

    @Override
    public SuperStepCommand getSuperStepCommand(int checkNum) {
        Stat s = null;
        List<String> list = new ArrayList<String>();

        try {
            // make sure that all staffs have reported the info
            while (true) {
                synchronized (mutex) {
                    list.clear();
                    list = zk.getChildren(bspZKRoot + "/"
                            + jobId.toString().substring(17) + "-sc" + "/"
                            + superStepCounter, true);
                    if (list.size() < checkNum) {
                        jip.reportLOG("[getSuperStepCommand]: " + list.size() + " instead of " + checkNum);
                        mutex.wait();
                    } else {
                        jip.reportLOG("[getSuperStepCommand]: " + list.size());
                        break;
                    }
                }
            }// while(true)

            // give the command to all staffs according to the report info
            SuperStepReportContainer[] ssrcs = new SuperStepReportContainer[checkNumBase];
            int counter = 0;
            for (String e : list) {
                s = zk.exists(bspZKRoot + "/" + jobId.toString().substring(17)
                        + "-sc" + "/" + superStepCounter + "/" + e, false);
                byte[] b = zk.getData(bspZKRoot + "/"
                        + jobId.toString().substring(17) + "-sc" + "/"
                        + superStepCounter + "/" + e, false, s);
                ssrcs[counter++] = new SuperStepReportContainer(new String(b));
            }
            SuperStepCommand ssc = jip.generateCommand(ssrcs);
            return ssc;
        } catch (KeeperException e) {
            e.printStackTrace();
            jip.reportLOG(jobId.toString() + "error: " + e.toString());
            return null;
        } catch (InterruptedException e) {
            e.printStackTrace();
            jip.reportLOG(jobId.toString() + "error: " + e.toString());
            return null;
        }
    }

    @SuppressWarnings("finally")
    @Override
    public boolean quitBarrier() {
        List<String> list = new ArrayList<String>();

        try {
            while (true) {
                synchronized (mutex) {
                    list.clear();
                    list = zk.getChildren(bspZKRoot + "/"
                            + jobId.toString().substring(17) + "-ss" + "/"
                            + superStepCounter, true);
                    if (list.size() > 0) {
                        mutex.wait();
                    } else {
                        break;
                    }
                }
            }// while(true)
        } catch (KeeperException e) {
            e.printStackTrace();
            jip.reportLOG(jobId.toString() + "error: " + e.toString());
        } catch (InterruptedException e) {
            e.printStackTrace();
            jip.reportLOG(jobId.toString() + "error: " + e.toString());
        } finally {
            jip.completedJob();
            return false;
        }
    }

    @Override
    public void process(WatchedEvent event) {

        synchronized (mutex) {
            mutex.notify();
        }
    }

    @Override
    public void recoveryBarrier(List<String> WMNames) {
        Log.info("recoveryBarrier: this.superStepCounter " + superStepCounter);
        
        faultSuperStepCounter = superStepCounter;
        int base = WMNames.size();
        
        switch (this.stageFlag) { 
        case Constants.SUPERSTEP_STAGE.FIRST_STAGE :   
            
            try{
                jip.reportLOG("recoveried: " + this.jobId.toString()  
                        + " enter the firstStageSuperStepBarrier of " + Integer.toString(superStepCounter));           
            
                for(int i=0; i<base*2; i++) {// WMNames.get(0)
                    zk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-ss" 
                            + "/" + Integer.toString(superStepCounter) + "/" 
                            + WMNames.get(0)  + "-recovery" + i, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);//slave2-recovery0
                    Log.info("first--recoveryBarrier: " + "recovery" + i);
                }               
                
                jip.reportLOG("recoveried: " + this.jobId.toString()  
                        + " enter the secondStageSuperStepBarrier(first) of " + Integer.toString(superStepCounter));
                
                for(int i=0; i<base; i++) {
                    zk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-sc" 
                            + "/" + Integer.toString(superStepCounter) + "/" + WMNames.get(i)  + "-recovery" + i, 
                            "RECOVERY".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    Log.info("second-(first)--recoveryBarrier: " + "recovery" + i);
                }                
            }catch(KeeperException e){
                e.printStackTrace();
            }catch(InterruptedException e){
                e.printStackTrace();
            }
            
            break;
            
        case Constants.SUPERSTEP_STAGE.SECOND_STAGE ://int 4
            
            try{
                jip.reportLOG("recoveried " + this.jobId.toString()  
                        + " enter the secondStageSuperStepBarrier(second) of superStepCounter: " + Integer.toString(superStepCounter));
                                                
                for(int i=0; i<base; i++) {
                    zk.create(bspZKRoot + "/" + jobId.toString().substring(17) + "-sc" 
                            + "/" + Integer.toString(superStepCounter) + "/" + WMNames.get(i) + "-recovery" + i, 
                            "RECOVERY".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    Log.info("second--recoveryBarrier: " + "recovery" + i);
                }
                            
            }catch(KeeperException e){
                e.printStackTrace();
            }catch(InterruptedException e){
                e.printStackTrace();
            }
            
            break;
    
        default :
                jip.reportLOG(jobId.toString() + " Unkonwn command of " );
            
        }//switch   
    }
}
