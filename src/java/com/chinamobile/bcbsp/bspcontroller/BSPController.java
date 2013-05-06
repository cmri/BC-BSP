/**
 * CopyRight by Chinamobile
 * 
 * BSPController.java
 * 
 * BSPController is the center of the whole system,
 * responsible to control all of the WorkerManagers
 * and to manage bsp jobs.
 */
package com.chinamobile.bcbsp.bspcontroller;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.ReflectionUtils;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.action.Directive;
import com.chinamobile.bcbsp.action.KillStaffAction;
import com.chinamobile.bcbsp.action.WorkerManagerAction;
import com.chinamobile.bcbsp.bspstaff.StaffInProgress;
import com.chinamobile.bcbsp.fault.storage.Fault;
import com.chinamobile.bcbsp.fault.storage.MonitorFaultLog;
import com.chinamobile.bcbsp.http.HttpServer;
import com.chinamobile.bcbsp.rpc.ControllerProtocol;
import com.chinamobile.bcbsp.rpc.JobSubmissionProtocol;
import com.chinamobile.bcbsp.rpc.WorkerManagerProtocol;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.JobProfile;
import com.chinamobile.bcbsp.util.JobStatus;
import com.chinamobile.bcbsp.util.StaffStatus;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.workermanager.WorkerManagerControlInterface;
import com.chinamobile.bcbsp.workermanager.WorkerManagerStatus;

/**
 * BSPController is responsible to control all the WorkerManagers and to manage
 * bsp jobs.
 * 
 * @author
 * @version
 */
public class BSPController implements JobSubmissionProtocol,
        ControllerProtocol, WorkerManagerControlInterface {

    private static final Log LOG = LogFactory.getLog(BSPController.class);
    private MonitorFaultLog mfl;
    private BSPConfiguration conf;

    // Constants
    public static enum State {
        INITIALIZING, RUNNING
    }

    private static final int FS_ACCESS_RETRY_PERIOD = 10000;

    // States
    State state = State.INITIALIZING;

    // Attributes
    private String controllerIdentifier;
    // private Server interServer;
    private Server controllerServer;
    
    //HTTP Server
    private HttpServer infoServer;
    private int infoPort;

    // Filesystem
    private FileSystem fs = null;
    private Path systemDir = null;

    // system directories are world-wide readable and owner readable
    final static FsPermission SYSTEM_DIR_PERMISSION = FsPermission
            .createImmutable(( short ) 0733); // rwx-wx-wx
    // system files should have 700 permission
    final static FsPermission SYSTEM_FILE_PERMISSION = FsPermission
            .createImmutable(( short ) 0700); // rwx------

    // Jobs' Meta Data
    private Integer nextJobId = Integer.valueOf(1);
    // private long startTime;
    private int totalSubmissions = 0; // how many jobs has been submitted by
    // clients
    private int runningClusterStaffs = 0; // currnetly running tasks
    private int maxClusterStaffs = 0; // max tasks that the Cluster can run

    private Map<BSPJobID, JobInProgress> jobs = new ConcurrentHashMap<BSPJobID, JobInProgress>();
    private StaffScheduler staffScheduler;

    // WorkerManagers cache
    private static ConcurrentMap<WorkerManagerStatus, WorkerManagerProtocol> whiteWorkerManagers = new ConcurrentHashMap<WorkerManagerStatus, WorkerManagerProtocol>();
    private static ConcurrentMap<WorkerManagerStatus, WorkerManagerProtocol> grayWorkerManagers = new ConcurrentHashMap<WorkerManagerStatus, WorkerManagerProtocol>();
    
    private final List<JobInProgressListener> jobInProgressListeners = new CopyOnWriteArrayList<JobInProgressListener>();

    private  static long HEART_BEAT_TIMEOUT, PAUSE_TIMEOUT;
    private CheckTimeOut cto;

    /**
     * This thread will run until stopping the cluster. It will check weather
     * the heart beat interval is time-out or not.
     * 
     * @author WangZhigang
     * 
     * Review comments:
     *   (1)The level of log is not clear. I think the HEART_BEAT_TIMEOUT should
     *      be the level of "warn" or "error", instead of "info".
     * Review time: 2011-11-30;
     * Reviewer: Hongxu Zhang.
     * 
     * Fix log:
     *   (1)Now, if a workermanager has been HEART_BEAT_TIMEOUT, it will be viewed
     *      as fault worker. So the level of log should be "error".
     * Fix time: 2011-12-04;
     * Programmer: Zhigang Wang.
     */
    public class CheckTimeOut extends Thread {
        public void run() {
            while (true) {
                long nowTime = System.currentTimeMillis();
                for (WorkerManagerStatus wms : whiteWorkerManagers.keySet()) {
                    long timeout = nowTime - wms.getLastSeen();
                    if (timeout > HEART_BEAT_TIMEOUT) {
                        LOG.error("[Fault Detective] The worker's time out is catched in WhiteList: "
                                + wms.getWorkerManagerName());
                        if(wms.getStaffReports().size() != 0) {
                            workerFaultPreProcess(wms);
                        }
                        removeWorker(wms);
                    }
                }
                
                for (WorkerManagerStatus wms : grayWorkerManagers.keySet()) {
                    long timeout = nowTime - wms.getLastSeen();
                    if (timeout > HEART_BEAT_TIMEOUT) {
                        LOG.error("[Fault Detective] The worker's time out is catched in GrayList: "
                                + wms.getWorkerManagerName());
                        if(wms.getStaffReports().size() != 0) {
                            workerFaultPreProcess(wms);
                        }
                        removeWorker(wms);
                    }
                    
                    timeout = nowTime - wms.getPauseTime();
                    if (timeout > PAUSE_TIMEOUT) {
                        LOG.warn(wms.getWorkerManagerName() + " will be transferred from [GrayList] to [WhiteList]");
                        WorkerManagerProtocol wmp = grayWorkerManagers.remove(wms);
                        wmp.clearFailedJobList();
                        wms.setPauseTime(0);
                        whiteWorkerManagers.put(wms, wmp);
                    }
                }

                try {
                    Thread.sleep(HEART_BEAT_TIMEOUT);
                } catch (Exception e) {
                    LOG.error("CheckTimeOut", e);
                }
            }
        }
    }

    public BSPController() {
        
    }
    
    /**
     * Start the BSPController process, listen on the indicated hostname/port
     */
    public BSPController(BSPConfiguration conf) throws IOException,
            InterruptedException {
        this(conf, generateNewIdentifier());
    }

    @SuppressWarnings("static-access")
    BSPController(BSPConfiguration conf, String identifier) throws IOException,
            InterruptedException {
        this.conf = conf;
        this.controllerIdentifier = identifier;
        this.mfl=new MonitorFaultLog();

        // Create the scheduler and init scheduler services
        Class<? extends StaffScheduler> schedulerClass = conf.getClass(
                "bsp.master.taskscheduler", SimpleStaffScheduler.class,
                StaffScheduler.class);
        this.staffScheduler = ( StaffScheduler ) ReflectionUtils.newInstance(
                schedulerClass, conf);

        String host = getAddress(conf).getHostName();
        int port = getAddress(conf).getPort();

        LOG.info("RPC BSPController: host " + host + " port " + port);

        this.controllerServer = RPC.getServer(this, host, port, conf);
        
        infoPort = conf.getInt("bsp.http.infoserver.port", 40026);
        // infoPort = 40026;
        infoServer = new HttpServer("bcbsp", host, infoPort, true, conf);
        infoServer.setAttribute("bcbspController", this);

        // starting webserver
        infoServer.start();

        this.HEART_BEAT_TIMEOUT = conf.getLong(Constants.HEART_BEAT_TIMEOUT, 5000);
        this.PAUSE_TIMEOUT = conf.getInt(Constants.SLEEP_TIMEOUT, 10000);
        this.cto = new CheckTimeOut();

        while (!Thread.currentThread().isInterrupted()) {
            try {
                if (fs == null) {
                    fs = FileSystem.get(conf);
                }
                // Clean up the system dir, which will only work if hdfs is out
                // of safe mode.
                if (systemDir == null) {
                    systemDir = new Path(getSystemDir());
                }

                LOG.info("Cleaning up the system directory");
                LOG.info(systemDir);

                fs.delete(systemDir, true);
                if (FileSystem.mkdirs(fs, systemDir, new FsPermission(
                        SYSTEM_DIR_PERMISSION))) {
                    break;
                }

                LOG.error("Mkdirs failed to create " + systemDir);

            } catch (AccessControlException ace) {
                LOG.warn("Failed to operate on bsp.system.dir (" + systemDir
                        + ") because of permissions.");
                LOG.warn("Manually delete the bsp.system.dir (" + systemDir
                        + ") and then start the BSPController.");
                LOG.warn("Bailing out ... ");
                throw ace;
            } catch (IOException ie) {
                LOG.error("problem cleaning system directory: " + systemDir, ie);
            }
            Thread.sleep(FS_ACCESS_RETRY_PERIOD);
        }

        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
        }

        deleteLocalFiles(Constants.BC_BSP_LOCAL_SUBDIR_CONTROLLER);
    }
    
    @Override
    public WorkerManagerProtocol removeWorkerFromWhite(WorkerManagerStatus wms) {
        if (whiteWorkerManagers.containsKey(wms)) {
            return whiteWorkerManagers.remove(wms);
        } else {
            return null;
        }
    }
    
    @Override
    public void addWorkerToGray(WorkerManagerStatus wms, WorkerManagerProtocol wmp) {
        if (!grayWorkerManagers.containsKey(wms)) {
            grayWorkerManagers.put(wms, wmp);
        }
    }
    
    @Override  
    public int getMaxFailedJobOnWorker() {
        return conf.getInt(Constants.BC_BSP_FAILED_JOB_PER_WORKER, 0);
    }

    /**
     * A WorkerManager registers with its status to BSPController when startup,
     * which will update WorkerManagers cache.
     * 
     * @param status
     *            to be updated in cache.
     * @return true if registering successfully; false if fail.
     */
    @Override
    public boolean register(WorkerManagerStatus status) throws IOException {
        if (null == status) {
            LOG.error("No worker server status.");
            throw new NullPointerException("No worker server status.");
        }

        Throwable e = null;

        try {
            WorkerManagerProtocol wc = ( WorkerManagerProtocol ) RPC
                .waitForProxy(WorkerManagerProtocol.class,
                        WorkerManagerProtocol.versionID,
                        resolveWorkerAddress(status.getRpcServer()),
                        this.conf);
            
            if (null == wc) {
                LOG.warn("Fail to create Worker client at host "
                        + status.getWorkerManagerName());
                return false;
            }
            
            if (grayWorkerManagers.containsKey(status)) {
                grayWorkerManagers.remove(status);
                whiteWorkerManagers.put(status, wc);
            } else if (!whiteWorkerManagers.containsKey(status)) {
                LOG.info(status.getWorkerManagerName() + " is registered to the cluster " +
                        "and the maxClusterStaffs changes from " + this.maxClusterStaffs + 
                        " to " + (this.maxClusterStaffs + status.getMaxStaffsCount()));
                maxClusterStaffs += status.getMaxStaffsCount();
                whiteWorkerManagers.put(status, wc);
            }
        } catch (UnsupportedOperationException u) {
            e = u;
        } catch (ClassCastException c) {
            e = c;
        } catch (NullPointerException n) {
            e = n;
        } catch (IllegalArgumentException i) {
            e = i;
        } catch (Exception ex) {
            e = ex;
        }
        if (e != null) {
            LOG.error(
                    "Fail to register WorkerManager "
                            + status.getWorkerManagerName(), e);
            return false;
        }
        return true;
    }

    private static InetSocketAddress resolveWorkerAddress(String data) {
        return new InetSocketAddress(data.split(":")[0], Integer.parseInt(data
                .split(":")[1]));
    }

    public void updateWhiteWorkerManagersKey(WorkerManagerStatus old,
            WorkerManagerStatus newKey) {
        synchronized (whiteWorkerManagers) {
            long time = System.currentTimeMillis();
            newKey.setLastSeen(time);
            WorkerManagerProtocol worker = whiteWorkerManagers.remove(old);
            whiteWorkerManagers.put(newKey, worker);
        }
    }
    
    public void updateGrayWorkerManagersKey(WorkerManagerStatus old,
            WorkerManagerStatus newKey) {
        synchronized (grayWorkerManagers) {
            long time = System.currentTimeMillis();
            newKey.setLastSeen(time);
            newKey.setPauseTime(old.getPauseTime());
            WorkerManagerProtocol worker = grayWorkerManagers.remove(old);
            grayWorkerManagers.put(newKey, worker);
        }
    }

    public synchronized boolean removeFromJobListener(BSPJobID jobId) {
        JobInProgress jip = whichJob(jobId);
        try {
            for (JobInProgressListener listener : jobInProgressListeners) {
                ArrayList<BSPJobID> removeJob = listener.jobRemoved(jip);
                for (BSPJobID removeID: removeJob) {
                    if (this.jobs.containsKey(removeID)) {
                        this.jobs.remove(removeID);
                    }
                }
            }
            return true;
        } catch (Exception e) {
            LOG.error("Fail to alter scheduler a job is moved.", e);
            return false;
        }
    }

    private void recordStaffFault(StaffStatus ts, boolean isRecovery) {

        Fault fault = ts.getFault();
        fault.setFaultStatus(isRecovery);
        mfl.faultLog(fault);
    }

    private void recordWorkerFault(WorkerManagerStatus wms, boolean isRecovery) {
        LOG.info("enter [Fault process]");
        if (wms.getWorkerFaultList().size() == 0) {
            Fault fault = new Fault(Fault.Type.NETWORK, Fault.Level.MAJOR,
                    wms.getWorkerManagerName(), null, null, null);
            fault.setFaultStatus(isRecovery);
            mfl.faultLog(fault);
        } else {
            for (Fault fault : wms.getWorkerFaultList()) {
                fault.setFaultStatus(isRecovery);
                mfl.faultLog(fault);
            }
        }
    }
    @Override
    public void recordFault(Fault f) {
        mfl.faultLog(f);
    }
    
    private String getTheLastWMName(Map<String, Integer> map) {         
        String lastLWName = null;
        int lastLaunchWorker = 0;
        int i = 0;            
        Set<String> keySet = map.keySet();
        Iterator<String> it = keySet.iterator();
        while (it.hasNext()) {
            lastLaunchWorker ++;
            it.next();
        }
        
        Iterator<String> iter = keySet.iterator();
        while (iter.hasNext()) {
            i++;
            String key = iter.next();
            if (i == lastLaunchWorker) {
                lastLWName = key;
            }
        }
        LOG.info("getTheLastWMName(Map<String, Integer> map:)" + " " + lastLWName);
        return lastLWName;
    }
    
    /**
     * If the fault can not be process, then return false, otherwise, return
     * true;
     */
    public boolean staffFaultPreProcess(StaffStatus ss, WorkerManagerStatus wms) {

        String workerManagerName = wms.getWorkerManagerName();
        StaffInProgress sip = null;

        JobInProgress jip = whichJob(ss.getJobId());
        sip = jip.findStaffInProgress(ss.getStaffId().getStaffID());
          
        if(jip.getStatus().getRunState() != JobStatus.RECOVERY) {
            jip.getStatus().setRunState(JobStatus.RECOVERY);  
        }       
        
        removeFromJobListener(ss.getJobId());
        
        jip.setAttemptRecoveryCounter();

        sip.getStaffs().remove(sip.getStaffID());
          
        ss.setRecovery(true);
        sip.getStaffStatus(ss.getStaffId()).setRunState(StaffStatus.State.STAFF_RECOVERY);
        
        if(jip.getNumAttemptRecovery() > jip.getMaxAttemptRecoveryCounter()) {
            try {
            } catch (Exception e) {
                LOG.error("[recordStaffFault]", e);
            }
            LOG.error("The recovery number is " + jip.getNumAttemptRecovery() 
                    + " and it's up to the Max Recovery Threshold " + jip.getMaxAttemptRecoveryCounter());
            return false;
            
        } else {
                        
            jip.cleanWMNames();
            jip.addWMNames(workerManagerName);
            
            Map<String, Integer> workerManagerToTimes = jip.getStaffToWMTimes().get(ss.getStaffId());
            String lastWMName = getTheLastWMName(workerManagerToTimes);
                              
            if(workerManagerToTimes.get(lastWMName) > jip.getMaxStaffAttemptRecoveryCounter()) {
                boolean success = jip.removeStaffFromWorker(workerManagerName, ss.getStaffId());
                if (!success) {
                    LOG.error("[staffFaultPreProcess]: Fail to remove staff " + ss.getStaffId() + " from worker " + workerManagerName);
                }
            }
            
            boolean gray = jip.addFailedWorker(wms);
            if (gray && whiteWorkerManagers.containsKey(wms)) {
                WorkerManagerProtocol wmp = whiteWorkerManagers.get(wms);
                wmp.addFailedJob(jip.getJobID());
                
                if (wmp.getFailedJobCounter() > getMaxFailedJobOnWorker()) {
                    removeWorkerFromWhite(wms);//white
                    wms.setPauseTime(System.currentTimeMillis());
                    addWorkerToGray(wms, wmp);//gray
                    LOG.info(wms.getWorkerManagerName() + " will be transferred from [WhiteList] to [GrayList]");
                }
            }
            
            jip.setPriority(Constants.PRIORITY.HIGHER);
            for (JobInProgressListener listener : jobInProgressListeners) {
                try {
                    listener.jobAdded(jip);
                    LOG.warn("listener.jobAdded(jip);");
                  } catch (IOException ioe) {
                    LOG.error("Fail to alter Scheduler a job is added.", ioe);
                  }
            }
          
            boolean isSuccessProcess = true;
            recordStaffFault(ss, isSuccessProcess);
            
            return true;
        }
    }

    private void removeWorker(WorkerManagerStatus wms) {
        if (whiteWorkerManagers.containsKey(wms)) {
            whiteWorkerManagers.remove(wms);
            LOG.error(wms.getWorkerManagerName() + " is removed from [WhiteList] because HeartBeatTimeOut " +
            		"and the maxClusterStaffs changes from " + maxClusterStaffs + " to " + (maxClusterStaffs - wms.getMaxStaffsCount()));
            maxClusterStaffs = maxClusterStaffs - wms.getMaxStaffsCount();
        } else if (grayWorkerManagers.containsKey(wms)) {
            grayWorkerManagers.remove(wms);
            LOG.error(wms.getWorkerManagerName() + " is removed from [GrayList] because HeartBeatTimeOut " +
                    "and the maxClusterStaffs changes from " + maxClusterStaffs + " to " + (maxClusterStaffs - wms.getMaxStaffsCount()));
            maxClusterStaffs = maxClusterStaffs - wms.getMaxStaffsCount();
        }
        boolean isSuccessProcess = true;
        recordWorkerFault(wms, isSuccessProcess);
    }
    
    public void workerFaultPreProcess(WorkerManagerStatus wms) {
        List<StaffStatus> staffStatus;
        Set<BSPJobID> bspJobIDs = new HashSet<BSPJobID>();
        
        staffStatus = wms.getStaffReports();
        LOG.info("staffStatus = wms.getStaffReports() size: ;" + staffStatus.size());
          
          //update the failed worker's staffs' status  
        for(StaffStatus ss : staffStatus) {

            JobInProgress jip = whichJob(ss.getJobId());
            LOG.info("workerFaultPreProcess--jip = whichJob(ss.getJobId()--jip.getJobID()) " + jip.getJobID());
            StaffInProgress sip = jip.findStaffInProgress(ss.getStaffId().getStaffID());
            
            ss.setRecovery(true);   
            sip.getStaffStatus(ss.getStaffId()).setRunState(StaffStatus.State.WORKER_RECOVERY);
                          
            if (!bspJobIDs.contains(ss.getJobId())) {
                bspJobIDs.add(ss.getJobId());
            }
            LOG.info("bspJobIDs.add(ss.getJobId()); size: ;" + bspJobIDs.size());
        }
        
        for(BSPJobID bspJobId : bspJobIDs) {
            
            JobInProgress jip = whichJob(bspJobId);
            
            //AttemptRecoveryCounter++
            if(jip.getStatus().getRunState() != JobStatus.RECOVERY) {
                jip.getStatus().setRunState(JobStatus.RECOVERY);
                jip.setAttemptRecoveryCounter();   
            }
            
            removeFromJobListener(bspJobId);
            
            StaffInProgress[] staffs = jip.getStaffInProgress();
            
            for(StaffInProgress sip : staffs) {
                if(sip.getStaffStatus(sip.getStaffID()).getRunState() == StaffStatus.State.WORKER_RECOVERY) {
                    // remove from active staffs
                    sip.getStaffs().remove(sip.getStaffID());

                    boolean success = jip.removeStaffFromWorker(wms.getWorkerManagerName(), sip.getStaffID());
                    if (!success) {
                        LOG.error("[staffFaultPreProcess]: Fail to remove staff " + sip.getStaffID() + " from worker " + wms.getWorkerManagerName());
                    }
                    
                }
            }
                  
            if(jip.getNumAttemptRecovery() > jip.getMaxAttemptRecoveryCounter()) {
               
                boolean isSuccessProcess = false;
                try {
                    recordWorkerFault(wms, isSuccessProcess);
                } catch (Exception e) {
                    LOG.error("[recordWorkerFault]", e);
                }
                LOG.error("The recovery number is " + jip.getNumAttemptRecovery() 
                        + " and it's up to the Max Recovery Threshold " + jip.getMaxAttemptRecoveryCounter());
                jip.killJob();

            } else {
                
                jip.cleanWMNames();
                jip.addWMNames(wms.getWorkerManagerName());

                jip.setPriority(Constants.PRIORITY.HIGHER);
                for (JobInProgressListener listener : jobInProgressListeners) {
                   try {
                     listener.jobAdded(jip);
                     LOG.warn("the recoveried job is added to the wait queue --- [listener.jobAdded(jip)]");
                   } catch (IOException ioe) {
                     LOG.error("Fail to alter Scheduler a job is added.", ioe);
                   }
                }
                LOG.info("enter [Fault process]");
            }
        }//for
    }
    
    /**
     * recovery for the controller's fault before local compute
     * @param e
     * @return
     */
    @Override
    public boolean recovery(BSPJobID jobId) {
        LOG.error("so bad ! The job is failed before dispatch to the workers ! please submit it again or quit computing!");
        if(-1 == jobId.getId()) {
            LOG.info(jobId.getJtIdentifier());
        } else {
            try {
                this.killJob(jobId);
            } catch (IOException e) {
                LOG.error("recovery", e);
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * exceptions reported by workers before local compute or after local compute
     * @param ss
     * @return
     */
    public boolean recovery(StaffStatus ss) {
        if(ss.getStage() == 2) {
            LOG.error("so bad ! Though trying several times to recovery the staff: " + ss.getStaffId() + " , the job is failed after local computing !");
            
            boolean isSuccessProcess = false;
            recordStaffFault(ss, isSuccessProcess);
            
            try {
                this.killJob(ss.getJobId());
            } catch (IOException e) {
                LOG.error("recovery", e);
            }
            return true;
            
        } else if(ss.getStage() == 0) {
            LOG.error("so bad ! The job is failed before local computing ! please submit it again !");
            
            boolean isSuccessProcess = false;
            recordStaffFault(ss, isSuccessProcess);
            
            try {
                this.killJob(ss.getJobId());
            } catch (IOException e) {
                LOG.error("recovery", e);
            }
            return true;
        } else {
            LOG.error("error: no such stage !");
            
            return false;
        }        
    }
    
    /** Check there is a fault or not. */
    public void checkFault(WorkerManagerStatus wms) {  
        try {
            // Note: Now the list of workerFault reported by WorkerManager is always 0.
            if (wms.getWorkerFaultList().size() != 0) {
                // some add operation of WorkerFaultList should exist in WorkerManager's IO catch statement
                 workerFaultPreProcess(wms);
              
            } else {
                List<StaffStatus> slist = wms.getStaffReports();
                 
                for (StaffStatus ss : slist) {
                    JobInProgress jip = whichJob(ss.getJobId());
                    if (jip == null) {                                    
                        continue;
                    }
                    switch (ss.getRunState()) {
                        case RUNNING : 
                            // TODO : Do nothing now.
                            break;
                        case FAULT :
                            LOG.error("[Fault Detective] The fault of " + ss.getStaffId() + " has been catched");
                            if(ss.getStage() == 0 || ss.getStage() == 2) {
                                //reported by workers before local compute  or  reported by workers after local compute
                                boolean isRecovery = recovery(ss);
                                if (!isRecovery) {
                                    killJob(jip);
                                }
                            } else if(ss.getStage() == 1){
                                //reported by workers local compute
                                boolean isRecovery = staffFaultPreProcess(ss, wms);
                                LOG.info("checkfault [isRecovery]: " + isRecovery);
                                if (!isRecovery) {
                                    killJob(jip);
                                }
                            } 
                            break;
                        default :
                            break;
                    }
                    

                    
                }
            }// else
        } catch (Exception e) {
            LOG.error("[checkFault]", e);
        }
    }

    @Override
    public boolean report(Directive directive) throws IOException {

        // check the type of directive is Response or not
        if (directive.getType().value() != Directive.Type.Response.value()) {
            throw new IllegalStateException("WorkerManager should report()"
                    + " with Response. But the Current report type is:"
                    + directive.getType());
        }

        // process the heart-beat information.
        WorkerManagerStatus status = directive.getStatus();
        
        if (whiteWorkerManagers.containsKey(status) || grayWorkerManagers.containsKey(status)) {
            WorkerManagerStatus ustus = null;
            for (WorkerManagerStatus old : whiteWorkerManagers.keySet()) {
                if (old.equals(status)) {
                    ustus = status;
                    updateWhiteWorkerManagersKey(old, ustus);
                    break;
                }
            }
            
            for (WorkerManagerStatus old : grayWorkerManagers.keySet()) {
                if (old.equals(status)) {
                    ustus = status;
                    updateGrayWorkerManagersKey(old, ustus);
                    break;
                }
            }
            
            checkFault(ustus);
        } else {
            throw new RuntimeException("WorkerManager not found."
                    + status.getWorkerManagerName());
        }

        return true;
    }

    private JobInProgress whichJob(BSPJobID id) {
        for (JobInProgress job : staffScheduler
                .getJobs(SimpleStaffScheduler.PROCESSING_QUEUE)) {
            if (job.getJobID().equals(id)) {
                return job;
            }
        }
        return null;
    }

    // /////////////////////////////////////////////////////////////
    // BSPController methods
    // /////////////////////////////////////////////////////////////

    // Get the job directory in system directory
    Path getSystemDirectoryForJob(BSPJobID id) {
        return new Path(getSystemDir(), id.toString());
    }

    String[] getLocalDirs() throws IOException {
        return conf.getStrings("Constants.BC_BSP_TMP_DIRECTORY");
    }

    void deleteLocalFiles() throws IOException {
        String[] localDirs = getLocalDirs();
        for (int i = 0; i < localDirs.length; i++) {
            FileSystem.getLocal(conf).delete(new Path(localDirs[i]), true);
        }
    }

    void deleteLocalFiles(String subdir) throws IOException {
        try {
            String[] localDirs = getLocalDirs();
            for (int i = 0; i < localDirs.length; i++) {
                FileSystem.getLocal(conf).delete(
                        new Path(localDirs[i], subdir), true);
            }
        } catch (NullPointerException e) {
            LOG.info(e);
        }
    }

    /**
     * Constructs a local file name. Files are distributed among configured
     * local directories.
     */
    Path getLocalPath(String pathString) throws IOException {
        return conf.getLocalPath(Constants.BC_BSP_LOCAL_DIRECTORY, pathString);
    }

    public static BSPController startMaster(BSPConfiguration conf)
            throws IOException, InterruptedException {
        return startMaster(conf, generateNewIdentifier());
    }

    public static BSPController startMaster(BSPConfiguration conf,
            String identifier) throws IOException, InterruptedException {
        BSPController result = new BSPController(conf, identifier);
        result.staffScheduler.setWorkerManagerControlInterface(result);
        result.staffScheduler.start();

        return result;
    }

    public static InetSocketAddress getAddress(Configuration conf) {
        String bspControllerStr = conf.get(Constants.BC_BSP_CONTROLLER_ADDRESS);
        return NetUtils.createSocketAddr(bspControllerStr);
    }

    /**
     * BSPController identifier
     * 
     * @return String BSPController identification number
     */
    private static String generateNewIdentifier() {
        return new SimpleDateFormat("yyyyMMddHHmm").format(new Date());
    }

    public void offerService() throws InterruptedException, IOException {

        this.cto.start();
        this.controllerServer.start();

        synchronized (this) {
            state = State.RUNNING;
        }
        LOG.info("Starting RUNNING");

        this.controllerServer.join();

        LOG.info("Stopped RPC Master server.");
    }

    // //////////////////////////////////////////////////
    // InterServerProtocol
    // //////////////////////////////////////////////////
    @Override
    public long getProtocolVersion(String protocol, long clientVersion)
            throws IOException {
        if (protocol.equals(ControllerProtocol.class.getName())) {
            return ControllerProtocol.versionID;
        } else if (protocol.equals(JobSubmissionProtocol.class.getName())) {
            return JobSubmissionProtocol.versionID;
        } else {
            throw new IOException("Unknown protocol to BSPController: "
                    + protocol);
        }
    }

    // //////////////////////////////////////////////////
    // JobSubmissionProtocol
    // //////////////////////////////////////////////////
    /**
     * This method returns new job id. The returned job id increases
     * sequentially.
     */
    @Override
    public BSPJobID getNewJobId() throws IOException {
        int id;
        synchronized (nextJobId) {
            id = nextJobId;
            nextJobId = Integer.valueOf(id + 1);
        }
        return new BSPJobID(this.controllerIdentifier, id);
    }

    @Override
    public JobStatus submitJob(BSPJobID jobID, String jobFile)
    {
        if (jobs.containsKey(jobID)) {
            // job already running, don't start twice
            LOG.warn("The job (" + jobID + ") was already submitted");
            return jobs.get(jobID).getStatus();
        }

        JobInProgress jip = null;
        try {
            jip = new JobInProgress(jobID, new Path(jobFile), this,
                    this.conf);
            jip.getGssc().setJobInProgressControlInterface(jip);
            return addJob(jobID, jip);
            
        } catch (IOException e) {
            LOG.error("Exception has been catched in BSPController--submitJob !", e);
            Fault f = new Fault(Fault.Type.DISK, Fault.Level.WARNING, jobID, e.toString());
            recordFault(f);
            recovery(jobID);
            killJob(jip);
            
            return null;
        }

        
    }

    // //////////////////////////////////////////////////
    // WorkerManagerControlInterface functions
    // //////////////////////////////////////////////////

    @SuppressWarnings("static-access")
    @Override
    public ClusterStatus getClusterStatus(boolean detailed) {
        int workerManagerCount = this.whiteWorkerManagers.size();
        String[] workerManagersName = new String[workerManagerCount];
        this.runningClusterStaffs = 0;

        int count = 0;
        for (Map.Entry<WorkerManagerStatus, WorkerManagerProtocol> entry : this.whiteWorkerManagers
                .entrySet()) {
            WorkerManagerStatus wms = entry.getKey();
            workerManagersName[count++] = wms.getWorkerManagerName();
            this.runningClusterStaffs += wms.getRunningStaffsCount();
        }

        if (detailed) {
            return new ClusterStatus(workerManagersName, this.maxClusterStaffs,
                    this.runningClusterStaffs, this.state);
        } else {
            return new ClusterStatus(workerManagerCount, this.maxClusterStaffs,
                    this.runningClusterStaffs, this.state);
        }
    }

    @Override
    public StaffAttemptID[] getStaffStatus(BSPJobID jobId) throws IOException {
        JobInProgress job = jobs.get(jobId);
        StaffAttemptID attemptID[] = job.getAttemptIDList();      
               
        return attemptID;
    }
    @Override
    public StaffStatus[] getStaffDetail(BSPJobID jobId){
        JobInProgress jip = jobs.get(jobId);
        StaffAttemptID attemptID[] = jip.getAttemptIDList();
        StaffInProgress sip[] = jip.getStaffInProgress();
        List<StaffStatus> staffStatusList = new ArrayList<StaffStatus>();
        for(int i = 0;i<sip.length;i++){
           staffStatusList.add(sip[i].getStaffStatus(attemptID[i]));
        }
        LOG.info("sizesize:"+staffStatusList.size());
        return staffStatusList.toArray(new StaffStatus[staffStatusList.size()]);
        
    }
    @Override
    public void setCheckFrequency(BSPJobID jobID,int cf){
        JobInProgress jip = jobs.get(jobID);
        jip.setCheckPointFrequency(cf);
    }
    
    @Override
    public void setCheckFrequencyNext(BSPJobID jobId) {
        JobInProgress jip = jobs.get(jobId);
        jip.setCheckPointNext();
    }
    
    @Override
    public WorkerManagerProtocol findWorkerManager(WorkerManagerStatus status) {
        return whiteWorkerManagers.get(status);
    }

    @Override
    public Collection<WorkerManagerProtocol> findWorkerManagers() {
        return whiteWorkerManagers.values();
    }

    @Override
    public Collection<WorkerManagerStatus> workerServerStatusKeySet() {
        return whiteWorkerManagers.keySet();
    }

    @Override
    public void addJobInProgressListener(JobInProgressListener listener) {
        jobInProgressListeners.add(listener);
    }

    @Override
    public void removeJobInProgressListener(JobInProgressListener listener) {
        jobInProgressListeners.remove(listener);
    }

    @SuppressWarnings("static-access")
    @Override
    public String[] getActiveWorkerManagersName() {
        int workerManagerCount = this.whiteWorkerManagers.size();
        workerManagerCount += this.grayWorkerManagers.size();
        String[] workerManagersName = new String[workerManagerCount];

        int count = 0;
        for (Map.Entry<WorkerManagerStatus, WorkerManagerProtocol> entry : this.whiteWorkerManagers
                .entrySet()) {
            WorkerManagerStatus wms = entry.getKey();
            workerManagersName[count++] = wms.getWorkerManagerName();
        }
        
        for (Map.Entry<WorkerManagerStatus, WorkerManagerProtocol> entry : this.grayWorkerManagers
                .entrySet()) {
            WorkerManagerStatus wms = entry.getKey();
            workerManagersName[count++] = wms.getWorkerManagerName();
        }

        return workerManagersName;
    }

    /**
     * Adds a job to the bsp master. Make sure that the checks are inplace
     * before adding a job. This is the core job submission logic
     * 
     * @param jobId
     *            The id for the job submitted which needs to be added
     */
    private synchronized JobStatus addJob(BSPJobID jobId, JobInProgress jip) {
        totalSubmissions++;
        synchronized (jobs) {
            jobs.put(jip.getProfile().getJobID(), jip);
            for (JobInProgressListener listener : jobInProgressListeners) {
                try {
                    listener.jobAdded(jip);
                } catch (IOException ioe) {
                    LOG.error("Fail to alter Scheduler a job is added.", ioe);
                    Fault f = new Fault(Fault.Type.DISK, Fault.Level.WARNING, jobId, ioe.toString());
                    recordFault(f);
                    recovery(jobId);
                    this.killJob(jip);
                }
            }
        }
        return jip.getStatus();
    }

    @Override
    public JobStatus[] jobsToComplete() throws IOException {
        return getJobStatus(jobs.values(), true);
    }

    @Override
    public JobStatus[] getAllJobs() throws IOException {
        return getJobStatus(jobs.values(), false);
    }

    private synchronized JobStatus[] getJobStatus(
            Collection<JobInProgress> jips, boolean toComplete) {
        if (jips == null) {
            return new JobStatus[] {};
        }
        List<JobStatus> jobStatusList = new ArrayList<JobStatus>();
        for (JobInProgress jip : jips) {
            JobStatus status = jip.getStatus();

            status.setStartTime(jip.getStartTime());
            // Sets the user name
            status.setUsername(jip.getProfile().getUser());

            if (toComplete) {
                if (status.getRunState() == JobStatus.RUNNING
                        || status.getRunState() == JobStatus.PREP) {
                    jobStatusList.add(status);
                }
            } else {
                jobStatusList.add(status);
            }
        }

        return jobStatusList.toArray(new JobStatus[jobStatusList.size()]);
    }

    @Override
    public synchronized String getFilesystemName() throws IOException {
        if (fs == null) {
            throw new IllegalStateException(
                    "FileSystem object not available yet");
        }
        return fs.getUri().toString();
    }

    /**
     * Return system directory to which BSP store control files.
     */
    @Override
    public String getSystemDir() {
        Path sysDir = new Path(conf.get(Constants.BC_BSP_SHARE_DIRECTORY));
        return fs.makeQualified(sysDir).toString();
    }

    @Override
    public JobProfile getJobProfile(BSPJobID jobid) throws IOException {
        synchronized (this) {
            JobInProgress jip = jobs.get(jobid);
            if (jip != null) {
                return jip.getProfile();
            }
        }
        return null;
    }

    @Override
    public JobStatus getJobStatus(BSPJobID jobid) throws IOException {
        synchronized (this) {
            JobInProgress jip = jobs.get(jobid);
            if (jip != null) {
                return jip.getStatus();
            }
        }
        return null;
    }

    @Override
    public void killJob(BSPJobID jobid) throws IOException {
        JobInProgress job = jobs.get(jobid);
        if (null == job) {
            LOG.warn("killJob(): JobId " + jobid.toString()
                    + " is not a valid job");
            return;
        }
        killJob(job);
    }

    private synchronized void killJob(JobInProgress jip) {
        LOG.info("Killing job " + jip.getJobID());
        
        StaffInProgress[] sips = jip.getStaffInProgress();
        for (int index = 0; index < sips.length; index++) {
            WorkerManagerStatus wms = sips[index].getWorkerManagerStatus();
            WorkerManagerProtocol wmp = findWorkerManager(wms);
            
            Directive directive = new Directive(getActiveWorkerManagersName(),
                    new WorkerManagerAction[] { new KillStaffAction(sips[index].getStaffID())});
            try {
                wmp.dispatch(jip.getJobID(), directive, false, false, 0);
            } catch (Exception e) {
               LOG.error("Fail to kill staff " + sips[index].getStaffID() 
                       + " on the WorkerManager " + wms.getWorkerManagerName()); 
            }
        }
        
        jip.killJob();
        LOG.warn(jip.getJobID() + " has been killed completely");
    }
    
    @Override
    public boolean killStaff(StaffAttemptID taskId, boolean shouldFail)
            throws IOException {
        return false;
    }

    public static BSPController constructController(
            Class<? extends BSPController> controllerClass, final Configuration conf) {
        try {
            Constructor<? extends BSPController> c = controllerClass
                    .getConstructor(Configuration.class);
            return c.newInstance(conf);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed construction of "
                            + "Master: "
                            + controllerClass.toString()
                            + ((e.getCause() != null) ? e.getCause()
                                    .getMessage() : ""), e);
        }
    }

    @SuppressWarnings("deprecation")
    public void shutdown() {
        try {
            LOG.info("Prepare to shutdown the BSPController");
            for (JobInProgress jip : jobs.values()) {
                if (jip.getStatus().getRunState() == JobStatus.RUNNING) {
                    jip.killJobRapid();
                    LOG.warn(jip.getJobID() + " has been killed by system");
                }
            }
            
            this.staffScheduler.stop();
            LOG.info("Succeed to stop the Scheduler Server");
            this.controllerServer.stop();
            LOG.info("Succeed to stop RPC Server on BSPController");
            this.cto.stop();
            cleanupLocalSystem();
            LOG.info("Succeed to cleanup temporary files on the local disk");   
        } catch (Exception e) {
            LOG.error("Fail to shutdown the BSPController");
        }
    }

    public void cleanupLocalSystem() {
        BSPConfiguration conf = new BSPConfiguration();
        File f = new File(conf.get(Constants.BC_BSP_LOCAL_DIRECTORY));
        try {
            deleteLocalDir(f);
        } catch (Exception e) {
            LOG.error("Failed to delete the local system : " + f, e);
        }
    }
    
    public void deleteLocalDir(File dir) { 
        if (dir == null || !dir.exists() || !dir.isDirectory()) 
            return;
        
        for (File file : dir.listFiles()) { 
            if (file.isFile()) 
                file.delete(); // delete the file 
            else if (file.isDirectory()) 
                deleteLocalDir(file); // recursive delete the subdir
        } 
        dir.delete();// delete the root dir
    } 
    
    public BSPController.State currentState() {
        return this.state;
    }

}
