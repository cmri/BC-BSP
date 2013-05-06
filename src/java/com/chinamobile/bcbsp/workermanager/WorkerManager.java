/**
 * CopyRight by Chinamobile
 * 
 * WorkerManager.java
 */
package com.chinamobile.bcbsp.workermanager;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.fault.storage.Fault;
import com.chinamobile.bcbsp.fault.storage.Fault.Level;

import org.apache.log4j.LogManager;

import com.chinamobile.bcbsp.rpc.ControllerProtocol;
import com.chinamobile.bcbsp.rpc.WorkerManagerProtocol;
import com.chinamobile.bcbsp.sync.SuperStepReportContainer;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.StaffStatus;
import com.chinamobile.bcbsp.util.ClassLoaderUtil;
import com.chinamobile.bcbsp.bspcontroller.BSPController;
import com.chinamobile.bcbsp.action.*;
import com.chinamobile.bcbsp.bspstaff.BSPStaffRunner;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.bspstaff.BSPStaff.WorkerAgentForStaffInterface;

/**
 * A WorkerManager is a process that manages staffs assigned by the
 * BSPController. Each WorkerManager contacts the BSPController, and it takes
 * assigned staffs and reports its status by means of periodical heart beats
 * with BSPController. Each WorkerManager is designed to run with HDFS or other
 * distributed storages. Basically, a WorkerManager and a data node should be
 * run on one physical node.
 * 
 * @author
 * @version
 */
public class WorkerManager implements Runnable, WorkerManagerProtocol,
        WorkerAgentProtocol {

    private static final Log LOG = LogFactory.getLog(WorkerManager.class);

    private volatile static int HEART_BEAT_INTERVAL;
    private static int CACHE_QUEUE_LENGTH = 20;

    private Configuration conf;

    // Constants
    static enum State {
        NORMAL, COMPUTE, SYNC, BARRIER, STALE, INTERRUPTED, DENIED
    };

    // Running States and its related things
    volatile boolean initialized = false;
    volatile boolean running = true;
    volatile boolean shuttingDown = false;
    private boolean justInited = true;

    // Attributes
    private String workerManagerName;
    private InetSocketAddress bspControllerAddr;

    // FileSystem
    private Path systemDirectory = null;
    private FileSystem systemFS = null;

    // Job
    private int failures;
    private int maxStaffsCount = 0;
    private Integer currentStaffsCount = 0;
    private int finishedStaffsCount = 0;

    private List<Fault> workerFaultList = null;
    private List<StaffStatus> reportStaffStatusList = null;
    private Map<StaffAttemptID, StaffInProgress> runningStaffs = null;
    private Map<StaffAttemptID, StaffInProgress> finishedStaffs = null;
    private Map<BSPJobID, RunningJob> runningJobs = null;
    private Map<BSPJobID, RunningJob> finishedJobs = null;
    private Map<BSPJobID, WorkerAgentForJob> runningJobtoWorkerAgent = null;

    private String rpcServer;
    private Server workerServer;
    private ControllerProtocol controllerClient;

    private InetSocketAddress staffReportAddress;
    private Server staffReportServer = null;

    private ArrayList<BSPJobID> failedJobList = new ArrayList<BSPJobID>();

    // For current free port counter. It will travel around 60001~65535
    private int currentFreePort = 60000;

    public WorkerManager(Configuration conf) throws IOException {
        LOG.info("worker start");
        this.conf = conf;

        String mode = conf.get(Constants.BC_BSP_CONTROLLER_ADDRESS);
        if (!mode.equals("local")) {
            bspControllerAddr = BSPController.getAddress(conf);
        }
    }

    @SuppressWarnings("static-access")
    public synchronized void initialize() throws IOException {
        if (this.conf.get(Constants.BC_BSP_WORKERMANAGER_RPC_HOST) != null) {
            this.workerManagerName = conf
                    .get(Constants.BC_BSP_WORKERMANAGER_RPC_HOST);
        }

        if (this.workerManagerName == null) {
            this.workerManagerName = DNS.getDefaultHost(
                    conf.get("bsp.dns.interface", "default"),
                    conf.get("bsp.dns.nameserver", "default"));
        }
        // check local disk
        checkLocalDirs(conf.getStrings(Constants.BC_BSP_LOCAL_DIRECTORY));
        deleteLocalFiles("workerManager");

        this.workerFaultList = new ArrayList<Fault>();
        this.reportStaffStatusList = new ArrayList<StaffStatus>();
        this.runningStaffs = new ConcurrentHashMap<StaffAttemptID, StaffInProgress>();
        this.finishedStaffs = new ConcurrentHashMap<StaffAttemptID, StaffInProgress>();
        this.runningJobs = new ConcurrentHashMap<BSPJobID, RunningJob>();
        this.finishedJobs = new ConcurrentHashMap<BSPJobID, RunningJob>();
        this.runningJobtoWorkerAgent = new ConcurrentHashMap<BSPJobID, WorkerAgentForJob>();

        this.conf
                .set(Constants.BC_BSP_WORKERAGENT_HOST, this.workerManagerName);
        this.conf.set(Constants.BC_BSP_WORKERMANAGER_RPC_HOST,
                this.workerManagerName);
        this.maxStaffsCount = conf.getInt(
                Constants.BC_BSP_WORKERMANAGER_MAXSTAFFS, 1);
        this.HEART_BEAT_INTERVAL = conf.getInt(Constants.HEART_BEAT_INTERVAL,
                1000);
        LOG.info("The max number of staffs is : " + this.maxStaffsCount);

        int rpcPort = -1;
        String rpcAddr = null;
        if (false == this.initialized) {
            rpcAddr = conf.get(Constants.BC_BSP_WORKERMANAGER_RPC_HOST,
                    Constants.DEFAULT_BC_BSP_WORKERMANAGER_RPC_HOST);
            rpcPort = conf
                    .getInt(Constants.BC_BSP_WORKERMANAGER_RPC_PORT, 5000);
            if (-1 == rpcPort || null == rpcAddr)
                throw new IllegalArgumentException("Error rpc address "
                        + rpcAddr + " port" + rpcPort);
            this.workerServer = RPC.getServer(this, rpcAddr, rpcPort, conf);
            this.workerServer.start();
            this.rpcServer = rpcAddr + ":" + rpcPort;

            LOG.info("Worker rpc server --> " + rpcServer);
        }

        String address = conf
                .get(Constants.BC_BSP_WORKERMANAGER_REPORT_ADDRESS);
        InetSocketAddress socAddr = NetUtils.createSocketAddr(address);
        String bindAddress = socAddr.getHostName();
        int tmpPort = socAddr.getPort();

        // RPC initialization
        this.staffReportServer = RPC.getServer(this, bindAddress, tmpPort, 10,
                false, this.conf);

        this.staffReportServer.start();

        // get the assigned address
        this.staffReportAddress = staffReportServer.getListenerAddress();
        LOG.info("WorkerManager up at: " + this.staffReportAddress);

        DistributedCache.purgeCache(this.conf);

        // establish the communication link to bsp master
        this.controllerClient = ( ControllerProtocol ) RPC.waitForProxy(
                ControllerProtocol.class, ControllerProtocol.versionID,
                bspControllerAddr, conf);

        // enroll in bsp controller
        if (-1 == rpcPort || null == rpcAddr) {
            throw new IllegalArgumentException("Error rpc address " + rpcAddr
                    + " port" + rpcPort);
        }

        if (!this.controllerClient.register(new WorkerManagerStatus(
                workerManagerName, cloneAndResetRunningStaffStatuses(),
                maxStaffsCount, currentStaffsCount, finishedStaffsCount,
                failures, this.rpcServer))) {
            LOG.error("There is a problem in establishing communication"
                    + " link with BSPController");
            throw new IOException("There is a problem in establishing"
                    + " communication link with BSPController.");
        }

        this.running = true;
        this.initialized = true;
    }

    /** Return the port at which the staff tracker bound to */
    public synchronized InetSocketAddress getStaffTrackerReportAddress() {
        return staffReportAddress;
    }
    
    @Override
    public boolean dispatch(BSPJobID jobId, Directive directive,
            boolean recovery, boolean changeWorkerState, int failCounter) {
        // update tasks status
        WorkerManagerAction[] actions = directive.getActions();
        LOG.info("Got Response from BSPController with "
                + ((actions != null) ? actions.length : 0) + " actions");
        // perform actions

        if (actions != null) {
            for (WorkerManagerAction action : actions) {
                try {
                    if (action instanceof LaunchStaffAction) {
                        if (recovery == true) {
                            String localPath = conf
                                    .get(Constants.BC_BSP_LOCAL_DIRECTORY) // /tmp/bcbsp/local
                                    + "/workerManager";
                            LOG.info("if(recovery == true)" + " " + localPath);

                            if (FileSystem.getLocal(conf).exists(
                                    new Path(localPath,
                                            (( LaunchStaffAction ) action)
                                                    .getStaff()
                                                    .getStaffAttemptId()
                                                    .toString()))) {
                                FileSystem.getLocal(conf).delete(
                                        new Path(localPath,
                                                (( LaunchStaffAction ) action)
                                                        .getStaff()
                                                        .getStaffAttemptId()
                                                        .toString()), true);
                            }
                        }
                        startNewStaff(( LaunchStaffAction ) action, directive,
                                recovery, changeWorkerState, failCounter);
                        return true;
                    } else {
                        KillStaffAction killAction = ( KillStaffAction ) action;
                        if (runningStaffs.containsKey(killAction.getStaffID())) {
                            StaffInProgress sip = runningStaffs.get(killAction
                                    .getStaffID());
                            sip.staffStatus
                                    .setRunState(StaffStatus.State.KILLED);
                            sip.killAndCleanup(true);
                        } else {
                            LOG.warn(killAction.getStaffID()
                                    + " is not in the runningStaffs "
                                    + "and the kill action is invalid.");
                        }
                        return false;
                    }
                } catch (IOException e) {
                    LOG.error("Exception has been catched in WorkerManager--dispatch !", e);
                    StaffInProgress sip = null;
                    sip = runningStaffs.get((( LaunchStaffAction ) action)
                            .getStaff().getStaffAttemptId());
                    sip.getStatus().setStage(0);                        // convenient for the call in controller
                    sip.setStaffStatus(Constants.SATAFF_STATUS.FAULT,
                            new Fault(Fault.Type.DISK, Level.WARNING, sip
                                    .getStatus().getGroomServer(),
                                    "IOException happened", sip.getStatus()
                                            .getJobId().toString(), sip
                                            .getStatus().getStaffId()
                                            .toString()));
                }
            }

        }
        return false;
    }

    private static void checkLocalDirs(String[] localDirs)
            throws DiskErrorException {
        boolean writable = false;

        if (localDirs != null) {
            for (int i = 0; i < localDirs.length; i++) {
                try {
                    DiskChecker.checkDir(new File(localDirs[i]));
                    LOG.info("Local System is Normal : " + localDirs[i]);
                    writable = true;
                } catch (DiskErrorException e) {
                    LOG.error("BSP Processor local", e);
                }
            }
        }

        if (!writable)
            throw new DiskErrorException(
                    "all local directories are not writable");
    }

    public String[] getLocalDirs() {
        return conf.getStrings(Constants.BC_BSP_LOCAL_DIRECTORY);
    }

    public void deleteLocalFiles() throws IOException {
        String[] localDirs = getLocalDirs();
        for (int i = 0; i < localDirs.length; i++) {
            File f = new File(localDirs[i]);
            deleteLocalDir(f);
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

    public void deleteLocalFiles(String subdir) throws IOException {
        try {
            String[] localDirs = getLocalDirs();
            for (int i = 0; i < localDirs.length; i++) {
                FileSystem.getLocal(this.conf).delete(
                        new Path(localDirs[i], subdir), true);
            }
        } catch (NullPointerException e) {
            LOG.error("[deleteLocalFiles]", e);
        }
    }

    public void cleanupStorage() throws IOException {
        deleteLocalFiles();
    }

    private void startCleanupThreads() throws IOException {

    }

    public void updateStaffStatistics(BSPJobID jobId) throws Exception {
        synchronized (currentStaffsCount) {
            currentStaffsCount--;
        }
        finishedStaffsCount++;
        if (finishedStaffs.size() > CACHE_QUEUE_LENGTH) {
            finishedStaffs.clear();
        }

        synchronized (runningJobs) {
            int counter = runningJobs.get(jobId).getStaffCounter();
            if (counter > 0) {
                runningJobs.get(jobId).setStaffCounter(counter - 1);
            }

            if (runningJobs.get(jobId).getStaffCounter() == 0) {
                if (finishedJobs.size() > CACHE_QUEUE_LENGTH) {
                    finishedJobs.clear();
                }
                finishedJobs.put(jobId, runningJobs.remove(jobId));
                runningJobtoWorkerAgent.get(jobId).close();
                runningJobtoWorkerAgent.remove(jobId);
            }
        }
    }

    public State offerService() throws Exception {
        while (running && !shuttingDown) {// && !upRecoveryThreshold
            try {
                this.reportStaffStatusList.clear();
                Iterator<Entry<StaffAttemptID, StaffInProgress>> runningStaffsIt = runningStaffs
                        .entrySet().iterator();
                Entry<StaffAttemptID, StaffInProgress> entry;
                while (runningStaffsIt.hasNext()) {
                    entry = runningStaffsIt.next();
                    switch (entry.getValue().getStatus().getRunState()) {
                        case COMMIT_PENDING:
                        case UNASSIGNED:
                            // TODO : Do nothing now.
                            break;
                        case RUNNING:
                            this.reportStaffStatusList.add(entry.getValue()
                                    .getStatus());
                            break;
                        case SUCCEEDED:
                            updateStaffStatistics(entry.getValue().getStatus()
                                    .getJobId());
                            runningStaffsIt.remove();
                            finishedStaffs
                                    .put(entry.getKey(), entry.getValue());
                            LOG.info(entry.getKey()
                                    + " has succeed and been removed from the runningStaffs");
                            break;
                        case FAULT:
                            if (entry.getValue().runner.isAlive()) {
                                entry.getValue().getStatus()
                                        .setPhase(StaffStatus.Phase.CLEANUP);
                                entry.getValue().runner.kill();
                            }
                            this.reportStaffStatusList.add(entry.getValue()
                                    .getStatus());
                            updateStaffStatistics(entry.getValue().getStatus()
                                    .getJobId());
                            runningStaffsIt.remove();
                            finishedStaffs
                                    .put(entry.getKey(), entry.getValue());
                            LOG.error(entry.getKey()
                                    + " is fault and has been removed from the runningStaffs");
                            break;
                        case STAFF_RECOVERY:
                            // TODO : Do nothing now.
                            break;
                        case WORKER_RECOVERY:
                            // TODO : Do nothing now.
                            break;
                        case FAILED:
                            // TODO : Do nothing now.
                            break;
                        case KILLED:
                            updateStaffStatistics(entry.getValue().getStatus()
                                    .getJobId());
                            runningStaffsIt.remove();
                            finishedStaffs
                                    .put(entry.getKey(), entry.getValue());
                            LOG.warn(entry.getKey()
                                    + " has been killed manually and removed from the runningStaffs");
                            break;
                        case FAILED_UNCLEAN:
                            // TODO : Do nothing now.
                            break;
                        case KILLED_UNCLEAN:
                            // TODO : This staff should be report and request
                            // the cleanup task in the future.
                            updateStaffStatistics(entry.getValue().getStatus()
                                    .getJobId());
                            runningStaffsIt.remove();
                            finishedStaffs
                                    .put(entry.getKey(), entry.getValue());
                            LOG.warn(entry.getKey()
                                    + " has been killed manually and removed from the runningStaffs");
                            break;
                        default:
                            LOG.error("Unknown StaffStatus.State: "
                                    + entry.getValue().getStatus()
                                            .getRunState());
                    }
                }

                WorkerManagerStatus gss = new WorkerManagerStatus(
                        this.workerManagerName, this.reportStaffStatusList,
                        maxStaffsCount, currentStaffsCount,
                        finishedStaffsCount, failures, this.rpcServer,
                        workerFaultList);
                try {
                    boolean ret = controllerClient.report(new Directive(gss));
                    synchronized (this) {
                        workerFaultList.clear();
                    }// list.add() need synchronize
                    if (!ret) {
                        LOG.error("fail to update");
                    }
                } catch (Exception ioe) {
                    LOG.error(
                            "Fail to communicate with BSPController for reporting.",
                            ioe);
                }

                Thread.sleep(HEART_BEAT_INTERVAL);
            } catch (InterruptedException ie) {
                LOG.error("[offerService]", ie);
            }
        }
        return State.NORMAL;
    }

    private void startNewStaff(LaunchStaffAction action, Directive directive,
            boolean recovery, boolean changeWorkerState, int failCounter) {
        Staff s = action.getStaff();
        BSPJob jobConf = null;
        try {
            jobConf = new BSPJob(s.getJobID(), s.getJobFile());
            jobConf.setInt("staff.fault.superstep", directive.getFaultSSStep());
        } catch (IOException e1) {
            LOG.error("Exception has been catched in WorkerManager--startNewStaff-jobConf", e1);
            StaffInProgress sip = runningStaffs.get((( LaunchStaffAction ) action).getStaff()
                    .getStaffAttemptId());
            sip.getStatus().setStage(0); // convenient for the call in
                                         // controller
            sip.setStaffStatus(Constants.SATAFF_STATUS.FAULT, new Fault(
                    Fault.Type.DISK, Level.WARNING, sip.getStatus()
                            .getGroomServer(), "IOException happened", sip
                            .getStatus().getJobId().toString(), sip.getStatus()
                            .getStaffId().toString()));
        }

        StaffInProgress sip = new StaffInProgress(s, jobConf,
                this.workerManagerName);
        
        sip.setFailCounter(failCounter);
        if (recovery == true) {
            sip.getStatus().setRecovery(true);
        }

        if (changeWorkerState == true) {
            sip.setChangeWorkerState(true);
        }

        try {
            localizeJob(sip, directive);
        } catch (IOException e) {
            LOG.error("Exception has been catched in WorkerManager--startNewStaff-localizeJob", e);
            sip = runningStaffs.get((( LaunchStaffAction ) action).getStaff()
                    .getStaffAttemptId());
            sip.getStatus().setStage(0); // convenient for the call in
                                         // controller
            sip.setStaffStatus(Constants.SATAFF_STATUS.FAULT, new Fault(
                    Fault.Type.DISK, Level.WARNING, sip.getStatus()
                            .getGroomServer(), "IOException happened", sip
                            .getStatus().getJobId().toString(), sip.getStatus()
                            .getStaffId().toString()));
        }
    }

    private void localizeJob(StaffInProgress sip, Directive directive)
            throws IOException {
        Staff staff = sip.getStaff();
        conf.addResource(staff.getJobFile());
        BSPJob defaultJobConf = new BSPJob(( BSPConfiguration ) conf);
        Path localJobFile = defaultJobConf
                .getLocalPath(Constants.BC_BSP_LOCAL_SUBDIR_WORKERMANAGER + "/"
                        + staff.getStaffID() + "/" + "job.xml");

        Path localJarFile = defaultJobConf
                .getLocalPath(Constants.BC_BSP_LOCAL_SUBDIR_WORKERMANAGER + "/"
                        + staff.getStaffID() + "/" + "job.jar");
        systemFS.copyToLocalFile(new Path(staff.getJobFile()), localJobFile);

        BSPConfiguration conf = new BSPConfiguration();
        conf.addResource(localJobFile);
        BSPJob jobConf = new BSPJob(conf, staff.getJobID().toString());

        Path jarFile = null;
        if(jobConf.getJar() != null){
            jarFile = new Path(jobConf.getJar());
        }
        jobConf.setJar(localJarFile.toString());

        if (jarFile != null) {
            systemFS.copyToLocalFile(jarFile, localJarFile);
            // also unjar the job.jar files in workdir
            File workDir = new File(
                    new File(localJobFile.toString()).getParent(), "work");
            if (!workDir.mkdirs()) {
                if (!workDir.isDirectory()) {
                    throw new IOException("Mkdirs failed to create "
                            + workDir.toString());
                }
            }
            RunJar.unJar(new File(localJarFile.toString()), workDir);
        }

        /** Add the user program jar to the system's classpath. */
        ClassLoaderUtil.addClassPath(localJarFile.toString());

        RunningJob rjob = addStaffToJob(staff.getJobID(), localJobFile, sip,
                directive, jobConf);
        rjob.localized = true;
        sip.setFaultSSStep(directive.getFaultSSStep());
        launchStaffForJob(sip, jobConf);
    }

    private void launchStaffForJob(StaffInProgress sip, BSPJob jobConf) {
        try {
            sip.setJobConf(jobConf);
            sip.launchStaff();
        } catch (IOException ioe) {
            LOG.error("Exception has been catched in WorkerManager--launchStaffForJob", ioe);
            sip.staffStatus.setRunState(StaffStatus.State.FAILED);

            sip.getStatus().setStage(0); // convenient for the call in
                                         // controller
            sip.setStaffStatus(Constants.SATAFF_STATUS.FAULT, new Fault(
                    Fault.Type.SYSTEMSERVICE, Fault.Level.INDETERMINATE, sip
                            .getStatus().getGroomServer(), ioe.toString(), sip
                            .getStatus().getJobId().toString(), sip.getStatus()
                            .getStaffId().toString()));
        }
    }

    private RunningJob addStaffToJob(BSPJobID jobId, Path localJobFile,
            StaffInProgress sip, Directive directive, BSPJob job) {
        synchronized (runningJobs) {
            RunningJob rJob = null;
            if (!runningJobs.containsKey(jobId)) {
                rJob = new RunningJob(jobId, localJobFile);
                rJob.localized = false;
                rJob.staffs = new HashSet<StaffInProgress>();
                rJob.jobFile = localJobFile;
                runningJobs.put(jobId, rJob);

                // Create a new WorkerAgentForJob for a new job
                try {
                    WorkerAgentForJob bspPeerForJob = new WorkerAgentForJob(
                            conf, jobId, job, this);
                    runningJobtoWorkerAgent.put(jobId, bspPeerForJob);
                } catch (IOException e) {
                    LOG.error("Failed to create a WorkerAgentForJob for a new job"
                            + jobId.toString());
                }

            } else {
                rJob = runningJobs.get(jobId);
            }
            rJob.staffs.add(sip);
            int counter = rJob.getStaffCounter();
            rJob.setStaffCounter(counter + 1);
            return rJob;
        }
    }

    /**
     * The data structure for initializing a job
     */
    static class RunningJob {
        private BSPJobID jobid;
        private Path jobFile;
        // keep this for later use
        Set<StaffInProgress> staffs;
        private int staffCounter = 0;
        boolean localized;
        boolean keepJobFiles;

        RunningJob(BSPJobID jobid, Path jobFile) {
            this.jobid = jobid;
            localized = false;
            staffs = new HashSet<StaffInProgress>();
            this.jobFile = jobFile;
            keepJobFiles = false;
        }

        Path getJobFile() {
            return jobFile;
        }

        BSPJobID getJobId() {
            return jobid;
        }

        public void setStaffCounter(int counter) {
            staffCounter = counter;
        }

        public int getStaffCounter() {
            return staffCounter;
        }
    }

    private synchronized List<StaffStatus> cloneAndResetRunningStaffStatuses() {
        List<StaffStatus> result = new ArrayList<StaffStatus>(
                runningStaffs.size());
        for (StaffInProgress sip : runningStaffs.values()) {
            StaffStatus status = sip.getStatus();
            result.add(( StaffStatus ) status.clone());
        }
        return result;
    }

    public void initFileSystem() throws Exception {
        if (justInited) {
            String dir = controllerClient.getSystemDir();
            if (dir == null) {
                LOG.error("Fail to get system directory.");
                throw new IOException("Fail to get system directory.");
            }
            systemDirectory = new Path(dir);
            systemFS = systemDirectory.getFileSystem(conf);
        }
        justInited = false;
    }

    public void run() {
        try {
            initialize();
            initFileSystem();
            startCleanupThreads();
            boolean denied = false;
            while (running && !shuttingDown && !denied) {
                boolean staleState = false;
                try {
                    while (running && !staleState && !shuttingDown && !denied) {
                        try {
                            State osState = offerService();
                            if (osState == State.STALE) {
                                staleState = true;
                            } else if (osState == State.DENIED) {
                                denied = true;
                            }
                        } catch (Exception e) {
                            if (!shuttingDown) {
                                LOG.warn(
                                        "Lost connection to BSP Controller ["
                                                + bspControllerAddr
                                                + "].  Retrying...", e);
                                try {
                                    Thread.sleep(5000);
                                } catch (InterruptedException ie) {
                                    LOG.error("[run]", ie);
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    LOG.error("[run]", e);
                }
                if (shuttingDown) {
                    return;
                }
                LOG.warn("Reinitializing local state");
                initialize();
                initFileSystem();
            }
        } catch (Exception ioe) {
            LOG.error("Got fatal exception in WorkerManager: "
                    + StringUtils.stringifyException(ioe));
            LOG.error("WorkerManager will quit abnormally!");
            close();
            return;
        }
    }

    public synchronized void shutdown() throws IOException {
        LOG.info("Prepare to shutdown the WorkerManager");
        shuttingDown = true;
        close();
    }

    public synchronized void close() {
        this.running = false;
        this.initialized = false;
        try {
            for (StaffInProgress sip : runningStaffs.values()) {
                if (sip.runner.isAlive()) {
                    sip.killAndCleanup(true);
                    LOG.info(sip.getStatus().getStaffId()
                            + " has been killed by system");
                }
            }
            LOG.info("Succeed to stop all Staff Process");

            for (Map.Entry<BSPJobID, WorkerAgentForJob> e : runningJobtoWorkerAgent
                    .entrySet()) {
                e.getValue().close();
            }
            LOG.info("Succeed to stop all WorkerAgentForJob");

            this.workerServer.stop();
            RPC.stopProxy(controllerClient);
            if (staffReportServer != null) {
                staffReportServer.stop();
                staffReportServer = null;
            }
            LOG.info("Succeed to stop all RPC Server");

            cleanupStorage();
            LOG.info("Succeed to cleanup temporary files on the local disk");
        } catch (Exception e) {
            LOG.error("Failed to execute the close()", e);
        }
    }

    public static Thread startWorkerManager(final WorkerManager hrs) {
        return startWorkerManager(hrs, "regionserver" + hrs.workerManagerName);
    }

    public static Thread startWorkerManager(final WorkerManager hrs,
            final String name) {
        Thread t = new Thread(hrs);
        t.setName(name);
        t.start();
        return t;
    }

    /**
     * StaffInProgress maintains all the info for a Staff that lives at this
     * WorkerManager. It maintains the Staff object, its StaffStatus, and the
     * BSPStaffRunner.
     * 
     * @author
     * @version
     */
    class StaffInProgress {
        Staff staff;
        WorkerAgentForStaffInterface staffAgent;
        public BSPJob jobConf;
        BSPJob localJobConf;
        BSPStaffRunner runner;
        volatile boolean done = false;
        volatile boolean wasKilled = false;
        private StaffStatus staffStatus;
        private String error = "no";
        private int faultSSStep = 0;
        private boolean changeWorkerState = false;
        private int failCounter = 0;

        public StaffInProgress(Staff staff, BSPJob jobConf, String workerManagerName) {
            this.staff = staff;
            this.jobConf = jobConf;
            this.localJobConf = null;
            this.staffStatus = new StaffStatus(staff.getJobID(),
                    staff.getStaffID(), 0, StaffStatus.State.UNASSIGNED,
                    "running", workerManagerName, StaffStatus.Phase.STARTING);
        }

        public void setStaffStatus(int stateStatus, Fault fault) {
            switch (stateStatus) {
                case Constants.SATAFF_STATUS.RUNNING:
                    this.staffStatus.setRunState(StaffStatus.State.RUNNING);
                    break;
                case Constants.SATAFF_STATUS.SUCCEED:
                    this.staffStatus.setRunState(StaffStatus.State.SUCCEEDED);
                    break;
                case Constants.SATAFF_STATUS.FAULT:
                    this.staffStatus.setRunState(StaffStatus.State.FAULT);
                    this.staffStatus.setFault(fault);
                    break;
                default:
                    LOG.error("Unknown StaffStatus.State: <Constants.SATAFF_STATUS>"
                            + stateStatus);
            }
        }

        public boolean getChangeWorkerState() {
            return changeWorkerState;
        }

        public void setChangeWorkerState(boolean changeWorkerState) {
            this.changeWorkerState = changeWorkerState;
        }

        public String getError() {
            return this.error;
        }

        public int getFaultSSStep() {
            return faultSSStep;
        }

        public void setFaultSSStep(int faultSSStep) {
            this.faultSSStep = faultSSStep;
        }
        
        public void setFailCounter(int failCounter) {
            this.failCounter = failCounter;
        }
        
        public int getFailCounter() {
            return this.failCounter;
        }

        private void localizeStaff(Staff task) throws IOException {
            Path localJobFile = this.jobConf
                    .getLocalPath(Constants.BC_BSP_LOCAL_SUBDIR_WORKERMANAGER
                            + "/" + task.getStaffID() + "/job.xml");
            Path localJarFile = this.jobConf
                    .getLocalPath(Constants.BC_BSP_LOCAL_SUBDIR_WORKERMANAGER
                            + "/" + task.getStaffID() + "/job.jar");

            String jobFile = task.getJobFile();
            systemFS.copyToLocalFile(new Path(jobFile), localJobFile);
            task.setJobFile(localJobFile.toString());

            localJobConf = new BSPJob(task.getJobID(), localJobFile.toString());
            localJobConf.set("bsp.task.id", task.getStaffID().toString());
            String jarFile = localJobConf.getJar();

            if (jarFile != null) {
                systemFS.copyToLocalFile(new Path(jarFile), localJarFile);
                localJobConf.setJar(localJarFile.toString());
            }

            LOG.debug("localizeStaff : " + localJobConf.getJar());
            LOG.debug("localizeStaff : " + localJobFile.toString());

            task.setConf(localJobConf);
        }

        public synchronized void setJobConf(BSPJob jobConf) {
            this.jobConf = jobConf;
        }

        public synchronized BSPJob getJobConf() {
            return localJobConf;
        }

        public void launchStaff() throws IOException {
            localizeStaff(staff);
            staffStatus.setRunState(StaffStatus.State.RUNNING);

            BSPJobID jobID = localJobConf.getJobID();
            runningJobtoWorkerAgent.get(jobID).addStaffCounter(
                    staff.getStaffAttemptId());
            runningJobtoWorkerAgent.get(jobID).setJobConf(jobConf);
            runningStaffs.put(staff.getStaffAttemptId(), this);
            synchronized (currentStaffsCount) {
                currentStaffsCount++;
            }
            this.runner = staff.createRunner(WorkerManager.this);
            this.runner.setFaultSSStep(this.faultSSStep);
            this.runner.start();
        }

        /**
         * This task has run on too long, and should be killed.
         */
        public synchronized void killAndCleanup(boolean wasFailure)
                throws IOException {
            onKillStaff();
            runner.kill();
        }

        private void onKillStaff() {
            if (this.staffAgent != null) {
                this.staffAgent.onKillStaff();
            }
        }

        public Staff getStaff() {
            return staff;
        }

        public synchronized StaffStatus getStatus() {
            return staffStatus;
        }

        public StaffStatus.State getRunState() {
            return staffStatus.getRunState();
        }

        public boolean wasKilled() {
            return wasKilled;
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof StaffInProgress)
                    && staff.getStaffID().equals(
                            (( StaffInProgress ) obj).getStaff().getStaffID());
        }

        @Override
        public int hashCode() {
            return staff.getStaffID().hashCode();
        }

        public void setStaffAgent(WorkerAgentForStaffInterface staffAgent) {
            this.staffAgent = staffAgent;
        }
    }

    public boolean isRunning() {
        return running;
    }

    public static WorkerManager constructWorkerManager(
            Class<? extends WorkerManager> workerManagerClass,
            final Configuration conf2) {
        try {
            Constructor<? extends WorkerManager> c = workerManagerClass
                    .getConstructor(Configuration.class);
            return c.newInstance(conf2);
        } catch (Exception e) {
            throw new RuntimeException("Failed construction of " + "WorkerManager: "
                    + workerManagerClass.toString(), e);
        }
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion)
            throws IOException {
        if (protocol.equals(WorkerManagerProtocol.class.getName())) {
            return WorkerManagerProtocol.versionID;
        } else if (protocol.equals(WorkerAgentProtocol.class.getName())) {
            return WorkerAgentProtocol.versionID;
        } else {
            throw new IOException("Unknown protocol to WorkerManager: "
                    + protocol);
        }
    }

    /**
     * The main() for child processes.
     * 
     * @author
     * @version
     */
    public static class Child {

        public static void main(String[] args) {
            BSPConfiguration defaultConf = new BSPConfiguration();
            // report address
            String host = args[0];
            int port = Integer.parseInt(args[1]);
            InetSocketAddress address = new InetSocketAddress(host, port);
            StaffAttemptID staffid = StaffAttemptID.forName(args[2]);
            int faultSSStep = Integer.parseInt(args[3]);
            String hostName = args[4];
            LOG.info(staffid + ": Child Starts");
            LOG.info("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*");
            WorkerAgentProtocol umbilical = null;
            Staff staff = null;
            BSPJob job = null;
            try {
                umbilical = ( WorkerAgentProtocol ) RPC.getProxy(
                        WorkerAgentProtocol.class,
                        WorkerAgentProtocol.versionID, address, defaultConf);

                staff = umbilical.getStaff(staffid);
                defaultConf.addResource(new Path(staff.getJobFile()));

                job = new BSPJob(staff.getJobID(), staff.getJobFile());

                // use job-specified working directory
                FileSystem.get(job.getConf()).setWorkingDirectory(
                        job.getWorkingDirectory());
                boolean recovery = umbilical.getStaffRecoveryState(staffid);
                boolean changeWorkerState = umbilical
                        .getStaffChangeWorkerState(staffid);
                int failCounter = umbilical.getFailCounter(staffid);
                job.setInt("staff.fault.superstep", faultSSStep);
                staff.run(job, staff, umbilical, recovery, changeWorkerState, failCounter,
                        hostName); // run the task
            } catch (ClassNotFoundException cnfE) {
                LOG.error("Exception has been catched in WorkerManager--Error running child", cnfE);
                // Report back any failures, for diagnostic purposes
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                cnfE.printStackTrace(new PrintStream(baos));

                umbilical.setStaffStatus(
                        staffid,
                        Constants.SATAFF_STATUS.FAULT,
                        new Fault(Fault.Type.SYSTEMSERVICE,
                                Fault.Level.CRITICAL, umbilical
                                        .getWorkerManagerName(job.getJobID(),
                                                staffid), cnfE.toString(),
                                job.toString(), staffid.toString()), 0);
            } catch (FSError e) {
                LOG.error("Exception has been catched in WorkerManager--FSError from child", e);
                umbilical.setStaffStatus(
                        staffid,
                        Constants.SATAFF_STATUS.FAULT,
                        new Fault(Fault.Type.SYSTEMSERVICE,
                                Fault.Level.CRITICAL, umbilical
                                        .getWorkerManagerName(job.getJobID(),
                                                staffid), e.toString(), job
                                        .toString(), staffid.toString()), 0);

            } catch (Throwable throwable) {
                LOG.error("Exception has been catched in WorkerManager--Error running child", throwable);
                // Report back any failures, for diagnostic purposes
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                throwable.printStackTrace(new PrintStream(baos));

                umbilical.setStaffStatus(
                        staffid,
                        Constants.SATAFF_STATUS.FAULT,
                        new Fault(Fault.Type.SYSTEMSERVICE,
                                Fault.Level.CRITICAL, umbilical
                                        .getWorkerManagerName(job.getJobID(),
                                                staffid), throwable.toString(),
                                job.toString(), staffid.toString()), 0);
            } finally {
                RPC.stopProxy(umbilical);
                MetricsContext metricsContext = MetricsUtil
                        .getContext("mapred");
                metricsContext.close();
                // Shutting down log4j of the child-vm...
                // This assumes that on return from Staff.run()
                // there is no more logging done.
                LogManager.shutdown();
            }
        }
    }

    @Override
    public Staff getStaff(StaffAttemptID staffid) throws IOException {
        StaffInProgress sip = runningStaffs.get(staffid);
        if (sip != null) {
            return sip.getStaff();
        } else {
            LOG.warn(staffid + " is not in the runningStaffs");
            return null;
        }
    }

    @Override
    public boolean getStaffRecoveryState(StaffAttemptID staffId) {
        return runningStaffs.get(staffId).getStatus().isRecovery();
    }

    @Override
    public boolean getStaffChangeWorkerState(StaffAttemptID staffId) {
        return runningStaffs.get(staffId).getChangeWorkerState();
    }

    @Override
    public int getFailCounter(StaffAttemptID staffId) {
        return this.runningStaffs.get(staffId).getFailCounter();
    }
    
    @Override
    public boolean ping(StaffAttemptID staffId) throws IOException {
        return false;
    }

    @Override
    public void done(StaffAttemptID staffId, boolean shouldBePromoted)
            throws IOException {
        // TODO Auto-generated method stub
    }

    @Override
    public void fsError(StaffAttemptID staffId, String message)
            throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public String getWorkerManagerName(BSPJobID jobId, StaffAttemptID staffId) {
        return runningJobtoWorkerAgent.get(jobId).getWorkerManagerName(jobId, staffId);
    }

    @Override
    public boolean localBarrier(BSPJobID jobId, StaffAttemptID staffId,
            int superStepCounter, SuperStepReportContainer ssrc) {
        return runningJobtoWorkerAgent.get(jobId).localBarrier(jobId, staffId,
                superStepCounter, ssrc);

    }

    @Override
    public int getNumberWorkers(BSPJobID jobId, StaffAttemptID staffId) {
        return runningJobtoWorkerAgent.get(jobId).getNumberWorkers(jobId,
                staffId);
    }

    @Override
    public void setNumberWorkers(BSPJobID jobId, StaffAttemptID staffId, int num) {
        runningJobtoWorkerAgent.get(jobId)
                .setNumberWorkers(jobId, staffId, num);
    }

    // nc
    @Override
    public void addStaffReportCounter(BSPJobID jobId) {
        runningJobtoWorkerAgent.get(jobId).addStaffReportCounter();
    }

    public String getWorkerManagerName() {
        return this.workerManagerName;
    }

    @Override
    public BSPJobID getBSPJobID() {
        return null;
    }

    @Override
    public void setStaffStatus(StaffAttemptID staffId, int staffStatus, Fault fault, int stage) {
        this.runningStaffs.get(staffId).setStaffStatus(staffStatus, fault);
        this.runningStaffs.get(staffId).getStatus().setStage(stage);
    }
    
    public StaffStatus getStaffStatus(StaffAttemptID staffId){
        return this.runningStaffs.get(staffId).getStatus();
    }

    /**
     * This method is used to set mapping table that shows the partition to the
     * worker. According to Job ID get WorkerAgentForJob and call its method to
     * set this mapping table.
     * 
     * @param jobId
     * @param partitionId
     * @param hostName
     */
    public void setWorkerNametoPartitions(BSPJobID jobId, int partitionId,
            String hostName) {
        this.runningJobtoWorkerAgent.get(jobId).setWorkerNametoPartitions(
                jobId, partitionId, hostName);
    }

    /**
     * Get the host name of the workerManager.
     * 
     * @return
     */
    public String getHostName() {
        return this.conf.get(Constants.BC_BSP_WORKERAGENT_HOST,
                Constants.DEFAULT_BC_BSP_WORKERAGENT_HOST);
    }
    
    @Override
    public void clearFailedJobList() {
        this.failedJobList.clear();
    }
    
    @Override
    public void addFailedJob(BSPJobID jobId) {
        this.failedJobList.add(jobId);
    }
    
    @Override
    public int getFailedJobCounter() {
        return this.failedJobList.size();
    }

    @Override
    public synchronized int getFreePort() {
        ServerSocket s;
        this.currentFreePort = this.currentFreePort + 1;

        int count = 0;

        for (; this.currentFreePort <= 65536; this.currentFreePort++) {

            count++;
            if (count > 5535) {
                LOG.info("[WorkerManager: getFreePort()] attempts to get a free port over 5535 times!");
                return 60000;
            }

            if (this.currentFreePort > 65535)
                this.currentFreePort = 60001;

            try {
                s = new ServerSocket(this.currentFreePort);
                s.close();
                return this.currentFreePort;
            } catch (IOException e) {
                LOG.error("[WokerManager] caught", e);
            }
        }

        return 60000;
    }

    @Override
    public void setStaffAgentAddress(StaffAttemptID staffID, String addr) {
        if (this.runningStaffs.containsKey(staffID)) {
            StaffInProgress sip = this.runningStaffs.get(staffID);
            String[] addrs = addr.split(":");
            InetSocketAddress address = new InetSocketAddress(addrs[0],
                    Integer.parseInt(addrs[1]));
            WorkerAgentForStaffInterface staffAgent = null;
            try {
                staffAgent = ( WorkerAgentForStaffInterface ) RPC.getProxy(
                        WorkerAgentForStaffInterface.class,
                        WorkerAgentForStaffInterface.versionID, address,
                        this.conf);
            } catch (IOException e) {
                LOG.error("[WorkerManager] caught: ", e);
            }
            sip.setStaffAgent(staffAgent);
        }
    }
}
