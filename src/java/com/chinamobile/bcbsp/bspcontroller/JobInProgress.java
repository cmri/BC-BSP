/**
 * CopyRight by Chinamobile
 * 
 * JobInProgress.java
 * 
 * JobInProgress is the center for controlling the job running. It maintains all
 * important information about the job and the staffs, including the status.
 */
package com.chinamobile.bcbsp.bspcontroller;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
//import java.net.URI;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.client.BSPJobClient;
import com.chinamobile.bcbsp.client.BSPJobClient.RawSplit;
import com.chinamobile.bcbsp.fault.storage.Fault;
import com.chinamobile.bcbsp.sync.GeneralSSController;
import com.chinamobile.bcbsp.sync.GeneralSSControllerInterface;
import com.chinamobile.bcbsp.sync.SuperStepCommand;
import com.chinamobile.bcbsp.sync.SuperStepReportContainer;
import com.chinamobile.bcbsp.util.*;
import com.chinamobile.bcbsp.api.AggregateValue;
import com.chinamobile.bcbsp.api.Aggregator;
import com.chinamobile.bcbsp.bspstaff.StaffInProgress;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.workermanager.WorkerManagerStatus;

/**
 * JobInProgress is the center for controlling the job running. It maintains all
 * important information about the job and the staffs, including the status.
 * 
 * @author
 * @version
 */
public class JobInProgress implements JobInProgressControlInterface {
    /**
     * Used when a kill is issued to a job which is initializing.
     */
    public static class KillInterruptedException extends InterruptedException {
        private static final long serialVersionUID = 1L;

        public KillInterruptedException(String msg) {
            super(msg);
        }
    }

    private static final Log LOG = LogFactory.getLog(JobInProgress.class);
    private boolean staffsInited = false;
    private boolean checkPointNext = false;

    private Configuration conf;
    private JobProfile profile;
    private JobStatus status;
    private Path jobFile = null;
    private Path localJobFile = null;
    private Path localJarFile = null;
    private LocalFileSystem localFs;

    private long startTime;
    private long launchTime;
    private long finishTime;

    private BSPJobID jobId;
    private BSPJob job;
    private final BSPController controller;
    
    private StaffInProgress staffs[] = new StaffInProgress[0];
    private int superStepCounter;
    //record taskID and staff status
    private List<StaffAttemptID> attemptIDList= new ArrayList<StaffAttemptID>();   
    
    private int numBSPStaffs = 0;
    private GeneralSSControllerInterface gssc;

    private HashMap<String, ArrayList<StaffAttemptID>> workersToStaffs = new HashMap<String, ArrayList<StaffAttemptID>>();
    //record the same staff run on different workers' times
    private LinkedHashMap<StaffAttemptID, Map<String, Integer>> staffToWMTimes = new LinkedHashMap<StaffAttemptID, Map<String, Integer>>();

    private int checkPointFrequency = 0;
    private int attemptRecoveryCounter = 0;
    private int maxAttemptRecoveryCounter = 0;
    private int maxStaffAttemptRecoveryCounter = 0;
    private int ableCheckPoint = 0;
    private int faultSSStep;
    
    private HashMap<WorkerManagerStatus, Integer> failedRecord = new HashMap<WorkerManagerStatus, Integer>();
    private ArrayList<WorkerManagerStatus> blackList = new ArrayList<WorkerManagerStatus>();

    /** the priority level of the job */
    private String priority = Constants.PRIORITY.NORMAL;// default
    
    // For Aggregation
    /** Map for user registered aggregate values. */
    private HashMap<String, Class<? extends AggregateValue<?>>> nameToAggregateValue = new HashMap<String, Class<? extends AggregateValue<?>>>();
    /** Map for user registered aggregatros. */
    private HashMap<String, Class<? extends Aggregator<?>>> nameToAggregator = new HashMap<String, Class<? extends Aggregator<?>>>();
    @SuppressWarnings("unchecked")
    private HashMap<String, ArrayList<AggregateValue>> aggregateValues = new HashMap<String, ArrayList<AggregateValue>>();
    @SuppressWarnings("unchecked")
    private HashMap<String, AggregateValue> aggregateResults = new HashMap<String, AggregateValue>();
    
    private List<String> WMNames = new ArrayList<String>();

    public JobInProgress(BSPJobID jobId, Path jobFile, BSPController controller,
            Configuration conf) throws IOException {
        this.jobId = jobId;
        this.localFs = FileSystem.getLocal(conf);
        this.jobFile = jobFile;
        this.controller = controller;
        this.status = new JobStatus(jobId, null, 0L, 0L, JobStatus.State.PREP
                .value());
        this.startTime = System.currentTimeMillis();
        this.superStepCounter = 0;
        this.maxAttemptRecoveryCounter = conf.getInt(
                Constants.BC_BSP_JOB_RECOVERY_ATTEMPT_MAX, 0);
        this.maxStaffAttemptRecoveryCounter = conf.getInt(
                Constants.BC_BSP_STAFF_RECOVERY_ATTEMPT_MAX, 0);

        this.localJobFile = controller
                .getLocalPath(Constants.BC_BSP_LOCAL_SUBDIR_CONTROLLER + "/"
                        + jobId + ".xml");
        this.localJarFile = controller
                .getLocalPath(Constants.BC_BSP_LOCAL_SUBDIR_CONTROLLER + "/"
                        + jobId + ".jar");

        Path jobDir = controller.getSystemDirectoryForJob(jobId);
        FileSystem fs = jobDir.getFileSystem(conf);
        fs.copyToLocalFile(jobFile, localJobFile);
        job = new BSPJob(jobId, localJobFile.toString());
        this.conf = job.getConf();
        this.numBSPStaffs = job.getNumBspStaff();
        this.profile = new JobProfile(job.getUser(), jobId, jobFile.toString(),
                job.getJobName());

        status.setUsername(job.getUser());
        status.setStartTime(startTime);

        String jarFile = job.getJar();
        if (jarFile != null) {
            fs.copyToLocalFile(new Path(jarFile), localJarFile);
        }
        
        this.priority = job.getPriority();

        setCheckPointFrequency();
        
        // For aggregation.
        /** Add the user program jar to the system's classpath. */
        ClassLoaderUtil.addClassPath(localJarFile.toString());
        loadAggregators();

        this.gssc = new GeneralSSController(jobId);
    }
    
    public JobInProgress(BSPJob job, BSPJobID jobId, BSPController controller, int staffNum, HashMap<Integer, String[]> locations) {
        this.jobId = jobId;
        this.controller = controller;
        this.superStepCounter = 0;
        this.numBSPStaffs = staffNum;
        
        staffs = new StaffInProgress[locations.size()];
        
        for (int i = 0; i < this.numBSPStaffs; i++) {
            RawSplit split = new RawSplit();
            split.setLocations(locations.get(i));
            split.setClassName("yes");
            staffs[i] = new StaffInProgress(this.jobId, null, this.controller, null, this, i, split);
        }
        
        this.job = job;
        loadAggregators();
    }
    
    @SuppressWarnings("unchecked")
    private void loadAggregators() {
        // load aggregators and aggregate values.
        int aggregateNum = this.job.getAggregateNum();
        String[] aggregateNames = this.job.getAggregateNames();
        for (int i = 0; i < aggregateNum; i ++) {
            String name = aggregateNames[i];
            this.nameToAggregator.put(name, this.job.getAggregatorClass(name));
            this.nameToAggregateValue.put(name, job.getAggregateValueClass(name));
            this.aggregateValues.put(name, new ArrayList<AggregateValue>());
        }
    }

    public BSPController getController() {
        return controller;
    }
    
    public BSPJob getJob() {
        return job;
    }

    public HashMap<StaffAttemptID, Map<String, Integer>> getStaffToWMTimes() {
        return staffToWMTimes;
    }
    
    public JobProfile getProfile() {
        return profile;
    }

    public JobStatus getStatus() {
        return status;
    }

    public synchronized long getLaunchTime() {
        return launchTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public String getPriority() {
        return priority;
    }

    public int getNumBspStaff() {
        return numBSPStaffs;
    }

    public StaffInProgress[] getStaffInProgress() {
        return staffs;
    }

    public long getFinishTime() {
        return finishTime;
    }
    
    public int getFaultSSStep() {
        return faultSSStep;
    }

    public void setFaultSSStep(int faultSSStep) {
        this.faultSSStep = faultSSStep;
    }

    public GeneralSSControllerInterface getGssc() {
        return gssc;
    }

    /**
     * If one task of this job is failed on this worker, then record the number of failing to execute the job on the worker.
     * If the failed number is more than a threshold, then this worker is gray for the job. That means return <code>true</code>,
     * else return <code>false</code>.
     * @param wms
     * @return
     */
    public boolean addFailedWorker(WorkerManagerStatus wms) {
        int counter = 1;
        if (this.failedRecord.containsKey(wms)) {
            counter = this.failedRecord.get(wms);
            counter++;
        }
        this.failedRecord.put(wms, counter);
        
        if (this.failedRecord.get(wms) > 2) {
            this.blackList.add(wms);
            LOG.warn("Warn: " + wms.getWorkerManagerName() + " is added into the BlackList of job "
                    + this.jobId.toString() + " because the failed attempts is up to threshold:" + 2);
            return true;
        } else {
            return false;
        }
    }
    
    public void addBlackListWorker(WorkerManagerStatus wms) {
        this.blackList.add(wms);
    }
    
    /**
     * @return the number of desired tasks.
     */
    public int desiredBSPStaffs() {
        return numBSPStaffs;
    }

    /**
     * @return The JobID of this JobInProgress.
     */
    public BSPJobID getJobID() {
        return jobId;
    }

    public synchronized StaffInProgress findStaffInProgress(StaffID id) {
        if (areStaffsInited()) {
            for (StaffInProgress sip : staffs) {
                if (sip.getStaffId().equals(id)) {
                    return sip;
                }
            }
        }
        return null;
    }

    public synchronized boolean areStaffsInited() {
        return this.staffsInited;
    }

    public String toString() {
        return "jobName:" + profile.getJobName() + "\n" + "submit user:"
                + profile.getUser() + "\n" + "JobId:" + jobId + "\n"
                + "JobFile:" + jobFile + "\n";
    }

    // ///////////////////////////////////////////////////
    // Create/manage tasks
    // ///////////////////////////////////////////////////

    public void initStaffs() throws IOException {
        if (staffsInited) {
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("numBSPStaffs: " + numBSPStaffs);
        }

        // read the input split info from HDFS
        Path sysDir = new Path(this.controller.getSystemDir());
        FileSystem fs = sysDir.getFileSystem(conf);
        DataInputStream splitFile = fs.open(new Path(conf
                .get(Constants.USER_BC_BSP_JOB_SPLIT_FILE)));
        RawSplit[] splits;
        try {
            splits = BSPJobClient.readSplitFile(splitFile);
        } finally {
            splitFile.close();
        }

        // adjust number of map staffs to actual number of splits

        this.staffs = new StaffInProgress[numBSPStaffs];
        for (int i = 0; i < numBSPStaffs; i++) {
            if (i < splits.length) {
                // this staff will load data from DFS
                staffs[i] = new StaffInProgress(getJobID(), this.jobFile
                        .toString(), this.controller, this.conf, this, i, splits[i]); 
            } else {
                // create a disable split. this only happen in Hash.
                RawSplit split = new RawSplit();
                split.setClassName("no");
                split.setDataLength(0);
                split.setBytes("no".getBytes(), 0, 2);
                split.setLocations(new String[] { "no" });
                // this staff will not load data from DFS
                staffs[i] = new StaffInProgress(getJobID(), this.jobFile
                        .toString(), this.controller, this.conf, this, i, split);
            }
          
        }
        // Update job status
        this.status.setRunState(JobStatus.RUNNING);
        staffsInited = true;
        LOG.debug("Job is initialized.");
    }

    public Staff obtainNewStaff(WorkerManagerStatus[] gss, int i,
            double staffsLoadFactor) {

        Staff result = null;
        try {
            if (!staffs[i].getRawSplit().getClassName().equals("no")) {
                // this staff need to load data according to the split info
                String[] locations = staffs[i].getRawSplit().getLocations();
                int tmp_count = 0;
                int currentStaffs = 0;
                int maxStaffs = 0;
                int loadStaffs = 0;
                String tmp_location = locations[0];
                WorkerManagerStatus gss_tmp;
                for (String location : locations) {
                    gss_tmp = findWorkerManagerStatus(gss, location);
                    if (gss_tmp == null) {
                        continue;
                    }
                    currentStaffs = gss_tmp.getRunningStaffsCount();
                    maxStaffs = gss_tmp.getMaxStaffsCount();
                    loadStaffs = Math.min(maxStaffs, ( int ) Math
                            .ceil(staffsLoadFactor * maxStaffs));
                    if ((loadStaffs - currentStaffs) > tmp_count) {
                        tmp_count = loadStaffs - currentStaffs;
                        tmp_location = location;
                    }
                }
                if (tmp_count > 0) {
                    WorkerManagerStatus status = findWorkerManagerStatus(gss,
                            tmp_location);
                    result = staffs[i].getStaffToRun(status);
                    updateStaffToWMTimes(i, tmp_location);
                } else {
                    result = staffs[i]
                            .getStaffToRun(findMaxFreeWorkerManagerStatus(gss,
                                    staffsLoadFactor));
                    updateStaffToWMTimes(i, tmp_location);
                }
            } else {
                result = staffs[i].getStaffToRun(findMaxFreeWorkerManagerStatus(
                        gss, staffsLoadFactor));
            }
        } catch (IOException ioe) {
            LOG.error("Exception has been catched in JobInProgress--obtainNewStaff !", ioe);
            Fault f = new Fault(Fault.Type.DISK, Fault.Level.WARNING, this.getJobID(), ioe.toString());                            
            this.getController().recordFault(f);
            this.getController().recovery(this.getJobID());
            try {
                this.getController().killJob(this.getJobID());
            } catch (IOException e) {
                LOG.error("Kill Exception", e);
            }
        }

        String name = staffs[i].getWorkerManagerStatus().getWorkerManagerName();
        LOG.info("obtainNewWorker--[Init]" + name);
        if (workersToStaffs.containsKey(name)) {
            workersToStaffs.get(name).add(result.getStaffAttemptId());
            LOG.info("The workerName has already existed and add the staff directly");
        } else {
            ArrayList<StaffAttemptID> list = new ArrayList<StaffAttemptID>();
            list.add(result.getStaffAttemptId());
            attemptIDList.add(result.getStaffAttemptId());            
            workersToStaffs.put(name, list);
            LOG.info("Add the workerName " + name + " and the size of all workers is " + this.workersToStaffs.size());
        }
        
        return result;
    }
    
    public void obtainNewStaff(WorkerManagerStatus[] gss, int i, double tasksLoadFactor, boolean recovery) {
        staffs[i].setChangeWorkerState(true);
        LOG.info("obtainNewStaff" + "  " + recovery);
        try {
            staffs[i].getStaffToRun(findMaxFreeWorkerManagerStatus(gss, 1.0), true);
        } catch (IOException ioe) {
            LOG.error("Exception has been catched in JobInProgress--obtainNewStaff !", ioe);
            Fault f = new Fault(Fault.Type.DISK, Fault.Level.WARNING, this.getJobID(), ioe.toString());                            
            this.getController().recordFault(f);
            this.getController().recovery(this.getJobID());
            try {
                this.getController().killJob(job.getJobID());
            } catch (IOException e) {
                LOG.error("IOException", e);
            }
        }
        String name = staffs[i].getWorkerManagerStatus().getWorkerManagerName();
        LOG.info("obtainNewWorker--[recovery]" + name);
        if(workersToStaffs.containsKey(name)){
            workersToStaffs.get(name).add(staffs[i].getS().getStaffAttemptId());
            LOG.info("The workerName has already existed and add the staff directly");
        }else{
            ArrayList<StaffAttemptID> list = new ArrayList<StaffAttemptID>();
            list.add(staffs[i].getS().getStaffAttemptId());
            workersToStaffs.put(name, list);
            LOG.info("Add the workerName " + name + " and the size of all workers is " + this.workersToStaffs.size());
        }
     }

     private void updateStaffToWMTimes(int i, String WorkerManagerName) { 
         if(staffToWMTimes.containsKey(staffs[i].getStaffID())) {
               Map<String, Integer> workerManagerToTimes = staffToWMTimes.get(staffs[i].getStaffID());
               
               int runTimes = 0;
       
               if(workerManagerToTimes.containsKey(WorkerManagerName)) {
                   runTimes = workerManagerToTimes.get(WorkerManagerName) + 1;
                   workerManagerToTimes.remove(WorkerManagerName);
                   workerManagerToTimes.put(WorkerManagerName, runTimes);
       
               } else {
                   workerManagerToTimes.put(WorkerManagerName, 1);
               }
               staffToWMTimes.remove(staffs[i].getStaffID());
               staffToWMTimes.put(staffs[i].getStaffID(), workerManagerToTimes);
         } else {
                
                 Map<String, Integer> workerManagerToTimes = new LinkedHashMap<String, Integer>();
                 workerManagerToTimes.put(WorkerManagerName, 1);
                 staffToWMTimes.put(staffs[i].getStaffID(), workerManagerToTimes);
             }
         LOG.info("updateStaffToWMTimes---staffId: " + staffs[i].getStaffID() + " StaffWMTimes: " + staffToWMTimes);
     }

    /** Find the WorkerManagerStatus according to the WorkerManager name */
    public WorkerManagerStatus findWorkerManagerStatus(
            WorkerManagerStatus[] wss, String name) {
        for (WorkerManagerStatus e : wss) {
            if (this.blackList.contains(e)) {
                continue;
            }
            if (e.getWorkerManagerName().indexOf(name) != -1)
                return e;
        }
        return null;
    }

    public WorkerManagerStatus findMaxFreeWorkerManagerStatus(
            WorkerManagerStatus[] wss, double staffsLoadFactor) {
        int currentStaffs = 0;
        int maxStaffs = 0;
        int loadStaffs = 0;
        int tmp_count = 0;
        WorkerManagerStatus status = null;
        for (WorkerManagerStatus wss_tmp : wss) {
            if (this.blackList.contains(wss_tmp)) {
                continue;
            }
            currentStaffs = wss_tmp.getRunningStaffsCount();
            maxStaffs = wss_tmp.getMaxStaffsCount();
            loadStaffs = Math.min(maxStaffs, ( int ) Math.ceil(staffsLoadFactor
                    * maxStaffs));
            if ((loadStaffs - currentStaffs) > tmp_count) {
                tmp_count = loadStaffs - currentStaffs;
                status = wss_tmp;
            }
        }
        return status;
    }

    public synchronized void updateStaffStatus(StaffInProgress sip,
            StaffStatus staffStatus) {
        sip.updateStatus(staffStatus); // update sip

        if (superStepCounter < staffStatus.getSuperstepCount()) {
            superStepCounter = ( int ) staffStatus.getSuperstepCount();
        }
    }
    
    public void setAttemptRecoveryCounter() {
        this.attemptRecoveryCounter ++;
        this.setFaultSSStep(superStepCounter);
    }
    
    public int getNumAttemptRecovery() {
        return this.attemptRecoveryCounter;
    }
    
    public int getMaxAttemptRecoveryCounter() {
        return maxAttemptRecoveryCounter;
    }
    
    public int getMaxStaffAttemptRecoveryCounter() {
        return maxStaffAttemptRecoveryCounter;
    }
    
    public void setPriority(String priority) {
        this.priority = priority;
    }
    public void setCheckPointFrequency() {
        int defaultF = conf.getInt(
                Constants.DEFAULT_BC_BSP_JOB_CHECKPOINT_FREQUENCY, 0);
        if (defaultF == 0) {
            this.checkPointFrequency = defaultF;
        } else {
            this.checkPointFrequency = conf.getInt(
                    Constants.USER_BC_BSP_JOB_CHECKPOINT_FREQUENCY, defaultF);
        }
    }
    
    public void setCheckPointFrequency(int cpf){
        this.checkPointFrequency = cpf;
        LOG.info("The current [CheckPointFrequency] is:" + this.checkPointFrequency);
    }
    
    public void setCheckPointNext() {
        this.checkPointNext = true;
        LOG.info("The next superstep [" + (this.superStepCounter) + " or " + (this.superStepCounter + 1) + "] will execute checkpoint operation");
    }
    
    public int getCheckPointFrequency() {
        return this.checkPointFrequency;
    }

    public boolean isCheckPoint() {
        if (this.checkPointFrequency == 0 || this.superStepCounter == 0) {
            return false;
        }

        if (this.checkPointNext) {
            return true;
        }
        
        if ((this.superStepCounter % this.checkPointFrequency) == 0) {
            return true;
        } else {
            return false;
        }
    } 
    
    public boolean isRecovery() {
        return this.status.getRunState() == JobStatus.RECOVERY;
    }

    @Override
    public void setAbleCheckPoint(int ableCheckPoint) {
        this.ableCheckPoint = ableCheckPoint;
        LOG.info("The ableCheckPoint is "
                + this.ableCheckPoint);
    }

    // Note: Client get the progress by this.status
    @Override
    public void setSuperStepCounter(int superStepCounter) {
        this.superStepCounter = superStepCounter;
        this.status.setprogress(this.superStepCounter + 1);
    }

    @Override
    public int getSuperStepCounter() {
        return this.superStepCounter;
    }

    @SuppressWarnings("unchecked")
    public String[] generalAggregate(SuperStepReportContainer[] ssrcs) {
        
        String[] results;
        
        // To get the aggregation values from the ssrcs.
        for(int i = 0; i < ssrcs.length; i ++) {
             String[] aggValues = ssrcs[i].getAggValues();
            for(int j = 0; j < aggValues.length; j ++) {
                String[] aggValueRecord = aggValues[j].split(Constants.KV_SPLIT_FLAG);
                String aggName = aggValueRecord[0];
                String aggValueString = aggValueRecord[1];
                AggregateValue aggValue = null;
                try {
                    aggValue = this.nameToAggregateValue.get(aggName).newInstance();
                    aggValue.initValue(aggValueString); // init the aggValue from its string form.
                } catch (InstantiationException e1) {
                    LOG.error("InstantiationException", e1);
                } catch (IllegalAccessException e2) {
                    LOG.error("IllegalAccessException", e2);
                }//end-try
                if (aggValue != null) {
                    ArrayList<AggregateValue> list = this.aggregateValues.get(aggName);
                    list.add(aggValue); // put the value to the values' list for aggregation ahead.
                }//end-if
            }//end-for
        }//end-for
        
        // To aggregate the values from the aggregateValues.
        this.aggregateResults.clear();// Clear the results' container before a new calculation.
        // To calculate the aggregations.
        for (Entry<String, Class<? extends Aggregator<?>>> entry : this.nameToAggregator.entrySet()) {
            
            Aggregator<AggregateValue> aggregator = null;
            
            try {
                aggregator = ( Aggregator<AggregateValue> ) entry.getValue().newInstance();
            } catch (InstantiationException e1) {
                LOG.error("InstantiationException", e1);
            } catch (IllegalAccessException e2) {
                LOG.error("IllegalAccessException", e2);
            }
            
            if (aggregator != null) {
                ArrayList<AggregateValue> aggVals = this.aggregateValues.get(entry.getKey());
                AggregateValue resultValue = aggregator.aggregate(aggVals);
                this.aggregateResults.put(entry.getKey(), resultValue);
                aggVals.clear();// Clear the initial aggregate values after aggregation completes.
            }
        }//end-for
        
        /** 
         * To encapsulate the aggregation values to the String[] results.
         * 
         * The aggValues should be in form as follows:
         * [ AggregateName \t AggregateValue.toString() ]
         */
        int aggSize = this.aggregateResults.size();
        results = new String[aggSize];
        int i_a = 0;
        for (Entry<String, AggregateValue> entry : this.aggregateResults.entrySet()) {
            results[i_a] = entry.getKey() + Constants.KV_SPLIT_FLAG + entry.getValue().toString();
            i_a ++;
        }

        return results;
    }

    @Override
    public SuperStepCommand generateCommand(SuperStepReportContainer[] ssrcs) {
        SuperStepCommand ssc = new SuperStepCommand();
        
        // Note: we must firstly judge whether the fault has happened.
        LOG.info("[generateCommand]---this.status.getRunState()" + this.status.getRunState());
        LOG.info("[generateCommand]---this.status.isRecovery()" + this.status.isRecovery());
        if (isRecovery()) {
            HashMap<Integer, String> partitionToWorkerManagerNameAndPort = convert();
            LOG.info("if (isRecovery())--partitionToWorkerManagerName :" + partitionToWorkerManagerNameAndPort);
            
            ssc.setCommandType(Constants.COMMAND_TYPE.START_AND_RECOVERY);
            ssc.setInitReadPath(conf
                    .get(Constants.BC_BSP_CHECKPOINT_WRITEPATH)
                    + "/"
                    + this.jobId.toString()
                    + "/"
                    + this.ableCheckPoint);
            LOG.info("ableCheckPoint: " + ableCheckPoint);
            ssc.setAbleCheckPoint(this.ableCheckPoint);
            ssc.setNextSuperStepNum(this.ableCheckPoint + 1);
            // If the COMMAND_TYPE is START_AND_RECOVERY, then the ssc.setPartitionToWorkerManagerName must be invoked.
            ssc.setPartitionToWorkerManagerNameAndPort(partitionToWorkerManagerNameAndPort);
            LOG.info("end--ssc.setPartitionToWorkerManagerName(partitionToWorkerManagerName);");
            
            this.status.setRunState(JobStatus.RUNNING);
            
            return ssc;
        }
        
        String[] aggValues = generalAggregate(ssrcs); // To aggregate from the ssrcs.
        ssc.setAggValues(aggValues); // To put the aggregation result values into the ssc.
        long counter = 0;
        for (int i = 0; i < ssrcs.length; i++) {
            if (ssrcs[i].getJudgeFlag() > 0) {
                counter += ssrcs[i].getJudgeFlag();
            }
        }
        if (counter > 0) {
            StringBuffer sb = new StringBuffer("[Active]" + counter);
            for (int i = 0; i < aggValues.length; i++) {
                sb.append("  ||  [AGG" + (i + 1) + "]" + aggValues[i]); 
            }
            LOG.info("STATISTICS DATA : " + sb.toString());
            
            if (isCheckPoint()) {
                this.checkPointNext = false;
                ssc.setOldCheckPoint(this.ableCheckPoint);
                LOG.info("jip--ableCheckPoint: " + this.ableCheckPoint);
                ssc.setCommandType(Constants.COMMAND_TYPE.START_AND_CHECKPOINT);
                ssc.setInitWritePath(conf
                        .get(Constants.BC_BSP_CHECKPOINT_WRITEPATH)
                        + "/"
                        + this.jobId.toString()
                        + "/"
                        + this.superStepCounter);
                ssc.setAbleCheckPoint(this.superStepCounter);
                ssc.setNextSuperStepNum(this.superStepCounter + 1);
            } else {
                ssc.setCommandType(Constants.COMMAND_TYPE.START);
                ssc.setNextSuperStepNum(this.superStepCounter + 1);
            }
        } else {
            ssc.setCommandType(Constants.COMMAND_TYPE.STOP);
            ssc.setNextSuperStepNum(this.superStepCounter);
        }
        return ssc;
    }
    // convert for SScommand
    private HashMap<Integer, String> convert() {
        StaffInProgress[] staffs = this.getStaffInProgress();
        
        HashMap<String, ArrayList<StaffAttemptID>> workersToStaffs = this.getWorkersToStaffs();
        HashMap<Integer, String> partitionToWorkerManagerNameAndPort = new HashMap<Integer, String>();
        ArrayList<StaffAttemptID> staffAttemptIDs = null;
        StaffAttemptID staffAttemptID = null;
        
        for (String workerManagerName : workersToStaffs.keySet()) {
            staffAttemptIDs = workersToStaffs.get(workerManagerName);
            for(int i=0; i<staffAttemptIDs.size(); i++) {
                staffAttemptID =  staffAttemptIDs.get(i);
                for(int j=0; j<staffs.length; j++) 
                {
                    if(staffAttemptID.equals(staffs[j].getStaffID())) {
                        partitionToWorkerManagerNameAndPort.put(staffs[j].getS().getPartition(), workerManagerName); 
                    }
                }
            }
        }
        return partitionToWorkerManagerNameAndPort;
    }

    public HashMap<String, ArrayList<StaffAttemptID>> getWorkersToStaffs() {
        return this.workersToStaffs;
    }
    
    public boolean removeStaffFromWorker(String workerName, StaffAttemptID staffId) {
        boolean success = false;
        
        if (this.workersToStaffs.containsKey(workerName)) {
            if (this.workersToStaffs.get(workerName).contains(staffId)) {
                this.workersToStaffs.get(workerName).remove(staffId);
                if (this.workersToStaffs.get(workerName).size() == 0) {
                    this.workersToStaffs.remove(workerName);
                    LOG.info("removeStaffFromWorker " + workerName);
                }
                
                success = true;
            }
        }
        return success;
    }

    /**
     * The job is dead. We're now GC'ing it, getting rid of the job from all
     * tables. Be sure to remove all of this job's tasks from the various
     * tables.
     */
    private void garbageCollect() {
        try {
            // Cleanup the ZooKeeper.
            gssc.cleanup();
            // Cleanup the local file.
            if (localJobFile != null) {
                localFs.delete(localJobFile, true);
                localJobFile = null;
            }
            if (localJarFile != null) {
                localFs.delete(localJarFile, true);
                localJarFile = null;
            }

            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path(profile.getJobFile()).getParent(), true);

        } catch (Exception e) {
            LOG.error("[garbageCollect> Error cleaning up]" + e.getMessage());
        }
    }
    
    @Override
    public void completedJob() {
        this.status.setRunState(JobStatus.SUCCEEDED);
        this.status.setprogress(this.superStepCounter + 1);
        this.finishTime = System.currentTimeMillis();
        this.status.setFinishTime(this.finishTime);
        this.controller.removeFromJobListener(this.jobId);
        cleanCheckpoint();
        
        garbageCollect();
        LOG.info("Job successfully done.");
    }

    @Override
    public void failedJob() {
        this.status.setRunState(JobStatus.FAILED);
        this.status.setprogress(this.superStepCounter + 1);
        this.finishTime = System.currentTimeMillis();
        this.status.setFinishTime(this.finishTime);
        this.controller.removeFromJobListener(jobId);
        
        gssc.stop();
        cleanCheckpoint();
        garbageCollect();
        LOG.warn("Job failed.");
    }

    public void killJob() {
        this.status.setRunState(JobStatus.KILLED);
        this.status.setprogress(this.superStepCounter + 1);
        this.finishTime = System.currentTimeMillis();
        this.status.setFinishTime(this.finishTime);
        for (int i = 0; i < staffs.length; i++) {
            staffs[i].kill();
        }
        this.controller.removeFromJobListener(jobId);
        
        gssc.stop();
        cleanCheckpoint();
        garbageCollect();
    }
    
    public void killJobRapid() {
        this.status.setRunState(JobStatus.KILLED);
        this.status.setprogress(this.superStepCounter);
        this.finishTime = System.currentTimeMillis();
        this.status.setFinishTime(this.finishTime);
        this.controller.removeFromJobListener(jobId);
        
        gssc.stop();
        cleanCheckpoint();
    }
    
    
    private boolean cleanCheckpoint() {
        try {
            String uri = conf.get(Constants.BC_BSP_CHECKPOINT_WRITEPATH) 
                                                + "/" + job.getJobID().toString() + "/";
            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            if(fs.exists(new Path(uri))) {
                fs.delete(new Path(uri), true); 
            }
            return true; 
        } catch (IOException e) {
            LOG.error("Exception has happened and been catched!", e);
            return false;
        } 
    }
    
    @Override
    public int getCheckNum() {
        return this.workersToStaffs.size();
    }

    @Override
    public void reportLOG(String log) {
        LOG.info("GeneralSSController: " + log);
    }
    
    public StaffAttemptID[] getAttemptIDList(){
        return attemptIDList.toArray(new StaffAttemptID[attemptIDList.size()]);
    }
    
    public void getRecoveryBarrier(List<String> WMNames) {
        gssc.recoveryBarrier(WMNames);
    }
    
    /**
     * Only for fault-tolerance.
     * If the command has been write on the ZooKeeper, return true, else return false.
     * 
     * @return
     */
    public boolean isCommandBarrier() {
        return this.gssc.isCommandBarrier();
    }

    public List<String> getWMNames() {
        return WMNames;
    }

    public void addWMNames(String name) {
        WMNames.add(name);
    }
    
    public void cleanWMNames() {
        this.WMNames.clear();
    }
}
