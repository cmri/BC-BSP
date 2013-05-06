/**
 * CopyRight by Chinamobile
 * 
 * SimpleStaffScheduler.java
 */
package com.chinamobile.bcbsp.bspcontroller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.JobStatus;
import com.chinamobile.bcbsp.util.StaffStatus;
import com.chinamobile.bcbsp.workermanager.WorkerManagerStatus;

import com.chinamobile.bcbsp.action.Directive;
import com.chinamobile.bcbsp.action.LaunchStaffAction;
import com.chinamobile.bcbsp.action.WorkerManagerAction;
import com.chinamobile.bcbsp.bspcontroller.JobInProgress;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.bspstaff.StaffInProgress;
import com.chinamobile.bcbsp.fault.storage.Fault;
import com.chinamobile.bcbsp.rpc.WorkerManagerProtocol;
import com.chinamobile.bcbsp.sync.SynchronizationServerInterface;
import com.chinamobile.bcbsp.sync.SynchronizationServer;

/**
 * A simple staff scheduler.
 * 
 * to be described by Wang Zhigang.
 * 
 * @author
 * @version
 */
class SimpleStaffScheduler extends StaffScheduler {

    private static final Log LOG = LogFactory.getLog(SimpleStaffScheduler.class);

    public static final String WAIT_QUEUE = "waitQueue";
    public static final String PROCESSING_QUEUE = "processingQueue";
    public static final String FINISHED_QUEUE = "finishedQueue";
    public static final String FAILED_QUEUE = "failedQueue";
    private static final int CACHE_QUEUE_LENGTH = 20;

    private QueueManager queueManager;
    private volatile boolean initialized;
    private SynchronizationServerInterface syncServer;
    private JobListener jobListener;
    private JobProcessor jobProcessor;

    private class JobListener extends JobInProgressListener {
        @Override
        public void jobAdded(JobInProgress job) throws IOException {
            queueManager.initJob(job); // init staff

            /** lock control(JobProcessor.run()--find(WAIT_QUEUE)) */
            synchronized (WAIT_QUEUE) {
                queueManager.addJob(WAIT_QUEUE, job);
                queueManager.resortWaitQueue(WAIT_QUEUE);
                LOG.info("JobListener: " + job.getJobID()
                                + " is added to the wait_queue");
            }
        }
        
        /**
         * Review comments:
         *   (1)If clients submit jobs continuously, FAILED_QUEUE and FINISHED_QUEUE will
         *      be too large to be resident in the memory. Then the BSPController will be crashed!
         * Review time: 2011-11-30;
         * Reviewer: Hongxu Zhang.
         * 
         * Fix log:
         *   (1)If the length of FINISHED_QUEUE or FAILED_QUEUE is more than CACHE_QUEUE_LENGTH,
         *      the job in the header of QUEUE will be cleanup.
         * Fix time: 2011-12-04;
         * Programmer: Zhigang Wang.
         */
        @Override
        public ArrayList<BSPJobID> jobRemoved(JobInProgress job) throws IOException {
        	if(job.getStatus().getRunState() == JobStatus.RECOVERY) {
                queueManager.moveJob(PROCESSING_QUEUE, FAILED_QUEUE, job);
                LOG.info("JobListener" + job.getJobID()
                        + " is removed from the PROCESSING_QUEUE");
            } else {
                queueManager.moveJob(PROCESSING_QUEUE, FINISHED_QUEUE, job);
            }
        	
        	ArrayList<BSPJobID> removeJob = new ArrayList<BSPJobID>();
        	ArrayList<JobInProgress> finished = 
        	    new ArrayList<JobInProgress>(queueManager.findQueue(FINISHED_QUEUE).getJobs());
        	ArrayList<JobInProgress> failed = 
                new ArrayList<JobInProgress>(queueManager.findQueue(FAILED_QUEUE).getJobs());
        	if (finished.size() > CACHE_QUEUE_LENGTH) {
        	    removeJob.add(finished.get(0).getJobID());
        	    queueManager.removeJob(FINISHED_QUEUE, finished.get(0));
        	}
        	if (failed.size() > CACHE_QUEUE_LENGTH) {
        	    removeJob.add(failed.get(0).getJobID());
        	    queueManager.removeJob(FAILED_QUEUE, failed.get(0));
        	}
        	
        	return removeJob;
        }
    }

    /**
     * JobProcessor
     * 
     * to be described by Wang Zhigang.
     * 
     * @author
     * @version
     */
    private class JobProcessor extends Thread implements Schedulable {
        JobProcessor() {
            super("JobProcess");
        }

        /**
         * run: scheduler thread. Main logic scheduling staff to
         * WorkerManager(s). Also, it will move JobInProgress from WAIT_QUEUE to
         * PROCESSING_QUEUE
         */
        public void run() {
            if (false == initialized) {
                throw new IllegalStateException(
                        "SimpleStaffScheduler initialization"
                                + " is not yet finished!");
            }

            while (initialized) {
                Queue<JobInProgress> queue;
                // add lock to WAIT_QUEUE
                synchronized (WAIT_QUEUE) {
                    queue = queueManager.findQueue(WAIT_QUEUE);
                }

                if (queue == null) {
                    throw new NullPointerException(WAIT_QUEUE
                            + " does not exist.");
                }

                // remove a job from the WAIT_QUEUE and check the ClusterStatus
                JobInProgress jip = queue.removeJob();
                ClusterStatus cs;
                while (true) {
                    try {
                        Thread.sleep(2000);
                        cs = controller.getClusterStatus(false);
                        if (jip.getNumBspStaff() <= (cs.getMaxClusterStaffs() - cs
                                .getRunningClusterStaffs())) {
                            break;
                        }
                    } catch (Exception e) {
                        // TODO : The NullPointerException maybe happen when stop the thread.
                    }
                }// while

                //schedule the job and add it to the PROCESSING_QUEUE
                chooseScheduler(jip);
                queueManager.addJob(PROCESSING_QUEUE, jip);
            }// while
        }// run

        /**
         * schedule: Schedule job to the chosen Worker
         * 
         * @param jip
         *            JobInProgress
         */
        
        public void chooseScheduler(JobInProgress jip) {
            if(jip.getStatus().getRunState() == JobStatus.RUNNING) {
                normalSchedule(jip);
            } else if(jip.getStatus().getRunState() == JobStatus.RECOVERY) {
                recoverySchedule(jip);
            } else {
                LOG.warn("Currently master only shcedules job in running state or revovery state. "
                          + "This may be refined in the future. JobId:" + jip.getJobID());
            }
        }
        
        /**
         * Review comments:
         *   (1)The name of variables is not coherent. For examples, I think the "groomServerManager"
         *      should be "controller", and the "tasksLoadFactor" should be "staffsLoadFactor".
         * Review time: 2011-11-30;
         * Reviewer: Hongxu Zhang.
         * 
         * Fix log:
         *   (1)The conflicting name of variables has been repaired.
         * Fix time: 2011-12-04;
         * Programmer: Zhigang Wang.
         */
        public void normalSchedule(JobInProgress job) {
            List<JobInProgress> jip_wait, jip_process;
            
            int remainingStaffsLoad = job.getNumBspStaff();
            synchronized (WAIT_QUEUE) {
                jip_wait = new ArrayList<JobInProgress>(queueManager.findQueue(WAIT_QUEUE).getJobs());
                for (JobInProgress jip : jip_wait) {
                    remainingStaffsLoad += jip.getNumBspStaff();
                }
            }
            
            int runningStaffLoad = 0;
            synchronized (PROCESSING_QUEUE) {
                jip_process = new ArrayList<JobInProgress>(queueManager.findQueue(PROCESSING_QUEUE).getJobs());
                for (JobInProgress jip : jip_process) {
                    runningStaffLoad += jip.getNumBspStaff();
                }
            }
            
            ClusterStatus clusterStatus = controller.getClusterStatus(false);
            double staffsLoadFactor = (( double ) (remainingStaffsLoad + runningStaffLoad))
                    / clusterStatus.getMaxClusterStaffs();

            // begin scheduling all staff(s) for the chosen job
            StaffInProgress[] staffs = job.getStaffInProgress();
            for (int i = 0; i < staffs.length; i++) {
                if (!staffs[i].isRunning() && !staffs[i].isComplete()) {
                    Collection<WorkerManagerStatus> glist = controller.workerServerStatusKeySet();
                    WorkerManagerStatus[] gss = ( WorkerManagerStatus[] ) glist
                            .toArray(new WorkerManagerStatus[glist.size()]);

                    // choose a reasonable worker for the chosen staff of the
                    Staff t = job.obtainNewStaff(gss, i, staffsLoadFactor);
                    if (job.getStatus().getRunState() == JobStatus.RUNNING) {
                        WorkerManagerProtocol worker = controller
                                .findWorkerManager(staffs[i]
                                        .getWorkerManagerStatus());
                        boolean success = false;
                        try {
                            // dispatch the staff to the worker
                            Directive d = new Directive(
                                    controller
                                            .getActiveWorkerManagersName(),
                                    new WorkerManagerAction[] { new LaunchStaffAction(
                                            t) });
                            success = worker.dispatch(t.getJobID(), d, false, false, job.getNumAttemptRecovery());
                                                        
                            job.updateStaffStatus(staffs[i], new StaffStatus(job.getJobID(), staffs[i].getStaffID(), 0,
                                    StaffStatus.State.UNASSIGNED, "running", "groomServer",
                                    StaffStatus.Phase.STARTING));
                            // update the WorkerManagerStatus Cache
                            WorkerManagerStatus new_gss = staffs[i]
                                    .getWorkerManagerStatus();
                            int currentStaffsCount = new_gss
                                    .getRunningStaffsCount();
                            new_gss
                                    .setRunningStaffsCount((currentStaffsCount + 1));
                            controller
                                    .updateWhiteWorkerManagersKey(staffs[i]
                                            .getWorkerManagerStatus(), new_gss);
                            LOG.info(t.getStaffAttemptId()
                                    + " is divided to the "
                                    + new_gss.getWorkerManagerName());
                        } catch (IOException ioe) {
                            WorkerManagerStatus wms = staffs[i].getWorkerManagerStatus();
                            LOG.error("Fail to assign staff-" + staffs[i].getStaffId() + " to "
                                    + wms.getWorkerManagerName());
                            if (!success) {
                                job.addBlackListWorker(staffs[i].getWorkerManagerStatus());
                                worker.addFailedJob(job.getJobID());
                                if (worker.getFailedJobCounter() > controller.getMaxFailedJobOnWorker()) {
                                    controller.removeWorkerFromWhite(wms);//white
                                    wms.setPauseTime(System.currentTimeMillis());
                                    controller.addWorkerToGray(wms, worker);//gray
                                    LOG.info(wms.getWorkerManagerName() 
                                            + " will be transferred from [WhiteList] to [GrayList]");
                                }
                                i--;
                            } else {
                                LOG.error("Exception has been catched in SimpleStaffScheduler--normalSchedule !", ioe);
                                Fault f = new Fault(Fault.Type.DISK, Fault.Level.WARNING, job.getJobID(), ioe.toString());                            
                                job.getController().recordFault(f);
                                job.getController().recovery(job.getJobID());
                                try {
                                    job.getController().killJob(job.getJobID());
                                } catch (IOException e) {
                                    LOG.error("Kill Job", e);
                                }
                            }
                        }
                    } else {
                        LOG.warn("Currently master only shcedules job in running state. "
                                        + "This may be refined in the future. JobId:"
                                        + job.getJobID());
                    }// if-else
                }// if
            }// for
            job.getGssc().setCheckNumBase();
            job.getGssc().start();
        }// schedule
        
        public void recoverySchedule(JobInProgress job) {
                    
            int remainingStaffsLoad = job.getNumBspStaff();     
            List<JobInProgress> jip_list;
            
            //add lock to WAIT_QUEUE
            synchronized(WAIT_QUEUE){
                jip_list = new ArrayList<JobInProgress>(queueManager.findQueue(WAIT_QUEUE).getJobs());
            }
            
            //calculate the load-factor
            for(JobInProgress jip : jip_list) {
                remainingStaffsLoad += jip.getNumBspStaff();
            }
            ClusterStatus clusterStatus = controller.getClusterStatus(false);
            @SuppressWarnings("unused")
            double staffsLoadFactor = ((double)remainingStaffsLoad)/clusterStatus.getMaxClusterStaffs();
            
            Collection<WorkerManagerStatus> glist = controller.workerServerStatusKeySet();
            LOG.info("recoverySchedule--glist.size(): " + glist.size());
            WorkerManagerStatus[] gss = (WorkerManagerStatus[]) glist.toArray(new WorkerManagerStatus[glist.size()]);
            LOG.info("recoverySchedule-- WorkerManagerStatus[] gss.size: " 
                    + gss.length + "gss[0]: " + gss[0].getWorkerManagerName());
                    
            StaffInProgress[] staffs = job.getStaffInProgress();
                    
            for(int i=0; i<staffs.length; i++) {
                WorkerManagerProtocol worker = null;
                boolean success = false;
                try {
                    if(staffs[i].getStaffStatus(staffs[i].getStaffID()).getRunState()
                            == StaffStatus.State.WORKER_RECOVERY) {
                        LOG.info("recoverySchedule ----WORKER_RECOVERY");
                        
                          job.obtainNewStaff(gss, i, 1.0, true);
                          worker = controller.findWorkerManager(staffs[i].getWorkerManagerStatus());
                          
                          Directive d = new Directive(controller.getActiveWorkerManagersName(),
                              new WorkerManagerAction[] { new LaunchStaffAction(staffs[i].getS()) });
                          d.setFaultSSStep(job.getFaultSSStep());
                          
                          if (staffs[i].getChangeWorkerState() == true) {
                             success = worker.dispatch(staffs[i].getS().getJobID(), d, 
                                     true, true, job.getNumAttemptRecovery());
                          } else {
                             success = worker.dispatch(staffs[i].getS().getJobID(), d, 
                                     true, false, job.getNumAttemptRecovery());
                          }
                                                           
                          //update the WorkerManagerStatus Cache
                          WorkerManagerStatus new_gss = staffs[i].getWorkerManagerStatus();
                          int currentStaffsCount = new_gss.getRunningStaffsCount();
                          new_gss.setRunningStaffsCount((currentStaffsCount+1));
                          controller.updateWhiteWorkerManagersKey(staffs[i].getWorkerManagerStatus(), new_gss);
                          LOG.info(staffs[i].getS().getStaffAttemptId() 
                                  + " is divided to the " + new_gss.getWorkerManagerName());
                                                              
                    } else if(staffs[i].getStaffStatus(staffs[i].getStaffID()).getRunState() 
                            == StaffStatus.State.STAFF_RECOVERY) {
                        
                        LOG.info("recoverySchedule ----STAFF_RECOVERY");
                        Map<String, Integer> workerManagerToTimes = job.getStaffToWMTimes().get(staffs[i].getStaffID());
                        @SuppressWarnings("unused")
                        String lastWMName = getTheLastWMName(workerManagerToTimes);
                        
                        job.obtainNewStaff(gss, i, 1.0, true);
                        worker = controller.findWorkerManager(staffs[i].getWorkerManagerStatus());
                        
                        Directive d = new Directive(controller.getActiveWorkerManagersName(),
                            new WorkerManagerAction[] { new LaunchStaffAction(staffs[i].getS()) });
                        d.setFaultSSStep(job.getFaultSSStep());
                        
                        if (staffs[i].getChangeWorkerState() == true) {
                           success = worker.dispatch(staffs[i].getS().getJobID(), d, 
                                   true, true, job.getNumAttemptRecovery());
                        } else {
                           success = worker.dispatch(staffs[i].getS().getJobID(), d, 
                                   true, false, job.getNumAttemptRecovery());
                        }
                                                         
                        //update the WorkerManagerStatus Cache
                        WorkerManagerStatus new_gss = staffs[i].getWorkerManagerStatus();
                        int currentStaffsCount = new_gss.getRunningStaffsCount();
                        new_gss.setRunningStaffsCount((currentStaffsCount+1));
                        controller.updateWhiteWorkerManagersKey(staffs[i].getWorkerManagerStatus(), new_gss);
                        LOG.info(staffs[i].getS().getStaffAttemptId() 
                                + " is divided to the " + new_gss.getWorkerManagerName());
                    } 
                } catch (Exception e) {
                    WorkerManagerStatus wms = staffs[i].getWorkerManagerStatus();
                    LOG.error("Fail to assign staff-" + staffs[i].getStaffId() + " to " + wms.getWorkerManagerName());
                    if (!success) {
                        job.addBlackListWorker(staffs[i].getWorkerManagerStatus());
                        worker.addFailedJob(job.getJobID());
                        if (worker.getFailedJobCounter() > controller.getMaxFailedJobOnWorker()) {
                            controller.removeWorkerFromWhite(wms);//white
                            wms.setPauseTime(System.currentTimeMillis());
                            controller.addWorkerToGray(wms, worker);//gray
                            LOG.info(wms.getWorkerManagerName() 
                                    + " will be transferred from [WhiteList] to [GrayList]");
                        }
                        i--;
                    } else {
                        LOG.error("Exception has been catched in SimpleStaffScheduler--recoverySchedule !", e);
                        Fault f = new Fault(Fault.Type.DISK, Fault.Level.WARNING, job.getJobID(), e.toString());                            
                        job.getController().recordFault(f);
                        job.getController().recovery(job.getJobID());
                        try {
                            job.getController().killJob(job.getJobID());
                        } catch (IOException oe) {
                            LOG.error("Kill Job", oe);
                        }
                    }
                }
                
            }
            
            job.getStatus().setRecovery(true);
            job.getRecoveryBarrier(job.getWMNames());
            LOG.warn("leave---jip.getRecoveryBarrier(ts, WMNames.size());");
            
            while (true) {
                try {
                    if (job.isCommandBarrier()) {
                        LOG.info("[recoverySchedule] quit");
                        break;
                    }
                    
                    Thread.sleep(1000);
                } catch (Exception e) {
                    Fault f = new Fault(Fault.Type.SYSTEMSERVICE, Fault.Level.INDETERMINATE,
                            job.getJobID(), e.toString());           
                    job.getController().recordFault(f);
                    job.getController().recovery(job.getJobID());
                    try {
                        job.getController().killJob(job.getJobID());
                    } catch (IOException ioe) {
                        LOG.error("[Kill Job Exception]", ioe);
                    }
                }
            }
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
            LOG.info("last---getTheLastWMName(Map<String, Integer> map:)" + " " + lastLWName);
            return lastLWName;
        }
    }// JobProcessor
    
    public SimpleStaffScheduler() {
        this.syncServer = new SynchronizationServer();
        this.jobListener = new JobListener();
        this.jobProcessor = new JobProcessor();

    }

    /**
     * start: create queues for job and the root directory on the ZooKeeper
     * cluster, and then start the simple scheduler thread
     */
    @Override
    public void start() {
        this.queueManager = new QueueManager(getConf());
        this.queueManager.createFCFSQueue(WAIT_QUEUE);
        this.queueManager.createFCFSQueue(PROCESSING_QUEUE);
        this.queueManager.createFCFSQueue(FINISHED_QUEUE);
        this.queueManager.createFCFSQueue(FAILED_QUEUE);
        this.controller.addJobInProgressListener(this.jobListener);
        this.initialized = true;

        // start the Synchronization Server and Scheduler Server.
        this.syncServer.startServer();
        this.jobProcessor.start();
    }

    /**
     * terminate: cleanup when close the cluster. Include: remove the
     * jobLinstener, delete the root directory on ZooKeeper, and stop the
     * scheduler thread
     */
    @SuppressWarnings("deprecation")
    @Override
    public void stop() {
        this.initialized = false;
        
        this.jobProcessor.stop();
        boolean isSuccess = this.syncServer.stopServer();
        if (isSuccess) {
            LOG.info("Success to cleanup the nodes on ZooKeeper");
        } else {
            LOG.error("Fail to cleanup the nodes on ZooKeeper");
        }
        
        if (this.jobListener != null) {
            this.controller.removeJobInProgressListener(this.jobListener);
        }
    }

    @Override
    public Collection<JobInProgress> getJobs(String queue) {
        return (queueManager.findQueue(queue)).getJobs();
    }
}
