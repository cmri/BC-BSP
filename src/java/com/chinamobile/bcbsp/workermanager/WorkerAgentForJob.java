/**
 * CopyRight by Chinamobile
 * 
 * WorkerAgentForJob.java
 */
package com.chinamobile.bcbsp.workermanager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

import com.chinamobile.bcbsp.Constants;

import com.chinamobile.bcbsp.api.AggregateValue;
import com.chinamobile.bcbsp.api.Aggregator;
import com.chinamobile.bcbsp.sync.SuperStepReportContainer;
import com.chinamobile.bcbsp.sync.WorkerSSController;
import com.chinamobile.bcbsp.sync.WorkerSSControllerInterface;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.StaffAttemptID;

/**
 * WorkerAgentForJob. 
 * 
 * It is create by WorkerManager for every job that running
 * on it. This class manages all staffs which belongs to the same job,
 * maintains public information, completes the local synchronization
 * and aggregation.
 * 
 * @author
 * @version
 */
public class WorkerAgentForJob implements WorkerAgentInterface {

    private Map<StaffAttemptID, SuperStepReportContainer> runningStaffInformation = new HashMap<StaffAttemptID, SuperStepReportContainer>();
    private volatile Integer staffReportCounter = 0;
    private WorkerSSControllerInterface wssc;
    private String workerManagerName;
    private int workerManagerNum = 0;

    private HashMap<Integer, String> partitionToWorkerManagerName = new HashMap<Integer, String>();
    private final Map<InetSocketAddress, WorkerAgentInterface> workers = 
        new ConcurrentHashMap<InetSocketAddress, WorkerAgentInterface>();
   
    private int portForJob;
    private InetSocketAddress workAddress;
    private Server server = null;
    private Configuration conf;

    private static final Log LOG = LogFactory.getLog(WorkerAgentForJob.class);

    private BSPJobID jobId;
    private BSPJob jobConf;
    private WorkerManager workerManager;
    
    // For Aggregation
    /** Map for user registered aggregate values. */
    private HashMap<String, Class<? extends AggregateValue<?>>> nameToAggregateValue = 
        new HashMap<String, Class<? extends AggregateValue<?>>>();
    /** Map for user registered aggregatros. */
    private HashMap<String, Class<? extends Aggregator<?>>> nameToAggregator = 
        new HashMap<String, Class<? extends Aggregator<?>>>();
    @SuppressWarnings("unchecked")
    private HashMap<String, ArrayList<AggregateValue>> aggregateValues = 
        new HashMap<String, ArrayList<AggregateValue>>();
    @SuppressWarnings("unchecked")
    private HashMap<String, AggregateValue> aggregateResults = new HashMap<String, AggregateValue>();

    public WorkerAgentForJob(Configuration conf, BSPJobID jobId, BSPJob jobConf, WorkerManager workerManager)
            throws IOException {

        this.jobId = jobId;
        this.jobConf = jobConf;
        this.workerManager = workerManager;

        this.workerManagerName = conf.get(Constants.BC_BSP_WORKERAGENT_HOST,
                Constants.BC_BSP_WORKERAGENT_HOST);
        this.wssc = new WorkerSSController(jobId, this.workerManagerName);

        this.conf = conf;
        String bindAddress = conf.get(Constants.BC_BSP_WORKERAGENT_HOST,
                Constants.DEFAULT_BC_BSP_WORKERAGENT_HOST);
        int bindPort = conf.getInt(Constants.BC_BSP_WORKERAGENT_PORT,
                Constants.DEFAULT_BC_BSP_WORKERAGENT_PORT);
        bindPort = bindPort + Integer.parseInt(jobId.toString().substring(17));
        portForJob = bindPort;

        // network e.g. ip address, port.
        workAddress = new InetSocketAddress(bindAddress, bindPort);
        reinitialize();
        
        // For Aggregation
        loadAggregators();
    }

    public void reinitialize() {

        try {
            LOG.info("reinitialize() the WorkerAgentForJob: "
                    + jobId.toString());

            server = RPC.getServer(this, workAddress.getHostName(), workAddress.getPort(), conf);
            server.start();
            LOG.info("WorkerAgent address:" + workAddress.getHostName()
                    + " port:" + workAddress.getPort());

        } catch (IOException e) {
            LOG.error("[reinitialize]", e);
        }
    }

    public WorkerAgentForJob(WorkerSSControllerInterface wssci) {
        this.wssc = wssci;
    }
    
    /**
     * Prepare to local synchronization, including computing all kinds of
     * information.
     * 
     * @return SupterStepReportContainer
     */
    @SuppressWarnings("unchecked")
    private SuperStepReportContainer prepareLocalBarrier() {
        int stageFlag = 0;
        long judgeFlag = 0;
        String[] dirFlag = { "1" };
        String[] aggValues;
        for (Entry<StaffAttemptID, SuperStepReportContainer> e : this.runningStaffInformation
                .entrySet()) {
            SuperStepReportContainer tmp = e.getValue();
            stageFlag = tmp.getStageFlag();
            dirFlag = tmp.getDirFlag();
            judgeFlag += tmp.getJudgeFlag();
            
            // Get the aggregation values from the ssrcs.
            aggValues = tmp.getAggValues();
            decapsulateAggregateValues(aggValues);
            
        }//end-for
        
        // Compute the aggregations for all staffs in the worker.
        localAggregate();
        
        // Encapsulate the aggregation values to String[] for the ssrc.
        String[] newAggValues = encapsulateAggregateValues();
        
        SuperStepReportContainer ssrc = new SuperStepReportContainer(stageFlag,
                dirFlag, judgeFlag, newAggValues); // newAggValues into the new ssrc.
        return ssrc;

    }

    public void addStaffReportCounter() {
        this.staffReportCounter++;
    }
    
    private void clearStaffReportCounter() {
        this.staffReportCounter = 0;
    }
    
    /**
     * All staffs belongs to the same job will use this to complete the local
     * synchronization and aggregation.
     * 
     * @param staffId
     * @param superStepCounter
     * @param args
     * @return
     */
    @Override
    public boolean localBarrier(BSPJobID jobId, StaffAttemptID staffId,
            int superStepCounter, SuperStepReportContainer ssrc) {
        this.runningStaffInformation.put(staffId, ssrc);
        synchronized (this.staffReportCounter) {
            addStaffReportCounter();
            LOG.info(staffId.toString() + " [staffReportCounter]" + this.staffReportCounter);
            LOG.info(staffId.toString() + " [staffCounter]" + ssrc.getLocalBarrierNum());
            if (this.staffReportCounter == ssrc.getLocalBarrierNum()) {
                clearStaffReportCounter();
                switch (ssrc.getStageFlag()) {
                    case Constants.SUPERSTEP_STAGE.FIRST_STAGE:
                        wssc.firstStageSuperStepBarrier(superStepCounter, ssrc);
                        break;
                    case Constants.SUPERSTEP_STAGE.SECOND_STAGE:
                        wssc.secondStageSuperStepBarrier(superStepCounter,
                                prepareLocalBarrier());
                        break;
                    case Constants.SUPERSTEP_STAGE.WRITE_CHECKPOINT_SATGE:
                    case Constants.SUPERSTEP_STAGE.READ_CHECKPOINT_STAGE:
                        if (ssrc.getStageFlag() == Constants.SUPERSTEP_STAGE.READ_CHECKPOINT_STAGE) {
                            String workerNameAndPort = ssrc.getPartitionId() + ":" + this.workerManagerName + ":" + ssrc.getPort2();
                            for (Entry<StaffAttemptID, SuperStepReportContainer> e : this.runningStaffInformation.entrySet()) {
                                if (e.getKey().equals(staffId)) {
                                    continue;
                                } else {
                                    String str = e.getValue().getPartitionId() + ":" + this.workerManagerName + ":" + e.getValue().getPort2();
                                    workerNameAndPort += Constants.KV_SPLIT_FLAG + str;
                                }
                            }
                            ssrc.setActiveMQWorkerNameAndPorts(workerNameAndPort);
                            wssc.checkPointStageSuperStepBarrier(superStepCounter, ssrc); 
                        } else {
                            wssc.checkPointStageSuperStepBarrier(superStepCounter, ssrc);
                        }       
                        break;
                    case Constants.SUPERSTEP_STAGE.SAVE_RESULT_STAGE:
                        wssc.saveResultStageSuperStepBarrier(superStepCounter,
                                ssrc);
                        break;
                    default:
                        LOG.error("The SUPERSTEP of " + ssrc.getStageFlag()
                                + " is not known");
                }
                return true;
            } else {
                return false;
            }
        }
    }

    @Override
    public int getNumberWorkers(BSPJobID jobId, StaffAttemptID staffId) {
        return this.workerManagerNum;
    }

    @Override
    public void setNumberWorkers(BSPJobID jobId, StaffAttemptID staffId, int num) {
        this.workerManagerNum = num;
    }

    @Override
    public void close() throws IOException {
        this.server.stop();
    }

    @Override
    public long getProtocolVersion(String arg0, long arg1) throws IOException {
        return WorkerAgentInterface.versionID;
    }

    @Override
    public String getWorkerManagerName(BSPJobID jobId, StaffAttemptID staffId) {
        return this.workerManagerName;
    }

    /**
     * Add a new task to the job
     * 
     * @param currentStaffStatus
     */
    public void addStaffCounter(StaffAttemptID staffId) {
        SuperStepReportContainer ssrc = new SuperStepReportContainer();
        this.runningStaffInformation.put(staffId, ssrc);
    }

    /**
     * Sets the job configuration
     * 
     * @param jobConf
     */
    public void setJobConf(BSPJob jobConf) {
        
    }

    /**
     * Get WorkerAgent BSPJobID
     */
    @Override
    public BSPJobID getBSPJobID() {
        return this.jobId;
    }

    protected WorkerAgentInterface getWorkerAgentConnection(
            InetSocketAddress addr) {
        WorkerAgentInterface worker;
        synchronized (this.workers) {
            worker = workers.get(addr);

            if (worker == null) {
                try {
                    worker = ( WorkerAgentInterface ) RPC.getProxy(
                            WorkerAgentInterface.class,
                            WorkerAgentInterface.versionID, addr, this.conf);
                } catch (IOException e) {

                }
                this.workers.put(addr, worker);
            }
        }

        return worker;
    }

    /**
     * This method is used to set mapping table that shows the partition to the
     * worker.
     * 
     * @param jobId
     * @param partitionId
     * @param hostName
     */
    public void setWorkerNametoPartitions(BSPJobID jobId, int partitionId,
            String hostName) {
        this.partitionToWorkerManagerName.put(partitionId, hostName + ":"
                + this.portForJob);
    }
    
    @SuppressWarnings("unchecked")
    private void loadAggregators() {
        int aggregateNum = this.jobConf.getAggregateNum();
        String[] aggregateNames = this.jobConf.getAggregateNames();
        for (int i = 0; i < aggregateNum; i ++) {
            String name = aggregateNames[i];
            this.nameToAggregator.put(name, this.jobConf.getAggregatorClass(name));
            this.nameToAggregateValue.put(name, jobConf.getAggregateValueClass(name));
            this.aggregateValues.put(name, new ArrayList<AggregateValue>());
        }
    }
    
    /**
     * To decapsulate the aggregation values from the String[].
     * 
     * The aggValues should be in form as follows:
     * [ AggregateName \t AggregateValue.toString() ]
     * 
     * @param aggValues
     *              String[]
     */
    @SuppressWarnings("unchecked")
    private void decapsulateAggregateValues(String[] aggValues) {
        
        for (int i = 0; i < aggValues.length; i ++) {
            String[] aggValueRecord = aggValues[i].split(Constants.KV_SPLIT_FLAG);
            String aggName = aggValueRecord[0];
            String aggValueString = aggValueRecord[1];
            AggregateValue aggValue = null;
            try {
                aggValue = this.nameToAggregateValue.get(aggName).newInstance();
                aggValue.initValue(aggValueString); // init the aggValue from its string form.
            } catch (InstantiationException e1) {
                LOG.error("InstantiationException", e1);
            } catch (IllegalAccessException e1) {
                LOG.error("IllegalAccessException", e1);
            }//end-try
            if (aggValue != null) {
                ArrayList<AggregateValue> list = this.aggregateValues.get(aggName);
                list.add(aggValue); // put the value to the values' list for aggregation ahead.
            }//end-if
        }//end-for
    }
    
    /**
     * To aggregate the values from the running staffs.
     */
    @SuppressWarnings("unchecked")
    private void localAggregate() {
        // Clear the results' container before the calculation of a new super step.
        this.aggregateResults.clear();
        // To calculate the aggregations.
        for (Entry<String, Class<? extends Aggregator<?>>> entry : this.nameToAggregator.entrySet()) {
            
            Aggregator<AggregateValue> aggregator = null;
            
            try {
                aggregator = ( Aggregator<AggregateValue> ) entry.getValue().newInstance();
            } catch (InstantiationException e1) {
                LOG.error("InstantiationException", e1);
            } catch (IllegalAccessException e1) {
                LOG.error("IllegalAccessException", e1);
            }
            
            if (aggregator != null) {
                ArrayList<AggregateValue> aggVals = this.aggregateValues.get(entry.getKey());
                AggregateValue resultValue = aggregator.aggregate(aggVals);
                this.aggregateResults.put(entry.getKey(), resultValue);
                aggVals.clear();// Clear the initial aggregate values after aggregation completes.
            }
        }
    }
    
    /** 
     * To encapsulate the aggregation values to the String[].
     * 
     * The aggValues should be in form as follows:
     * [ AggregateName \t AggregateValue.toString() ]
     * 
     * @return String[]
     */
    @SuppressWarnings("unchecked")
    private String[] encapsulateAggregateValues() {
        
        int aggSize = this.aggregateResults.size();
        
        String[] aggValues = new String[aggSize];
        
        int i_a = 0;
        for (Entry<String, AggregateValue> entry : this.aggregateResults.entrySet()) {
            aggValues[i_a] = entry.getKey() + Constants.KV_SPLIT_FLAG + entry.getValue().toString();
            i_a ++;
        }
        
        return aggValues;
    }
    
    public synchronized int getFreePort() {
        return this.workerManager.getFreePort();
    }

    @Override
    public void setStaffAgentAddress(StaffAttemptID staffID, String addr) {
        this.workerManager.setStaffAgentAddress(staffID, addr);
    }
}
