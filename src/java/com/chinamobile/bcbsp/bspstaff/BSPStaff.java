/**
 * CopyRight by Chinamobile
 * 
 * BSPStaff.java
 */
package com.chinamobile.bcbsp.bspstaff;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.util.ReflectionUtils;

import com.chinamobile.bcbsp.ActiveMQBroker;
import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.AggregateValue;
import com.chinamobile.bcbsp.api.Aggregator;
import com.chinamobile.bcbsp.api.BSP;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.api.Partitioner;
import com.chinamobile.bcbsp.api.RecordParse;
import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.comm.BSPMessage;
import com.chinamobile.bcbsp.comm.Communicator;
import com.chinamobile.bcbsp.comm.CommunicatorInterface;
import com.chinamobile.bcbsp.fault.storage.Checkpoint;
import com.chinamobile.bcbsp.fault.storage.Fault;
import com.chinamobile.bcbsp.graph.GraphDataFactory;
import com.chinamobile.bcbsp.graph.GraphDataInterface;
import com.chinamobile.bcbsp.io.InputFormat;
import com.chinamobile.bcbsp.io.OutputFormat;
import com.chinamobile.bcbsp.io.RecordReader;
import com.chinamobile.bcbsp.io.RecordWriter;
import com.chinamobile.bcbsp.partition.HashPartitioner;
import com.chinamobile.bcbsp.partition.HashWithBalancerWritePartition;
import com.chinamobile.bcbsp.partition.HashWritePartition;
import com.chinamobile.bcbsp.partition.NotDivideWritePartition;
import com.chinamobile.bcbsp.partition.RecordParseDefault;
import com.chinamobile.bcbsp.partition.WritePartition;
import com.chinamobile.bcbsp.sync.StaffSSController;
import com.chinamobile.bcbsp.sync.StaffSSControllerInterface;
import com.chinamobile.bcbsp.sync.SuperStepCommand;
import com.chinamobile.bcbsp.sync.SuperStepReportContainer;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.workermanager.WorkerAgentProtocol;
import com.chinamobile.bcbsp.workermanager.WorkerManager;

/**
 * BSPStaff
 * 
 * A BSPStaff is an entity that executes the local computation of a BSPJob. A
 * BSPJob usually consists of many BSPStaffs which are distributed among the
 * workers.
 * 
 * @author
 * @version
 */
public class BSPStaff extends Staff {

    private WorkerAgentForStaffInterface staffAgent;
    private BSPJob bspJob;

    private ActiveMQBroker activeMQBroker;
    private int activeMQPort;
    private CommunicatorInterface communicator;

    // split information
    private BytesWritable rawSplit = new BytesWritable();
    private String rawSplitClass;
    private GraphDataInterface graphData;

    // <partitionID--hostName:port1-port2>
    private HashMap<Integer, String> partitionToWorkerManagerHostWithPorts = new HashMap<Integer, String>();
    private HashMap<Integer, Integer> hashBucketToPartition = null;

    // variable for barrier
    private StaffSSControllerInterface sssc;
    private int staffNum = 0;
    private int workerMangerNum = 0;
    private int localBarrierNum = 0;

    // variable for local computation
    private int maxSuperStepNum = 0;
    private int currentSuperStepCounter = 0;
    private long activeCounter = 0;
    private boolean flag = true;
    private SuperStepCommand ssc;

    // For Partition
    private Partitioner<Text> partitioner;
    private int numCopy = 100;
    private int lost = 0;

    // For Aggregation
    /** Map for user registered aggregate values. */
    private HashMap<String, Class<? extends AggregateValue<?>>> nameToAggregateValue = 
        new HashMap<String, Class<? extends AggregateValue<?>>>();
    /** Map for user registered aggregatros. */
    private HashMap<String, Class<? extends Aggregator<?>>> nameToAggregator = 
        new HashMap<String, Class<? extends Aggregator<?>>>();

    // Map to cache of the aggregate values aggregated for each vertex.
    @SuppressWarnings("unchecked")
    private HashMap<String, AggregateValue> aggregateValues = new HashMap<String, AggregateValue>();
    // Map to instance of the aggregate values for the current vertex.
    @SuppressWarnings("unchecked")
    private HashMap<String, AggregateValue> aggregateValuesCurrent = new HashMap<String, AggregateValue>();

    // Map to cache of the aggregate values calculated last super step.
    @SuppressWarnings("unchecked")
    private HashMap<String, AggregateValue> aggregateResults = new HashMap<String, AggregateValue>();

    private RecordParse recordParse = null;
    private static final Log LOG = LogFactory.getLog(BSPStaff.class);
    
    private int recoveryTimes = 0;

    public BSPStaff() {

    }

    public BSPStaff(BSPJobID jobId, String jobFile, StaffAttemptID staffId,
            int partition, String splitClass, BytesWritable split) {
        this.jobId = jobId;
        this.jobFile = jobFile;
        this.sid = staffId;
        this.partition = partition;

        this.rawSplitClass = splitClass;
        this.rawSplit = split;
    }

    public int getStaffNum() {
        return staffNum;
    }

    public int getNumCopy() {
        return numCopy;
    }

    public void setNumCopy(int numCopy) {
        this.numCopy = numCopy;
    }

    public HashMap<Integer, Integer> getHashBucketToPartition() {
        return this.hashBucketToPartition;
    }

    public void setHashBucketToPartition(
            HashMap<Integer, Integer> hashBucketToPartition) {
        this.hashBucketToPartition = hashBucketToPartition;
    }

    public GraphDataInterface getGraphData() {
        return graphData;
    }

    public void setGraphData(GraphDataInterface graph) {
        this.graphData = graph;
    }

    @Override
    public BSPStaffRunner createRunner(WorkerManager workerManager) {
        return new BSPStaffRunner(this, workerManager, this.bspJob);
    }

    /**
     * loadData: load data for the staff
     * 
     * @param job
     * @param umbilical
     * @return boolean
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws InterruptedException
     */
    @SuppressWarnings("unchecked")
    public boolean loadData(BSPJob job, WorkerAgentProtocol workerAgent,
            WorkerAgentForStaffInterface aStaffAgent)
            throws ClassNotFoundException, IOException, InterruptedException {

        // rebuild the input split
        RecordReader input = null;
        org.apache.hadoop.mapreduce.InputSplit split = null;
        if (rawSplitClass.equals("no")) {
            input = null;
        } else {

            DataInputBuffer splitBuffer = new DataInputBuffer();
            splitBuffer.reset(rawSplit.getBytes(), 0, rawSplit.getLength());
            SerializationFactory factory = new SerializationFactory(
                    job.getConf());
            Deserializer<? extends org.apache.hadoop.mapreduce.InputSplit> deserializer = 
                ( Deserializer<? extends org.apache.hadoop.mapreduce.InputSplit> ) factory
                    .getDeserializer(job.getConf()
                            .getClassByName(rawSplitClass));
            deserializer.open(splitBuffer);
            split = deserializer.deserialize(null);

            // rebuild the InputFormat class according to the user configuration
            InputFormat inputformat = ( InputFormat ) ReflectionUtils
                    .newInstance(
                            job.getConf()
                                    .getClass(
                                            Constants.USER_BC_BSP_JOB_INPUT_FORMAT_CLASS,
                                            InputFormat.class), job.getConf());
            inputformat.initialize(job.getConf());
            input = inputformat.createRecordReader(split, job);
            input.initialize(split, job.getConf());
        }
        SuperStepReportContainer ssrc = new SuperStepReportContainer();
        ssrc.setPartitionId(this.partition);
        this.numCopy = ( int ) (1 / (job.getConf().getFloat(
                Constants.USER_BC_BSP_JOB_BALANCE_FACTOR,
                ( float ) Constants.USER_BC_BSP_JOB_BALANCE_FACTOR_DEFAULT)));
        ssrc.setNumCopy(numCopy);
        ssrc.setCheckNum(this.staffNum);
        StaffSSControllerInterface sssc = new StaffSSController(this.jobId,
                this.sid, workerAgent);
        long start = System.currentTimeMillis();
        LOG.info("in BCBSP with PartitionType is: Hash" + " start time:" + start);
        if (this.staffNum == 1
                || job.getConf().getBoolean(Constants.USER_BC_BSP_JOB_ISDIVIDE,
                        false)) {

            this.partitioner = ( Partitioner<Text> ) ReflectionUtils
                    .newInstance(
                            job.getConf()
                                    .getClass(
                                            Constants.USER_BC_BSP_JOB_PARTITIONER_CLASS,
                                            HashPartitioner.class), job
                                    .getConf());
            this.partitioner.setNumPartition(this.staffNum);
            this.partitioner.intialize(job, split);

            WritePartition writePartition = new NotDivideWritePartition();

            RecordParse recordParse = ( RecordParse ) ReflectionUtils
                    .newInstance(
                            job.getConf()
                                    .getClass(
                                            Constants.USER_BC_BSP_JOB_RECORDPARSE_CLASS,
                                            RecordParseDefault.class), job
                                    .getConf());
            recordParse.init(job);

            writePartition.setRecordParse(recordParse);
            writePartition.setStaff(this);
            writePartition.write(input);

            ssrc.setDirFlag(new String[] { "1" });
            ssrc.setCheckNum(this.staffNum);
            sssc.loadDataBarrier(ssrc, Constants.PARTITION_TYPE.HASH);

            LOG.info("The number of verteices from other staff that cound not be parsed:"
                    + this.lost);
            LOG.info("in BCBSP with PartitionType is:HASH"
                    + " the number of HeadNode in this partition is:"
                    + graphData.sizeForAll());

            graphData.finishAdd();
            ssrc.setCheckNum(this.staffNum * 2);
            ssrc.setDirFlag(new String[] { "2" });
            sssc.loadDataBarrier(ssrc, Constants.PARTITION_TYPE.HASH);

        } else {
            this.partitioner = ( Partitioner<Text> ) ReflectionUtils
                    .newInstance(
                            job.getConf()
                                    .getClass(
                                            Constants.USER_BC_BSP_JOB_PARTITIONER_CLASS,
                                            HashPartitioner.class), job
                                    .getConf());

            WritePartition writePartition = ( WritePartition ) ReflectionUtils
                    .newInstance(
                            job.getConf()
                                    .getClass(
                                            Constants.USER_BC_BSP_JOB_WRITEPARTITION_CLASS,
                                            HashWritePartition.class), job
                                    .getConf());
            int multiple = 1;
            if (writePartition instanceof HashWithBalancerWritePartition) {
                this.partitioner.setNumPartition(this.staffNum * numCopy);
                multiple = 2;
            } else {
                this.partitioner.setNumPartition(this.staffNum);
                multiple = 1;
            }
            this.partitioner.intialize(job, split);
            RecordParse recordParse = ( RecordParse ) ReflectionUtils
                    .newInstance(
                            job.getConf()
                                    .getClass(
                                            Constants.USER_BC_BSP_JOB_RECORDPARSE_CLASS,
                                            RecordParseDefault.class), job
                                    .getConf());
            recordParse.init(job);
            this.recordParse = ( RecordParse ) ReflectionUtils.newInstance(
                    job.getConf().getClass(
                            Constants.USER_BC_BSP_JOB_RECORDPARSE_CLASS,
                            RecordParseDefault.class), job.getConf());
            this.recordParse.init(job);

            writePartition.setPartitioner(partitioner);
            writePartition.setRecordParse(recordParse);
            writePartition.setStaff(this);
            writePartition.setWorkerAgent(aStaffAgent);
            writePartition.setSsrc(ssrc);
            writePartition.setSssc(sssc);

            writePartition.setTotalCatchSize(job.getConf().getInt(
                    Constants.USER_BC_BSP_JOB_TOTALCACHE_SIZE,
                    Constants.USER_BC_BSP_JOB_TOTALCACHE_SIZE_DEFAULT));

            int threadNum = job.getConf().getInt(
                    Constants.USER_BC_BSP_JOB_SENDTHREADNUMBER,
                    Constants.USER_BC_BSP_JOB_SENDTHREADNUMBER_DEFAULT);
            if (threadNum > this.staffNum)
                threadNum = this.staffNum - 1;
            writePartition.setSendThreadNum(threadNum);
            writePartition.write(input);

            ssrc.setDirFlag(new String[] { "1" });
            ssrc.setCheckNum(this.staffNum * multiple);
            sssc.loadDataBarrier(ssrc, Constants.PARTITION_TYPE.HASH);

            LOG.info("The number of verteices from other staff that cound not be parsed:"
                    + this.lost);
            LOG.info("in BCBSP with PartitionType is:HASH"
                    + " the number of HeadNode in this partition is:"
                    + graphData.sizeForAll());

            graphData.finishAdd();

            ssrc.setCheckNum(this.staffNum * (multiple + 1));
            ssrc.setDirFlag(new String[] { "2" });
            sssc.loadDataBarrier(ssrc, Constants.PARTITION_TYPE.HASH);
        }

        long end = System.currentTimeMillis();
        LOG.info("in BCBSP with PartitionType is:HASH" + " end time:" + end);
        LOG.info("in BCBSP with PartitionType is:HASH" + " using time:"
                + ( float ) (end - start) / 1000 + " seconds");

        return true;
    }

    /**
     * saveResult: save the local computation result on the HDFS(SequenceFile)
     * 
     * @param job
     * @param staff
     * @return boolean
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public boolean saveResult(BSPJob job, Staff staff, WorkerAgentProtocol workerAgent) {
        try {
            OutputFormat outputformat = ( OutputFormat ) ReflectionUtils
                    .newInstance(
                            job.getConf()
                                    .getClass(
                                            Constants.USER_BC_BSP_JOB_OUTPUT_FORMAT_CLASS,
                                            OutputFormat.class), job.getConf());
            outputformat.initialize(job.getConf());
            RecordWriter output = outputformat
                    .getRecordWriter(job, this.sid);
            for (int i = 0; i < graphData.sizeForAll(); i++) {
                Vertex<?, ?, Edge> vertex = graphData.getForAll(i);
                StringBuffer outEdges = new StringBuffer();
                for (Edge edge : vertex.getAllEdges()) {
                    outEdges.append(edge.getVertexID() + Constants.SPLIT_FLAG
                            + edge.getEdgeValue() + Constants.SPACE_SPLIT_FLAG);
                }
                if (outEdges.length() > 0) {
                    int j = outEdges.length();
                    outEdges.delete(j - 1, j - 1);
                }
                output.write(new Text(vertex.getVertexID()
                        + Constants.SPLIT_FLAG + vertex.getVertexValue()),
                        new Text(outEdges.toString()));
            }
            output.close(job);
            graphData.clean();
        } catch (Exception e) {
            LOG.error("Exception has been catched in BSPStaff--saveResult !", e);
            BSPConfiguration conf = new BSPConfiguration();
            if(this.recoveryTimes < conf.getInt(Constants.BC_BSP_JOB_RECOVERY_ATTEMPT_MAX, 0)) {
                recovery(job, staff, workerAgent);
            } else {
                workerAgent.setStaffStatus(
                        this.sid, Constants.SATAFF_STATUS.FAULT,
                        new Fault(Fault.Type.DISK, Fault.Level.INDETERMINATE, workerAgent
                                        .getWorkerManagerName(job.getJobID(),
                                                this.sid), e.toString(), job
                                        .toString(), this.sid.toString()), 2);
                LOG.info("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*");
                LOG.error("Other Exception has happened and been catched, "
                        + "the exception will be reported to WorkerManager", e); 
            }
        }
        return true;
    }

    public boolean recovery(BSPJob job, Staff staff, WorkerAgentProtocol workerAgent) {
        this.recoveryTimes ++;
        boolean success = saveResult(job, staff, workerAgent);
        if(success == true) {
            return true;
        } else {
            return false;
        }
    }
    
    // Just for testing
    public void displayFirstRoute() {
        for (Entry<Integer, String> e : this.partitionToWorkerManagerNameAndPort
                .entrySet()) {
            LOG.info("partitionToWorkerManagerName : " + e.getKey() + " "
                    + e.getValue());
        }
    }

    // Just for testing
    public void displaySecondRoute() {
        for (Entry<Integer, Integer> e : this.hashBucketToPartition.entrySet()) {
            LOG.info("partitionToRange : " + e.getKey() + " " + e.getValue());
        }
    }

    public int getLocalBarrierNumber(String hostName) {
        int localBarrierNumber = 0;
        for (Entry<Integer, String> entry : this.partitionToWorkerManagerNameAndPort
                .entrySet()) {
            String workerManagerName = entry.getValue().split(":")[0];
            if (workerManagerName.equals(hostName)) {
                localBarrierNumber++;
            }
        }
        return localBarrierNumber;
    }

    private boolean deleteOldCheckpoint(int oldCheckpoint, BSPJob job) {
        LOG.info("deleteOldCheckpoint--oldCheckpoint: " + oldCheckpoint);
        try {
            Configuration conf = new Configuration();
            BSPConfiguration bspConf = new BSPConfiguration();
            String uri = bspConf.get(Constants.BC_BSP_HDFS_NAME)
                    + job.getConf().get(Constants.BC_BSP_CHECKPOINT_WRITEPATH)
                    + "/" + job.getJobID().toString() + "/" + oldCheckpoint
                    + "/";

            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            if (fs.exists(new Path(uri))) {
                fs.delete(new Path(uri), true);
            }

        } catch (IOException e) {
            LOG.error("Exception has happened and been catched!", e);
            return false;
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    /**
     * run the local computation.
     * @param job
     * @param staff
     * @param workerAgent
     * @param recovery
     * @param changeWorkerState
     * @param failCounter
     * @param hostName
     * @return 
     * Review comment:
     *      (1) The codes inside this method are too messy.
     * Review time: 2011-11-30
     * Reviewer: Hongxu Zhang.
     * 
     * Fix log:
     *      (1) To make the codes neat and well-organized, I use more empty lines and annotations
     *      to organize the codes.
     * Fix time:    2011-12-1
     * Programmer: Hu Zheng.
     */
    public void run(BSPJob job, Staff staff, WorkerAgentProtocol workerAgent,
            boolean recovery, boolean changeWorkerState, int failCounter, String hostName) {
        
        // record the number of failures of this staff
        LOG.info("BSPStaff---run()--changeWorkerState: " + changeWorkerState);
        staff.setFailCounter(failCounter);
        LOG.info("[HostName] " + hostName);        

        // initialize the relative variables
        this.bspJob = job;
        long start = 0, end = 0;
        
        int superStepCounter = 0;
        this.maxSuperStepNum = job.getNumSuperStep();
        
        this.staffNum = job.getNumBspStaff();
        SuperStepReportContainer ssrc = new SuperStepReportContainer();
        ssrc.setPartitionId(this.partition);
        sssc = new StaffSSController(this.jobId, this.sid, workerAgent);
        
        Checkpoint cp = new Checkpoint(job);
        
        if(graphDataFactory == null)
            graphDataFactory = new GraphDataFactory(job.getConf());
        try {
            if (recovery == false) {
                //if it is a recovery staff
                // schedule Staff Barrier
                ssrc.setCheckNum(this.staffNum);
                int partitionRPCPort = workerAgent.getFreePort();
                ssrc.setPort1(partitionRPCPort);
                this.activeMQPort = workerAgent.getFreePort();
                ssrc.setPort2(this.activeMQPort);
                LOG.info("[BSPStaff] Get the port for partitioning RPC is : " + partitionRPCPort + "!");
                LOG.info("[BSPStaff] Get the port for ActiveMQ Broker is : " + this.activeMQPort + "!");
                this.partitionToWorkerManagerHostWithPorts = sssc.scheduleBarrier(ssrc);
                
                //record the map from partitions to workermanagers
                for (Integer e : this.partitionToWorkerManagerHostWithPorts.keySet()) {
                    String[] nameAndPorts = this.partitionToWorkerManagerHostWithPorts.get(e).split(":");
                    String[] ports = nameAndPorts[1].split("-");
                    this.partitionToWorkerManagerNameAndPort.put(e, nameAndPorts[0] + ":" + ports[1]);
                }

                // For partition and for WorkerManager to invoke rpc method of Staff.
                this.staffAgent = new WorkerAgentForStaff(job.getConf());
                workerAgent.setStaffAgentAddress(this.sid, this.staffAgent.address());

                //initialize the number of local staffs and the number of workers of the same job
                this.localBarrierNum = getLocalBarrierNumber(hostName);
                this.workerMangerNum = workerAgent.getNumberWorkers(this.jobId, this.sid);
                displayFirstRoute();

                // load Data for the staff
                /**
                 * Review comment:
                 *      there are too many if else structure 
                 *      which may lead to a obstacle against extension and reusing 
                 * Review time: 2011-11-30
                 * Reviewer: HongXu Zhang
                 * 
                 * Fix log:
                 *      we use the factory pattern to implement the creation of a graph data object
                 * Fix time: 2011-12-2
                 * Programmer: Hu Zheng
                 */
                int version = job.getGraphDataVersion();
                this.graphData = this.graphDataFactory.createGraphData(version, this);
                
                /** Clock */
                start = System.currentTimeMillis();
                loadData(job, workerAgent, this.staffAgent);
                end = System.currentTimeMillis();
                LOG.info("[==>Clock<==] <load Data> used " + (end - start) / 1000f + " seconds");
                
            } else {
                
                LOG.info("The recoveried staff begins to read checkpoint");
                LOG.info("The fault SuperStepCounter is : " + job.getInt("staff.fault.superstep", 0));
                
                //schedule a barrier
                this.ssc = sssc.secondStageSuperStepBarrierForRecovery(job.getInt("staff.fault.superstep", 0));
                
                this.setPartitionToWorkerManagerNameAndPort(ssc.getPartitionToWorkerManagerNameAndPort());
                this.localBarrierNum = getLocalBarrierNumber(hostName);
                ArrayList<String> tmp = new ArrayList<String>();
                for (String str : this.partitionToWorkerManagerNameAndPort.values()) {
                    if (!tmp.contains(str)) {
                        tmp.add(str);
                    }
                }
                workerAgent.setNumberWorkers(this.jobId, this.sid, tmp.size());
                tmp.clear();
                this.workerMangerNum = workerAgent.getNumberWorkers(this.jobId, this.sid);
                this.currentSuperStepCounter = ssc.getAbleCheckPoint();
                
                // clean first
                int version = job.getGraphDataVersion();
                this.graphData = this.graphDataFactory.createGraphData(version, this);
                this.graphData.clean();
                this.graphData = cp.readCheckPoint(new Path(ssc.getInitReadPath()), job, staff);
                
                ssrc.setLocalBarrierNum(this.localBarrierNum);
                ssrc.setStageFlag(Constants.SUPERSTEP_STAGE.READ_CHECKPOINT_STAGE);
                ssrc.setDirFlag(new String[] { "read" });
                ssrc.setCheckNum(this.workerMangerNum * 1);
                
                // Get the new port of ActiveMQ.
                this.activeMQPort = workerAgent.getFreePort();
                ssrc.setPort2(this.activeMQPort);
                LOG.info("[BSPStaff] ReGet the port for ActiveMQ Broker is : " + this.activeMQPort + "!");
                
                this.partitionToWorkerManagerNameAndPort = sssc.checkPointStageSuperStepBarrier(
                        this.currentSuperStepCounter, ssrc);
                displayFirstRoute();
                this.currentSuperStepCounter = ssc.getNextSuperStepNum();

                this.partitioner = ( Partitioner<Text> ) ReflectionUtils
                        .newInstance(
                                job.getConf()
                                        .getClass(
                                                Constants.USER_BC_BSP_JOB_PARTITIONER_CLASS,
                                                HashPartitioner.class), job
                                        .getConf());

                WritePartition writePartition = ( WritePartition ) ReflectionUtils
                        .newInstance(
                                job.getConf()
                                        .getClass(
                                                Constants.USER_BC_BSP_JOB_WRITEPARTITION_CLASS,
                                                HashWritePartition.class), job
                                        .getConf());

                if (writePartition instanceof HashWithBalancerWritePartition) {
                    this.partitioner.setNumPartition(this.staffNum * numCopy);
                } else {
                    this.partitioner.setNumPartition(this.staffNum);
                }
                org.apache.hadoop.mapreduce.InputSplit split = null;
                if (rawSplitClass.equals("no")) {

                } else {

                    DataInputBuffer splitBuffer = new DataInputBuffer();
                    splitBuffer.reset(rawSplit.getBytes(), 0,
                            rawSplit.getLength());
                    SerializationFactory factory = new SerializationFactory(
                            job.getConf());
                    Deserializer<? extends org.apache.hadoop.mapreduce.InputSplit> deserializer = ( Deserializer<? extends org.apache.hadoop.mapreduce.InputSplit> ) factory
                            .getDeserializer(job.getConf().getClassByName(
                                    rawSplitClass));
                    deserializer.open(splitBuffer);
                    split = deserializer.deserialize(null);
                }
                this.partitioner.intialize(job, split);
                displayFirstRoute();
            }
        } catch (ClassNotFoundException cnfE) {
            LOG.error("Exception has been catched in BSPStaff--run--before local computing !", cnfE);
            workerAgent.setStaffStatus(
                    staff.getStaffAttemptId(),
                    Constants.SATAFF_STATUS.FAULT,
                    new Fault(Fault.Type.SYSTEMSERVICE, Fault.Level.CRITICAL,
                            workerAgent.getWorkerManagerName(job.getJobID(),
                                    staff.getStaffAttemptId()),
                            cnfE.toString(), job.toString(), staff
                                    .getStaffAttemptId().toString()), 0);
            return;
        } catch (IOException ioE) {
            LOG.error("Exception has been catched in BSPStaff--run--before local computing !", ioE);
            workerAgent.setStaffStatus(
                    staff.getStaffAttemptId(),
                    Constants.SATAFF_STATUS.FAULT,
                    new Fault(Fault.Type.DISK, Fault.Level.INDETERMINATE,
                            workerAgent.getWorkerManagerName(job.getJobID(),
                                    staff.getStaffAttemptId()), ioE.toString(),
                            job.toString(), staff.getStaffAttemptId()
                                    .toString()), 0);
            return;
        } catch (InterruptedException iE) {
            LOG.error("Exception has been catched in BSPStaff--run--before local computing !", iE);
            workerAgent.setStaffStatus(
                    staff.getStaffAttemptId(),
                    Constants.SATAFF_STATUS.FAULT,
                    new Fault(Fault.Type.SYSTEMSERVICE, Fault.Level.CRITICAL,
                            workerAgent.getWorkerManagerName(job.getJobID(),
                                    staff.getStaffAttemptId()), iE.toString(),
                            job.toString(), staff.getStaffAttemptId()
                                    .toString()), 0);
            return;
        }
        
        BSP bsp = ( BSP ) ReflectionUtils.newInstance(
                job.getConf().getClass(Constants.USER_BC_BSP_JOB_WORK_CLASS,
                        BSP.class), job.getConf());
        
        /** Clock */
        start = System.currentTimeMillis();
        // load aggregators and aggregate values.
        loadAggregators(job);
        end = System.currentTimeMillis();
        LOG.info("[==>Clock<==] <loadAggregators> used " + (end - start) / 1000f + " seconds");
        
        try {
            // configuration before local computation
            bsp.setup(staff);
            
            /** Clock */
            start = System.currentTimeMillis();
            
            // Start an ActiveMQ Broker, create a communicator, initialize it, and start it
            startActiveMQBroker(hostName);
            this.communicator = new Communicator(this.jobId, job,
                    this.getPartition(), partitioner);
            this.communicator.initialize(this.getHashBucketToPartition(),
                    this.getPartitionToWorkerManagerNameAndPort(),
                    this.graphData);
            this.communicator.start();
            
            end = System.currentTimeMillis();
            LOG.info("[==>Clock<==] <Initialize Communicator> used " + (end - start) / 1000f + " seconds");

            // begin local computation
            while (this.flag) {
                this.activeCounter = 0;
                
                if (recovery == false) {
                    superStepCounter = this.currentSuperStepCounter;
                } else {
                    superStepCounter = 0;
                    recovery = false;
                }

                // Begin the communicator.From this moment,
                // the parallel sending and receiving threads have begun.
                this.communicator.begin(superStepCounter);

                // Initialize before each super step.
                SuperStepContext ssContext = new SuperStepContext(job, superStepCounter);
                publishAggregateValues(ssContext);
                bsp.initBeforeSuperStep(ssContext);
                initBeforeSuperStepForAggregateValues(ssContext);

                /** Clock */
                start = System.currentTimeMillis();
                LOG.info("BSPStaff--run: superStepCounter: " + superStepCounter);
                long loadGraphTime = 0;
                long aggregateTime = 0;
                long computeTime = 0;
                long collectMsgsTime = 0;
                int tmpCounter = this.graphData.sizeForAll();
                for (int i = 0; i < tmpCounter; i++) {
                    /** Clock */
                    long tmpStart = System.currentTimeMillis();
                    Vertex vertex = graphData.getForAll(i);
                    if (vertex == null) {
                        LOG.error("Fail to get the HeadNode of index[" + i
                                + "] " + "and the system will skip the record");
                        continue;
                    }
                    loadGraphTime = loadGraphTime + (System.currentTimeMillis() - tmpStart);
                    // Get the incomed message queue for this vertex.
                    ConcurrentLinkedQueue<BSPMessage> messages = this.communicator
                            .getMessageQueue(String.valueOf(vertex
                                    .getVertexID()));
                    
                    // Aggregate the new values for each vertex. Clock the time cost.
                    tmpStart = System.currentTimeMillis();      
                    aggregate(messages, job, vertex, this.currentSuperStepCounter);
                    aggregateTime = aggregateTime + (System.currentTimeMillis() - tmpStart);
                    
                    // If the vertex is inactive and the size of messages for it is 0, skip the compute.
                    boolean activeFlag = graphData.getActiveFlagForAll(i);
                    if (!activeFlag && (messages.size() == 0))
                        continue;

                    Iterator<BSPMessage> messagesIter = messages.iterator();
                    // Call the compute function for local computation.
                    BSPStaffContext context = new BSPStaffContext(job, vertex,
                            superStepCounter);
                    /*Publish the total result aggregate values into the bsp's cache 
                     * for the user's function's accession in the next super step.*/
                    publishAggregateValues(context);
                    /** Clock */
                    tmpStart = System.currentTimeMillis();
                    bsp.compute(messagesIter, context);
                    computeTime = computeTime
                            + (System.currentTimeMillis() - tmpStart);
                    /** Clock */
                    messages.clear();

                    // Write the new vertex value to the graph.
                    this.graphData.set(i, context.getVertex(),
                            context.getActiveFLag());

                    /** Clock */
                    tmpStart = System.currentTimeMillis();
                    // Collect the messages sent by this node.
                    collectMessages(context);
                    collectMsgsTime = collectMsgsTime
                            + (System.currentTimeMillis() - tmpStart);
                    /** Clock */
                }// end-for

                LOG.info("[BSPStaff] Vertex computing is over for the super step <"
                        + this.currentSuperStepCounter + ">");
                
                end = System.currentTimeMillis();
                /** Clocks */
                LOG.info("[==>Clock<==] <Vertex computing> used "
                        + (end - start) / 1000f + " seconds");
                LOG.info("[==>Clock<==] ...(Load Graph Data Time) used "
                        + loadGraphTime / 1000f + " seconds");
                LOG.info("[==>Clock<==] ...(Aggregate Time) used "
                        + aggregateTime / 1000f + " seconds");
                LOG.info("[==>Clock<==] ...(Compute Time) used " + computeTime
                        / 1000f + " seconds");
                LOG.info("[==>Clock<==] ...(Collect Messages Time) used "
                        + collectMsgsTime / 1000f + " seconds");
                /** Clocks */

                /** Clock */
                start = System.currentTimeMillis();
                // Notify the communicator that there will be no more messages
                // for sending.
                this.communicator.noMoreMessagesForSending();
                // Wait for all of the messages have been sent over.
                while (true) {
                    if (this.communicator.isSendingOver())
                        break;
                }
                end = System.currentTimeMillis();
                LOG.info("[==>Clock<==] <Wait for sending over> used " + (end - start)/1000f + " seconds");
                /**Clock*/
                LOG.info("===========Sending Over============");
                
                /** Clock */
                start = end;
                // Barrier for sending messages over.                
                ssrc.setLocalBarrierNum(this.localBarrierNum);
                ssrc.setStageFlag(Constants.SUPERSTEP_STAGE.FIRST_STAGE);//
                ssrc.setDirFlag(new String[] { "1" });
                ssrc.setCheckNum(this.workerMangerNum);
                sssc.firstStageSuperStepBarrier(this.currentSuperStepCounter,
                        ssrc);
                end = System.currentTimeMillis();
                /** Clock */
                LOG.info("[==>Clock<==] <Sending over sync> used "
                        + (end - start) / 1000f + " seconds");
                
                /** Clock */
                start = end;
                // Notify the communicator that there will be no more messages
                // for receiving. Wait for the receiving thread died.
                this.communicator.noMoreMessagesForReceiving(); 
                while (true) {
                    if (this.communicator.isReceivingOver())
                        break;
                }
                end = System.currentTimeMillis();
                /**Clock*/
                LOG.info("[==>Clock<==] <Wait for receiving over> used " + (end - start)/1000f + " seconds");
                LOG.info("===========Receiving Over===========");
                
                /**Clock*/
                start = end;
                // Barrier for receiving messages over.
                ssrc.setLocalBarrierNum(this.localBarrierNum);
                ssrc.setStageFlag(Constants.SUPERSTEP_STAGE.FIRST_STAGE);
                ssrc.setDirFlag(new String[] { "2" });
                ssrc.setCheckNum(this.workerMangerNum * 2);
                sssc.firstStageSuperStepBarrier(this.currentSuperStepCounter,
                        ssrc);
                end = System.currentTimeMillis();
                /** Clock */
                LOG.info("[==>Clock<==] <Receiving over sync> used "
                        + (end - start) / 1000f + " seconds");
                
                this.graphData.showMemoryInfo();
                // Exchange the incoming and incomed queues.
                this.communicator.exchangeIncomeQueues();

                LOG.info("[BSPStaff] Communicator has received "
                        + this.communicator.getIncomedQueuesSize()
                        + " messages totally for the super step <"
                        + this.currentSuperStepCounter + ">");

                // decide whether to continue the next super-step or not
                if ((this.currentSuperStepCounter + 1) >= this.maxSuperStepNum) {
                    this.communicator.clearOutgoingQueues();
                    this.communicator.clearIncomedQueues();
                    this.activeCounter = 0;
                } else {
                    this.activeCounter = this.graphData.getActiveCounter();
                }
                LOG.info("[Active Vertex]" + this.activeCounter);
                
                /** Clock */
                start = System.currentTimeMillis();
                // Encapsulate the aggregate values into String[].
                String[] aggValues = encapsulateAggregateValues();
                end = System.currentTimeMillis();
                LOG.info("[==>Clock<==] <Encapsulate aggregate values> used "
                        + (end - start) / 1000f + " seconds");
                /** Clock */
                
                /** Clock */
                start = end;
                // Set the aggregate values into the super step report container.
                ssrc.setAggValues(aggValues);

                ssrc.setLocalBarrierNum(this.localBarrierNum);
                ssrc.setStageFlag(Constants.SUPERSTEP_STAGE.SECOND_STAGE);
                LOG.info("[WorkerManagerNum]" + this.workerMangerNum);
                ssrc.setCheckNum(this.workerMangerNum + 1);
                ssrc.setJudgeFlag(this.activeCounter
                        + this.communicator.getIncomedQueuesSize());
                this.ssc = sssc.secondStageSuperStepBarrier(
                        this.currentSuperStepCounter, ssrc);

                LOG.info("[==>Clock<==] <StaffSSController's rebuild session> used "
                        + StaffSSController.rebuildTime / 1000f + " seconds");

                StaffSSController.rebuildTime = 0;
                if (ssc.getCommandType() == Constants.COMMAND_TYPE.START_AND_RECOVERY) {
                    LOG.info("[Command]--[routeTableSize]"
                            + ssc.getPartitionToWorkerManagerNameAndPort()
                                    .size());
                    this.setPartitionToWorkerManagerNameAndPort(ssc
                            .getPartitionToWorkerManagerNameAndPort());
                    ArrayList<String> tmp = new ArrayList<String>();
                    for (String str : this.partitionToWorkerManagerNameAndPort
                            .values()) {
                        if (!tmp.contains(str)) {
                            tmp.add(str);
                        }
                    }
                    this.localBarrierNum = getLocalBarrierNumber(hostName);
                    workerAgent.setNumberWorkers(this.jobId, this.sid,
                            tmp.size());
                    tmp.clear();
                    this.workerMangerNum = workerAgent.getNumberWorkers(
                            this.jobId, this.sid);

                    displayFirstRoute();
                }
                end = System.currentTimeMillis();
                /** Clock */
                LOG.info("[==>Clock<==] <SuperStep sync> used " + (end - start)
                        / 1000f + " seconds");
                
                /** Clock */
                start = end;
                // Get the aggregate values from the super step command.
                // Decapsulate the aggregate values from String[].
                aggValues = this.ssc.getAggValues();                
                if (aggValues != null) {
                    decapsulateAggregateValues(aggValues);
                }
                end = System.currentTimeMillis();
                /** Clock */
                LOG.info("[==>Clock<==] <Decapsulate aggregate values> used "
                        + (end - start) / 1000f + " seconds");
                
                /** Clock */
                start = end;
                switch (ssc.getCommandType()) {
                    case Constants.COMMAND_TYPE.START:
                        LOG.info("Get the CommandTye is : START");
                        this.currentSuperStepCounter = ssc
                                .getNextSuperStepNum();
                        this.flag = true;
                        break;
                    case Constants.COMMAND_TYPE.START_AND_CHECKPOINT:
                        LOG.info("Get the CommandTye is : START_AND_CHECKPOINT");
                        boolean success = cp.writeCheckPoint(this.graphData,
                                new Path(ssc.getInitWritePath()), job, staff);
                        if (success == true) {
                            deleteOldCheckpoint(ssc.getOldCheckPoint(), job);
                        }

                        ssrc.setLocalBarrierNum(this.localBarrierNum);
                        ssrc.setStageFlag(Constants.SUPERSTEP_STAGE.WRITE_CHECKPOINT_SATGE);
                        ssrc.setDirFlag(new String[] { "write" });
                        ssrc.setCheckNum(this.workerMangerNum * 3);
                        sssc.checkPointStageSuperStepBarrier(
                                this.currentSuperStepCounter, ssrc);

                        this.currentSuperStepCounter = ssc
                                .getNextSuperStepNum();
                        this.flag = true;
                        break;
                    case Constants.COMMAND_TYPE.START_AND_RECOVERY:
                        LOG.info("Get the CommandTye is : START_AND_RECOVERY");
                        this.currentSuperStepCounter = ssc.getAbleCheckPoint();

                        // clean first
                        int version = job.getGraphDataVersion();
                        this.graphData = this.graphDataFactory.createGraphData(version, this);
                        this.graphData.clean();
                        this.graphData = cp.readCheckPoint(
                                new Path(ssc.getInitReadPath()), job, staff);

                        ssrc.setPartitionId(this.partition);
                        ssrc.setLocalBarrierNum(this.localBarrierNum);
                        ssrc.setStageFlag(Constants.SUPERSTEP_STAGE.READ_CHECKPOINT_STAGE);
                        ssrc.setDirFlag(new String[] { "read" });
                        ssrc.setCheckNum(this.workerMangerNum * 1);
                        
                        ssrc.setPort2(this.activeMQPort);
                        LOG.info("[BSPStaff] Get the port for ActiveMQ Broker is : " + this.activeMQPort + "!");
                        this.partitionToWorkerManagerNameAndPort = sssc.checkPointStageSuperStepBarrier(
                                this.currentSuperStepCounter, ssrc);
                        displayFirstRoute();
                        this.communicator.setPartitionToWorkerManagerNamePort(
                                this.partitionToWorkerManagerNameAndPort);
                        
                        this.currentSuperStepCounter = ssc.getNextSuperStepNum();
                        
                        this.communicator.clearOutgoingQueues();
                        this.communicator.clearIncomedQueues();
                        
                        recovery = true;
                        this.flag = true;
                        break;
                    case Constants.COMMAND_TYPE.STOP:
                        LOG.info("Get the CommandTye is : STOP");
                        LOG.info("Staff will save the computation result and then quit!");
                        this.currentSuperStepCounter = ssc.getNextSuperStepNum();
                        this.flag = false;
                        break;
                    default:
                        LOG.error("ERROR! "
                                + ssc.getCommandType()
                                + " is not a valid CommandType, so the staff will save the "
                                + "computation result and quit!");
                        flag = false;
                }
                // Report the status at every superstep.
                workerAgent.setStaffStatus(this.sid, Constants.SATAFF_STATUS.RUNNING, null, 1);
            } 

            this.communicator.complete();
            
        } catch (IOException ioe) {
            LOG.error("Exception has been catched in BSPStaff--run--during local computing !", ioe);
            workerAgent.setStaffStatus(
                    this.sid, Constants.SATAFF_STATUS.FAULT,
                    new Fault(Fault.Type.DISK, Fault.Level.CRITICAL,
                            workerAgent.getWorkerManagerName(job.getJobID(),
                                    this.sid), ioe.toString(), job.toString(),
                            this.sid.toString()), 1);
            LOG.info("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*");
            LOG.error("IO Exception has happened and been catched, "
                    + "the exception will be reported to WorkerManager", ioe);
            LOG.error("Staff will quit abnormally");
            return;
        } catch (Exception e) {
            LOG.error("Exception has been catched in BSPStaff--run--during local computing !", e);
            workerAgent.setStaffStatus(
                    this.sid, Constants.SATAFF_STATUS.FAULT,
                    new Fault(Fault.Type.SYSTEMSERVICE,
                            Fault.Level.INDETERMINATE, workerAgent
                                    .getWorkerManagerName(job.getJobID(),
                                            this.sid), e.toString(), job
                                    .toString(), this.sid.toString()), 1);
            LOG.info("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*");
            LOG.error("Other Exception has happened and been catched, "
                    + "the exception will be reported to WorkerManager", e);
            LOG.error("Staff will quit abnormally");
            return;
        }
        // save the computation result
        try {
            saveResult(job, staff, workerAgent);

            ssrc.setLocalBarrierNum(this.localBarrierNum);
            ssrc.setStageFlag(Constants.SUPERSTEP_STAGE.SAVE_RESULT_STAGE);
            ssrc.setDirFlag(new String[] { "1", "2", "write", "read" });
            sssc.saveResultStageSuperStepBarrier(this.currentSuperStepCounter,
                    ssrc);
            // cleanup after local computation
            bsp.cleanup(staff);
            stopActiveMQBroker();
            done(workerAgent);
            workerAgent.setStaffStatus(this.sid, Constants.SATAFF_STATUS.SUCCEED, null, 1);
            LOG.info("The max SuperStep num is " + this.maxSuperStepNum);
            LOG.info("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*");
            LOG.info("Staff is completed successfully");
        } catch (Exception e) {
            LOG.error("Exception has been catched in BSPStaff--run--after local computing !", e);
            workerAgent.setStaffStatus(
                    this.sid, Constants.SATAFF_STATUS.FAULT,
                    new Fault(Fault.Type.SYSTEMSERVICE,
                            Fault.Level.INDETERMINATE, workerAgent
                                    .getWorkerManagerName(job.getJobID(),
                                            this.sid), e.toString(), job
                                    .toString(), this.sid.toString()), 2);
            LOG.info("=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*");
            LOG.error("Other Exception has happened and been catched, "
                    + "the exception will be reported to WorkerManager", e);
        }
    }

    public BSPJob getConf() {
        return this.bspJob;
    }

    public void setConf(BSPJob bspJob) {
        this.bspJob = bspJob;
    }

    /** Write and read split info to WorkerManager */
    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, rawSplitClass);
        rawSplit.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        rawSplitClass = Text.readString(in);
        rawSplit.readFields(in);
    }

    private void collectMessages(BSPStaffContext context) {
        Iterator<BSPMessage> it = context.getMessages();
        while (it.hasNext()) {
            try {
                this.communicator.send(it.next());
            } catch (Exception e) {
                LOG.error("<collectMessages>", e);
            }
        }
        context.cleanMessagesCache();
    }

    public HashMap<Integer, String> getPartitionToWorkerManagerNameAndPort() {
        return this.partitionToWorkerManagerNameAndPort;
    }

    @SuppressWarnings("unchecked")
    private void loadAggregators(BSPJob job) {
        int aggregateNum = job.getAggregateNum();
        String[] aggregateNames = job.getAggregateNames();
        for (int i = 0; i < aggregateNum; i++) {
            String name = aggregateNames[i];
            this.nameToAggregator.put(name, job.getAggregatorClass(name));
            this.nameToAggregateValue.put(name,
                    job.getAggregateValueClass(name));
        }
        try {
            // Instanciate each aggregate values.
            for (Entry<String, Class<? extends AggregateValue<?>>> entry : this.nameToAggregateValue
                    .entrySet()) {
                String aggName = entry.getKey();
                AggregateValue aggValue;
                aggValue = entry.getValue().newInstance();
                this.aggregateValuesCurrent.put(aggName, aggValue);
            }
        } catch (InstantiationException e) {
            LOG.error("[BSPStaff:loadAggregators]", e);
        } catch (IllegalAccessException e) {
            LOG.error("[BSPStaff:loadAggregators]", e);
        }
    }
    
    @SuppressWarnings("unchecked")
    private void initBeforeSuperStepForAggregateValues(SuperStepContext ssContext) {
        for (Entry<String, AggregateValue> entry : this.aggregateValuesCurrent
                .entrySet()) {
            AggregateValue aggValue = entry.getValue();
            aggValue.initBeforeSuperStep(ssContext);
        }
    }

    @SuppressWarnings("unchecked")
    private void aggregate(ConcurrentLinkedQueue<BSPMessage> messages,
            BSPJob job, Vertex vertex, int superStepCount) {
        try {
            for (Entry<String, Class<? extends AggregateValue<?>>> entry : this.nameToAggregateValue
                    .entrySet()) {

                String aggName = entry.getKey();

                // Init the aggregate value for this head node.
                AggregateValue aggValue1 = this.aggregateValuesCurrent.get(aggName);
                AggregationContext aggContext = new AggregationContext(job,
                        vertex, superStepCount);
                publishAggregateValues(aggContext);
                aggValue1.initValue(messages.iterator(), aggContext);

                // Get the current aggregate value.
                AggregateValue aggValue0;
                aggValue0 = this.aggregateValues.get(aggName);
                

                // Get the aggregator for this kind of aggregate value.
                Aggregator<AggregateValue> aggregator;
                aggregator = ( Aggregator<AggregateValue> ) this.nameToAggregator
                        .get(aggName).newInstance();

                // Aggregate
                if (aggValue0 == null) { // the first time aggregate.
                    aggValue0 = ( AggregateValue ) aggValue1.clone();
                    this.aggregateValues.put(aggName, aggValue0);
                } else {
                    ArrayList<AggregateValue> tmpValues = new ArrayList<AggregateValue>();
                    tmpValues.add(aggValue0);
                    tmpValues.add(aggValue1);
                    AggregateValue aggValue = aggregator
                    .aggregate(tmpValues);
                    this.aggregateValues.put(aggName, aggValue);
                }
            }
        } catch (InstantiationException e) {
            LOG.error("[BSPStaff:aggregate]", e);
        } catch (IllegalAccessException e) {
            LOG.error("[BSPStaff:aggregate]", e);
        }
    }

    /**
     * To encapsulate the aggregation values to the String[].
     * 
     * The aggValues should be in form as follows: [ AggregateName \t
     * AggregateValue.toString() ]
     * 
     * @return String[]
     */
    @SuppressWarnings("unchecked")
    private String[] encapsulateAggregateValues() {

        int aggSize = this.aggregateValues.size();

        String[] aggValues = new String[aggSize];

        int i_a = 0;
        for (Entry<String, AggregateValue> entry : this.aggregateValues
                .entrySet()) {
            aggValues[i_a] = entry.getKey() + Constants.KV_SPLIT_FLAG
                    + entry.getValue().toString();
            i_a++;
        }
        // The cache for this super step should be cleared for next super step.
        this.aggregateValues.clear();

        return aggValues;
    }

    /**
     * To decapsulate the aggregation values from the String[].
     * 
     * The aggValues should be in form as follows: [ AggregateName \t
     * AggregateValue.toString() ]
     * 
     * @param aggValues
     *            String[]
     */
    @SuppressWarnings("unchecked")
    private void decapsulateAggregateValues(String[] aggValues) {

        for (int i = 0; i < aggValues.length; i++) {
            String[] aggValueRecord = aggValues[i]
                    .split(Constants.KV_SPLIT_FLAG);
            String aggName = aggValueRecord[0];
            String aggValueString = aggValueRecord[1];
            AggregateValue aggValue = null;
            try {
                aggValue = this.nameToAggregateValue.get(aggName).newInstance();
                aggValue.initValue(aggValueString); // init the aggValue from
                // its string form.
            } catch (InstantiationException e1) {
                LOG.error("ERROR", e1);
            } catch (IllegalAccessException e1) {
                LOG.error("ERROR", e1);
            }// end-try
            if (aggValue != null) {
                this.aggregateResults.put(aggName, aggValue);
            }// end-if
        }// end-for
    }

    /**
     * To publish the aggregate values into the bsp's cache for user's accession
     * for the next super step.
     * 
     * @param BSPStaffContext
     *            context
     */
    @SuppressWarnings("unchecked")
    private void publishAggregateValues(BSPStaffContext context) {
        for (Entry<String, AggregateValue> entry : this.aggregateResults
                .entrySet()) {
            context.addAggregateValues(entry.getKey(), entry.getValue());
        }
    }

    /**
     * To publish the aggregate values into the super step context for 
     * the bsp.initBeforeSuperStep for the next super step.
     * 
     * @param context
     *            SuperStepContext
     */
    @SuppressWarnings("unchecked")
    private void publishAggregateValues(SuperStepContext context) {
        for (Entry<String, AggregateValue> entry : this.aggregateResults
                .entrySet()) {
            context.addAggregateValues(entry.getKey(), entry.getValue());
        }
    }
    
    /**
     * To publish the aggregate values into the aggregation context for the 
     * aggregation value's init of each vertex.
     * 
     * @param context
     *            AggregationContext
     */
    @SuppressWarnings("unchecked")
    private void publishAggregateValues(AggregationContext context) {
        for (Entry<String, AggregateValue> entry : this.aggregateResults
                .entrySet()) {
            context.addAggregateValues(entry.getKey(), entry.getValue());
        }
    }

    /**
     * WorkerAgentForStaffInterface.java
     */
    public interface WorkerAgentForStaffInterface extends VersionedProtocol {
        public static final long versionID = 0L;

        /**
         * This method is used to worker which this worker's partition id equals
         * belongPartition.
         * 
         * @param jobId
         * @param staffId
         * @param belongPartition
         * @return
         */
        public WorkerAgentForStaffInterface getWorker(BSPJobID jobId,
                StaffAttemptID staffId, int belongPartition);

        /**
         * This method is used to put the HeadNode to WorkerAgentForJob's map.
         * 
         * @param jobId
         * @param staffId
         * @param belongPartition
         *            the partitionID which the HeadNode belongs to
         * @param hnlist
         *            HeadNode list
         */
        public void putHeadNode(BSPJobID jobId, StaffAttemptID staffId,
                int belongPartition, BytesWritable data);

        /**
         * Get the address of this WorkerAgentForStaff.
         * 
         * @return address
         */
        public String address();

        /**
         * This method will be invoked before the staff be killed, to notice the
         * staff to do some cleaning operations.
         */
        public void onKillStaff();

    }

    /**
     * WorkerAgentForStaff.java
     * 
     * @author root
     * 
     */
    public class WorkerAgentForStaff implements WorkerAgentForStaffInterface {

        // <partitionID, hostName:port1-port2>
        private HashMap<Integer, String> partitionToWorkerManagerHostWithPorts = 
            new HashMap<Integer, String>();
        private final Map<InetSocketAddress, WorkerAgentForStaffInterface> workers = 
            new ConcurrentHashMap<InetSocketAddress, WorkerAgentForStaffInterface>();

        private InetSocketAddress workAddress;
        private Server server = null;
        private Configuration conf;

        public WorkerAgentForStaff(Configuration conf) {
            this.partitionToWorkerManagerHostWithPorts = 
                BSPStaff.this.partitionToWorkerManagerHostWithPorts;
            this.conf = conf;
            String[] hostandports = 
                this.partitionToWorkerManagerHostWithPorts.get(BSPStaff.this.partition).split(":");
            LOG.info(this.partitionToWorkerManagerHostWithPorts.get(BSPStaff.this.partition));
            String[] ports = hostandports[1].split("-");
            workAddress = new InetSocketAddress(hostandports[0], Integer.parseInt(ports[0]));
            reinitialize();
        }

        private void reinitialize() {

            try {
                LOG.info("reinitialize() the WorkerAgentForStaff: "
                        + jobId.toString());

                server = RPC.getServer(this, workAddress.getHostName(),
                        workAddress.getPort(), conf);
                server.start();
                LOG.info("WorkerAgentForStaff address:"
                        + workAddress.getHostName() + " port:"
                        + workAddress.getPort());

            } catch (IOException e) {
                LOG.error("[reinitialize]", e);
            }
        }

        protected WorkerAgentForStaffInterface getWorkerAgentConnection(
                InetSocketAddress addr) {
            WorkerAgentForStaffInterface worker;
            synchronized (this.workers) {
                worker = workers.get(addr);

                if (worker == null) {
                    try {
                        worker = ( WorkerAgentForStaffInterface ) RPC.getProxy(
                                WorkerAgentForStaffInterface.class,
                                WorkerAgentForStaffInterface.versionID, addr,
                                this.conf);
                    } catch (IOException e) {
                        LOG.error("[getWorkerAgentConnection]", e);
                    }
                    this.workers.put(addr, worker);
                }
            }

            return worker;
        }

        private InetSocketAddress getAddress(String peerName) {
            String[] workerAddrParts = peerName.split(":");
            return new InetSocketAddress(workerAddrParts[0],
                    Integer.parseInt(workerAddrParts[1]));
        }

        /**
         * This method is used to get worker
         * 
         * @param jobId
         * @param staffId
         * @param belongPartition
         * @return
         */
        public WorkerAgentForStaffInterface getWorker(BSPJobID jobId,
                StaffAttemptID staffId, int belongPartition) {

            String dstworkerName = null;
            dstworkerName = this.partitionToWorkerManagerHostWithPorts
                    .get(belongPartition);// hostName:port1-port2

            String[] hostAndPorts = dstworkerName.split(":");
            String[] ports = hostAndPorts[1].split("-");
            dstworkerName = hostAndPorts[0] + ":" + ports[0];

            WorkerAgentForStaffInterface work = workers
                    .get(getAddress(dstworkerName));
            if (work == null) {
                work = getWorkerAgentConnection(getAddress(dstworkerName));
            }
            return work;
        }

        /**
         * This method is used to put the HeadNode to WorkerAgentForJob's map.
         * 
         * @param jobId
         * @param staffId
         * @param belongPartition
         *            the partitionID which the HeadNode belongs to
         * @param hnlist
         *            HeadNode list
         */
        @SuppressWarnings("unchecked")
        public void putHeadNode(BSPJobID jobId, StaffAttemptID staffId,
                int belongPartition, BytesWritable data) {
            DataInputStream in = new DataInputStream(new BufferedInputStream(
                    new ByteArrayInputStream(data.getBytes())));

            try {

                while (true) {
                    Text key = new Text();
                    key.readFields(in);
                    Text value = new Text();
                    value.readFields(in);
                    if (key.getLength() > 0 && value.getLength() > 0) {

                        if (BSPStaff.this.recordParse == null) {
                            //LOG.error("Test Null: BSPStaff.this.recordParse is NULL");
                        }
                        Vertex vertex = BSPStaff.this.recordParse.recordParse(
                                key.toString(), value.toString());
                        if (vertex == null) {
                            BSPStaff.this.lost++;
                            continue;
                        }
                        BSPStaff.this.graphData.addForAll(vertex);
                    } else {
                        break;
                    }

                }
            } catch (IOException e) {
                LOG.error("ERROR", e);
            }

        }

        @Override
        public long getProtocolVersion(String arg0, long arg1)
                throws IOException {
            return WorkerAgentForStaffInterface.versionID;
        }

        @Override
        public String address() {
            String hostName = this.workAddress.getHostName();
            int port = this.workAddress.getPort();
            return new String(hostName + ":" + port);
        }

        @Override
        public void onKillStaff() {
            BSPStaff.this.stopActiveMQBroker();
        }
    }

    private void startActiveMQBroker(String hostName) {
        // brokerName = "hostName-partitionID"
        this.activeMQBroker = new ActiveMQBroker(hostName + "-"
                + this.partition);
        try {
            this.activeMQBroker.startBroker(this.activeMQPort);
            LOG.info("[BSPStaff] starts ActiveMQ Broker successfully!");
        } catch (Exception e) {
            LOG.error("[BSPStaff] caught: ", e);
        }
    }

    private void stopActiveMQBroker() {

        if (this.activeMQBroker != null) {
            try {
                this.activeMQBroker.stopBroker();
                LOG.info("[BSPStaff] stops ActiveMQ Broker successfully!");
            } catch (Exception e) {
                LOG.error("[BSPStaff] caught: ", e);
            }
        }
    }
}