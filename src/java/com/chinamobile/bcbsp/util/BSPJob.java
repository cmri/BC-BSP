/**
 * BSPJob.java
 */
package com.chinamobile.bcbsp.util;

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Enumeration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.chinamobile.bcbsp.BSPConfiguration;
import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.AggregateValue;
import com.chinamobile.bcbsp.api.Aggregator;
import com.chinamobile.bcbsp.api.BSP;
import com.chinamobile.bcbsp.api.Combiner;
import com.chinamobile.bcbsp.api.Edge;
import com.chinamobile.bcbsp.api.Partitioner;
import com.chinamobile.bcbsp.api.RecordParse;
import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.client.BSPJobClient;
import com.chinamobile.bcbsp.client.RunningJob;
import com.chinamobile.bcbsp.fault.storage.Fault;
import com.chinamobile.bcbsp.io.InputFormat;
import com.chinamobile.bcbsp.io.OutputFormat;
import com.chinamobile.bcbsp.partition.WritePartition;


/**
 * BSPJob
 * 
 * BSPJob A BSP job configuration. BSPJob is the primary interface for a user to
 * describe a BSP job to the BC-BSP framework for execution.
 * 
 * @author
 * @version
 */
public class BSPJob extends BSPJobContext {
    private static final Log LOG = LogFactory.getLog(BSPJob.class);
    public static enum JobState {
        DEFINE, RUNNING
    };

    private JobState state = JobState.DEFINE;
    private BSPJobClient jobClient;
    private RunningJob info;

    /* == For Aggregators and AggregateValues registrition == */
    private int aggregateNum = 0;
    private ArrayList<String> aggregateNames = new ArrayList<String>();

    /**
     * Register an Aggregate map with an Aggregator and a Value.
     * 
     * @param aggregateName
     * @param aggregatorClass
     * @param aggregateValueClass
     */
    public void registerAggregator(String aggregateName,
            Class<? extends Aggregator<?>> aggregatorClass,
            Class<? extends AggregateValue<?>> aggregateValueClass) {

        conf.setClass(aggregateName + ".aggregator", aggregatorClass,
                Aggregator.class);
        conf.setClass(aggregateName + ".aggregateValue", aggregateValueClass,
                AggregateValue.class);

        this.aggregateNames.add(aggregateName);
        this.aggregateNum++;
    }

    public void completeAggregatorRegister() {

        conf.setInt(Constants.USER_BC_BSP_JOB_AGGREGATE_NUM, this.aggregateNum);

        int size = this.aggregateNames.size();
        String[] aggNames = new String[size];
        for (int i = 0; i < size; i++) {
            aggNames[i] = this.aggregateNames.get(i);
        }
        conf.setStrings(Constants.USER_BC_BSP_JOB_AGGREGATE_NAMES, aggNames);
    }

    public int getAggregateNum() {
        return conf.getInt(Constants.USER_BC_BSP_JOB_AGGREGATE_NUM, 0);
    }

    public String[] getAggregateNames() {
        return conf.getStrings(Constants.USER_BC_BSP_JOB_AGGREGATE_NAMES);
    }

    @SuppressWarnings("unchecked")
    public Class<? extends Aggregator<?>> getAggregatorClass(
            String aggregateName) {
        return ( Class<? extends Aggregator<?>> ) conf.getClass(aggregateName
                + ".aggregator", Aggregator.class);
    }

    @SuppressWarnings("unchecked")
    public Class<? extends AggregateValue<?>> getAggregateValueClass(
            String aggregateName) {
        return ( Class<? extends AggregateValue<?>> ) conf.getClass(
                aggregateName + ".aggregateValue", AggregateValue.class);
    }

    /* == For Aggregators and AggregateValues registrition == */

    public BSPJob() throws IOException {
        this(new BSPConfiguration());
    }

    public BSPJob(BSPConfiguration conf) throws IOException {
        super(conf, null);
        jobClient = new BSPJobClient(conf);
    }

    public BSPJob(BSPConfiguration conf, String jobName) throws IOException {
        this(conf);
        setJobName(jobName);
    }

    public BSPJob(BSPJobID jobID, String jobFile) throws IOException {
        super(new Path(jobFile), jobID);
    }

    public BSPJob(BSPConfiguration conf, Class<?> exampleClass)
            throws IOException {
        this(conf);
        setJarByClass(exampleClass);
    }

    public BSPJob(BSPConfiguration conf, int numStaff) {
        super(conf, null);
        this.setNumBspStaff(numStaff);
    }

    private void ensureState(JobState state) throws IllegalStateException {
        if (state != this.state) {
            throw new IllegalStateException("Job in state " + this.state
                    + " instead of " + state);
        }
    }

    // /////////////////////////////////////
    // BC-BSP Job Configuration
    // /////////////////////////////////////

    public void setJobName(String name) throws IllegalStateException {
        ensureState(JobState.DEFINE);
        conf.set(Constants.USER_BC_BSP_JOB_NAME, name);
    }

    public String getJobName() {
        return conf.get(Constants.USER_BC_BSP_JOB_NAME, "");
    }

    public void setUser(String user) {
        conf.set(Constants.USER_BC_BSP_JOB_USER_NAME, user);
    }

    public String getUser() {
        return conf.get(Constants.USER_BC_BSP_JOB_USER_NAME);
    }

    public void setWorkingDirectory(Path dir) throws IOException {
        ensureState(JobState.DEFINE);
        dir = new Path(getWorkingDirectory(), dir);
        conf.set(Constants.USER_BC_BSP_JOB_WORKING_DIR, dir.toString());
    }

    public Path getWorkingDirectory() throws IOException {
        String name = conf.get(Constants.USER_BC_BSP_JOB_WORKING_DIR);

        if (name != null) {
            return new Path(name);
        } else {
            try {
                Path dir = FileSystem.get(conf).getWorkingDirectory();
                conf.set(Constants.USER_BC_BSP_JOB_WORKING_DIR, dir.toString());
                return dir;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Set the {@link Partitioner} for the job.
     * 
     * @param partitioner
     *            the <code>Partitioner</code> to use
     * @throws IllegalStateException
     *             if the job is submitted
     */
    @SuppressWarnings("unchecked")
    public void setPartitionerClass(Class<? extends Partitioner> partitioner)
            throws IllegalStateException {
        conf.setClass(Constants.USER_BC_BSP_JOB_PARTITIONER_CLASS, partitioner,
                Partitioner.class);
    }

    /**
     * Set the {@link WritePartition} for the job.
     * 
     * @param writePartition
     *            the <code>Partitioner</code> to use
     * @throws IllegalStateException
     *             if the job is submitted
     */
    public void setWritePartition(Class<? extends WritePartition> writePartition)
            throws IllegalStateException {
        conf.setClass(Constants.USER_BC_BSP_JOB_WRITEPARTITION_CLASS,
                writePartition, WritePartition.class);
    }

    /**
     * This method is used to indicate whether the data is divided.IsDivide is
     * true not divided.
     * 
     * @param isDivide
     */
    public void setIsDivide(boolean isDivide) {
        conf.setBoolean(Constants.USER_BC_BSP_JOB_ISDIVIDE, isDivide);
    }

    /**
     * This method sets the number of thread that those use to send graph data
     * to other worker.
     * 
     * @param number
     */
    public void setSendThreadNumber(int number) {
        conf.setInt(Constants.USER_BC_BSP_JOB_SENDTHREADNUMBER, number);
    }

    /**
     * This method sets cache size. In MB. Sending data when catch is full.
     * 
     * @param number
     *            cache size is number MB
     */
    public void setTotalCacheSize(int size) {
        conf.setInt(Constants.USER_BC_BSP_JOB_TOTALCACHE_SIZE, size);
    }

    /**
     * This method is used to set the balance factor. The balance factor is used
     * to determine the number of hash bucket‘s copy. If parameter "factor" is
     * 0.01 means that each hash bucket has 100 copies. That is, 1/0.01.
     * 
     * @param factor
     */
    public void setBalanceFactor(float factor) {
        conf.setFloat(Constants.USER_BC_BSP_JOB_BALANCE_FACTOR, factor);
    }

    /**
     * Set the {@link RecordParse} for the job.
     * 
     * @param recordParse
     *            the <code>RecordParse</code> to use
     * @throws IllegalStateException
     *             if the job is submitted
     */
    public void setRecordParse(Class<? extends RecordParse> recordParse)
            throws IllegalStateException {
        conf.setClass(Constants.USER_BC_BSP_JOB_RECORDPARSE_CLASS, recordParse,
                RecordParse.class);
    }

    /**
     * Set the {@link BC-BSP InputFormat} for the job.
     * 
     * @param cls
     *            the <code>InputFormat</code> to use
     * @throws IllegalStateException
     */
    @SuppressWarnings("unchecked")
    public void setInputFormatClass(Class<? extends InputFormat> cls)
            throws IllegalStateException {
        conf.setClass(Constants.USER_BC_BSP_JOB_INPUT_FORMAT_CLASS, cls,
                InputFormat.class);
    }

    /**
     * Get the {@link BC-BSP InputFormat} class for the job.
     * 
     * @return the {@link BC-BSP InputFormat} class for the job.
     */
    @SuppressWarnings("unchecked")
    public Class<? extends InputFormat<?, ?>> getInputFormatClass()
            throws ClassNotFoundException {
        return ( Class<? extends InputFormat<?, ?>> ) conf
                .getClass(Constants.USER_BC_BSP_JOB_INPUT_FORMAT_CLASS,
                        InputFormat.class);
    }
    
    /**
     * Set the split size. The unit is MB;
     * 
     * @param splitSize
     */
    public void setSplitSize(int splitSize) {
        conf.setLong(Constants.USER_BC_BSP_JOB_SPLIT_SIZE, (splitSize * 1024 * 1024));
    }
    
    /**
     * Get the split size.
     * The unit is byte.
     * If user does not set it, return 0, than means the system will generate split
     * according to the actual block size and the staff slots.
     * 
     * @return
     */
    public long getSplitSize() {
        return conf.getLong(Constants.USER_BC_BSP_JOB_SPLIT_SIZE, 0);
    }

    /**
     * Set the {@link BC-BSP OutputFormat} for the job.
     * 
     * @param cls
     *            the <code>OutputFormat</code> to use
     * @throws IllegalStateException
     */
    @SuppressWarnings("unchecked")
    public void setOutputFormatClass(Class<? extends OutputFormat> cls)
            throws IllegalStateException {
        conf.setClass(Constants.USER_BC_BSP_JOB_OUTPUT_FORMAT_CLASS, cls,
                OutputFormat.class);
    }

    /**
     * Set the BC-BSP algorithm class for the job.
     * 
     * @param cls
     * @throws IllegalStateException
     */
    public void setBspClass(Class<? extends BSP> cls)
            throws IllegalStateException {
        ensureState(JobState.DEFINE);
        conf.setClass(Constants.USER_BC_BSP_JOB_WORK_CLASS, cls, BSP.class);
    }

    /**
     * Get the BC-BSP algorithm class for the job.
     * 
     * @return
     */
    @SuppressWarnings("unchecked")
    public Class<? extends BSP> getBspClass() {
        return ( Class<? extends BSP> ) conf.getClass(
                Constants.USER_BC_BSP_JOB_WORK_CLASS, BSP.class);
    }

    public void setJar(String jar) {
        conf.set(Constants.USER_BC_BSP_JOB_JAR, jar);
    }

    public void setJarByClass(Class<?> cls) {
        String jar = findContainingJar(cls);
        if (jar != null) {
            conf.set(Constants.USER_BC_BSP_JOB_JAR, jar);
        }
    }

    public String getJar() {
        return conf.get(Constants.USER_BC_BSP_JOB_JAR);
    }

    public void setNumBspStaff(int staffNum) {
        conf.setInt(Constants.USER_BC_BSP_JOB_STAFF_NUM, staffNum);
    }

    public int getNumBspStaff() {
        return conf.getInt(Constants.USER_BC_BSP_JOB_STAFF_NUM, 1);
    }

    public void setPriority(String PRIORITY) {
        conf.set(Constants.USER_BC_BSP_JOB_PRIORITY, PRIORITY);
    }

    public String getPriority() {
        return conf.get(Constants.USER_BC_BSP_JOB_PRIORITY,
                Constants.PRIORITY.NORMAL);
    }

    public void setPartitionType(String PartitionType) {
        conf.set(Constants.USER_BC_BSP_JOB_PARTITION_TYPE, PartitionType);
    }

    public String getPartitionType() {
        return conf.get(Constants.USER_BC_BSP_JOB_PARTITION_TYPE,
                Constants.PARTITION_TYPE.HASH);
    }

    public void setNumPartition(int partitions) {
        conf.setInt(Constants.USER_BC_BSP_JOB_PARTITION_NUM, partitions);
    }

    public int getNumPartition() {
        return conf.getInt(Constants.USER_BC_BSP_JOB_PARTITION_NUM, 0);
    }

    public void setNumSuperStep(int superStepNum) {
        conf.setInt(Constants.USER_BC_BSP_JOB_SUPERSTEP_MAX, superStepNum);
    }

    public int getNumSuperStep() {
        return conf.getInt(Constants.USER_BC_BSP_JOB_SUPERSTEP_MAX, 1);
    }

    private static String findContainingJar(Class<?> my_class) {
        ClassLoader loader = my_class.getClassLoader();
        String class_file = my_class.getName().replaceAll("\\.", "/")
                + ".class";
        try {
            for (Enumeration<URL> itr = loader.getResources(class_file); itr
                    .hasMoreElements();) {
                URL url = itr.nextElement();
                if ("jar".equals(url.getProtocol())) {
                    String toReturn = url.getPath();
                    if (toReturn.startsWith("file:")) {
                        toReturn = toReturn.substring("file:".length());
                    }
                    toReturn = URLDecoder.decode(toReturn, "UTF-8");
                    return toReturn.replaceAll("!.*$", "");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    // /////////////////////////////////////
    // Methods for Job Control
    // /////////////////////////////////////

    public long progress() throws IOException {
        ensureState(JobState.RUNNING);
        return info.progress();
    }

    public boolean isComplete() throws IOException {
        ensureState(JobState.RUNNING);
        return info.isComplete();
    }

    public boolean isSuccessful() throws IOException {
        ensureState(JobState.RUNNING);
        return info.isSuccessful();
    }

    public void killJob() throws IOException {
        ensureState(JobState.RUNNING);
        info.killJob();
    }

    public void killStaff(StaffAttemptID taskId) throws IOException {
        ensureState(JobState.RUNNING);
        info.killStaff(taskId, false);
    }

    public void failStaff(StaffAttemptID taskId) throws IOException {
        ensureState(JobState.RUNNING);
        info.killStaff(taskId, true);
    }

    public void submit() throws IOException, ClassNotFoundException,
            InterruptedException {
        ensureState(JobState.DEFINE);
        info = jobClient.submitJobInternal(this);
        state = JobState.RUNNING;
    }

    public boolean waitForCompletion(boolean verbose) {       
        try{
            completeAggregatorRegister(); // To set the params for aggregators.
    
            if (state == JobState.DEFINE) {
                submit();
            }
            if (verbose) {
                jobClient.monitorAndPrintJob(this, info);
            } else {
                info.waitForCompletion();
            }
            return isSuccessful();
            
        } catch(ClassNotFoundException lnfE) {
            LOG.error("Exception has been catched in BSPJob--waitForCompletion !", lnfE);
            Fault f = new Fault(Fault.Type.DISK, Fault.Level.WARNING, "null", lnfE.toString());                            
            jobClient.getJobSubmitClient().recordFault(f);
            jobClient.getJobSubmitClient().recovery(new BSPJobID("before have an BSPJobId !", -1));
            
            return false;
        } catch(InterruptedException iE) {
            LOG.error("Exception has been catched in BSPJob--waitForCompletion !", iE);
            Fault f = new Fault(Fault.Type.SYSTEMSERVICE, Fault.Level.INDETERMINATE, "null", iE.toString());                            
            jobClient.getJobSubmitClient().recordFault(f);
            jobClient.getJobSubmitClient().recovery(new BSPJobID("before have an BSPJobId !", -1));
            
            return false;
        } catch(IOException ioE) {
            LOG.error("Exception has been catched in BSPJob--waitForCompletion !", ioE);
            Fault f = new Fault(Fault.Type.SYSTEMSERVICE, Fault.Level.INDETERMINATE, "null", ioE.toString());                            
            jobClient.getJobSubmitClient().recordFault(f);
            jobClient.getJobSubmitClient().recovery(new BSPJobID("before have an BSPJobId !", -1));
            
            return false;
        }
    }

    // /////////////////////////////////////
    // Combiner and Communication
    // /////////////////////////////////////
    /**
     * Get the user defined Combiner class.
     * 
     * @return
     */
    @SuppressWarnings("unchecked")
    public Class<? extends Combiner> getCombiner() {
        return ( Class<? extends Combiner> ) conf.getClass(
                Constants.USER_BC_BSP_JOB_COMBINER_CLASS, Combiner.class);
    }

    /**
     * Check the combiner set flag.
     * 
     * @return true if user has set combiner implementation, or flase otherwise.
     */
    public boolean isCombinerSetFlag() {
        return conf.getBoolean(Constants.USER_BC_BSP_JOB_COMBINER_DEFINE_FLAG,
                false);
    }
    
    /**
     * Check the combiner set flag specially for receiving end.
     * 
     * @return true if user not set the receive combiner set flag to be false,
     *  or false otherwise.
     */
    public boolean isReceiveCombinerSetFlag() {
        return (conf.getBoolean(Constants.USER_BC_BSP_JOB_COMBINER_RECEIVE_FLAG, false) 
                && conf.getBoolean(Constants.USER_BC_BSP_JOB_COMBINER_DEFINE_FLAG, false));
    }

    /**
     * Set the user defined Combiner class.
     * 
     * @param combinerClass
     */
    public void setCombiner(Class<? extends Combiner> combinerClass) {
        conf.setClass(Constants.USER_BC_BSP_JOB_COMBINER_CLASS, combinerClass,
                Combiner.class);
        conf.setBoolean(Constants.USER_BC_BSP_JOB_COMBINER_DEFINE_FLAG, true);
        conf.setBoolean(Constants.USER_BC_BSP_JOB_COMBINER_RECEIVE_FLAG, true);
    }
    
    /**
     * Set the receive combiner set flag for the receiving end.
     * 
     * @param flag
     */
    public void setReceiveCombinerSetFlag(boolean flag) {
        conf.setBoolean(Constants.USER_BC_BSP_JOB_COMBINER_RECEIVE_FLAG, flag);
    }

    /**
     * Set the threshold for sending messages. A queue for one WorkerManager
     * will not be sent until its size overs the threshold. This threshold still
     * works without combiner set.
     * 
     * @param threshold
     */
    public void setSendThreshold(int threshold) {
        conf.setInt(Constants.USER_BC_BSP_JOB_SEND_THRESHOLD, threshold);
    }

    /**
     * Get the threshold for sending messages. A queue for one WorkerManager
     * will not be sent until its size overs the threshold. This threshold still
     * works without combiner set.
     * 
     * @return send threshold
     */
    public int getSendThreshold() {
        return conf.getInt(Constants.USER_BC_BSP_JOB_SEND_THRESHOLD, 0);
    }

    /**
     * Set the threshold for combining messages on sending side. A queue for one
     * WorkerManager will not be combined until its size overs the threshold.
     * This threshold only works with combiner set.
     * 
     * @param threshold
     */
    public void setSendCombineThreshold(int threshold) {
        conf.setInt(Constants.USER_BC_BSP_JOB_SEND_COMBINE_THRESHOLD, threshold);
    }

    /**
     * Get the threshold for combining messages on sending side. A queue for one
     * WorkerManager will not be combined until its size overs the threshold.
     * This threshold only works with combiner set.
     * 
     * @return send combine threshold
     */
    public int getSendCombineThreshold() {
        return conf.getInt(Constants.USER_BC_BSP_JOB_SEND_COMBINE_THRESHOLD, 0);
    }

    /**
     * Set the threshold for combining messages on receiving side. A queue for
     * one HeadNode will not be combined until its size overs the threshold.
     * This threshold only works with combiner set.
     * 
     * @param threshold
     */
    public void setReceiveCombineThreshold(int threshold) {
        conf.setInt(Constants.USER_BC_BSP_JOB_RECEIVE_COMBINE_THRESHOLD,
                threshold);
    }

    /**
     * Get the threshold for combining messages on receiving side. A queue for
     * one HeadNode will not be combined until its size overs the threshold.
     * This threshold only works with combiner set.
     * 
     * @return receive combine threshold
     */
    public int getReceiveCombineThreshold() {
        return conf.getInt(Constants.USER_BC_BSP_JOB_RECEIVE_COMBINE_THRESHOLD, 0);
    }
    
    public void setMessagePackSize(int size) {
        conf.setInt(Constants.USER_BC_BSP_JOB_MESSAGE_PACK_SIZE, size);
    }
    
    public int getMessagePackSize() {
        return conf.getInt(Constants.USER_BC_BSP_JOB_MESSAGE_PACK_SIZE, 0);
    }
    
    public void setMaxProducerNum(int num) {
        conf.setInt(Constants.USER_BC_BSP_JOB_MAX_PRODUCER_NUM, num);
    }
    
    public int getMaxProducerNum() {
        return conf.getInt(Constants.USER_BC_BSP_JOB_MAX_PRODUCER_NUM, 0);
    }
    
    public void setMaxConsumerNum(int num) {
        conf.setInt(Constants.USER_BC_BSP_JOB_MAX_CONSUMER_NUM, num);
    }
    
    public int getMaxConsumerNum() {
        return conf.getInt(Constants.USER_BC_BSP_JOB_MAX_CONSUMER_NUM, 1);
    }

    // /////////////////////////////////////
    // Memory and disk administration
    // /////////////////////////////////////

    public final int MEMORY_VERSION = 0;
    public final int DISK_VERSION = 1;
    public final int BDB_VERSION = 2;
    

    
    /**
     * Set the data percent in memory of the graph and messages data.
     * 
     * @param dataPercent
     */
    public void setMemoryDataPercent(float dataPercent) {
        conf.setFloat(Constants.USER_BC_BSP_JOB_MEMORY_DATA_PERCENT, dataPercent);
    }
    
    /**
     * Get the data percent in memory of the graph and messages data.
     * 
     * @return
     */
    public float getMemoryDataPercent() {
        return conf.getFloat(Constants.USER_BC_BSP_JOB_MEMORY_DATA_PERCENT, 0.8f);
    }
    
    /**
     * Set the beta parameter for the proportion of the data memory for the
     * graph data.
     * 
     * @param beta
     */
    public void setBeta(float beta) {
        conf.setFloat(Constants.USER_BC_BSP_JOB_MEMORY_BETA, beta);
    }

    /**
     * Get the beta parameter for the proportion fo the data memory for the
     * graph data. Default is 0.5 for user undefined.
     * 
     * @return beta
     */
    public float getBeta() {
        return conf.getFloat(Constants.USER_BC_BSP_JOB_MEMORY_BETA, 0.5f);
    }

    /**
     * Set the hash bucket number for the memory administration of both the
     * graph data and the messages.
     * 
     * @param hash
     *            bucket number
     */
    public void setHashBucketNumber(int number) {
        conf.setInt(Constants.USER_BC_BSP_JOB_MEMORY_HASHBUCKET_NO, number);
    }

    /**
     * Get the hash bucket number for the memory administration of both the
     * graph data and the messages. Default is 32 for user undefined.
     * 
     * @return hash bucket number
     */
    public int getHashBucketNumber() {
        return conf.getInt(Constants.USER_BC_BSP_JOB_MEMORY_HASHBUCKET_NO, 32);
    }

    /**
     * Set the implementation version for the graph data management. There are 3
     * choices: MEMORY_VERSION, DISK_VERSION, BDB_VERSION.
     * 
     * @param version
     */
    public void setGraphDataVersion(int version) {
        conf.setInt(Constants.USER_BC_BSP_JOB_GRAPH_DATA_VERSION, version);
    }

    /**
     * Get the implementation version for the graph data management. Default is
     * MEMORY_VERSION for user undefined.
     * 
     * @return the graph data version
     */
    public int getGraphDataVersion() {
        return conf.getInt(Constants.USER_BC_BSP_JOB_GRAPH_DATA_VERSION,
                this.MEMORY_VERSION);
    }

    /**
     * Set the implementation version for the message queues management. There
     * are 3 choices: MEMEORY_VERSION, DISK_VERSION, BDB_VERSION.
     * 
     * @param version
     */
    public void setMessageQueuesVersion(int version) {
        conf.setInt(Constants.USER_BC_BSP_JOB_MESSAGE_QUEUES_VERSION, version);
    }

    /**
     * Get the implementation version for the message queues management. Default
     * is MEMORY_VERSION for user undefined.
     * 
     * @return the message queues version
     */
    public int getMessageQueuesVersion() {
        return conf.getInt(Constants.USER_BC_BSP_JOB_MESSAGE_QUEUES_VERSION,
                this.MEMORY_VERSION);
    }
    
    // /////////////////////////////////////
    // Graph Data Structure
    // /////////////////////////////////////
    
    /**
     * Set the Vertex class type for the job.
     * 
     * @param cls
     * @throws IllegalStateException
     */
    public void setVertexClass(Class<? extends Vertex<?,?,?>> cls)
            throws IllegalStateException {
        conf.setClass(Constants.USER_BC_BSP_JOB_VERTEX_CLASS, cls, Vertex.class);
    }

    /**
     * Get the Vertex class type for the job.
     * 
     * @return
     */
    @SuppressWarnings("unchecked")
    public Class<? extends Vertex<?,?,?>> getVertexClass() {
        return ( Class<? extends Vertex<?,?,?>> ) conf.getClass(
                Constants.USER_BC_BSP_JOB_VERTEX_CLASS, Vertex.class);
    }
    
    /**
     * Set the Edge class type for the job.
     * 
     * @param cls
     * @throws IllegalStateException
     */
    public void setEdgeClass(Class<? extends Edge<?,?>> cls)
            throws IllegalStateException {
        conf.setClass(Constants.USER_BC_BSP_JOB_EDGE_CLASS, cls, Edge.class);
    }

    /**
     * Get the Edge class type for the job.
     * 
     * @return
     */
    @SuppressWarnings("unchecked")
    public Class<? extends Edge<?,?>> getEdgeClass() {
        return ( Class<? extends Edge<?,?>> ) conf.getClass(
                Constants.USER_BC_BSP_JOB_EDGE_CLASS, Edge.class);
    }
}
