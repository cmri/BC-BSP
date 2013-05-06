/**
 * CopyRight by Chinamobile
 * 
 * Communicator.java
 */
package com.chinamobile.bcbsp.comm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.api.Combiner;
import com.chinamobile.bcbsp.api.Partitioner;
import com.chinamobile.bcbsp.graph.GraphDataInterface;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;

/**
 * Communicator
 * 
 * The communication tool for BSP. It manages the outgoing and incoming queues
 * of each staff.
 * 
 * @author
 * @version
 */
public class Communicator implements CommunicatorInterface {


    private static final Log LOG = LogFactory.getLog(Communicator.class);

    private BSPJobID jobID = null;
    private BSPJob job = null;
    private int partitionID = 0;

    private GraphDataInterface graphData = null;
    
    private Sender sender = null;
    private Receiver receiver = null;

    /** Route Table: HashID---PartitionID */
    private HashMap<Integer, Integer> hashBucketToPartition = null;

    /** Route Table: PartitionID---WorkerManagerNameAndPort(HostName:Port) */
    private HashMap<Integer, String> partitionToWorkerManagerNameAndPort = null;

    /** The message queues manager */
    private MessageQueuesInterface messageQueues = null;
    
    private Partitioner<Text> partitioner;
    
    // Sending message counter for a super step.
    private long sendMessageCounter;
    // Outgoing message counter for a super step.
    private long outgoMessageCounter;

    /**
     * Constructor
     */
    public Communicator(BSPJobID jobID, BSPJob job, int partitionID,
            Partitioner<Text> partitioner) {
        this.jobID = jobID;
        this.job = job;
        this.partitionID = partitionID;
        this.partitioner = partitioner;
        
        this.sendMessageCounter = 0;
        this.outgoMessageCounter = 0;
        
        int version = job.getMessageQueuesVersion();
        if (version == job.MEMORY_VERSION) {
            this.messageQueues = new MessageQueuesForMem();
        }else if (version == job.DISK_VERSION) {
            this.messageQueues = new MessageQueuesForDisk(job, partitionID);
        }else if (version == job.BDB_VERSION) {
            this.messageQueues = new MessageQueuesForBDB(job, partitionID);
        }
    }

    /**
     * Initialize the communicator
     * 
     * @param ahashBucketToPartition
     * @param aPartitionToWorkerManagerNameAndPort
     * @param aGraphData
     */
    @Override
    public void initialize(HashMap<Integer, Integer> ahashBucketToPartition,
            HashMap<Integer, String> aPartitionToWorkerManagerNameAndPort,
            GraphDataInterface aGraphData) {

        this.hashBucketToPartition = ahashBucketToPartition;

        this.partitionToWorkerManagerNameAndPort = aPartitionToWorkerManagerNameAndPort;
        
        this.graphData = aGraphData;
    }

    public void setBSPJobID(BSPJobID jobID) {
        this.jobID = jobID;
    }

    public BSPJobID getBSPJobID() {
        return this.jobID;
    }

    public BSPJob getJob() {
        return job;
    }

    public void setJob(BSPJob job) {
        this.job = job;
    }

    public void setPartitionID(int partitionID) {
        this.partitionID = partitionID;
    }

    public int getPartitionID() {
        return this.partitionID;
    }
    
    public MessageQueuesInterface getMessageQueues() {
        return this.messageQueues;
    }

    /**
     * Get the dstPartitionID from the vertexID.
     * 
     * @param vertexID
     * @return dstPartitionID
     */
    @SuppressWarnings("unused")
    private int getDstPartitionID(int vertexID) {

        int dstPartitionID = 0;

        for (Entry<Integer, Integer> e : this.hashBucketToPartition.entrySet()) {

            List<Integer> rangeList = new ArrayList<Integer>(e.getValue());

            if ((vertexID <= rangeList.get(1))
                    && (vertexID >= rangeList.get(0))) {

                dstPartitionID = e.getKey(); // destination partition id

                break;
            }
        }
        return dstPartitionID;
    }

    /**
     * Get the dstWorkerManagerName from the dstPartitionID
     * 
     * @param dstPartitionID
     * @return dstWorkerManagerName
     */
    private String getDstWorkerManagerNameAndPort(int dstPartitionID) {

        String dstWorkerManagerNameAndPort = null;

        dstWorkerManagerNameAndPort = this.partitionToWorkerManagerNameAndPort
                .get(dstPartitionID);

        return dstWorkerManagerNameAndPort;
    }
    
    @Override
    public void setPartitionToWorkerManagerNamePort(HashMap<Integer, String> value) {
        this.partitionToWorkerManagerNameAndPort = value;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.chinamobile.bcbsp.comm.CommunicatorInterface#GetMessageIterator(java
     * .lang.String)
     */
    @Override
    public Iterator<BSPMessage> getMessageIterator(String vertexID)
            throws IOException {

        ConcurrentLinkedQueue<BSPMessage> incomedQueue = messageQueues
                .removeIncomedQueue(vertexID);
        
        Iterator<BSPMessage> iterator = incomedQueue.iterator();
        
        return iterator;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.chinamobile.bcbsp.comm.CommunicatorInterface#GetMessageQueue(java
     * .lang.String)
     */
    @Override
    public ConcurrentLinkedQueue<BSPMessage> getMessageQueue(String vertexID)
            throws IOException {
        return messageQueues.removeIncomedQueue(vertexID);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.chinamobile.bcbsp.comm.CommunicatorInterface#Send(java.lang.String,
     * com.chinamobile.bcbsp.bsp.BSPMessage)
     */
    @Override
    public void send(BSPMessage msg) throws IOException {
        Text vertexID=new Text(msg.getDstVertexID());
        int dsthashID = this.partitioner.getPartitionID(vertexID);
        int dstPartitionID = -1;

        if (this.hashBucketToPartition != null)
            dstPartitionID = this.hashBucketToPartition.get(dsthashID);
        else
            dstPartitionID = dsthashID;
        msg.setDstPartition(dstPartitionID);

        // The destination partition is just in this staff.
        if (dstPartitionID == this.partitionID) {

            String dstVertexID = msg.getDstVertexID();

            messageQueues.incomeAMessage(dstVertexID, msg);
        } else {

            String dstWorkerManagerNameAndPort = this
                    .getDstWorkerManagerNameAndPort(dstPartitionID);

            String outgoingIndex = dstWorkerManagerNameAndPort;

            messageQueues.outgoAMessage(outgoingIndex, msg);
            
            this.outgoMessageCounter ++;
        }
        
        this.sendMessageCounter ++;
    }

    @Override
    public void sendToAllEdges(BSPMessage msg) throws IOException {
        
    }

    @Override
    public void clearAllQueues() {
        messageQueues.clearAllQueues();
    }

    @Override
    public void clearOutgoingQueues() {
        messageQueues.clearOutgoingQueues();
    }

    @Override
    public void clearIncomingQueues() {
        messageQueues.clearIncomingQueues();
    }

    @Override
    public void clearIncomedQueues() {
        messageQueues.clearIncomedQueues();
    }

    @Override
    public void exchangeIncomeQueues() {
        
        messageQueues.showMemoryInfo();
        
        messageQueues.exchangeIncomeQueues();
        
        LOG.info("[Communicator] has sent " + this.sendMessageCounter + " messages totally.");
        LOG.info("[Communicator] has outgo " + this.outgoMessageCounter + " messages totally.");
        
        this.sendMessageCounter = 0;
        this.outgoMessageCounter = 0;
    }

    @Override
    public int getOutgoingQueuesSize() {

        return messageQueues.getOutgoingQueuesSize();
    }

    @Override
    public int getIncomingQueuesSize() {

        return messageQueues.getIncomingQueuesSize();
    }

    @Override
    public int getIncomedQueuesSize() {

        return messageQueues.getIncomedQueuesSize();
    }

    @Override
    public void start() {

        int edgeSize = this.graphData.getEdgeSize();

        this.sender = new Sender(this);
        this.sender.setCombineThreshold((int)(edgeSize*0.4/10));
        this.sender.setSendThreshold((int)(edgeSize*0.4/10));
        this.sender.setMessagePackSize(1000);
        this.sender.setMaxProducerNum(20);
        this.sender.start();
    }

    @Override
    public void noMoreMessagesForSending() { // To notify the sender no more
                                             // messages for sending.

        this.sender.setNoMoreMessagesFlag(true);
    }

    @Override
    public boolean isSendingOver() {

        boolean result = false;

        if (this.sender.isOver()) {
            result = true;
        }
        return result;
    }

    @Override
    public void noMoreMessagesForReceiving() { // To notify the receiver no more
                                               // messages for receiving.

        this.receiver.setNoMoreMessagesFlag(true);

    }

    @Override
    public boolean isReceivingOver() {

        boolean result = false;

        if (!this.receiver.isAlive()) { // When the receiver is not alive,
                                        // return true.
            result = true;
        }
        return result;
    }

    public boolean isSendCombineSetFlag() {
        return this.job.isCombinerSetFlag();
    }
    
    public boolean isReceiveCombineSetFlag() {
        return this.job.isReceiveCombinerSetFlag();
    }

    public Combiner getCombiner() {
        if (this.job != null) {
            return ( Combiner ) ReflectionUtils.newInstance(
                    job.getConf().getClass(
                            Constants.USER_BC_BSP_JOB_COMBINER_CLASS,
                            Combiner.class), job.getConf());
        } else
            return null;
    }

    @Override
    public void begin(int superStepCount) {
        String localhostNameAndPort = this.getDstWorkerManagerNameAndPort(this.partitionID);
        String brokerName = localhostNameAndPort.split(":")[0] + "-" + this.partitionID;
        this.receiver = new Receiver(this, brokerName);
        this.receiver.setCombineThreshold(8);
        this.receiver.start();
        
        this.sender.begin(superStepCount);
    }

    @Override
    public void complete() {
        this.sender.complete();
    }

}
