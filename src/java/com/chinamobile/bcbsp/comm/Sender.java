/**
 * CopyRight by Chinamobile
 * 
 * Sender.java
 */
package com.chinamobile.bcbsp.comm;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chinamobile.bcbsp.api.Combiner;
import com.chinamobile.bcbsp.util.BSPJobID;

/**
 * Sender
 * 
 * A sender belongs to a communicator, for sending
 * messages from the outgoing queues.
 * 
 * @author
 * @version
 */
public class Sender extends Thread {

    // For log
    private static final Log LOG = LogFactory.getLog(Sender.class);
    // For time
    private Long connectTime = 0L;/**Clock*/
    private Long sendTime = 0L;/**Clock*/
    
    private boolean condition = false;
    
    BSPJobID jobID = null;
    
    private int messageCount = 0; // Total count.

    private Communicator communicator = null;
    
    private MessageQueuesInterface messageQueues = null;
    
    boolean combinerFlag = false;
    private Combiner combiner = null;
    private CombinerTool combinerTool = null;

    private int combineThreshold;
    private int sendThreshold;
    private int packSize;
    private int maxProducerNum;

    private boolean noMoreMessagesFlag = false;
    
    private ProducerPool producerPool = null;
    
    //The current progress.
    private volatile int superStepCounter = 0;
    //The flag for a super step.
    private volatile boolean over = false;
    //The flag for the whole staff.
    private volatile boolean completed = false;

    public Sender(Communicator comm) {
        
        this.communicator = comm;
        this.sendThreshold = this.communicator.getJob().getSendThreshold();
        this.packSize = this.communicator.getJob().getMessagePackSize();
        this.jobID = this.communicator.getBSPJobID();
        this.messageQueues = this.communicator.getMessageQueues();
        
        this.combinerFlag = this.communicator.isSendCombineSetFlag();
        // Get the combiner from the communicator.
        if (combinerFlag) {
            this.combiner = this.communicator.getCombiner();
            this.combineThreshold = this.communicator.getJob().getSendCombineThreshold();
        }
        
        this.maxProducerNum = this.communicator.getJob().getMaxProducerNum();
        this.superStepCounter = 0;
    }

    public void setNoMoreMessagesFlag(boolean flag) {
        this.noMoreMessagesFlag = flag;
    }
    
    public boolean getNoMoreMessagesFlag() {
        return this.noMoreMessagesFlag;
    }
    
    public void setSendThreshold(int aSendThreshold) {
        if (this.sendThreshold == 0) {
            this.sendThreshold = aSendThreshold;
        }
    }
    
    public void setCombineThreshold(int aCombineThreshold) {
        if (this.combineThreshold == 0) {
            this.combineThreshold = aCombineThreshold;
        }
    }
    
    public void setMessagePackSize(int size) {
        if (this.packSize == 0) {
            this.packSize = size;
        }
    }
    
    public void setMaxProducerNum(int num) {
        if (this.maxProducerNum == 0) {
            this.maxProducerNum = num;
        }
    }
    /**Clock*/
    public void addConnectTime(long time) {
        synchronized (this.connectTime) {
            this.connectTime += time;
        }
    }
    /**Clock*/
    public void addSendTime(long time) {
        synchronized (this.sendTime) {
            this.sendTime += time;
        }
    }

    public void run() {
        
        try {
        
        LOG.info("[Sender] starts successfully!");
        
        this.initialize();
        
        synchronized(this) {
            try {
                while (!this.condition) {
                    wait(); //wait for first begin.
                }
                this.condition = false;
            } catch (InterruptedException e) {
                LOG.error("[Sender] caught: ", e);
            }
        }
        
        do { // while for totally complete for this staff.

            LOG.info("[Sender] begins to work for sueper step <" + this.superStepCounter + ">!");
            
            this.connectTime = 0L;/**Clock*/
            this.sendTime = 0L;/**Clock*/
        
            ProducerTool producer = null;

            if (combinerFlag && !this.noMoreMessagesFlag) {
                this.combinerTool = new CombinerTool(this, messageQueues, combiner, combineThreshold);
                this.combinerTool.start();
            }

            String outgoingIndex = null;
            ConcurrentLinkedQueue<BSPMessage> maxQueue = null;
            int maxSize = 0;

            while (true) { // Keep sending until the whole outgoingQueues is empty
                // and no more messages will come.

                maxSize = 0;
                maxQueue = null;

                //outgoingIndex = this.messageQueues.getMaxOutgoingQueueIndex();
                outgoingIndex = this.messageQueues.getNextOutgoingQueueIndex();

                if (outgoingIndex != null) {
                    maxSize = this.messageQueues.getOutgoingQueueSize(outgoingIndex);
                }
                // When the whole outgoingQueues are empty and no more messages will
                // come, exit while.
                if (outgoingIndex == null) {

                    if (this.noMoreMessagesFlag) {
                        
                        if (this.messageQueues.getOutgoingQueuesSize() > 0) {
                            continue;
                        }
                        
                        if (!this.combinerFlag) {
                            /** no more messages and no combiner set*/
                            LOG.info("[Sender] exits while for no more messages and no combiner set.");
                            break;
                        }
                        else {
                            if (this.combinerTool == null) {
                                /** no more messages and no combiner created*/
                                LOG.info("[Sender] exits while for no more messages and no combiner created.");
                                break;
                            } else if (!this.combinerTool.isAlive()){
                                /** no more messages and combiner has exited*/
                                LOG.info("[Sender] exits while for no more messages and combiner has exited.");
                                break;
                            }
                        }
                    } else {
                        try {
                            Thread.sleep(500); // Wait for 500ms until next check.
                        } catch (Exception e) {
                            LOG.error("[Sender] caught: ", e);
                        }
                        continue;
                    }
                } 
                else if (maxSize > this.sendThreshold || this.noMoreMessagesFlag) {

                    // Get a producer tool to send the maxQueue
                    producer = this.producerPool.getProducer(outgoingIndex, this.superStepCounter, this);
                   
                    if (producer.isIdle()) {
                        
                        // Get the queue out of the map.
                        maxQueue = this.messageQueues.removeOutgoingQueue(outgoingIndex);

                        if (maxQueue == null)
                            continue;

                        maxSize = maxQueue.size();
                        
                        producer.setPackSize(this.packSize);
                        producer.addMessages(maxQueue);
                        this.messageCount += maxSize;
                        // Set the producer's state to busy to start it.
                        producer.setIdle(false);
                        
                    }
                    
                    if (producer.isFailed()) {
                        this.messageQueues.removeOutgoingQueue(outgoingIndex);
                    }

                }
            } // while

            LOG.info("[Sender] has started " + producerPool.getProducerCount() + " producers totally.");

            //Tell all producers that no more messages.
            this.producerPool.finishAll();

            while (true) { // Wait for the producers finish message sending.
                int running = this.producerPool.getActiveProducerCount(this.superStepCounter);

                LOG.info("[Sender] There are still <" + running + "> ProducerTools alive.");

                if (running == 0) {
                    break;
                }

                try {
                    Thread.sleep(500); //Wait for 500ms until next check.
                } catch (Exception e) {
                    LOG.error("[Sender] caught: ", e);
                }

            } // while

            LOG.info("[Sender] has sent " + this.messageCount + " messages totally for super step <"
                    + this.superStepCounter + ">! And enter waiting......");
            LOG.info("[==>Clock<==] <Sender's create connection> used " + this.connectTime/1000f + " seconds");/**Clock*/
            LOG.info("[==>Clock<==] <Sender's send messages> used " + this.sendTime/1000f + " seconds");/**Clock*/
            
            this.over = true;
            
            synchronized(this) {
                try {
                    while (!this.condition) {
                        wait();// wait for next begin.
                    }
                    this.condition = false;
                } catch (InterruptedException e) {
                    LOG.error("[Sender] caught: ", e);
                }
            }
       
        } while (!this.completed);
        
        LOG.info("[Sender] exits.");
        
        } catch (Exception e) {
            LOG.error("Sender caught: ", e);
        }

    } // run
    
    private void initialize() {
        LOG.info("========== Initialize Sender ==========");
        LOG.info("[Send Threshold] = " + this.sendThreshold);
        LOG.info("[Message Pack Size] = " + this.packSize);
        if (this.combinerFlag) {
            LOG.info("[Combine Threshold for sending] = " + this.combineThreshold);
        }
        LOG.info("[Max Producer Number] = " + this.maxProducerNum);
        LOG.info("=======================================");
        
        this.producerPool = new ProducerPool(this.maxProducerNum, this.jobID.toString());
    }
    
    /**
     * Begin the sender's task for a super step.
     * 
     * @param superStepCount
     */
    public void begin(int superStepCount) {
        this.superStepCounter = superStepCount;
        
        if (this.producerPool == null) {
            this.producerPool = new ProducerPool(this.maxProducerNum, this.jobID.toString());
            LOG.error("Test Null this.producePool is null and it is re-initialized");
        }
        
        this.producerPool.setActiveProducerProgress(superStepCount);
        this.producerPool.cleanFailedProducer();
        
        this.noMoreMessagesFlag = false;
        this.over = false;
        this.messageCount = 0;
        synchronized(this) {
            this.condition = true;
            notify();
        }
    }
    
    /**
     * Justify if the sender has finished the task for the current super step.
     * 
     * @return over
     */
    public boolean isOver() {
        return this.over;
    }
    
    /**
     * To notice the sender to complete and return.
     */
    public void complete() {
        
        this.producerPool.completeAll();
        
        this.completed = true;
        synchronized(this) {
            this.condition = true;
            notify();
        }
    }
}
