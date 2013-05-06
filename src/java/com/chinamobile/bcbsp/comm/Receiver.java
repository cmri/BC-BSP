/**
 * CopyRight by Chinamobile
 * 
 * Receiver.java
 */
package com.chinamobile.bcbsp.comm;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chinamobile.bcbsp.api.Combiner;

/**
 * Receiver
 * 
 * A receiver belongs to a communicator, for receiving
 * messages and put them into the incoming queues.
 * 
 * @author
 * @version
 */
public class Receiver extends Thread {

    // For log
    private static final Log LOG = LogFactory.getLog(Receiver.class);
    
    private Communicator communicator = null;
    //brokerName for this staff to receive messages.
    private String brokerName;
    
    private MessageQueuesInterface messageQueues = null;

    private int consumerNum = 1;
    private ArrayList<ConsumerTool> consumers = null;
    
    boolean combinerFlag = false;
    private Combiner combiner = null;
    private int combineThreshold = 3;
    
    private boolean noMoreMessagesFlag = false;
    
    private Long messageCount = 0L;

    public Receiver(Communicator comm, String brokerName) {
        this.communicator = comm;
        this.brokerName = brokerName;
        this.combinerFlag = this.communicator.isReceiveCombineSetFlag();
        // Get the combiner from the communicator.
        if (combinerFlag) {
            this.combiner = this.communicator.getCombiner();
            this.combineThreshold = this.communicator.getJob().getReceiveCombineThreshold();
        }
        
        int usrProducerNum = this.communicator.getJob().getMaxProducerNum();
        int usrConsumerNum = this.communicator.getJob().getMaxConsumerNum();
        if (usrConsumerNum <= usrProducerNum) {
            this.consumerNum = usrConsumerNum;
        }
        
        this.consumers = new ArrayList<ConsumerTool>();
    }
    
    public void addMessageCount(long count) {
        synchronized (this.messageCount) {
            this.messageCount += count;
        }
    }

    public void run() {
        
        try {
        LOG.info("[Receiver] starts successfully!");
        LOG.info("========== Initialize Receiver ==========");
        if (this.combinerFlag) {
            LOG.info("[Combine Threshold for receiving] = " + this.combineThreshold);
        }
        else {
            LOG.info("No combiner or combiner turned off for receiving!!!");
        }
        LOG.info("[Consumer Number] = " + this.consumerNum);
        LOG.info("=========================================");
        
        this.messageQueues = this.communicator.getMessageQueues();

        ConsumerTool consumer = null;
        
        for (int i = 0; i < this.consumerNum; i ++) {
            consumer = new ConsumerTool(this, this.messageQueues, this.brokerName, "BSP");

            consumer.start(); // Start the consumer.
            
            this.consumers.add(consumer);
        }
        
        int maxSize = 0;
        String incomingIndex = null;
        ConcurrentLinkedQueue<BSPMessage> maxQueue = null;
        
        while (true) { // Wait until the consumerTool has finished, the Receiver
                       // will finish.
            
            if (combinerFlag) {
                
                maxSize = 0;
                maxQueue = null;
                
                incomingIndex = this.messageQueues.getMaxIncomingQueueIndex();
                
                if (incomingIndex != null) {
                    maxSize = this.messageQueues.getIncomingQueueSize(incomingIndex);
                }

                // When the longest queue's length reaches the threshold
                if (maxSize >= combineThreshold) {

                    // Get the queue out of the map.
                    maxQueue = this.messageQueues.removeIncomingQueue(incomingIndex);

                    // Combine the max queue into just one message.
                    BSPMessage message = combine(maxQueue);
                    
                    this.messageQueues.incomeAMessage(incomingIndex, message);
                }
                
            } else {
                try { // For no combiner define, just sleep for next check.
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    LOG.error("[Receiver] caught", e);
                }
            }

            Iterator<ConsumerTool> itr = consumers.iterator();
            int running = 0;
            while (itr.hasNext()) {
                ConsumerTool thread = itr.next();
                if (thread.isAlive()) {
                    running++;
                }
            }
            if (running <= 0) { //All consumers are finished.
                break;
            }
        }

        LOG.info("[Receiver] has received " + this.messageCount + " BSPMessages totally!");
        LOG.info("[Receiver] exits.");
        } catch (Exception e) {
            LOG.error("[Receiver] caught: ", e);
        }
    }

    /**
     * To tell the consumer tool that there are no
     * more messages for it.
     * 
     * @param flag
     */
    public void setNoMoreMessagesFlag(boolean flag) {
        this.noMoreMessagesFlag = flag;
    }
    
    public boolean getNoMoreMessagesFlag() {
        return this.noMoreMessagesFlag;
    }
    
    public void setCombineThreshold(int aCombineThreshold) {
        if (this.combineThreshold == 0) {
            this.combineThreshold = aCombineThreshold;
        }
    }
    
    private BSPMessage combine(ConcurrentLinkedQueue<BSPMessage> incomingQueue) {
        
        BSPMessage msg = this.combiner.combine(incomingQueue.iterator());
        return msg;
    }
}
