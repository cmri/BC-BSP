/**
 * CopyRight by Chinamobile
 * 
 * CombinerTool.java
 */
package com.chinamobile.bcbsp.comm;

import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chinamobile.bcbsp.api.Combiner;

/**
 * CombinerTool
 * 
 * CombinerTool is a single thread belongs to a Sender,
 * to do combination operation for outgoing queues that
 * is over the sendingCombineThreshold.
 * 
 * @author
 * @version
 */
public class CombinerTool extends Thread {

    // For log
    private static final Log LOG = LogFactory.getLog(CombinerTool.class);
    
    private MessageQueuesInterface messageQueues = null;
    
    private Combiner combiner = null;
    
    private int combineThreshold = 1000;//default
    
    private Sender sender = null;
    
    /** Notes the count since last combination of the queue indexed by String 
     *  so that if the new increasing count overs the threshold, we will combine them, 
     *  otherwise, do not combine them.*/
    private HashMap<String, Integer> combinedCountsMap = new HashMap<String, Integer>();
    
    public CombinerTool(Sender aSender, MessageQueuesInterface msgQueues,
            Combiner combiner, int threshold) {
        this.sender = aSender;
        this.messageQueues = msgQueues;
        this.combiner = combiner;
        this.combineThreshold = threshold;
    }
    
    public void run() {
        
        LOG.info("[CombinerTool] Start!");
        long time = 0;
        long totalBeforeSize = 0;
        long totalAfterSize = 0;
        
        String outgoingIndex = null;
        ConcurrentLinkedQueue<BSPMessage> maxQueue = null;
        int maxSize = 0;
        int lastCount = 0;
        
        while (!this.sender.getNoMoreMessagesFlag()) {
            
            outgoingIndex = this.messageQueues.getMaxOutgoingQueueIndex();
            
            if (outgoingIndex != null) {
                maxSize = this.messageQueues.getOutgoingQueueSize(outgoingIndex);
            }
            lastCount = (this.combinedCountsMap.get(outgoingIndex) == null? 
                    0: this.combinedCountsMap.get(outgoingIndex));
            // If new updated size is over the threshold
            if ((maxSize - lastCount) > this.combineThreshold) {
                // Get the queue out of the map.
                maxQueue = this.messageQueues.removeOutgoingQueue(outgoingIndex);
                
                if (maxQueue == null)
                    continue;
                long start = System.currentTimeMillis();/**Clock*/
                // Combine the messages.
                maxQueue = combine(maxQueue);
                long end = System.currentTimeMillis();/**Clock*/
                time = time + end - start;
                
                int maxSizeBefore = maxSize;
                totalBeforeSize += maxSizeBefore;
                maxSize = maxQueue.size();
                totalAfterSize += maxSize;
                // Note the count after combination.
                this.combinedCountsMap.put(outgoingIndex, maxQueue.size());
                // Put the combined messages back to the outgoing queue.
                Iterator<BSPMessage> iter = maxQueue.iterator();
                while (iter.hasNext()) {
                    this.messageQueues.outgoAMessage(outgoingIndex, iter.next());
                }
                maxQueue.clear();
            }
            else {
                try {
                    Thread.sleep(500); // Wait for 500ms until next check.
                } catch (Exception e) {
                    LOG.error("[CombinerTool] caught:", e);
                }
            }
        }
        
        LOG.info("[CombinerTool] has combined totally (" + totalBeforeSize + ") messages into (" + totalAfterSize + 
                "). Compression rate = " + (float)totalAfterSize*100/totalBeforeSize + "%.");
        LOG.info("[CombinerTool] has used time: " + time/1000f + " seconds totally!");
        LOG.info("[CombinerTool] Die!");
        
    }// end-run
    
    
    private ConcurrentLinkedQueue<BSPMessage> combine(ConcurrentLinkedQueue<BSPMessage> outgoingQueue) {
        
        // Map of outgoing queues indexed by destination vertex ID.
        TreeMap<String, ConcurrentLinkedQueue<BSPMessage>> outgoingQueues = new TreeMap<String, ConcurrentLinkedQueue<BSPMessage>>();
        
        ConcurrentLinkedQueue<BSPMessage> tempQueue = null;
        BSPMessage tempMessage = null;
        
        // Traverse the outgoing queue and put the messages with the same dstVertexID into the same queue in the tree map.
        Iterator<BSPMessage> iter = outgoingQueue.iterator();
        String dstVertexID = null; 
        while (iter.hasNext()) {
            tempMessage = iter.next();
            dstVertexID = tempMessage.getDstVertexID();
            tempQueue = outgoingQueues.get(dstVertexID);
            if (tempQueue == null) {
                tempQueue = new ConcurrentLinkedQueue<BSPMessage>();
            }
            tempQueue.add(tempMessage);
            outgoingQueues.put(dstVertexID, tempQueue);
        }
        
        // The result queue for return.
        ConcurrentLinkedQueue<BSPMessage> resultQueue = new ConcurrentLinkedQueue<BSPMessage>();

        // Do combine operation for each of the outgoing queues.
        for (Entry<String, ConcurrentLinkedQueue<BSPMessage>> entry : outgoingQueues.entrySet()) {
            tempQueue = entry.getValue();
            tempMessage = this.combiner.combine(tempQueue.iterator());
            resultQueue.add(tempMessage);
        }
        
        outgoingQueue.clear();
        outgoingQueues.clear();
        return resultQueue;
    }
}
