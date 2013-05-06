/**
 * CopyRight by Chinamobile
 * 
 * MessageQueuesForMem.java
 */
package com.chinamobile.bcbsp.comm;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * MessageQueuesForMem
 * implements MessageQueuesInterface for only
 * memory storage of the message queues.
 * 
 * @author
 * @version
 */
public class MessageQueuesForMem implements MessageQueuesInterface {

    /**
     * Outgoing Message Queues hash indexed by <WorkerManagerName:Port>
     */
    private final ConcurrentHashMap<String, ConcurrentLinkedQueue<BSPMessage>> outgoingQueues = new ConcurrentHashMap<String, ConcurrentLinkedQueue<BSPMessage>>();

    /**
     * Incoming Message Queues hash indexed by vertexID(int)
     */
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<BSPMessage>> incomingQueues = new ConcurrentHashMap<String, ConcurrentLinkedQueue<BSPMessage>>();

    /**
     * Incomed Message Queues hash indexed by vertexID(int)
     */
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<BSPMessage>> incomedQueues = new ConcurrentHashMap<String, ConcurrentLinkedQueue<BSPMessage>>();
    
    private int nextOutgoingQueueCount = 1;
    
    @Override
    public void clearAllQueues() {
        clearIncomedQueues();
        clearIncomingQueues();
        clearOutgoingQueues();
    }

    @Override
    public void clearIncomedQueues() {
        this.incomedQueues.clear();
    }

    @Override
    public void clearIncomingQueues() {
        this.incomingQueues.clear();
    }

    @Override
    public void clearOutgoingQueues() {
        this.outgoingQueues.clear();
    }

    @Override
    public void exchangeIncomeQueues() {
        
        ConcurrentHashMap<String, ConcurrentLinkedQueue<BSPMessage>> tempQueues = this.incomedQueues;

        this.incomedQueues = this.incomingQueues;

        this.incomingQueues = tempQueues;
    }

    @Override
    public int getIncomedQueuesSize() {
        
        int size = 0;

        Entry<String, ConcurrentLinkedQueue<BSPMessage>> entry;
        Iterator<Entry<String, ConcurrentLinkedQueue<BSPMessage>>> it = this.incomedQueues
                .entrySet().iterator();
        ConcurrentLinkedQueue<BSPMessage> queue;

        while (it.hasNext()) {

            entry = it.next();

            queue = entry.getValue();

            size += queue.size();

        }

        return size;
    }

    @Override
    public int getIncomingQueuesSize() {
        
        int size = 0;

        Entry<String, ConcurrentLinkedQueue<BSPMessage>> entry;
        Iterator<Entry<String, ConcurrentLinkedQueue<BSPMessage>>> it = this.incomingQueues
                .entrySet().iterator();
        ConcurrentLinkedQueue<BSPMessage> queue;

        while (it.hasNext()) {

            entry = it.next();

            queue = entry.getValue();

            size += queue.size();

        }

        return size;
    }

    @Override
    public String getMaxIncomingQueueIndex() {
        
        int maxSize = 0;
        String maxIndex = null;
        ConcurrentLinkedQueue<BSPMessage> tempQueue = null;
        Entry<String, ConcurrentLinkedQueue<BSPMessage>> entry = null;
        Iterator<Entry<String, ConcurrentLinkedQueue<BSPMessage>>> it = 
            this.incomingQueues.entrySet().iterator();

        while (it.hasNext()) { // Get the queue with the max size

            entry = it.next();
            tempQueue = entry.getValue();

            if (tempQueue.size() > maxSize) {
                maxSize = tempQueue.size();
                maxIndex = entry.getKey();
            } // if

        } // while
        
        return maxIndex;
    }

    @Override
    public String getMaxOutgoingQueueIndex() {
        
        int maxSize = 0;
        String maxIndex = null;
        ConcurrentLinkedQueue<BSPMessage> tempQueue = null;
        Entry<String, ConcurrentLinkedQueue<BSPMessage>> entry = null;
        Iterator<Entry<String, ConcurrentLinkedQueue<BSPMessage>>> it = 
            this.outgoingQueues.entrySet().iterator();
        
        while (it.hasNext()) { // Get the queue with the max size

            entry = it.next();
            tempQueue = entry.getValue();

            if (tempQueue.size() > maxSize) {
                maxSize = tempQueue.size();
                maxIndex = entry.getKey();
            } // if

        } // while
        
        return maxIndex;
    }

    @Override
    public int getOutgoingQueuesSize() {
        
        int size = 0;

        Entry<String, ConcurrentLinkedQueue<BSPMessage>> entry;
        Iterator<Entry<String, ConcurrentLinkedQueue<BSPMessage>>> it = this.outgoingQueues
                .entrySet().iterator();
        ConcurrentLinkedQueue<BSPMessage> queue;

        while (it.hasNext()) {

            entry = it.next();

            queue = entry.getValue();

            size += queue.size();

        }

        return size;
    }

    @Override
    public void incomeAMessage(String dstVertexID, BSPMessage msg) {
       
        ConcurrentLinkedQueue<BSPMessage> incomingQueue = incomingQueues.get(dstVertexID);

        if (incomingQueue == null) {
            incomingQueue = new ConcurrentLinkedQueue<BSPMessage>();
        }
        incomingQueue.add(msg);
        incomingQueues.put(dstVertexID, incomingQueue);
    }

    @Override
    public void outgoAMessage(String outgoingIndex, BSPMessage msg) {
        
        ConcurrentLinkedQueue<BSPMessage> outgoingQueue = outgoingQueues.get(outgoingIndex);

        if (outgoingQueue == null) {
            outgoingQueue = new ConcurrentLinkedQueue<BSPMessage>();
        }
        outgoingQueue.add(msg);
        outgoingQueues.put(outgoingIndex, outgoingQueue);
    }

    @Override
    public ConcurrentLinkedQueue<BSPMessage> removeIncomedQueue(String dstVertexID) {
        
        ConcurrentLinkedQueue<BSPMessage> incomedQueue = incomedQueues.remove(dstVertexID);
        
        if (incomedQueue == null) {
            incomedQueue = new ConcurrentLinkedQueue<BSPMessage>();
        }
        return incomedQueue;
    }

    @Override
    public int getIncomingQueueSize(String dstVertexID) {
        
        if (incomingQueues.containsKey(dstVertexID)) {
            return incomingQueues.get(dstVertexID).size();
        } else {
            return 0;
        }
    }
    
    @Override
    public ConcurrentLinkedQueue<BSPMessage> removeIncomingQueue(String dstVertexID) {
        
        ConcurrentLinkedQueue<BSPMessage> incomingQueue = incomingQueues.remove(dstVertexID);
        
        if (incomingQueue == null) {
            incomingQueue = new ConcurrentLinkedQueue<BSPMessage>();
        }
        return incomingQueue;
    }

    @Override
    public ConcurrentLinkedQueue<BSPMessage> removeOutgoingQueue(String index) {
        
        ConcurrentLinkedQueue<BSPMessage> outgoingQueue = outgoingQueues.remove(index);
        
        if (outgoingQueue == null) {
            outgoingQueue = new ConcurrentLinkedQueue<BSPMessage>();
        }
        return outgoingQueue;
    }

    @Override
    public int getOutgoingQueueSize(String index) {
        
        if (outgoingQueues.containsKey(index)) {
            return outgoingQueues.get(index).size();
        } else {
            return 0;
        }
    }

    @Override
    public void showMemoryInfo() {
    }

    @Override
    public String getNextOutgoingQueueIndex() {
        
        String nextIndex = null;
        
        synchronized(this.outgoingQueues) {
            
            int size = this.outgoingQueues.size();
            
            if (size == 0) {
                return null;
            }
            
            if (this.nextOutgoingQueueCount > size) {
                this.nextOutgoingQueueCount = 1;
            }
            
            Entry<String, ConcurrentLinkedQueue<BSPMessage>> entry = null;
            Iterator<Entry<String, ConcurrentLinkedQueue<BSPMessage>>> it = this.outgoingQueues.entrySet().iterator();
            
            for (int i = 0; i < this.nextOutgoingQueueCount; i ++) {
                entry = it.next();
                nextIndex = entry.getKey();
            }
            this.nextOutgoingQueueCount++;
        }
        
        return nextIndex;
    }
}
