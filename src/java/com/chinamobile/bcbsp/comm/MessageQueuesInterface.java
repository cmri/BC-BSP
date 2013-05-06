/**
 * CopyRight by Chinamobile
 * 
 * MessageQueuesInterface.java
 */
package com.chinamobile.bcbsp.comm;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * The interface for Message Queues, which
 * manages the message queues.
 * 
 * @author
 * @version
 */
public interface MessageQueuesInterface {

    /**
     * Add a message into the incoming message queue
     * with the destination vertexID for index.
     * 
     * @param dstVertexID
     * @param msg
     */
    public void incomeAMessage(String dstVertexID, BSPMessage msg);
    
    /**
     * Add a message into the outgoing message queue
     * with the destination "WorkerManagerName:Port"
     * for index.
     * 
     * @param outgoingIndex
     * @param msg
     */
    public void outgoAMessage(String outgoingIndex, BSPMessage msg);
    
    /**
     * Remove and return the incomed message queue for
     * dstVertexID as the destination.
     * 
     * @param dstVertexID
     * @return
     *      ConcurrentLinkedQueue<BSPMessage>
     */
    public ConcurrentLinkedQueue<BSPMessage> removeIncomedQueue(String dstVertexID);
    
    /**
     * Get the current longest outgoing queue's index.
     * If the outgoing queues are all empty, return null.
     * 
     * @return
     *      String
     */
    public String getMaxOutgoingQueueIndex();
    
    /**
     * Get the next outgoing queue's index.
     * This method will be used for traversal of
     * the outgoing queues' map looply.
     * If the outgoing queues are all empty, return null.
     * 
     * @return
     */
    public String getNextOutgoingQueueIndex() throws Exception;
    
    /**
     * Remove and return the outgoing message queue for
     * index as the queue's index.
     * 
     * @param index
     * @return
     *      ConcurrentLinkedQueue<BSPMessage>
     */
    public ConcurrentLinkedQueue<BSPMessage> removeOutgoingQueue(String index);
    
    /**
     * Get the current longest incoming queue's index.
     * If the incoming queues are all empty, return -1.
     * 
     * @return
     *      int
     */
    public String getMaxIncomingQueueIndex();
    
    /**
     * Remove and return the incoming message queue for
     * dstVertexID as the queue's index.
     * 
     * @param dstVertexID
     * @return
     *      ConcurrentLinkedQueue<BSPMessage>
     */
    public ConcurrentLinkedQueue<BSPMessage> removeIncomingQueue(String dstVertexID);
    
    public int getIncomingQueueSize(String dstVertexID);
    
    public int getOutgoingQueueSize(String index);
    
    public void exchangeIncomeQueues();
    
    public void clearAllQueues();
    
    public void clearOutgoingQueues();
    
    public void clearIncomingQueues();
    
    public void clearIncomedQueues();
    
    public int getOutgoingQueuesSize();
    
    public int getIncomingQueuesSize();
    
    public int getIncomedQueuesSize();
    
    public void showMemoryInfo();
}
