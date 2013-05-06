/**
 * CopyRight by Chinamobile
 * 
 * CommunicatorInterface.java
 */
package com.chinamobile.bcbsp.comm;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.chinamobile.bcbsp.graph.GraphDataInterface;

/**
 * CommunicatorInterface
 * 
 * @author
 * @version
 */
public interface CommunicatorInterface {

    /**
     * Initialize the communicator
     * 
     * @param ahashBucketToPartition
     * @param aPartitionToWorkerManagerNameAndPort
     * @param aGraphData
     */
    void initialize(HashMap<Integer,Integer> ahashBucketToPartition,
            HashMap<Integer, String> aPartitionToWorkerManagerNameAndPort, 
            GraphDataInterface aGraphData);

    /**
     * Send a BSPMessage. Messages sent by this method are not guaranteed to be
     * received in a sent order.
     * 
     * @param msg
     * @throws IOException
     */
    void send(BSPMessage msg) throws IOException;

    /**
     * Send a BSPMessage to all vertices of it's outgoing edges. Messages sent
     * by this method are not guaranteed to be received in a sent order.
     * 
     * @param msg
     * @throws IOException
     */
    void sendToAllEdges(BSPMessage msg) throws IOException;

    /**
     * Get the BSPMessage Iterator filled with messages sent to this vertexID in
     * the last super step.
     * 
     * @param vertexID
     * @return The message iterator
     * @throws IOException
     */
    Iterator<BSPMessage> getMessageIterator(String vertexID) throws IOException;
    
    /**
     * Get the BSPMessage Queue filled with messages sent to this vertexID in
     * the last super step.
     * 
     * @param vertexID
     * @return
     * @throws IOException
     */
    ConcurrentLinkedQueue<BSPMessage> getMessageQueue(String vertexID) throws IOException;

    /**
     * To start the sender and the receiver of the communicator.
     */
    void start();
    
    /**
     * To begin the sender's and the receiver's tasks for the super step.
     * 
     * @param superStepCount
     */
    void begin(int superStepCount);
    
    /**
     * To notify the sender and the receiver to complete.
     */
    void complete();

    /**
     * To notify the communicator that there are no more messages for sending
     * for this super step.
     */
    void noMoreMessagesForSending();

    /**
     * To ask the communicator if the sending process has finished sending all
     * of the messages in the outgoing queue of the communicator.
     * 
     * @return true if sending has finished, otherwise false.
     */
    boolean isSendingOver();

    /**
     * To notify the communicator that there are no more messages for receving
     * for this super step.
     */
    void noMoreMessagesForReceiving();

    /**
     * To ask the communicator if the receiving process has finished receiving
     * all of the remaining messages from the incoming queue for it.
     * 
     * @return true if receiving has finished, otherwise false.
     */
    boolean isReceivingOver();

    /**
     * To exchange the incoming and incomed queues for each super step.
     */
    void exchangeIncomeQueues();

    /**
     * Get the outgoing queues' size.
     * 
     * @return int outgoing queues' size
     */
    int getOutgoingQueuesSize();

    /**
     * Get the incoming queues' size.
     * 
     * @return int incoming queues' size
     */
    int getIncomingQueuesSize();

    /**
     * Get the incomed queues' size.
     * 
     * @return int incomed queues' size
     */
    int getIncomedQueuesSize();

    /**
     * To clear all the queues.
     */
    void clearAllQueues();

    /**
     * To clear the outgoing queues.
     */
    void clearOutgoingQueues();
    
    public void setPartitionToWorkerManagerNamePort(HashMap<Integer, String> value);

    /**
     * To clear the incoming queues.
     */
    void clearIncomingQueues();

    /**
     * To clear the incomed queues.
     */
    void clearIncomedQueues();

}
