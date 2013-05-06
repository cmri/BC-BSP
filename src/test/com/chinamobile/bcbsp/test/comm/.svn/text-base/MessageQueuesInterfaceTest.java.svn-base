/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.comm;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.chinamobile.bcbsp.comm.BSPMessage;
import com.chinamobile.bcbsp.comm.MessageQueuesForDisk;
import com.chinamobile.bcbsp.comm.MessageQueuesForMem;
import com.chinamobile.bcbsp.comm.MessageQueuesInterface;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;
import java.util.Arrays;

@RunWith(Parameterized.class)
public class MessageQueuesInterfaceTest {
    private static BSPJob job;
    private static int partitionId = 0;
    MessageQueuesInterface msgQueues;
    BSPMessage msg1 ,msg2, msg3;

    public MessageQueuesInterfaceTest(MessageQueuesInterface msgQueues){

        this.msgQueues = msgQueues;
    }
    @Parameters
    public static Collection<Object[]> getParameters(){
        job = mock(BSPJob.class);
        when(job.getMemoryDataPercent()).thenReturn(0.8f);
        when(job.getBeta()).thenReturn(0.5f);
        when(job.getHashBucketNumber()).thenReturn(32);
        when(job.getJobID()).thenReturn(new BSPJobID("jtIdentifier",2));
        job.getBeta();
        return Arrays.asList(new Object[][] {
                { new MessageQueuesForDisk(job, partitionId) },
                { new MessageQueuesForMem()}//,
                //{ new MessageQueuesForBDB(job, partitionId)}
        });
    }
    @Before
    public void setUp() throws Exception {
        //message 1 and 2 are going to sent to Partition0,and msg3 is to partition1     
        msg1 = new BSPMessage(0, "001", Bytes.toBytes("tags1"),Bytes.toBytes("data1"));
        msg2 = new BSPMessage(0, "001", Bytes.toBytes("tags2"),Bytes.toBytes("data2"));
        msg3 = new BSPMessage(1, "003", Bytes.toBytes("tags3"),Bytes.toBytes("data3"));
        
        msgQueues.incomeAMessage(msg1.getDstVertexID(), msg1);
        msgQueues.incomeAMessage(msg2.getDstVertexID(), msg2);
        msgQueues.incomeAMessage(msg3.getDstVertexID(), msg3);
        
        msgQueues.outgoAMessage("001:8080", msg1);
        msgQueues.outgoAMessage("001:8080", msg2);
        msgQueues.outgoAMessage("001:8080", msg3);
        msgQueues.outgoAMessage("002:8080", msg3);
                

        
    }

    @After
    public void tearDown() throws Exception {
       msgQueues.clearAllQueues(); 
    }

    @Test
    public void testIncomeAMessage() {
        int size = 3; //msgQueues.getIncomingQueuesSize();
        msgQueues.incomeAMessage(msg1.getDstVertexID(), msg1);
        assertEquals("after incomeAmessage, the size of the incomingqueues should be increased by 1.",
                size + 1, msgQueues.getIncomingQueuesSize());
        
    }

    @Test
    public void testOutgoAMessage() {
        int size = 4; //msgQueues.getIncomingQueuesSize();
        msgQueues.outgoAMessage("001:8080", msg1);
        assertEquals("after outgoAmessage, the size of the outgoqueues should be increased by 1.",
                size + 1, msgQueues.getOutgoingQueuesSize());
    }

    @Test
    public void testRemoveIncomedQueue() {
        int removedSize = msgQueues.getIncomingQueueSize("001");
        msgQueues.exchangeIncomeQueues();

        
        ConcurrentLinkedQueue<BSPMessage> msgqueue = msgQueues.removeIncomedQueue("001");
        assertEquals("Given the index, remove it's RemoveQueue.", removedSize, msgqueue.size());
        assertEquals("After remove, the size of given index's incomingqueue should be 0.",
                0, msgQueues.removeIncomedQueue("001").size());
    }

    @Test
    public void testGetMaxOutgoingQueueIndex() {
        String maxIndex = msgQueues.getMaxOutgoingQueueIndex();
        assertEquals("check maxoutgoingQueueIndex.", "001:8080", maxIndex);
        
    }

    @Test
    public void testGetNextOutgoingQueueIndex() throws Exception {

        assertEquals("get next outgoingQueueIndex",
                "001:8080",msgQueues.getNextOutgoingQueueIndex());
        assertEquals("get next outgoingQueueIndex",
                "002:8080",msgQueues.getNextOutgoingQueueIndex());
        assertEquals("get next outgoingQueueIndex",
                "001:8080",msgQueues.getNextOutgoingQueueIndex());

    }

    @Test
    public void testRemoveOutgoingQueue() {
        int removedSize = msgQueues.getOutgoingQueueSize("001:8080");
        
        ConcurrentLinkedQueue<BSPMessage> msgqueue = msgQueues.removeOutgoingQueue("001:8080");
        assertEquals("Given the index, remove it's OutgoingQueue.", removedSize, msgqueue.size());
        assertEquals("After remove, the size of given index's outgoingingqueue should be 0.",
                0, msgQueues.getOutgoingQueueSize("001:8080"));
    }

    @Test
    public void testGetMaxIncomingQueueIndex() {
        String maxIndex = msgQueues.getMaxIncomingQueueIndex();
        assertEquals("check maxIncomingQueueIndex.", "001", maxIndex);
        
    }

    @Test
    public void testRemoveIncomingQueue() {
        int removedSize = msgQueues.getIncomingQueueSize("001");
        ConcurrentLinkedQueue<BSPMessage> msgqueue = msgQueues.removeIncomingQueue("001");
        assertEquals("Given the index, remove it's IncomingQueue.", removedSize, msgqueue.size());
        assertEquals("After remove, the size of given index's incomingqueue should be 0.",
                0, msgQueues.getIncomingQueueSize("001"));
    }

    @Test
    public void testGetIncomingQueueSize() {
        assertEquals("Given the index, getIncomingQueueSize.", 2, msgQueues.getIncomingQueueSize("001"));
    }

    @Test
    public void testGetOutgoingQueueSize() {
        assertEquals("Given the index, getOutgoingQueueSize.", 3, msgQueues.getOutgoingQueueSize("001:8080"));
    }

    @Test
    public void testExchangeIncomeQueues() {
        assertEquals("check the size of  incoming queue before exchangeIncomeQueues", 
                3, msgQueues.getIncomingQueuesSize());
        assertEquals("check the size of  incomed queue before exchangeIncomeQueues", 
                0, msgQueues.getIncomedQueuesSize());
        msgQueues.exchangeIncomeQueues();
        assertEquals("check the size of  incoming queue after exchangeIncomeQueues", 
                0, msgQueues.getIncomingQueuesSize());
        assertEquals("check the size of  incomed queue after exchangeIncomeQueues", 
                3, msgQueues.getIncomedQueuesSize());
    }

    @Test
    public void testClearAllQueues() {
        msgQueues.clearAllQueues();
        assertEquals("after clear, the size of incomingQueues is 0",
                0, msgQueues.getIncomingQueuesSize());
        assertEquals("after clear, the size of incomedQueues is 0",
                0, msgQueues.getIncomedQueuesSize());
        assertEquals("after clear, the size of outgoingQueues is 0",
                0, msgQueues.getOutgoingQueuesSize());
    }

    @Test
    public void testClearOutgoingQueues() {
        msgQueues.clearOutgoingQueues();
        assertEquals("After clear, the size of outgoingQueues is 0",
                0, msgQueues.getOutgoingQueuesSize());
    }

    @Test
    public void testClearIncomingQueues() {
        msgQueues.clearIncomingQueues();
        assertEquals("After clear, the size of incomingQueues should be 0",
                0, msgQueues.getIncomingQueuesSize());
    }

    @Test
    public void testClearIncomedQueues() {
        msgQueues.exchangeIncomeQueues();
        msgQueues.clearIncomedQueues();
        assertEquals("After clear, the size of incomedQueues should be 0",
                0, msgQueues.getIncomedQueuesSize());
    }

    @Test
    public void testGetOutgoingQueuesSize() {
        assertEquals("After setUp, the size of outgoingqueuesSize should be 4.",
                4, msgQueues.getOutgoingQueuesSize());
    }

    @Test
    public void testGetIncomingQueuesSize() {
        assertEquals("incomingqueues'size should be 3.",
                3, msgQueues.getIncomingQueuesSize());
    }

    @Test
    public void testGetIncomedQueuesSize() {
        msgQueues.exchangeIncomeQueues();
        assertEquals("incomedqueues'size should be 3.",
                3, msgQueues.getIncomedQueuesSize());
    }

}
