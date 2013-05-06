/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.comm;

import org.apache.activemq.broker.BrokerService;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.chinamobile.bcbsp.comm.BSPMessage;
import com.chinamobile.bcbsp.comm.Communicator;
import com.chinamobile.bcbsp.comm.MessageQueuesForMem;
import com.chinamobile.bcbsp.comm.MessageQueuesInterface;
import com.chinamobile.bcbsp.comm.Receiver;
import com.chinamobile.bcbsp.comm.Sender;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;

import static org.mockito.Mockito.*;
public class SenderAndReceiverTest {

    @Test
    public void testRun() throws Exception {
        MessageQueuesInterface msgQueues = new MessageQueuesForMem();
        BSPMessage msg1 = new BSPMessage(0, "001", Bytes.toBytes("tags1"),Bytes.toBytes("data1"));
        BSPMessage msg2 = new BSPMessage(0, "001", Bytes.toBytes("tags2"),Bytes.toBytes("data2"));
        BSPMessage msg3 = new BSPMessage(0, "003", Bytes.toBytes("tags3"),Bytes.toBytes("data3"));
        msgQueues.outgoAMessage("localhost:61616", msg1);
        msgQueues.outgoAMessage("localhost:61616", msg2);
        msgQueues.outgoAMessage("localhost:61616", msg3);
        msgQueues.outgoAMessage("localhost:61616", msg3);
        
        Communicator comm = mock(Communicator.class);
        BSPJob job = mock(BSPJob.class);
        when(job.getSendCombineThreshold()).thenReturn(1);
        when(job.getMessagePackSize()).thenReturn(500); 
        when(job.getMaxProducerNum()).thenReturn(5);
        when(job.getMaxConsumerNum()).thenReturn(5);
        BSPJobID jobid = mock(BSPJobID.class);
        when(jobid.toString()).thenReturn("jobid123");
        
        when(comm.getJob()).thenReturn(job);
        when(comm.getBSPJobID()).thenReturn(jobid);
        when(comm.getMessageQueues()).thenReturn(msgQueues);
        when(comm.isSendCombineSetFlag()).thenReturn(false);

        
        Sender sender = new Sender(comm);
        Receiver receiver = new Receiver(comm, "localhost-0");
        
               
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        broker.setBrokerName("localhost-0");
        broker.addConnector("tcp://localhost:61616");
        broker.start();
        
        sender.start();
        receiver.start();
        sender.begin(0);
        
        while(true){
            if(msgQueues.getIncomingQueuesSize() == 4){
                receiver.setNoMoreMessagesFlag(true);
                break;
            }
        }
        
    }

}
