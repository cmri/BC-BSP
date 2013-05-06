/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.comm;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.activemq.broker.BrokerService;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.chinamobile.bcbsp.comm.BSPMessage;
import com.chinamobile.bcbsp.comm.ConsumerTool;
import com.chinamobile.bcbsp.comm.MessageQueuesForMem;
import com.chinamobile.bcbsp.comm.ProducerTool;
import com.chinamobile.bcbsp.comm.Receiver;
import com.chinamobile.bcbsp.comm.Sender;

public class ProducerToolAndConsumerToolTest {
    

    @Test
    public void testRun() throws Exception {
        //message manager which hold the outoingQueues and incomingQueue
        MessageQueuesForMem messageQueues = new MessageQueuesForMem();

        //the msgQueue is to be sent by consumerTool
        ConcurrentLinkedQueue<BSPMessage> msgQueue = new ConcurrentLinkedQueue<BSPMessage>();      
        BSPMessage msg1 = new BSPMessage(0, "001", Bytes.toBytes("tags1"),Bytes.toBytes("data1"));
        BSPMessage msg2 = new BSPMessage(0, "001", Bytes.toBytes("tags2"),Bytes.toBytes("data2"));
        BSPMessage msg3 = new BSPMessage(0, "003", Bytes.toBytes("tags3"),Bytes.toBytes("data3"));
        msgQueue.add(msg1);
        msgQueue.add(msg2);
        msgQueue.add(msg3);
        
        assertEquals("Before sending, msgQueue has 3 message.",
                3, msgQueue.size());
        assertEquals("Before receive, messageQueuesInterface has 0 incoming messages.",
                0, messageQueues.getIncomingQueuesSize());
        
        Receiver receiver = mock(Receiver.class);
        when(receiver.getNoMoreMessagesFlag()).thenReturn(false);
        
        Sender sender = mock(Sender.class);
        String hostNameAndPort = "localhost:61616";       
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        broker.setBrokerName("localhost-0");
        broker.addConnector("tcp://localhost:61616");
        broker.start();
        
        ThreadGroup group = new ThreadGroup("sender");
        ProducerTool producerTool;      
        producerTool = new ProducerTool(group, 0, msgQueue, hostNameAndPort, "BSP", sender);
        producerTool.setPackSize(500);
        producerTool.addMessages(msgQueue);
        producerTool.setIdle(false);   
        producerTool.start();
        
        ConsumerTool consumerTool = new ConsumerTool(receiver, messageQueues, "localhost-0", "BSP");
        consumerTool.start();
        
        while(true){
            if(messageQueues.getIncomingQueuesSize() == 3){
                when(receiver.getNoMoreMessagesFlag()).thenReturn(true); 
            }
            if(producerTool.isIdle() && receiver.getNoMoreMessagesFlag()){                
                break;
            }
        }     
        while(consumerTool.isAlive()){};
        broker.stop();
        
        assertEquals("After send, msgQueue is empty.",
                0, msgQueue.size());
        assertEquals("After receive, messageQueuesInterface has 3 incoming messages.",
                3, messageQueues.getIncomingQueuesSize());
        
    }
}
