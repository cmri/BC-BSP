/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.comm;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import java.util.Iterator;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import static org.mockito.Mockito.*;
import com.chinamobile.bcbsp.api.Combiner;
import com.chinamobile.bcbsp.comm.BSPMessage;
import com.chinamobile.bcbsp.comm.CombinerTool;
import com.chinamobile.bcbsp.comm.MessageQueuesForMem;
import com.chinamobile.bcbsp.comm.Sender;

class SumCombiner extends Combiner {
    public BSPMessage combine(Iterator<BSPMessage> messages){
        BSPMessage msg;
        double sum = 0.0;
        do{
            msg = messages.next();
            String tmpValue = new String(msg.getData());
            sum += Double.parseDouble(tmpValue);
        }while(messages.hasNext());
        String newData = Double.toString(sum);
        msg = new BSPMessage(msg.getDstPartition(), msg.getDstVertexID(), newData.getBytes());
        
        return msg;
    }
}
public class CombinerToolTest {
    @Test
    public void testRun(){
        //message manager which hold the outoingQueues and incomingQueue
        MessageQueuesForMem messageQueues = new MessageQueuesForMem();

        //the msgQueue is to be sent by consumerTool

        String value = "10.0";
        BSPMessage msg1 = new BSPMessage(0, "001", Bytes.toBytes("tags1"),Bytes.toBytes(value));
        BSPMessage msg2 = new BSPMessage(0, "001", Bytes.toBytes("tags2"),Bytes.toBytes(value));
        BSPMessage msg3 = new BSPMessage(0, "003", Bytes.toBytes("tags3"),Bytes.toBytes(value));
        messageQueues.outgoAMessage("127.0.0.1:61616", msg1);
        messageQueues.outgoAMessage("127.0.0.1:61616", msg2);
        messageQueues.outgoAMessage("127.0.0.1:61616", msg3);
        
        assertEquals("Before combine, there are 3 messages in outgoingQueues.",
                3, messageQueues.getOutgoingQueuesSize());
        
        Combiner combiner = new SumCombiner();
        Sender sender = mock(Sender.class);
        when(sender.getNoMoreMessagesFlag()).thenReturn(false);
        CombinerTool combinertool = new CombinerTool(sender, messageQueues, combiner, 1);
        combinertool.start();
        while(true){
            if(messageQueues.getOutgoingQueuesSize() == 2){
                when(sender.getNoMoreMessagesFlag()).thenReturn(true);
                break;
            }
            
        }
        assertEquals("After combine, there are 2 messages in outgoingQueues.",
                2, messageQueues.getOutgoingQueuesSize());
        while(true){
            if(!combinertool.isAlive()){
                break;
            }
        }
    }
}
