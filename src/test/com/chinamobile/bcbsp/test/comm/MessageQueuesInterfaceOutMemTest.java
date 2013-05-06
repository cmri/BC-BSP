/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.test.comm;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.Random;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.comm.BSPMessage;
import com.chinamobile.bcbsp.comm.MessageQueuesForDisk;
import com.chinamobile.bcbsp.comm.MessageQueuesInterface;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.util.BSPJobID;

public class MessageQueuesInterfaceOutMemTest {
    private static BSPJob job;
    private static int partitionId = 0;
    private long maxHeapSize;
    MessageQueuesInterface msgQueues;
    BSPMessage msg1 ,msg2, msg3;
    @Before
    public void setUp(){
        job = mock(BSPJob.class);
        when(job.getMemoryDataPercent()).thenReturn(0.8f);
        when(job.getBeta()).thenReturn(0.5f);
        when(job.getHashBucketNumber()).thenReturn(32);
        when(job.getJobID()).thenReturn(new BSPJobID("jtIdentifier",2));
        job.getBeta();
        
        // Get the memory mxBean.
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        // Get the heap memory usage.
        MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();
        maxHeapSize = memoryUsage.getMax();
    }
    
    @After
    public void tearDown() throws Exception {
       msgQueues.clearAllQueues(); 
    }
    
    @SuppressWarnings("static-access")
    @Test
    public void testDisk(){
        
        msgQueues = new MessageQueuesForDisk(this.job, this.partitionId);
        Random rd = new Random();
        int v;
        byte [] data = new byte[1024];//each message's size is 1k
        for(int i = 0; i <maxHeapSize*1.5/1024; i ++){
            v = rd.nextInt(1024*800);
            BSPMessage msg = new BSPMessage(v%20, String.valueOf(v), Bytes.toBytes("tags1"),data); 
            msgQueues.incomeAMessage(msg.getDstVertexID(), msg);
        }
    }
}
