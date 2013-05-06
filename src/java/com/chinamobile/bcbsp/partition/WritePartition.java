/**
 * CopyRight by Chinamobile
 * 
 * WritePartition.java
 */
package com.chinamobile.bcbsp.partition;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;

import com.chinamobile.bcbsp.api.Partitioner;
import com.chinamobile.bcbsp.api.RecordParse;
import com.chinamobile.bcbsp.bspstaff.BSPStaff;
import com.chinamobile.bcbsp.bspstaff.BSPStaff.WorkerAgentForStaffInterface;
import com.chinamobile.bcbsp.io.*;
import com.chinamobile.bcbsp.sync.StaffSSControllerInterface;
import com.chinamobile.bcbsp.sync.SuperStepReportContainer;
/**
 * WritePartition
 * 
 * This abstract class is the primary interface for users to define their own
 * partition method.The user must provide a no-argument constructor.
 * 
 * @author
 * @version
 */
public abstract class WritePartition {

    protected WorkerAgentForStaffInterface workerAgent = null;
    protected BSPStaff staff = null;
    protected Partitioner<Text> partitioner = null;
    protected RecordParse recordParse = null;
    protected String separator = ":";
    protected StaffSSControllerInterface sssc;
    protected SuperStepReportContainer ssrc;
    protected int TotalCacheSize = 0;
    protected int sendThreadNum=0;
    
    protected SerializationFactory serializationFactory = new SerializationFactory(
            new Configuration());
    protected Serializer<Text> keyserializer = serializationFactory
            .getSerializer(Text.class); 
    protected Serializer<Text> valueserializer = serializationFactory
    .getSerializer(Text.class); 
    
    public void setWorkerAgent(WorkerAgentForStaffInterface workerAgent) {
        this.workerAgent = workerAgent;
    }

    public void setStaff(BSPStaff staff) {
        this.staff = staff;
    }

    public void setPartitioner(Partitioner<Text> partitioner) {
        this.partitioner = partitioner;
    }

    public void setSssc(StaffSSControllerInterface sssc) {
        this.sssc = sssc;
    }

    public void setSsrc(SuperStepReportContainer ssrc) {
        this.ssrc = ssrc;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }
    
    public void setRecordParse(RecordParse recordParse) {
        this.recordParse = recordParse;
    }

    public void setTotalCatchSize(int TotalCacheSize) {
        this.TotalCacheSize = TotalCacheSize;
    }

    public void setSendThreadNum(int sendThreadNum){
        this.sendThreadNum=sendThreadNum;
    }

    /**
     * This method is used to partition graph vertexes. Writing Each vertex to
     * the corresponding partition. In this method calls recordParse method to
     * create an HeadNode object. The last call partitioner's getPartitionId
     * method to calculate the HeadNode belongs to partition's id. If the
     * HeadNode belongs local partition then written to the local partition or
     * send it to the appropriate partition.
     * 
     * @param recordReader
     * @throws IOException
     * @throws InterruptedException
     */
    @SuppressWarnings("unchecked")
    public abstract void write(RecordReader recordReader) throws IOException, InterruptedException;
}
