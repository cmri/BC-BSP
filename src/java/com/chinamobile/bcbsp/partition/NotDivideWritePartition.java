/**
 * CopyRight by Chinamobile
 * 
 * NotDivideWritePartition.java
 */
package com.chinamobile.bcbsp.partition;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.io.RecordReader;
import com.chinamobile.bcbsp.api.Partitioner;
import com.chinamobile.bcbsp.bspstaff.*;
import com.chinamobile.bcbsp.bspstaff.BSPStaff.WorkerAgentForStaffInterface;
/**
 * NotDivideWritePartition
 * 
 * Implements the partition method which don't need to divide.The user must provide a no-argument constructor.
 * 
 * @author
 * @version
 */
public class NotDivideWritePartition extends WritePartition {
    public static final Log LOG = LogFactory
            .getLog(NotDivideWritePartition.class);

    public NotDivideWritePartition() {
    }

    public NotDivideWritePartition(WorkerAgentForStaffInterface workerAgent,
            BSPStaff staff, Partitioner<Text> partitioner) {
        this.workerAgent = workerAgent;
        this.staff = staff;
        this.partitioner = partitioner;
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
    @Override
    public void write(RecordReader recordReader) throws IOException,
            InterruptedException {
        int headNodeNum = 0;
        int local = 0;
        int lost = 0;
        
        try {
            while (recordReader != null && recordReader.nextKeyValue()) {
                headNodeNum++;

                Text key = new Text(recordReader.getCurrentKey().toString());
                Text value = new Text(recordReader.getCurrentValue().toString());

                Vertex vertex = this.recordParse.recordParse(
                        key.toString(), value.toString());

                if (vertex == null) {
                    lost++;
                    continue;
                }
                staff.getGraphData().addForAll(vertex);
                local++;

            }
            LOG.info("The number of vertices that were read from the input file: "
                    + headNodeNum);
            LOG.info("The number of vertices that were put into the partition: "
                    + local);
            LOG.info("The number of vertices that were sent to other partitions: " + 0);
            LOG.info("The number of verteices in the partition that cound not be parsed:"+lost);

        } catch (IOException e) {
            throw e;
        } catch (InterruptedException e) {
            throw e;
        }
    }
}
