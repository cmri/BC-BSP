/**
 * CopyRight by Chinamobile
 * 
 * HashWritePartition.java
 */
package com.chinamobile.bcbsp.partition;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;

import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.io.RecordReader;
import com.chinamobile.bcbsp.util.ThreadPool;
import com.chinamobile.bcbsp.util.ThreadSignle;
import com.chinamobile.bcbsp.api.Partitioner;
import com.chinamobile.bcbsp.bspstaff.*;
import com.chinamobile.bcbsp.bspstaff.BSPStaff.WorkerAgentForStaffInterface;
/**
 * HashWritePartition
 * 
 * Implements hash-based partition method.The user must provide a no-argument constructor.
 * 
 * @author
 * @version
 */
public class HashWritePartition extends WritePartition {
    public static final Log LOG = LogFactory.getLog(HashWritePartition.class);

    public HashWritePartition() {
    }

    public HashWritePartition(WorkerAgentForStaffInterface workerAgent,
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
        int send = 0;
        int lost = 0;
        ThreadPool tpool = new ThreadPool(this.sendThreadNum);

        int bufferSize = ( int ) ((this.TotalCacheSize * 1024 * 1024) / (this.staff
                .getStaffNum() + this.sendThreadNum));
        byte[][] buffer = new byte[this.staff.getStaffNum()][bufferSize];
        int bufindex[] = new int[this.staff.getStaffNum()];

        BytesWritable kbytes = new BytesWritable();
        int ksize = 0;
        BytesWritable vbytes = new BytesWritable();
        int vsize = 0;
        DataOutputBuffer bb = new DataOutputBuffer();

        try {
            this.keyserializer.open(bb);
            this.valueserializer.open(bb);
        } catch (IOException e) {
            throw e;
        }

        
        try {
            while (recordReader != null && recordReader.nextKeyValue()) {
                headNodeNum++;

                Text key = new Text(recordReader.getCurrentKey().toString());
                Text value = new Text(recordReader.getCurrentValue().toString());
                //LOG.info("KEY:" + key.toString() + "\tVALUE:" + value.toString());
                int pid = -1;
                Text vertexID = this.recordParse.getVertexID(key);
                if (vertexID != null) {
                    pid = this.partitioner.getPartitionID(vertexID);
                } else {
                    lost++;
                    continue;
                }

                if (pid == this.staff.getPartition()) {
                    local++;

                    Vertex vertex = this.recordParse.recordParse(
                            key.toString(), value.toString());
                    if (vertex == null){
                         lost++;
                         continue;
                    }
                       
                    staff.getGraphData().addForAll(vertex);

                } else {
                    send++;

                    bb.reset();
                    this.keyserializer.serialize(key);
                    kbytes.set(bb.getData(), 0, bb.getLength());
                    ksize = kbytes.getLength();

                    bb.reset();
                    this.valueserializer.serialize(value);
                    vbytes.set(bb.getData(), 0, bb.getLength());
                    vsize = vbytes.getLength();

                    if ((buffer[pid].length - bufindex[pid]) > (ksize + vsize)) {
                        // There is enough space
                        System.arraycopy(kbytes.getBytes(), 0, buffer[pid],
                                bufindex[pid], ksize);
                        bufindex[pid] += ksize;

                        System.arraycopy(vbytes.getBytes(), 0, buffer[pid],
                                bufindex[pid], vsize);
                        bufindex[pid] += vsize;

                    } else if (buffer[pid].length < (ksize + vsize)) {
                        // Super record. Record size is greater than the
                        // capacity of
                        // the buffer. So sent directly.

                        ThreadSignle t = tpool.getThread();
                        while (t == null) {
                            t = tpool.getThread();
                        }

                        t.setWorker(this.workerAgent.getWorker(
                                staff.getJobID(), staff.getStaffID(), pid));
                        t.setJobId(staff.getJobID());
                        t.setTaskId(staff.getStaffID());
                        t.setBelongPartition(pid);

                        BytesWritable data = new BytesWritable();
                        byte[] tmp = new byte[vsize + ksize];
                        System.arraycopy(kbytes.getBytes(), 0, tmp, 0, ksize);
                        System.arraycopy(vbytes.getBytes(), 0, tmp, ksize,
                                vsize);
                        data.set(tmp, 0, (ksize + vsize));
                        t.setData(data);
                        tmp = null;
                        LOG.info("Using Thread is: " + t.getThreadNumber());
                        LOG.info("this is a super record");
                        t.setStatus(true);
                    } else {// Not enough space

                        ThreadSignle t = tpool.getThread();
                        while (t == null) {
                            t = tpool.getThread();
                        }

                        t.setWorker(this.workerAgent.getWorker(
                                staff.getJobID(), staff.getStaffID(), pid));
                        t.setJobId(staff.getJobID());
                        t.setTaskId(staff.getStaffID());
                        t.setBelongPartition(pid);

                        BytesWritable data = new BytesWritable();
                        data.set(buffer[pid], 0, bufindex[pid]);
                        t.setData(data);

                        LOG.info("Using Thread is: " + t.getThreadNumber());

                        t.setStatus(true);

                        bufindex[pid] = 0;
                        // store data
                        System.arraycopy(kbytes.getBytes(), 0, buffer[pid],
                                bufindex[pid], ksize);
                        bufindex[pid] += ksize;

                        System.arraycopy(vbytes.getBytes(), 0, buffer[pid],
                                bufindex[pid], vsize);
                        bufindex[pid] += vsize;
                    }

                }
            }

            for (int i = 0; i < this.staff.getStaffNum(); i++) {
                if (bufindex[i] != 0) {
                    
                    ThreadSignle t = tpool.getThread();
                    while (t == null) {
                        t = tpool.getThread();
                    }

                    t.setWorker(this.workerAgent.getWorker(staff.getJobID(),
                            staff.getStaffID(), i));
                    t.setJobId(staff.getJobID());
                    t.setTaskId(staff.getStaffID());
                    t.setBelongPartition(i);

                    BytesWritable data = new BytesWritable();
                    data.set(buffer[i], 0, bufindex[i]);
                    t.setData(data);
                    LOG.info("Using Thread is: " + t.getThreadNumber());

                    t.setStatus(true);

                }
            }

            tpool.cleanup();
            tpool = null;
            buffer = null;
            bufindex = null;

            LOG.info("The number of vertices that were read from the input file: "
                    + headNodeNum);
            LOG.info("The number of vertices that were put into the partition: "
                    + local);
            LOG.info("The number of vertices that were sent to other partitions: "
                    + send);
            LOG.info("The number of verteices in the partition that cound not be parsed:"+lost);

        } catch (IOException e) {
            throw e;
        } catch (InterruptedException e) {
            throw e;
        }
    }
}
