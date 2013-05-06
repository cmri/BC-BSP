/**
 * CopyRight by Chinamobile
 * 
 * Staff.java
 */
package com.chinamobile.bcbsp.bspstaff;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.chinamobile.bcbsp.graph.GraphDataFactory;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.workermanager.WorkerAgentProtocol;
import com.chinamobile.bcbsp.workermanager.WorkerManager;

/**
 * Staff
 * 
 * Base class for Staff.
 * 
 * @author
 * @version
 */
public abstract class Staff implements Writable {

    protected BSPJobID jobId;
    protected String jobFile;
    protected StaffAttemptID sid;
    protected int partition = 0;
    protected int failCounter = 0;
    protected GraphDataFactory graphDataFactory;

    protected LocalDirAllocator lDirAlloc;
    //route table
    protected HashMap<Integer, String> partitionToWorkerManagerNameAndPort = new HashMap<Integer, String>();

    public Staff() {
        jobId = new BSPJobID();
        sid = new StaffAttemptID();
    }
    
    public GraphDataFactory getGraphDataFactory(){
        return graphDataFactory;
    }

    public Staff(BSPJobID jobId, String jobFile, StaffAttemptID sid,
            int partition, String splitClass, BytesWritable split) {
        this.jobId = jobId;
        this.jobFile = jobFile;
        this.sid = sid;
        this.partition = partition;
    }

    public void setJobFile(String jobFile) {
        this.jobFile = jobFile;
    }

    public String getJobFile() {
        return jobFile;
    }

    public StaffAttemptID getStaffAttemptId() {
        return this.sid;
    }

    public StaffAttemptID getStaffID() {
        return sid;
    }

    public void setFailCounter(int failCounter) {
        this.failCounter = failCounter;
    }
    
    public int getFailCounter() {
        return this.failCounter;
    }
    
    /**
     * Get the job name for this task.
     * 
     * @return the job name
     */
    public BSPJobID getJobID() {
        return jobId;
    }

    /**
     * Get the index of this task within the job.
     * 
     * @return the integer part of the task id
     */
    public int getPartition() {
        return partition;
    }

    @Override
    public String toString() {
        return sid.toString();
    }
    
    public void setPartitionToWorkerManagerNameAndPort(
            HashMap<Integer, String> partitionToWorkerManagerNameAndPort) {
        this.partitionToWorkerManagerNameAndPort = partitionToWorkerManagerNameAndPort;
    }

    public HashMap<Integer, String> getPartitionToWorkerManagerNameAndPort() {
        return this.partitionToWorkerManagerNameAndPort;
    }

    /** Write and read */
    @Override
    public void write(DataOutput out) throws IOException {
        jobId.write(out);
        Text.writeString(out, jobFile);
        sid.write(out);
        out.writeInt(partition);
        out.writeInt(this.failCounter);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        jobId.readFields(in);
        jobFile = Text.readString(in);
        sid.readFields(in);
        partition = in.readInt();
        this.failCounter = in.readInt();
    }

    /**
     * Run this staff as a part of the named job. This method is executed in the
     * child process.
     * 
     * @param umbilical
     *            for progress reports
     * @throws ClassNotFoundException
     * @throws InterruptedException 
     */
    public abstract void run(BSPJob job, Staff task,
            WorkerAgentProtocol umbilical, boolean recovery, 
            boolean changeWorkerState, int failCounter, String hostName) throws IOException,
            ClassNotFoundException, InterruptedException;

    public abstract BSPStaffRunner createRunner(WorkerManager groom);

    public void done(WorkerAgentProtocol umbilical) throws IOException {
        umbilical.done(getStaffID(), true);
    }

    public abstract BSPJob getConf();

    public abstract void setConf(BSPJob localJobConf);

}
