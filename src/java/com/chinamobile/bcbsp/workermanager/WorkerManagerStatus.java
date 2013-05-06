/**
 * CopyRight by Chinamobile
 * 
 * WorkerManagerStatus.java
 */
package com.chinamobile.bcbsp.workermanager;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

import com.chinamobile.bcbsp.fault.storage.Fault;
import com.chinamobile.bcbsp.util.StaffStatus;

/**
 * WorkerManagerStatus
 * 
 * @author
 * @version
 */

public class WorkerManagerStatus implements Writable {
    public static final Log LOG = LogFactory.getLog(WorkerManagerStatus.class);

    static {
        WritableFactories.setFactory(WorkerManagerStatus.class,
                new WritableFactory() {
                    public Writable newInstance() {
                        return new WorkerManagerStatus();
                    }
                });
    }
    private List<Fault> workerFaultList;
    private String workerManagerName;
    private String rpc;
    private List<StaffStatus> staffReports;
    private int maxStaffsCount;
    private int runningStaffsCount;
    private int finishedStaffsCount;
    private int failedStaffsCount;
    volatile long lastSeen;
    volatile long pauseTime;

    public WorkerManagerStatus() {
        this.staffReports = new CopyOnWriteArrayList<StaffStatus>();
        this.workerFaultList = new ArrayList<Fault>();
    }

    public WorkerManagerStatus(String workerManagerName,
            List<StaffStatus> staffReports, int maxStaffsCount, int runningStaffsCount, 
            int finishedStaffsCount, int failedStaffsCount, String rpc) {
        this(workerManagerName, staffReports, maxStaffsCount, runningStaffsCount, 
                finishedStaffsCount, failedStaffsCount, rpc,new ArrayList<Fault>());
    }
    
    public WorkerManagerStatus(String workerManagerName,
            List<StaffStatus> staffReports, int maxStaffsCount,
            int runningStaffsCount, int finishedStaffsCount,
            int failedStaffsCount) {
        this(workerManagerName, staffReports,
                maxStaffsCount, runningStaffsCount, finishedStaffsCount,
                failedStaffsCount, "");
    }
    
    
    public WorkerManagerStatus(String workerManagerName, 
            List<StaffStatus> staffReports, int maxStaffsCount, int runningStaffsCount, 
            int finishedStaffsCount, int failedStaffsCount, String rpc,List<Fault> workerFaultList) {
        this.workerManagerName = workerManagerName;
        this.rpc = rpc;
        this.staffReports = new ArrayList<StaffStatus>(staffReports);
        this.maxStaffsCount = maxStaffsCount;
        this.runningStaffsCount = runningStaffsCount;
        this.finishedStaffsCount = finishedStaffsCount;
        this.failedStaffsCount = failedStaffsCount;
        this.workerFaultList = new ArrayList<Fault>(workerFaultList);
    }
    
    public String getWorkerManagerName() {
        return this.workerManagerName;
    }

    public String getRpcServer() {
        return this.rpc;
    }

    public List<StaffStatus> getStaffReports() {
        return this.staffReports;
    }

    public int getMaxStaffsCount() {
        return this.maxStaffsCount;
    }

    public void setRunningStaffsCount(int runningStaffsCount) {
        this.runningStaffsCount = runningStaffsCount;
    }

    public int getRunningStaffsCount() {
        return this.runningStaffsCount;
    }

    public int getFinishedStaffsCount() {
        return this.finishedStaffsCount;
    }

    public void setFailedStaffsCount(int failedStaffsCount) {
        this.failedStaffsCount = failedStaffsCount;
    }

    public int getFailedStaffsCount() {
        return this.failedStaffsCount;
    }

    public void setLastSeen(long lastSeen) {
        this.lastSeen = lastSeen;
    }

    public long getLastSeen() {
        return lastSeen;
    }
    
    public void setPauseTime(long pauseTime) {
        this.pauseTime = pauseTime;
    }
    
    public long getPauseTime() {
        return this.pauseTime;
    }

    public List<Fault> getWorkerFaultList() {
        return workerFaultList;
    }

    /**
     * For BSPController to distinguish between different WorkerManagers,
     * because BSPController stores using WorkerManagerStatus as key.
     */
    @Override
    public int hashCode() {
        int result = 17;
        result = 37 * result + workerManagerName.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object o) {
        WorkerManagerStatus s = ( WorkerManagerStatus ) o;
        if (s.getWorkerManagerName().equals(this.workerManagerName)) {
            return true;
        } else {
            return false;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.workerManagerName = Text.readString(in);
        this.rpc = Text.readString(in);

        int staffCount = in.readInt();
        StaffStatus status;
        this.staffReports.clear();
        for (int i = 0; i < staffCount; i++) {
            status = new StaffStatus();
            status.readFields(in);
            this.staffReports.add(status);
        }

        int workerFaultCount = in.readInt();
        Fault fault;
        this.workerFaultList.clear();
        for (int i = 0; i < workerFaultCount; i++) {
            fault = new Fault();
            fault.readFields(in);
            this.workerFaultList.add(fault);
        }
        this.maxStaffsCount = in.readInt();
        this.runningStaffsCount = in.readInt();
        this.finishedStaffsCount = in.readInt();
        this.failedStaffsCount = in.readInt();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, this.workerManagerName);
        Text.writeString(out, this.rpc);

        out.writeInt(this.staffReports.size());
        for (StaffStatus staffStatus : this.staffReports) {
            staffStatus.write(out);
        }
        
        out.writeInt(this.workerFaultList.size());
        for (Fault fault : this.workerFaultList) {
             fault.write(out);
        }
        
        out.writeInt(this.maxStaffsCount);
        out.writeInt(this.runningStaffsCount);
        out.writeInt(this.finishedStaffsCount);
        out.writeInt(this.failedStaffsCount);
    }
}
