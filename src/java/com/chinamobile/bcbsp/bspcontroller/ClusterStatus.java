/**
 * CopyRight by Chinamobile
 * 
 * ClusterStatus.java
 */
package com.chinamobile.bcbsp.bspcontroller;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * ClusterStatus
 * 
 * @author
 * @version
 */
public class ClusterStatus implements Writable {

    private int activeWorkerManagersCount;
    private String[] activeWorkerManagersName;
    private int maxClusterStaffs;
    private int runningClusterStaffs;
    private BSPController.State state;

    
    public ClusterStatus() {
        
    }

    public ClusterStatus(int activeWorkerManagersCount, int maxClusterStaffs,
            int runningClusterStaffs, BSPController.State state) {
        this.activeWorkerManagersCount = activeWorkerManagersCount;
        this.maxClusterStaffs = maxClusterStaffs;
        this.runningClusterStaffs = runningClusterStaffs;
        this.state = state;
    }

    public ClusterStatus(String[] activeWorkerManagersName,
            int maxClusterStaffs, int runningClusterStaffs,
            BSPController.State state) {
        this(activeWorkerManagersName.length, maxClusterStaffs,
                runningClusterStaffs, state);
        this.activeWorkerManagersName = activeWorkerManagersName;
    }

    public int getActiveWorkerManagersCount() {
        return this.activeWorkerManagersCount;
    }

    public String[] getActiveWorkerManagersName() {
        return this.activeWorkerManagersName;
    }

    public int getMaxClusterStaffs() {
        return this.maxClusterStaffs;
    }

    public int getRunningClusterStaffs() {
        return this.runningClusterStaffs;
    }

    public BSPController.State getBSPControllerState() {
        return this.state;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.activeWorkerManagersCount);
        if (this.activeWorkerManagersCount == 0) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            WritableUtils.writeCompressedStringArray(out,
                    this.activeWorkerManagersName);
        }
        out.writeInt(this.maxClusterStaffs);
        out.writeInt(this.runningClusterStaffs);
        WritableUtils.writeEnum(out, this.state);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.activeWorkerManagersCount = in.readInt();
        boolean detailed = in.readBoolean();
        if (detailed) {
            this.activeWorkerManagersName = WritableUtils
                    .readCompressedStringArray(in);
        }
        this.maxClusterStaffs = in.readInt();
        this.runningClusterStaffs = in.readInt();
        this.state = WritableUtils.readEnum(in, BSPController.State.class);
    }
}
