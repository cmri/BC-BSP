/**
 * StaffStatus.jave
 */
package com.chinamobile.bcbsp.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.chinamobile.bcbsp.fault.storage.Fault;

/**
 * StaffStatus
 * 
 * Describes the current status of a staff. This is not intended to be a
 * comprehensive piece of data.
 * 
 * @author
 * @version
 */
public class StaffStatus implements Writable, Cloneable {
    static final Log LOG = LogFactory.getLog(StaffStatus.class);

    // enumeration for reporting current phase of a task.
    public static enum Phase {
        STARTING, COMPUTE, BARRIER_SYNC, CLEANUP
    }

    public static enum State {
        RUNNING, SUCCEEDED, FAULT, FAILED, UNASSIGNED, KILLED, COMMIT_PENDING, FAILED_UNCLEAN, KILLED_UNCLEAN, STAFF_RECOVERY, WORKER_RECOVERY
    }

    private BSPJobID jobId;
    private StaffAttemptID staffId;
    private float progress;
    private volatile State runState;
    private String stateString;
    private String groomServer;
    private long superstepCount;

    private long startTime;
    private long finishTime;

    private volatile Phase phase = Phase.STARTING;
    boolean recovery = false;
    int stage = 1;//0---before superStep 1---superSteping 2 ---after superStep

    private int faultFlag = 0;
    private Fault fault;
    /**
   * 
   */
    public StaffStatus() {
        jobId = new BSPJobID();
        staffId = new StaffAttemptID();
        fault = new Fault();
        this.superstepCount = 0;
    }

    public StaffStatus(BSPJobID jobId, StaffAttemptID staffId, float progress,
            State runState, String stateString, String groomServer, Phase phase) {
        this.jobId = jobId;
        this.staffId = staffId;
        this.progress = progress;
        this.runState = runState;
        this.stateString = stateString;
        this.groomServer = groomServer;
        this.phase = phase;
        this.superstepCount = 0;
    }

    // //////////////////////////////////////////////////
    // Accessors and Modifiers
    // //////////////////////////////////////////////////

    public int getStage() {
        return stage;
    }

    public void setStage(int stage) {
        this.stage = stage;
    }
    
    public boolean isRecovery() {
        return recovery;
    }

    public void setRecovery(boolean recoveried) {
        this.recovery = recoveried;
    }
    
    public BSPJobID getJobId() {
        return jobId;
    }

    public StaffAttemptID getStaffId() {
        return staffId;
    }

    public float getProgress() {
        return progress;
    }

    public void setProgress(float progress) {
        this.progress = progress;
    }

    public State getRunState() {
        return runState;
    }

    public void setRunState(State state) {
        this.runState = state;
    }

    public String getStateString() {
        return stateString;
    }

    public void setStateString(String stateString) {
        this.stateString = stateString;
    }

    public String getGroomServer() {
        return groomServer;
    }

    public void setGroomServer(String groomServer) {
        this.groomServer = groomServer;
    }

    public long getFinishTime() {
        return finishTime;
    }

    void setFinishTime(long finishTime) {
        this.finishTime = finishTime;
    }

    /**
     * Get start time of the task.
     * 
     * @return 0 is start time is not set, else returns start time.
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Set startTime of the task.
     * 
     * @param startTime
     *            start time
     */
    void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    /**
     * Get current phase of this task.
     * 
     * @return .
     */
    public Phase getPhase() {
        return this.phase;
    }

    /**
     * Set current phase of this task.
     * 
     * @param phase
     *            phase of this task
     */
    public void setPhase(Phase phase) {
        this.phase = phase;
    }

    /**
     * Update the status of the task.
     * 
     * This update is done by ping thread before sending the status.
     * 
     * @param progress
     * @param state
     * @param counters
     */
    synchronized void statusUpdate(float progress, String state) {
        setProgress(progress);
        setStateString(state);
    }

    /**
     * Update the status of the task.
     * 
     * @param status
     *            updated status
     */
    synchronized void statusUpdate(StaffStatus status) {
        this.progress = status.getProgress();
        this.runState = status.getRunState();
        this.stateString = status.getStateString();

        if (status.getStartTime() != 0) {
            this.startTime = status.getStartTime();
        }
        if (status.getFinishTime() != 0) {
            this.finishTime = status.getFinishTime();
        }

        this.phase = status.getPhase();
    }

    /**
     * Update specific fields of task status
     * 
     * This update is done in BSPMaster when a cleanup attempt of task reports
     * its status. Then update only specific fields, not all.
     * 
     * @param runState
     * @param progress
     * @param state
     * @param phase
     * @param finishTime
     */
    synchronized void statusUpdate(State runState, float progress,
            String state, Phase phase, long finishTime) {
        setRunState(runState);
        setProgress(progress);
        setStateString(state);
        setPhase(phase);
        if (finishTime != 0) {
            this.finishTime = finishTime;
        }
    }

    /**
     * @return The number of BSP super steps executed by the task.
     */
    public long getSuperstepCount() {
        return superstepCount;
    }
    
    public Fault getFault() {
        return fault;
    }

    public void setFault(Fault fault) {
        this.fault = fault;
        this.faultFlag = 1;
    }

    /**
     * Increments the number of BSP super steps executed by the task.
     */
    public void incrementSuperstepCount() {
        superstepCount += 1;
    }

    @Override
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException cnse) {
            // Shouldn't happen since we do implement Clonable
            throw new InternalError(cnse.toString());
        }
    }

    // ////////////////////////////////////////////
    // Writable
    // ////////////////////////////////////////////

    @Override
    public void readFields(DataInput in) throws IOException {
      this.jobId.readFields(in);
      this.staffId.readFields(in);
      this.progress = in.readFloat();
      this.runState = WritableUtils.readEnum(in, State.class);
      this.stateString = Text.readString(in);
      this.phase = WritableUtils.readEnum(in, Phase.class);
      this.startTime = in.readLong();
      this.finishTime = in.readLong();
      this.superstepCount = in.readLong();
      this.faultFlag = in.readInt();
      if(this.faultFlag == 1){
          this.fault.readFields(in);
      }
      this.recovery = in.readBoolean();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      jobId.write(out);
      staffId.write(out);
      out.writeFloat(progress);
      WritableUtils.writeEnum(out, runState);
      Text.writeString(out, stateString);
      WritableUtils.writeEnum(out, phase);
      out.writeLong(startTime);
      out.writeLong(finishTime);
      out.writeLong(superstepCount);
     if(this.faultFlag == 0){
         out.writeInt(this.faultFlag);
      }else{
          out.writeInt(this.faultFlag);
      this.fault.write(out);
      }
     out.writeBoolean(recovery);
    }
}
