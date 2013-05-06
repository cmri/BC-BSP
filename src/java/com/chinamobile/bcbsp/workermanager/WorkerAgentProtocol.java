/**
 * CopyRight by Chinamobile
 * 
 * WorkerAgentProtocol.java
 */
package com.chinamobile.bcbsp.workermanager;

import java.io.IOException;

import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.StaffAttemptID;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.fault.storage.Fault;

/**
 * Protocol that staff child process uses to contact its parent process.
 */
public interface WorkerAgentProtocol extends WorkerAgentInterface {

    /** Called when a child staff process starts, to get its staff. */
    Staff getStaff(StaffAttemptID staffid) throws IOException;

    /**
     * Periodically called by child to check if parent is still alive.
     * 
     * @return True if the staff is known
     */
    boolean ping(StaffAttemptID staffid) throws IOException;

    /**
     * Report that the staff is successfully completed. Failure is assumed if the
     * staff process exits without calling this.
     * 
     * @param staffid
     *            staff's id
     * @param shouldBePromoted
     *            whether to promote the staff's output or not
     */
    void done(StaffAttemptID staffid, boolean shouldBePromoted)
            throws IOException;

    /** Report that the staff encounted a local filesystem error. */
    void fsError(StaffAttemptID staffId, String message) throws IOException;

    /**
     * The function is used for detect fault.
     * 
     * @param normal
     * @param info
     */
    public void setStaffStatus(StaffAttemptID staffId, int staffStatus, Fault fault, int stage);
    
    public boolean getStaffRecoveryState(StaffAttemptID staffId);
    public boolean getStaffChangeWorkerState(StaffAttemptID staffid);
    public int getFailCounter(StaffAttemptID staffId);
    
    public void addStaffReportCounter(BSPJobID jobId);
}
