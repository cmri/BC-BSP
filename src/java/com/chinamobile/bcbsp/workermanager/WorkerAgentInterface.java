/**
 * CopyRight by Chinamobile
 * 
 * WorkerAgentInterface.java
 */
package com.chinamobile.bcbsp.workermanager;

import java.io.Closeable;

import com.chinamobile.bcbsp.Constants;
import com.chinamobile.bcbsp.rpc.BSPRPCProtocolVersion;
import com.chinamobile.bcbsp.sync.SuperStepReportContainer;
import com.chinamobile.bcbsp.util.BSPJobID;
import com.chinamobile.bcbsp.util.StaffAttemptID;

/**
 * Worker Agent Interface.
 * 
 * @author
 * @version
 */
public interface WorkerAgentInterface extends BSPRPCProtocolVersion, Closeable,
        Constants {

    /**
     * All staffs belongs to the same job will use this to complete the local
     * synchronization and aggregation.
     * 
     * @param jobId
     * @param staffId
     * @param superStepCounter
     * @param args
     * @return
     */
    public boolean localBarrier(BSPJobID jobId, StaffAttemptID staffId,
            int superStepCounter, SuperStepReportContainer ssrc);

    /**
     * Get the number of workers which run the job
     * 
     * @param jobId
     * @param staffId
     * @return
     */
    public int getNumberWorkers(BSPJobID jobId, StaffAttemptID staffId);

    /**
     * Set the number of workers which run the job
     * 
     * @param jobId
     * @param staffId
     */
    public void setNumberWorkers(BSPJobID jobId, StaffAttemptID staffId, int num);

    /**
     * @return The workerManagerName.
     */
    public String getWorkerManagerName(BSPJobID jobId, StaffAttemptID staffId);

    /**
     * NEU change in version-0.2.5.1 new function: Get WorkerAgent BSPJobID
     * 
     * @return BSPJobID
     */
    public BSPJobID getBSPJobID();

    /**
     * This method is used to set mapping table that shows the partition to the
     * worker.
     * 
     * @param jobId
     * @param partitionId
     * @param hostName
     */
    public void setWorkerNametoPartitions(BSPJobID jobId, int partitionId,
            String hostName);
    
    /**
     * Get a free port.
     * 
     * @return port
     */
    public int getFreePort();
    
    /**
     * Set the WorkerAgentStaffInterface's address for the staff with staffID.
     * 
     * @param staffID
     * @param addr
     */
    public void setStaffAgentAddress(StaffAttemptID staffID, String addr);
}
