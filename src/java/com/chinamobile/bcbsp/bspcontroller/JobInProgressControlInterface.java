/**
 * CopyRight by Chinamobile
 * 
 * JobInProgressControlInterface.java
 */
package com.chinamobile.bcbsp.bspcontroller;

import com.chinamobile.bcbsp.sync.SuperStepCommand;
import com.chinamobile.bcbsp.sync.SuperStepReportContainer;

/**
 * JobInProgressControlInterface. 
 * It is implemented in JobInProgress
 * 
 * @author
 * @version
 */

public interface JobInProgressControlInterface {

    /**
     * Set the SuperStepCounter in JobInProgress
     * 
     * @param superStepCounter
     */
    public void setSuperStepCounter(int superStepCounter);

    /**
     * Get the SuperStepCounter from JobInProgress
     * 
     * @return
     */
    public int getSuperStepCounter();

    public void setAbleCheckPoint(int ableCheckPoint);

    /**
     * Get the number of BSPStaffs in the job
     * 
     * @return
     */
    public int getNumBspStaff();

    /**
     * Get the CheckNum for SuperStep
     * 
     * @return
     */
    public int getCheckNum();

    /**
     * Get the SuperStepCommand for the next SuperStep according to the
     * SuperStepReportContainer. The SuperStepCommand include the general
     * aggregation information.
     * 
     * @param ssrcs
     * @return
     */
    public SuperStepCommand generateCommand(SuperStepReportContainer[] ssrcs);

    /**
     * The job has been completed.
     */
    public void completedJob();

    /**
     * The job is failed.
     */
    public void failedJob();

    /**
     * Output the information of log in JobInProgressControlInterface.
     * 
     * @param log
     */
    public void reportLOG(String log);
}
