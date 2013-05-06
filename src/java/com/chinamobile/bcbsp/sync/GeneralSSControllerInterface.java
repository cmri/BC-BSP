/**
 * CopyRight by Chinamobile
 * 
 * GeneralSSControllerInterface.java
 */
package com.chinamobile.bcbsp.sync;

import java.util.List;

import com.chinamobile.bcbsp.bspcontroller.JobInProgressControlInterface;

/**
 * GeneralSSControllerInterface
 * 
 * This is the interface class for controlling the general
 * SuperStep synchronization.
 * 
 * @author
 * @version
 */

public interface GeneralSSControllerInterface {

    /**
     * Set the handle of JobInProgress in GeneralSSController.
     * 
     * @param jip
     */
    public void setJobInProgressControlInterface(
            JobInProgressControlInterface jip);

    /**
     * Set the checkNumBase.
     * 
     * @param jip
     */
    public void setCheckNumBase();

    /**
     * Prepare to SuperStep.
     */
    public void setup();

    /**
     * Cleanup after the job is finished.
     */
    public void cleanup();

    /**
     * Start the SuperStepControl.
     */
    public void start();

    /**
     * Stop the SuperStepControl.
     */
    public void stop();

    /**
     * First stage of one SuperStep: make sure that all staffs have completed
     * the local work.
     * 
     * @param checkNum
     * @return
     */
    public boolean generalSuperStepBarrier(int checkNum);

    /**
     * Second stage of SuperStep: get the relative information and local
     * aggregation values and generate the SuperStepCommand.
     * 
     * @param checkNum
     * @return
     */
    public SuperStepCommand getSuperStepCommand(int checkNum);

    /**
     * The job has finished.
     * 
     * @return
     */
    public boolean quitBarrier();
    
    public void recoveryBarrier(List<String> WMNames);
    
    /**
     * Only for fault-tolerance.
     * If the command has been write on the ZooKeeper, return true, else return false.
     * 
     * @return
     */
    public boolean isCommandBarrier();
}
