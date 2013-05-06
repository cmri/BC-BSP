/**
 * CopyRight by Chinamobile
 * 
 * WorkerSSControllerInterface.java
 */
package com.chinamobile.bcbsp.sync;

/**
 * WorkerSSControllerInterface
 * 
 * WorkerSSControllerInterface for local synchronization and aggregation. This class is
 * connected to WorkerAgentForJob.
 * 
 * @author
 * @version
 */

public interface WorkerSSControllerInterface {

    /**
     * Make sure that all staffs have completed the local computation and
     * message-receiving.
     * 
     * @param superStepCounter
     * @param ssrc
     * @return
     */
    public boolean firstStageSuperStepBarrier(int superStepCounter,
            SuperStepReportContainer ssrc);

    /**
     * Report the local information
     * 
     * @param superStepCounter
     * @param ssrc
     * @return
     */
    public boolean secondStageSuperStepBarrier(int superStepCounter,
            SuperStepReportContainer ssrc);

    /**
     * Make sure that all staffs have completed the checkpoint-write operation.
     * 
     * @param superStepCounter
     * @param ssrc
     * @return
     */
    public boolean checkPointStageSuperStepBarrier(int superStepCounter,
            SuperStepReportContainer ssrc);

    /**
     * Make sure that all staffs have saved the computation result and the job
     * finished successfully.
     * 
     * @param superStepCounter
     * @param ssrc
     * @return
     */
    public boolean saveResultStageSuperStepBarrier(int superStepCounter,
            SuperStepReportContainer ssrc);

}
