/**
 * CopyRight by Chinamobile
 * 
 * StaffSSControllerInterface.java
 */
package com.chinamobile.bcbsp.sync;

import java.util.HashMap;
import java.util.List;

/**
 * StaffSSControllerInterface
 * 
 * StaffSSController for completing the staff SuperStep synchronization control.
 * This class is connected to BSPStaff.
 * 
 * @author
 * @version
 */

public interface StaffSSControllerInterface {

    /**
     * Make sure than all staffs have started successfully. This function should
     * create the route table.
     * 
     * @param ssrc
     * @return
     */
    public HashMap<Integer, String> scheduleBarrier(
            SuperStepReportContainer ssrc);

    /**
     * Make sure that all staffs complete loading data This function is used for
     * Constatns.PARTITION_TYPE.RANGE
     * 
     * @param ssrc
     * @return
     */
    public HashMap<Integer, List<Integer>> loadDataBarrier(
            SuperStepReportContainer ssrc);

    /**
     * Make sure that all staffs complete loading data This function is used for
     * Constatns.PARTITION_TYPE.HASH
     * 
     * @param ssrc
     * @param partitionType
     * @return
     */
    public boolean loadDataBarrier(SuperStepReportContainer ssrc,
            String partitionType);

    /**
     * Make sure that all staffs complete Balancing data This function is used
     * for Constatns.PARTITION_TYPE.HASH
     * 
     * @param ssrc
     * @param partitionType
     * @return
     */

    public HashMap<Integer, Integer> loadDataInBalancerBarrier(
            SuperStepReportContainer ssrc, String partitionType);

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
     * Report the local information and get the next SuperStep command from
     * JobInProgress
     * 
     * @param superStepCounter
     * @param ssrc
     * @return
     */
    public SuperStepCommand secondStageSuperStepBarrier(int superStepCounter,
            SuperStepReportContainer ssrc);

    /**
     * Make sure that all staffs have completed the checkpoint-write operation.
     * 
     * @param superStepCounter
     * @param ssrc
     * @return
     */
    public HashMap<Integer, String> checkPointStageSuperStepBarrier(int superStepCounter,
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
    
    public SuperStepCommand secondStageSuperStepBarrierForRecovery(int superStepCounter);
}
