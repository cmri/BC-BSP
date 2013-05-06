/**
 * CopyRight by Chinamobile
 * 
 * BSPInterface.java
 */
package com.chinamobile.bcbsp.api;

import java.util.Iterator;

import com.chinamobile.bcbsp.bspstaff.BSPStaffContextInterface;
import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.bspstaff.SuperStepContextInterface;
import com.chinamobile.bcbsp.comm.BSPMessage;

/**
 * BSPInterface
 * 
 * Interface BSP defines the basic operations needed to implement the BSP
 * algorithm.
 * 
 * @author
 * @version
 */
public interface BSPInterface {

    /**
     * Setup before invoking the {@link com.chinamobile.bcbsp.staff.BSPInterface.compute} function.
     * User can make some preparations in this function.
     * 
     * @param staff
     */
    public void setup(Staff staff);
    
    /**
     * Initialize before each super step.
     * User can init some global variables for each super step.
     * 
     * @param context SuperStepContextInterface
     */
    public void initBeforeSuperStep(SuperStepContextInterface context);
    
    /**
     * A user defined function for programming in the BSP style.
     * 
     * Applications can use the {@link com.chinamobile.bcbsp.bsp.WorkerAgent} to
     * handle the communication and synchronization between processors.
     * 
     * @param Iterator<BSPMessage> messages
     * @param BSPStaffContextInterface context
     * @throws Exception
     */
    public void compute(Iterator<BSPMessage> messages, BSPStaffContextInterface context) throws Exception;

    /**
     * Cleanup after finishing the staff.
     * User can define the specific work in this function.
     * 
     * @param staff
     */
    public void cleanup(Staff staff);

}
