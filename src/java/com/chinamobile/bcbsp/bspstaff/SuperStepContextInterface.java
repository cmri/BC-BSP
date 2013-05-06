/**
 * CopyRight by Chinamobile
 * 
 * SuperStepContextInterface.java
 */
package com.chinamobile.bcbsp.bspstaff;

import com.chinamobile.bcbsp.api.AggregateValue;
import com.chinamobile.bcbsp.util.BSPJob;

/**
 * SuperStepContextInterface
 * This is a context for init before each super step.
 * 
 * @author Bai Qiushi
 * @version 1.0
 */
public interface SuperStepContextInterface {

    /**
     * Get the current superstep counter.
     * 
     * @return
     */
    public int getCurrentSuperStepCounter();
    
    /**
     * Get the BSP Job Configuration.
     * 
     * @return
     */
    public BSPJob getJobConf();
    
    /**
     * User interface to get an aggregate value aggregated from the previous super step.
     * 
     * @param name
     * @return
     */
    @SuppressWarnings("unchecked")
    public AggregateValue getAggregateValue(String name);
}
