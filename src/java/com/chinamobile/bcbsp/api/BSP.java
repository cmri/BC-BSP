/**
 * CopyRight by Chinamobile
 * 
 * BSP.java
 */
package com.chinamobile.bcbsp.api;

import com.chinamobile.bcbsp.bspstaff.Staff;
import com.chinamobile.bcbsp.bspstaff.SuperStepContextInterface;

/**
 * BSP
 * 
 * This class provides an abstract implementation of the BSP interface.
 * 
 * @author
 * @version
 */
public abstract class BSP implements BSPInterface {
    
    /**
     * The default implementation of setup does nothing.
     * 
     * @param staff
     */
    public void setup(Staff staff) {
        
    }
    
    /**
     * The default implementation of initBeforeSuperStep does nothing.
     * 
     * @param context
     */
    @Override
    public void initBeforeSuperStep(SuperStepContextInterface context) {
        
    }
   
    /**
     * The default implementation of cleanup does nothing.
     * 
     * @param staff
     */
    public void cleanup(Staff staff) {
        
    }
}
