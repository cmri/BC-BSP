/**
 * CopyRight by Chinamobile
 * 
 * BSPStaffContextInterface.java
 * 
 * BSPStaffRunner.java
 */
package com.chinamobile.bcbsp.bspstaff;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chinamobile.bcbsp.util.BSPJob;
import com.chinamobile.bcbsp.workermanager.WorkerManager;

/**
 * BSPStaffRunner
 * 
 * Base class that runs a staff in a separate process.
 * 
 * @author
 * @version
 */
public class BSPStaffRunner extends StaffRunner {

    public static final Log LOG = LogFactory.getLog(BSPStaffRunner.class);

    public BSPStaffRunner(BSPStaff bspStaff, WorkerManager wm, BSPJob conf) {
        super(bspStaff, wm, conf);
    }

}
