/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.bspstaff;

import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;

public class TestStaffRunner extends TestCase {
    
    private StaffRunner runner = new StaffRunner(null, null, null);
    
    @Before
    public void setUp() throws Exception {
        runner.setFaultSSStep(20);
    }
    
    @Test
    public void testGetFaultSSStep() {
        assertEquals(runner.getFaultSSStep(), 20);
    }
}
