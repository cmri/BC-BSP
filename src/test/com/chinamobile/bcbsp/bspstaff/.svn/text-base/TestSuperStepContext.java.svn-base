/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.bspstaff;

import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.examples.AggregateValueVertexNum;

import junit.framework.TestCase;

public class TestSuperStepContext extends TestCase {
    
    private SuperStepContext context;
    
    private int currentSuperStepCounter = 5;
    private AggregateValueVertexNum aggValue;
    
    @Before
    public void setUp() throws Exception {
        context = new SuperStepContext(null, currentSuperStepCounter);
    }
    
    @Test
    public void testGetCurrentSuperStepCounter() {
        assertEquals(context.getCurrentSuperStepCounter(), currentSuperStepCounter);
    }
    
    @Test
    public void testGetAggregateValue() {
        aggValue = new AggregateValueVertexNum();
        aggValue.setValue(1L);
        context.addAggregateValues("SUM", aggValue);
        assertEquals(context.getAggregateValue("SUM").getValue(), aggValue.getValue());
    }
}
