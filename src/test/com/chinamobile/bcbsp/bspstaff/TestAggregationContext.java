/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.bspstaff;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.examples.AggregateValueVertexNum;
import com.chinamobile.bcbsp.examples.PREdge;
import com.chinamobile.bcbsp.examples.PRVertex;

import junit.framework.TestCase;

public class TestAggregationContext extends TestCase {
    
    private AggregationContext context;
    @SuppressWarnings("unchecked")
    private Vertex vertex;
    private int currentSuperStepCounter = 5;
    private AggregateValueVertexNum aggValue;
    
    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        vertex = new PRVertex();
        vertex.setVertexID(100);
        vertex.setVertexValue(10.0f);
        PREdge edge = new PREdge();
        edge.setVertexID(200);
        vertex.addEdge(edge);
        context = new AggregationContext(null, vertex, currentSuperStepCounter);
        
        aggValue = new AggregateValueVertexNum();
        aggValue.setValue(1L);
        context.addAggregateValues("SUM", aggValue);
    }
    
    @After
    public void tearDown() throws Exception {
        // do nothing.
    }
    
    @Test
    public void testGetCurrentSuperStepCounter() {
        assertEquals(context.getCurrentSuperStepCounter(), currentSuperStepCounter);
    }
    
    @Test
    public void testGetOutgoingEdgesNum() {
        assertEquals(context.getOutgoingEdgesNum(), 1);
    }
    
    @Test
    public void testGetVertexID() {
        assertEquals(context.getVertexID(), vertex.getVertexID().toString());
    }
    
    @Test
    public void testGetVertexValue() {
        assertEquals(context.getVertexValue(), vertex.getVertexValue().toString());
    }
    
    @Test
    public void testGetAggregateValue() {
        assertEquals(context.getAggregateValue("SUM").getValue(), aggValue.getValue());
    }
}
