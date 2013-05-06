/**
 * CopyRight by Chinamobile
 */
package com.chinamobile.bcbsp.bspstaff;

import org.junit.Before;
import org.junit.Test;

import com.chinamobile.bcbsp.api.Vertex;
import com.chinamobile.bcbsp.examples.AggregateValueVertexNum;
import com.chinamobile.bcbsp.examples.PREdge;
import com.chinamobile.bcbsp.examples.PRVertex;

import junit.framework.TestCase;

public class TestBSPStaffContext extends TestCase {
    
    private BSPStaffContext context;
    
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
        context = new BSPStaffContext(null, vertex, currentSuperStepCounter);
        
        aggValue = new AggregateValueVertexNum();
        aggValue.setValue(1L);
        context.addAggregateValues("SUM", aggValue);
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
    public void testGetVertex() {
        Vertex testVertex = context.getVertex();
        assertEquals(testVertex.getVertexID().toString(), vertex.getVertexID().toString());
        assertEquals(testVertex.getVertexValue().toString(), vertex.getVertexValue().toString());
    }
    
    @Test
    public void testGetAggregateValue() {
        assertEquals(context.getAggregateValue("SUM").getValue(), aggValue.getValue());
    }
    
    @Test
    public void testGetActiveFLag() {
        context.voltToHalt();
        assertEquals(context.getActiveFLag(), false);
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testUpdateVertex() {
        vertex = new PRVertex();
        vertex.setVertexID(400);
        vertex.setVertexValue(40.0f);
        PREdge edge = new PREdge();
        edge.setVertexID(800);
        vertex.addEdge(edge);
        
        context.updateVertex(vertex);
        assertEquals(context.getVertex().getVertexID().toString(), vertex.getVertexID().toString());
    }
}
